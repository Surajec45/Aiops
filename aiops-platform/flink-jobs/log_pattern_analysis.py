"""
Flink Log Pattern Analysis Job
================================
Source : otel-logs  (OTLP JSON)
Sink   : correlated-incidents

Algorithm
---------
1. Parse OTLP log envelopes → (service, body, severity_number, ts_ms).
2. Match body against known error patterns.
3. Tumbling 2-minute windows keyed by (service, pattern_name).
4. Emit log_pattern signal when count >= LOG_PATTERN_COUNT_THRESHOLD (default 3).

Run:
    flink run -py log_pattern_analysis.py
"""

import json
import os
import re
import uuid
from datetime import datetime, timezone

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSink,
    KafkaSource,
    KafkaRecordSerializationSchema,
    OffsetsInitializer,
)
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows, Time

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
LOG_PATTERN_COUNT_THRESHOLD = int(os.getenv("LOG_PATTERN_COUNT_THRESHOLD", "3"))
WINDOW_SECONDS = int(os.getenv("LOG_WINDOW_SECONDS", "120"))

# (pattern_name, regex_string, base_severity)
LOG_PATTERNS = [
    ("oom",              r"out.of.memory|oom.?kill|cannot allocate",          0.95),
    ("connection_reset", r"connection.reset|connection.refused|econnrefused", 0.85),
    ("timeout",          r"timeout|timed.out|deadline.exceeded",              0.80),
    ("panic",            r"\bpanic\b|fatal.error|segfault|sigsegv",           0.95),
    ("disk_full",        r"no.space.left|disk.full|enospc",                   0.90),
    ("maxmemory",        r"maxmemory|eviction|evicted",                       0.85),
    ("high_latency",     r"latency.*(increased|spike|high)|slow.query",       0.75),
    ("auth_failure",     r"auth.*fail|unauthorized|403|401",                  0.70),
    ("db_error",         r"database.error|sql.error|query.fail",              0.80),
]
_COMPILED = [(n, re.compile(p, re.I), s) for n, p, s in LOG_PATTERNS]


def parse_otlp_logs(raw_json: str):
    """Yields (service, pattern_name, base_severity, body_sample, sev_num, ts_ms)."""
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError:
        return

    service = "unknown"
    for attr in data.get("resource", {}).get("attributes", []):
        if attr.get("key") == "service.name":
            service = attr["value"].get("stringValue", "unknown")

    log_rec = data.get("logRecord", {})
    if not log_rec and "scopeLogs" in data:
        try:
            log_rec = data["scopeLogs"][0]["logRecords"][0]
        except (IndexError, KeyError):
            log_rec = {}

    body = log_rec.get("body", {}).get("stringValue", "")
    sev_num = log_rec.get("severityNumber", 0)
    ts_ms = int(log_rec.get("timeUnixNano", 0)) // 1_000_000 or int(datetime.now(timezone.utc).timestamp() * 1000)

    if not body:
        return

    for pattern_name, regex, base_sev in _COMPILED:
        if regex.search(body):
            yield (service, pattern_name, base_sev, body[:200], sev_num, ts_ms)


class CountAccumulator:
    __slots__ = ("count", "base_severity", "sample")

    def __init__(self):
        self.count = 0
        self.base_severity = 0.0
        self.sample = ""


class CountAggregate(AggregateFunction):
    def create_accumulator(self):
        return CountAccumulator()

    def add(self, value, acc: CountAccumulator):
        acc.count += 1
        acc.base_severity = value[2]
        if not acc.sample:
            acc.sample = value[3]
        return acc

    def get_result(self, acc: CountAccumulator):
        return (acc.count, acc.base_severity, acc.sample)

    def merge(self, a: CountAccumulator, b: CountAccumulator):
        c = CountAccumulator()
        c.count = a.count + b.count
        c.base_severity = a.base_severity or b.base_severity
        c.sample = a.sample or b.sample
        return c


class PatternEmitter(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        service, pattern_name = key
        for (count, base_severity, sample) in elements:
            if count < LOG_PATTERN_COUNT_THRESHOLD:
                continue
            severity = min(1.0, base_severity + 0.02 * (count - LOG_PATTERN_COUNT_THRESHOLD))
            signal = {
                "id": f"sig-{uuid.uuid4().hex[:12]}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "service": service,
                "type": "log_pattern",
                "severity": round(severity, 4),
                "description": (
                    f"Log pattern '{pattern_name}' detected {count}x for '{service}' "
                    f"in {WINDOW_SECONDS}s: \"{sample[:120]}\""
                ),
                "metadata": {
                    "pattern": pattern_name,
                    "count": count,
                    "window_sec": WINDOW_SECONDS,
                    "sample_body": sample,
                },
            }
            out.collect(json.dumps(signal))


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("otel-logs")
        .set_group_id("flink-log-pattern")
        .set_starting_offsets(OffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw = env.from_source(
        source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "otel-logs-source",
    )

    matched = raw.flat_map(
        lambda s: list(parse_otlp_logs(s)),
        output_type=Types.TUPLE([
            Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.INT(), Types.LONG()
        ]),
    )

    signals = (
        matched
        .key_by(lambda r: (r[0], r[1]))  # (service, pattern_name)
        .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
        .aggregate(CountAggregate(), PatternEmitter())
    )

    # Sink to raw-signals — the signal_correlator job groups these into CorrelatedIncidents
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("raw-signals")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    signals.sink_to(sink)
    env.execute("Log Pattern Analysis")


if __name__ == "__main__":
    run()
