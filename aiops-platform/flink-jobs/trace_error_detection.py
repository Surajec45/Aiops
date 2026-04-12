"""
Flink Trace Error Rate Detection Job
======================================
Source : otel-traces  (OTLP JSON)
Sink   : correlated-incidents

Algorithm
---------
1. Parse OTLP trace envelopes → (service, span_id, is_error, ts_ms).
2. Tumbling 1-minute windows keyed by service.
3. Emit trace_error signal when error_rate >= TRACE_ERROR_RATE_THRESHOLD (default 5 %).

Run:
    flink run -py trace_error_detection.py
"""

import json
import os
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
TRACE_ERROR_RATE_THRESHOLD = float(os.getenv("TRACE_ERROR_RATE_THRESHOLD", "0.05"))
WINDOW_SECONDS = int(os.getenv("TRACE_WINDOW_SECONDS", "60"))
MIN_SPAN_COUNT = int(os.getenv("TRACE_MIN_SPAN_COUNT", "10"))


def parse_otlp_traces(raw_json: str):
    """Yields (service, span_id, is_error, ts_ms)."""
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError:
        return

    for rs in data.get("resourceSpans", []):
        service = "unknown"
        for attr in rs.get("resource", {}).get("attributes", []):
            if attr.get("key") == "service.name":
                service = attr["value"].get("stringValue", "unknown")

        for ss in rs.get("scopeSpans", []):
            for span in ss.get("spans", []):
                span_id = span.get("spanId", "")
                # OTLP status: 0=unset, 1=ok, 2=error
                is_error = span.get("status", {}).get("code", 0) == 2
                ts_ns = int(span.get("startTimeUnixNano", 0))
                ts_ms = ts_ns // 1_000_000 or int(datetime.now(timezone.utc).timestamp() * 1000)
                yield (service, span_id, is_error, ts_ms)


class ErrorRateAccumulator:
    __slots__ = ("total", "errors")

    def __init__(self):
        self.total = 0
        self.errors = 0


class ErrorRateAggregate(AggregateFunction):
    def create_accumulator(self):
        return ErrorRateAccumulator()

    def add(self, value, acc: ErrorRateAccumulator):
        acc.total += 1
        if value[2]:  # is_error
            acc.errors += 1
        return acc

    def get_result(self, acc: ErrorRateAccumulator):
        return (acc.total, acc.errors)

    def merge(self, a: ErrorRateAccumulator, b: ErrorRateAccumulator):
        c = ErrorRateAccumulator()
        c.total = a.total + b.total
        c.errors = a.errors + b.errors
        return c


class ErrorRateEmitter(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        service = key
        for (total, errors) in elements:
            if total < MIN_SPAN_COUNT:
                continue
            error_rate = errors / total
            if error_rate < TRACE_ERROR_RATE_THRESHOLD:
                continue
            severity = min(1.0, error_rate * 2)
            signal = {
                "id": f"sig-{uuid.uuid4().hex[:12]}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "service": service,
                "type": "trace_error",
                "severity": round(severity, 4),
                "description": (
                    f"Elevated trace error rate for '{service}': "
                    f"{error_rate:.1%} ({errors}/{total} spans in {WINDOW_SECONDS}s)"
                ),
                "metadata": {
                    "error_rate": error_rate,
                    "error_count": errors,
                    "total_spans": total,
                    "window_sec": WINDOW_SECONDS,
                },
            }
            out.collect(json.dumps(signal))


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("otel-traces")
        .set_group_id("flink-trace-error")
        .set_starting_offsets(OffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw = env.from_source(
        source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "otel-traces-source",
    )

    span_rows = raw.flat_map(
        lambda s: list(parse_otlp_traces(s)),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.BOOLEAN(), Types.LONG()]),
    )

    signals = (
        span_rows
        .key_by(lambda r: r[0])  # key by service
        .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
        .aggregate(ErrorRateAggregate(), ErrorRateEmitter())
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
    env.execute("Trace Error Rate Detection")


if __name__ == "__main__":
    run()
