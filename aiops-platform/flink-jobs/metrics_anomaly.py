"""
Flink Metrics Anomaly Detection Job
====================================
Source : otel-metrics  (OTLP JSON, one resourceMetrics envelope per message)
Sink   : correlated-incidents  (StructuredSignal JSON)

Algorithm
---------
1. Parse OTLP metric envelopes → flat (service, metric_name, value, ts) rows.
2. Compute per-(service, metric_name) rolling statistics over a 5-minute window.
3. Emit a metric_anomaly signal when |z-score| >= ZSCORE_THRESHOLD (default 3.0).

Run (inside Flink cluster):
    flink run -py metrics_anomaly.py
"""

import json
import os
import uuid
from datetime import datetime, timezone

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
    OffsetsInitializer,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import SlidingEventTimeWindows, Time

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
ZSCORE_THRESHOLD = float(os.getenv("ZSCORE_THRESHOLD", "3.0"))
WINDOW_MINUTES = int(os.getenv("METRICS_WINDOW_MINUTES", "5"))
SLIDE_SECONDS = int(os.getenv("METRICS_SLIDE_SECONDS", "30"))


# ── OTLP parser ───────────────────────────────────────────────────────────────

def parse_otlp_metrics(raw_json: str):
    """
    Yields (service, metric_name, value, ts_ms) tuples from an OTLP JSON envelope.
    """
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError:
        return

    for rm in data.get("resourceMetrics", []):
        service = "unknown"
        for attr in rm.get("resource", {}).get("attributes", []):
            if attr.get("key") == "service.name":
                service = attr["value"].get("stringValue", "unknown")

        for sm in rm.get("scopeMetrics", []):
            for metric in sm.get("metrics", []):
                name = metric.get("name", "unknown")
                for data_key in ("gauge", "sum"):
                    for dp in metric.get(data_key, {}).get("dataPoints", []):
                        val = dp.get("asDouble") or dp.get("asInt") or 0.0
                        ts_ms = int(dp.get("timeUnixNano", 0)) // 1_000_000 or int(datetime.now(timezone.utc).timestamp() * 1000)
                        yield (service, name, float(val), ts_ms)
                for dp in metric.get("histogram", {}).get("dataPoints", []):
                    count = dp.get("count", 0)
                    s = dp.get("sum", 0.0)
                    if count:
                        ts_ms = int(dp.get("timeUnixNano", 0)) // 1_000_000 or int(datetime.now(timezone.utc).timestamp() * 1000)
                        yield (service, f"{name}.avg", s / count, ts_ms)


# ── Welford aggregate ─────────────────────────────────────────────────────────

class WelfordAccumulator:
    __slots__ = ("n", "mean", "M2", "last_value")

    def __init__(self):
        self.n = 0
        self.mean = 0.0
        self.M2 = 0.0
        self.last_value = 0.0


class WelfordAggregate(AggregateFunction):
    def create_accumulator(self):
        return WelfordAccumulator()

    def add(self, value, acc: WelfordAccumulator):
        # value = (service, metric_name, float_val, ts_ms)
        x = value[2]
        acc.last_value = x
        acc.n += 1
        delta = x - acc.mean
        acc.mean += delta / acc.n
        delta2 = x - acc.mean
        acc.M2 += delta * delta2
        return acc

    def get_result(self, acc: WelfordAccumulator):
        import math
        variance = acc.M2 / acc.n if acc.n > 1 else 0.0
        std = math.sqrt(variance)
        return (acc.n, acc.mean, std, acc.last_value)

    def merge(self, a: WelfordAccumulator, b: WelfordAccumulator):
        # Parallel Welford merge
        combined = WelfordAccumulator()
        combined.n = a.n + b.n
        if combined.n == 0:
            return combined
        delta = b.mean - a.mean
        combined.mean = (a.mean * a.n + b.mean * b.n) / combined.n
        combined.M2 = a.M2 + b.M2 + delta * delta * a.n * b.n / combined.n
        combined.last_value = b.last_value
        return combined


class AnomalyEmitter(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        import math
        for (n, mean, std, last_val) in elements:
            if n < 10 or std < 1e-9:
                continue
            z = (last_val - mean) / std
            if abs(z) < ZSCORE_THRESHOLD:
                continue
            service, metric_name = key
            severity = min(1.0, abs(z) / (ZSCORE_THRESHOLD * 2))
            direction = "spike" if z > 0 else "drop"
            signal = {
                "id": f"sig-{uuid.uuid4().hex[:12]}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "service": service,
                "type": "metric_anomaly",
                "severity": round(severity, 4),
                "description": (
                    f"Metric '{metric_name}' {direction}: "
                    f"value={last_val:.2f}, mean={mean:.2f}, z={z:.2f}"
                ),
                "metadata": {
                    "metric_name": metric_name,
                    "value": last_val,
                    "mean": mean,
                    "stddev": std,
                    "zscore": z,
                    "window_samples": n,
                },
            }
            out.collect(json.dumps(signal))


# ── job entry point ───────────────────────────────────────────────────────────

def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("otel-metrics")
        .set_group_id("flink-metrics-anomaly")
        .set_starting_offsets(OffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "otel-metrics-source",
    )

    # Flat-map OTLP envelopes → (service, metric_name, value, ts_ms)
    metric_rows = raw_stream.flat_map(
        lambda s: list(parse_otlp_metrics(s)),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT(), Types.LONG()]),
    )

    # Key by (service, metric_name), apply sliding window + Welford stats
    anomalies = (
        metric_rows
        .key_by(lambda r: (r[0], r[1]))
        .window(SlidingEventTimeWindows.of(
            Time.minutes(WINDOW_MINUTES),
            Time.seconds(SLIDE_SECONDS),
        ))
        .aggregate(WelfordAggregate(), AnomalyEmitter())
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

    anomalies.sink_to(sink)
    env.execute("Metrics Anomaly Detection")


if __name__ == "__main__":
    run()
