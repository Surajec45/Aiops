"""
Flink Signal Correlator Job
=============================
Source : raw-signals  (StructuredSignal JSON from all detector jobs)
Sink   : correlated-incidents

This is the final stage of the preprocessing pipeline. It:
1. Consumes StructuredSignal messages from all three detectors.
2. Keys by service name.
3. Applies a 60-second event-time tumbling window.
4. Groups all signals for the same service within the window.
5. Emits a single CorrelatedIncident envelope if max_severity >= 0.5.

Why a separate correlator job?
- Flink guarantees exactly-once processing across the window boundary.
- The RCA engine receives ONE enriched incident per service per window,
  not N individual signals — preventing alert storms.
- The window join is stateful and fault-tolerant via Flink checkpointing.

Run:
    flink run -py signal_correlator.py
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import List

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.time import Duration
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
CORRELATION_WINDOW_SEC = int(os.getenv("CORRELATION_WINDOW_SEC", "60"))
MIN_SEVERITY = float(os.getenv("CORRELATION_MIN_SEVERITY", "0.5"))
# Allowed late data up to 10 seconds (handles minor clock skew between detectors)
ALLOWED_LATENESS_SEC = int(os.getenv("CORRELATION_ALLOWED_LATENESS_SEC", "10"))


# ── timestamp extractor ───────────────────────────────────────────────────────

class SignalTimestampAssigner(TimestampAssigner):
    """Extracts event time from the signal's ISO timestamp field."""

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            data = json.loads(value)
            ts_str = data.get("timestamp", "")
            if ts_str:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
        except Exception:
            pass
        return record_timestamp


# ── aggregate: collect all signals in window ──────────────────────────────────

class SignalListAccumulator:
    __slots__ = ("signals",)

    def __init__(self):
        self.signals: List[str] = []  # raw JSON strings


class SignalListAggregate(AggregateFunction):
    def create_accumulator(self):
        return SignalListAccumulator()

    def add(self, value: str, acc: SignalListAccumulator):
        acc.signals.append(value)
        return acc

    def get_result(self, acc: SignalListAccumulator):
        return acc.signals

    def merge(self, a: SignalListAccumulator, b: SignalListAccumulator):
        c = SignalListAccumulator()
        c.signals = a.signals + b.signals
        return c


# ── process window: emit CorrelatedIncident ───────────────────────────────────

class CorrelationEmitter(ProcessWindowFunction):
    def process(self, service: str, context, elements, out):
        all_signals = []
        for signal_list in elements:
            for raw in signal_list:
                try:
                    all_signals.append(json.loads(raw))
                except json.JSONDecodeError:
                    continue

        if not all_signals:
            return

        max_severity = max(s.get("severity", 0.0) for s in all_signals)
        if max_severity < MIN_SEVERITY:
            return

        signal_types = list({s.get("type", "unknown") for s in all_signals})
        descriptions = [s.get("description", "") for s in all_signals]

        incident = {
            "incident_id": f"INC-{uuid.uuid4().hex[:10].upper()}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "primary_affected_service": service,
            "max_severity": round(max_severity, 4),
            "signal_count": len(all_signals),
            "signal_types": signal_types,
            "signals": all_signals,
            # topology_snapshot is intentionally empty here —
            # the RCA engine fetches live topology via MCP tools
            "topology_snapshot": {},
            "summary": (
                f"{len(all_signals)} correlated signal(s) for '{service}' "
                f"[{', '.join(signal_types)}]: {descriptions[0][:120]}"
            ),
        }

        out.collect(json.dumps(incident))


# ── job entry point ───────────────────────────────────────────────────────────

def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Enable checkpointing every 30 seconds for fault tolerance
    env.enable_checkpointing(30_000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("raw-signals")
        .set_group_id("flink-signal-correlator")
        .set_starting_offsets(OffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(ALLOWED_LATENESS_SEC))
        .with_timestamp_assigner(SignalTimestampAssigner())
    )

    raw = env.from_source(source, watermark_strategy, "raw-signals-source")

    incidents = (
        raw
        .key_by(lambda s: json.loads(s).get("service", "unknown"))
        .window(TumblingEventTimeWindows.of(Time.seconds(CORRELATION_WINDOW_SEC)))
        .allowed_lateness(Time.seconds(ALLOWED_LATENESS_SEC))
        .aggregate(SignalListAggregate(), CorrelationEmitter())
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("correlated-incidents")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    incidents.sink_to(sink)
    env.execute("Signal Correlator")


if __name__ == "__main__":
    run()
