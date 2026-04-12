"""
IncidentConsumer
================
Consumes pre-processed, correlated incident messages from the
`correlated-incidents` Kafka topic (produced by the signal-processor service).

Each message is a CorrelatedIncident JSON envelope that already contains:
  - primary_affected_service
  - max_severity
  - correlated signals (metrics, traces, logs)
  - topology_snapshot (upstream/downstream services from Neo4j)

The consumer builds an IncidentContext and hands it to the RCAOrchestrator.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaConsumer

from context_builder import ContextBuilder
from orchestrator import RCAOrchestrator
from schemas.signals import IncidentContext, SignalType, StructuredSignal
from utils.neo4j_client import Neo4jClient
from utils.otlp_mapper import OTLPMapper

# Topic to consume from — correlated-incidents produced by signal-processor.
# Falls back to raw otel-logs if signal-processor is not deployed.
INCIDENT_TOPIC = os.getenv("INCIDENT_TOPIC", "correlated-incidents")
FALLBACK_TOPIC = os.getenv("FALLBACK_TOPIC", "otel-logs")
USE_FALLBACK = os.getenv("USE_FALLBACK_TOPIC", "false").lower() == "true"


class IncidentConsumer:
    """Kafka consumer; schedules async RCA on the FastAPI event loop from a worker thread."""

    def __init__(self, orchestrator: RCAOrchestrator):
        self.orchestrator = orchestrator
        self.neo4j = Neo4jClient()
        self.context_builder = ContextBuilder(self.neo4j)
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = FALLBACK_TOPIC if USE_FALLBACK else INCIDENT_TOPIC
        self.consumer = None
        self.active_incidents: dict[str, float] = {}
        self.dedupe_window = int(os.getenv("INCIDENT_COOLDOWN_SEC", "600"))
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        self._workflow_timeout_sec = float(os.getenv("RCA_WORKFLOW_TIMEOUT_SEC", "1800"))

    def start(self, main_loop: Optional[asyncio.AbstractEventLoop] = None):
        self._main_loop = main_loop
        thread = threading.Thread(target=self._run, daemon=True)
        thread.start()

    def _run(self):
        print(f"RCA Engine connecting to Kafka at {self.bootstrap_servers}, topic='{self.topic}'")
        connected = False
        while not connected:
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    group_id="rca-engine-group",
                    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                    api_version=(7, 4, 0),
                )
                connected = True
                print(f"RCA Engine connected. Listening on '{self.topic}'")
            except Exception as e:
                print(f"Kafka not available yet, retrying in 5s… ({e})")
                time.sleep(5)

        for message in self.consumer:
            raw = message.value
            try:
                if USE_FALLBACK:
                    self._handle_raw_log(raw)
                else:
                    self._handle_correlated_incident(raw)
            except Exception as e:
                print(f"Error processing message: {e}")

    # ── correlated-incidents path ─────────────────────────────────────────────

    def _handle_correlated_incident(self, raw: dict):
        """
        Processes a CorrelatedIncident envelope from the signal-processor.
        The envelope already contains pre-correlated signals and topology.
        """
        service = raw.get("primary_affected_service", "unknown")
        max_severity = raw.get("max_severity", 0.0)
        incident_id = raw.get("incident_id", f"INC-{int(time.time())}")

        print(
            f"[consumer] Received correlated incident {incident_id} "
            f"for '{service}' (severity={max_severity:.2f}, "
            f"signals={raw.get('signal_count', 0)}, "
            f"types={raw.get('signal_types', [])})"
        )

        if max_severity < 0.5:
            print(f"[consumer] Skipping low-severity incident for '{service}'")
            return

        now = time.time()
        last = self.active_incidents.get(service, 0)
        if now - last < self.dedupe_window:
            print(f"[consumer] Cooldown active for '{service}', skipping")
            return
        self.active_incidents[service] = now

        # Build StructuredSignal list from the correlated signals
        signals: list[StructuredSignal] = []
        for s in raw.get("signals", []):
            try:
                signals.append(StructuredSignal(
                    id=s.get("id", f"sig-{time.time()}"),
                    timestamp=s.get("timestamp") or datetime.now(timezone.utc).isoformat(),
                    service=s.get("service", service),
                    type=SignalType(s.get("type", "log_pattern")),
                    severity=float(s.get("severity", 0.0)),
                    description=s.get("description", ""),
                    metadata=s.get("metadata", {}),
                ))
            except Exception as e:
                print(f"[consumer] Skipping malformed signal: {e}")

        if not signals:
            # Synthesize a single signal from the envelope summary
            signals = [StructuredSignal(
                id=f"sig-{incident_id}",
                timestamp=raw.get("timestamp") or datetime.now(timezone.utc).isoformat(),
                service=service,
                type=SignalType.FAILURE_RISK,
                severity=max_severity,
                description=raw.get("summary", f"Correlated incident for {service}"),
                metadata={"signal_types": raw.get("signal_types", [])},
            )]

        context = IncidentContext(
            incident_id=incident_id,
            timestamp=raw.get("timestamp") or datetime.now(timezone.utc).isoformat(),
            primary_affected_service=service,
            topology_snapshot=raw.get("topology_snapshot", {}),
            signals=signals,
            time_window_minutes=15,
        )

        self._trigger_rca(context)

    # ── fallback: raw otel-logs path (legacy) ─────────────────────────────────

    def _handle_raw_log(self, raw: dict):
        signal = OTLPMapper.map_log_to_signal(raw)
        print(
            f"[consumer] Raw log signal from '{signal.service}': "
            f"{signal.type} (severity={signal.severity:.2f})"
        )

        if signal.severity <= 0.8:
            return

        now = time.time()
        last = self.active_incidents.get(signal.service, 0)
        if now - last < self.dedupe_window:
            print(f"[consumer] Cooldown active for '{signal.service}', skipping")
            return
        self.active_incidents[signal.service] = now

        try:
            self.neo4j.update_dependency(signal.service, signal.service)
        except Exception as e:
            print(f"[consumer] Warning: failed to ensure service node: {e}")

        context = self.context_builder.build_context(signal)
        self._trigger_rca(context)

    # ── shared RCA trigger ────────────────────────────────────────────────────

    def _trigger_rca(self, context: IncidentContext):
        print(f"[consumer] Triggering RCA for incident {context.incident_id} "
              f"(service='{context.primary_affected_service}')")
        coro = self.orchestrator.run_workflow(context)

        if self._main_loop is not None and self._main_loop.is_running():
            fut = asyncio.run_coroutine_threadsafe(coro, self._main_loop)
            try:
                result = fut.result(timeout=self._workflow_timeout_sec)
                print(f"[consumer] RCA result: {result.root_cause} "
                      f"(confidence={result.confidence:.2f})")
            except Exception as e:
                print(f"[consumer] RCA workflow failed: {e}")
        else:
            try:
                result = asyncio.run(coro)
                print(f"[consumer] RCA result: {result.root_cause} "
                      f"(confidence={result.confidence:.2f})")
            except Exception as e:
                print(f"[consumer] RCA workflow failed: {e}")
