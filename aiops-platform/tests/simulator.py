"""
Telemetry Simulator
====================
Produces realistic OTLP-shaped messages to all three Kafka topics:
  - otel-logs    : error log patterns (picked up by signal-processor LogPatternAnalyzer)
  - otel-metrics : metric data points with anomaly spikes (MetricsAnomalyDetector)
  - otel-traces  : spans with errors and CLIENT peer.service attrs (TraceErrorDetector + topology)

Scenario: Redis memory exhaustion causes payment-service timeouts,
          which cascade to api-gateway and frontend.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# ── helpers ───────────────────────────────────────────────────────────────────

def _ts_ns() -> int:
    return int(time.time() * 1e9)


def _ts_ms() -> int:
    return int(time.time() * 1e3)


def _resource(service: str) -> dict:
    return {
        "attributes": [
            {"key": "service.name", "value": {"stringValue": service}},
            {"key": "service.version", "value": {"stringValue": "1.0.0"}},
        ]
    }


# ── log payloads ──────────────────────────────────────────────────────────────

def _log(service: str, body: str, severity_number: int) -> dict:
    return {
        "resource": _resource(service),
        "logRecord": {
            "timeUnixNano": str(_ts_ns()),
            "body": {"stringValue": body},
            "severityNumber": severity_number,
            "severityText": "ERROR" if severity_number >= 17 else "WARN",
        },
    }


LOG_PAYLOADS = [
    # Redis — maxmemory pattern (triggers LogPatternAnalyzer 'maxmemory' rule)
    _log("redis", "WARN maxmemory policy eviction: key evicted due to maxmemory limit", 13),
    _log("redis", "ERROR maxmemory limit reached, evictions occurring at high rate", 17),
    _log("redis", "ERROR maxmemory: cannot allocate new keys, eviction in progress", 17),
    _log("redis", "CRITICAL maxmemory exhausted, all writes are being rejected", 21),
    # payment-service — timeout pattern
    _log("payment-service", "ERROR timeout waiting for redis response: deadline exceeded after 5000ms", 17),
    _log("payment-service", "ERROR connection refused to redis:6379 after 3 retries", 17),
    _log("payment-service", "ERROR database error: query failed due to connection timeout", 17),
    # api-gateway — cascading errors
    _log("api-gateway", "ERROR upstream payment-service returned 503 Service Unavailable", 17),
    _log("api-gateway", "WARN high latency spike detected on /checkout endpoint: 4200ms", 13),
]


# ── metric payloads ───────────────────────────────────────────────────────────

def _gauge_metric(service: str, name: str, value: float) -> dict:
    return {
        "resourceMetrics": [{
            "resource": _resource(service),
            "scopeMetrics": [{
                "metrics": [{
                    "name": name,
                    "gauge": {
                        "dataPoints": [{
                            "timeUnixNano": str(_ts_ns()),
                            "asDouble": value,
                        }]
                    }
                }]
            }]
        }]
    }


def _histogram_metric(service: str, name: str, count: int, sum_val: float) -> dict:
    return {
        "resourceMetrics": [{
            "resource": _resource(service),
            "scopeMetrics": [{
                "metrics": [{
                    "name": name,
                    "histogram": {
                        "dataPoints": [{
                            "timeUnixNano": str(_ts_ns()),
                            "count": count,
                            "sum": sum_val,
                            "bucketCounts": [0, 0, count // 4, count // 2, count // 4],
                            "explicitBounds": [0.1, 0.5, 1.0, 5.0],
                        }]
                    }
                }]
            }]
        }]
    }


# Baseline metrics (normal range) — sent first to warm up rolling stats
BASELINE_METRICS = [
    _gauge_metric("redis", "redis_memory_used_bytes", 50_000_000),
    _gauge_metric("redis", "redis_memory_used_bytes", 52_000_000),
    _gauge_metric("redis", "redis_memory_used_bytes", 51_000_000),
    _gauge_metric("redis", "redis_connected_clients", 45),
    _gauge_metric("redis", "redis_connected_clients", 47),
    _gauge_metric("payment-service", "http_server_requests_seconds_count", 120),
    _gauge_metric("payment-service", "http_server_requests_seconds_count", 125),
    _gauge_metric("api-gateway", "http_server_requests_seconds_count", 300),
    _gauge_metric("api-gateway", "http_server_requests_seconds_count", 310),
]

# Anomaly metrics — spike values that will trigger Z-score detection
ANOMALY_METRICS = [
    # Redis memory spike (3x normal)
    _gauge_metric("redis", "redis_memory_used_bytes", 150_000_000),
    _gauge_metric("redis", "redis_memory_used_bytes", 155_000_000),
    # Redis evictions spike
    _gauge_metric("redis", "redis_evicted_keys_total", 5000),
    # payment-service error rate spike
    _gauge_metric("payment-service", "http_server_requests_errors_total", 450),
    # High latency histogram
    _histogram_metric("payment-service", "http_server_requests_seconds", 100, 420.0),  # avg 4.2s
    # api-gateway error rate
    _gauge_metric("api-gateway", "http_server_requests_errors_total", 200),
]


# ── trace payloads ────────────────────────────────────────────────────────────

def _span(
    service: str,
    name: str,
    span_id: str,
    parent_span_id: str | None,
    is_error: bool,
    peer_service: str | None = None,
    duration_ms: int = 100,
) -> dict:
    attrs = []
    if peer_service:
        attrs.append({"key": "peer.service", "value": {"stringValue": peer_service}})
    if is_error:
        attrs.append({"key": "error", "value": {"boolValue": True}})

    return {
        "resourceSpans": [{
            "resource": _resource(service),
            "scopeSpans": [{
                "spans": [{
                    "traceId": uuid.uuid4().hex[:32],
                    "spanId": span_id,
                    "parentSpanId": parent_span_id or "",
                    "name": name,
                    "kind": 3 if peer_service else 1,  # 3=CLIENT, 1=SERVER
                    "startTimeUnixNano": str(_ts_ns()),
                    "endTimeUnixNano": str(_ts_ns() + duration_ms * 1_000_000),
                    "status": {"code": 2 if is_error else 1},
                    "attributes": attrs,
                }]
            }]
        }]
    }


def _build_trace_scenario() -> list[dict]:
    """
    Builds a realistic trace scenario:
    frontend → api-gateway → payment-service → redis (error)
    """
    traces = []
    for _ in range(20):  # 20 requests, most failing
        root_id = uuid.uuid4().hex[:16]
        gw_id = uuid.uuid4().hex[:16]
        pay_id = uuid.uuid4().hex[:16]
        redis_id = uuid.uuid4().hex[:16]

        is_failing = _ < 16  # 80% error rate

        traces.append(_span("frontend", "GET /checkout", root_id, None, is_failing, duration_ms=4500 if is_failing else 200))
        traces.append(_span("api-gateway", "POST /payment", gw_id, root_id, is_failing, duration_ms=4200 if is_failing else 180))
        traces.append(_span("payment-service", "charge", pay_id, gw_id, is_failing, duration_ms=4000 if is_failing else 150))
        traces.append(_span("payment-service", "redis.get", redis_id, pay_id, is_failing, peer_service="redis", duration_ms=3800 if is_failing else 2))

    return traces


# ── main ──────────────────────────────────────────────────────────────────────

def connect_producer() -> KafkaProducer:
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode(),
                api_version=(7, 4, 0),
            )
            print("Simulator connected to Kafka")
            return p
        except Exception as e:
            print(f"Kafka not ready, retrying in 5s… ({e})")
            time.sleep(5)


def produce_mock_telemetry():
    producer = connect_producer()

    print("\n=== Phase 1: Sending baseline metrics (warm-up rolling stats) ===")
    for payload in BASELINE_METRICS:
        producer.send("otel-metrics", payload)
    producer.flush()
    print(f"Sent {len(BASELINE_METRICS)} baseline metric data points")

    print("\n=== Phase 2: Sending anomaly metrics ===")
    for payload in ANOMALY_METRICS:
        producer.send("otel-metrics", payload)
    producer.flush()
    print(f"Sent {len(ANOMALY_METRICS)} anomaly metric data points")

    print("\n=== Phase 3: Sending error log patterns ===")
    for payload in LOG_PAYLOADS:
        producer.send("otel-logs", payload)
        time.sleep(0.1)  # slight spread to trigger window counts
    producer.flush()
    print(f"Sent {len(LOG_PAYLOADS)} log records")

    print("\n=== Phase 4: Sending trace scenario (80% error rate) ===")
    traces = _build_trace_scenario()
    for payload in traces:
        producer.send("otel-traces", payload)
    producer.flush()
    print(f"Sent {len(traces)} spans across {len(traces) // 4} trace scenarios")

    print("\nSimulator complete. Signal-processor should detect anomalies within ~60s.")


if __name__ == "__main__":
    print("Starting Telemetry Simulator…")
    time.sleep(5)  # wait for Kafka to be ready
    produce_mock_telemetry()
