import asyncio
from datetime import datetime

import path_setup

path_setup.ensure_platform_path()

from dotenv import load_dotenv

from schemas.signals import IncidentContext, SignalType, StructuredSignal
from orchestrator import RCAOrchestrator

load_dotenv(dotenv_path="../../.env")


async def run_test():
    context = IncidentContext(
        incident_id="INC-12345",
        timestamp=datetime.now(),
        primary_affected_service="payment-service",
        topology_snapshot={
            "nodes": [
                {"id": "api-gateway", "type": "gateway"},
                {"id": "payment-service", "type": "service"},
                {"id": "redis", "type": "database"},
            ],
            "edges": [
                {"source": "api-gateway", "target": "payment-service"},
                {"source": "payment-service", "target": "redis"},
            ],
        },
        signals=[
            StructuredSignal(
                id="sig-001",
                timestamp=datetime.now(),
                service="redis",
                type=SignalType.METRIC_ANOMALY,
                severity=1.0,
                description="Redis GET latency increased from 2ms to 120ms",
                metadata={"latency_ms": 120},
            ),
            StructuredSignal(
                id="sig-002",
                timestamp=datetime.now(),
                service="payment-service",
                type=SignalType.TRACE_ERROR,
                severity=0.8,
                description="Timeout error in database/redis call",
                metadata={"operation": "get_user_info"},
            ),
        ],
        time_window_minutes=15,
    )

    print("Initializing Orchestrator with LangGraph...")
    orchestrator = RCAOrchestrator()

    print("\nRunning Workflow...")
    result = await orchestrator.run_workflow(context)

    print("\n--- Final RCA Conclusion ---")
    print(f"Root Cause: {result.root_cause}")
    print(f"Confidence: {result.confidence}")
    print(f"Evidence: {result.evidence}")
    print(f"Explanation: {result.explanation}")


if __name__ == "__main__":
    asyncio.run(run_test())
