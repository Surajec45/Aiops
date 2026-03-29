from datetime import datetime, timedelta
from typing import List
from utils.neo4j_client import Neo4jClient
from schemas.signals import IncidentContext, StructuredSignal

class ContextBuilder:
    def __init__(self, neo4j_client: Neo4jClient):
        self.neo4j = neo4j_client

    def build_context(self, trigger_signal: dict, window_minutes: int = 15) -> IncidentContext:
        """
        Assembles all necessary facts for the RCA Agents.
        """
        service_name = trigger_signal.get("service")
        
        # 1. Fetch Topology from Neo4j
        topology = self.neo4j.get_topology(service_name)
        
        # 2. Fetch Historical Signals (Simulated placeholder)
        # In production, this would query a Signal Table in Kafka/Flink or a DB.
        signals = [StructuredSignal(**trigger_signal)]
        
        # Add a simulated 'neighboring' anomaly to demonstrate multi-signal reasoning
        if service_name != "redis":
             signals.append(StructuredSignal(
                id="sig-sup-001",
                timestamp=datetime.now(),
                service="redis",
                type="metric_anomaly",
                severity=0.85,
                description="Upstream dependency 'redis' exhibiting high latency",
                metadata={"latency_ms": 150}
            ))

        return IncidentContext(
            incident_id=f"INC-{int(datetime.now().timestamp())}",
            timestamp=datetime.now(),
            primary_affected_service=service_name,
            topology_snapshot=topology,
            signals=signals,
            time_window_minutes=window_minutes
        )
