from datetime import datetime
from typing import List
from schemas.signals import IncidentContext, StructuredSignal

class ContextBuilder:
    def __init__(self, neo4j_client=None):
        # Neo4jClient is no longer strictly needed here, as agents use MCP
        self.neo4j = neo4j_client

    def build_context(self, trigger_signal: StructuredSignal, window_minutes: int = 15) -> IncidentContext:
        """
        Assembles initial facts for the RCA Agents. 
        Note: Topology is now explored dynamically by agents via MCP tools.
        """
        service_name = trigger_signal.service
        
        # We still keep the primary signals
        signals = [trigger_signal]
        
        # We can still add some local context if available, 
        # but the heavy lifting of exploring dependencies is now done by the AI agents.
        
        return IncidentContext(
            incident_id=f"INC-{int(datetime.now().timestamp())}",
            timestamp=datetime.now(),
            primary_affected_service=service_name,
            topology_snapshot={}, # Empty, agents will fetch what they need
            signals=signals,
            time_window_minutes=window_minutes
        )
