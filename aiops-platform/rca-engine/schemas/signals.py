from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime
from enum import Enum

class SignalType(str, Enum):
    METRIC_ANOMALY = "metric_anomaly"
    LOG_PATTERN = "log_pattern"
    TRACE_ERROR = "trace_error"
    DEPENDENCY_CHANGE = "dependency_change"
    FAILURE_RISK = "failure_risk"

class StructuredSignal(BaseModel):
    id: str
    timestamp: datetime
    service: str
    type: SignalType
    severity: float  # 0.0 to 1.0 (0 = normal, 1 = critical)
    description: str
    metadata: Dict  # Key-value pairs for evidence (e.g. latency: 500ms)

class IncidentContext(BaseModel):
    incident_id: str
    timestamp: datetime
    primary_affected_service: str
    topology_snapshot: Dict  # Graph structure from Neo4j
    signals: List[StructuredSignal]
    time_window_minutes: int

class RCAConclusion(BaseModel):
    incident_id: str
    root_cause: str
    confidence: float
    evidence: List[str]
    explanation: str
    suggested_action: Optional[str] = None
