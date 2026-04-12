from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, field_validator


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
    severity: float          # 0.0 – 1.0
    description: str
    metadata: Dict           # evidence key-value pairs

    @field_validator("timestamp", mode="before")
    @classmethod
    def _parse_ts(cls, v):
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v


class IncidentContext(BaseModel):
    incident_id: str
    timestamp: datetime
    primary_affected_service: str
    topology_snapshot: Optional[Dict] = {}
    signals: List[StructuredSignal]
    time_window_minutes: int

    @field_validator("timestamp", mode="before")
    @classmethod
    def _parse_ts(cls, v):
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v


class RCAConclusion(BaseModel):
    incident_id: str
    root_cause: str
    confidence: float
    evidence: List[str]
    explanation: str
    suggested_action: Optional[str] = None


class CorrelatedIncident(BaseModel):
    """
    Produced by the signal-processor service.
    Consumed by the RCA engine consumer.
    """
    incident_id: str
    timestamp: datetime
    primary_affected_service: str
    max_severity: float
    signal_count: int
    signal_types: List[str]
    signals: List[StructuredSignal]
    topology_snapshot: Optional[Dict] = {}
    summary: str

    @field_validator("timestamp", mode="before")
    @classmethod
    def _parse_ts(cls, v):
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v
