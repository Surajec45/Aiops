import json
from datetime import datetime
from schemas.signals import StructuredSignal, SignalType

class OTLPMapper:
    """Maps raw OTLP JSON data from Kafka into our internal StructuredSignal schema."""

    @staticmethod
    def map_log_to_signal(raw_data: dict) -> StructuredSignal:
        # Note: OTLP JSON structure varies by version. 
        # This implementation assumes the flattened/simplified JSON exported by most collectors.
        
        # 1. Extract Service Name
        res_attr = raw_data.get("resource", {}).get("attributes", [])
        service_name = "unknown"
        for attr in res_attr:
            if attr.get("key") == "service.name":
                service_name = attr.get("value", {}).get("stringValue", "unknown")
                break
        
        # 2. Extract Body and Severity
        log_record = raw_data.get("logRecord", {}) # Simplification for demo
        # If it's a list (Standard OTLP), we'd iterate, but Kafka exporter often flattens one record per message.
        if not log_record and "scopeLogs" in raw_data:
             log_record = raw_data["scopeLogs"][0]["logRecords"][0]

        description = log_record.get("body", {}).get("stringValue", "No log body")
        severity_num = log_record.get("severityNumber", 0)
        
        # OTLP SeverityNumber: 17-24 are Error/Critical
        # Map 1-24 to 0.0-1.0
        severity_float = min(1.0, severity_num / 24.0)

        return StructuredSignal(
            id=f"log-{datetime.now().timestamp()}",
            timestamp=datetime.now(),
            service=service_name,
            type=SignalType.LOG_PATTERN,
            severity=severity_float,
            description=description,
            metadata={"raw_severity": severity_num, "source": "otel-logs"}
        )
