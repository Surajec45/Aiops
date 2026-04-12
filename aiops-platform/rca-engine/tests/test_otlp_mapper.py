from schemas.signals import SignalType
from utils.otlp_mapper import OTLPMapper


def test_map_log_to_signal_flat_otlp():
    raw = {
        "resource": {
            "attributes": [
                {"key": "service.name", "value": {"stringValue": "payment-api"}}
            ]
        },
        "logRecord": {
            "body": {"stringValue": "upstream timeout"},
            "severityNumber": 21,
        },
    }
    sig = OTLPMapper.map_log_to_signal(raw)
    assert sig.service == "payment-api"
    assert sig.type == SignalType.LOG_PATTERN
    assert sig.description == "upstream timeout"
    assert 0.0 <= sig.severity <= 1.0
    assert sig.metadata.get("raw_severity") == 21


def test_map_log_to_signal_scope_logs_shape():
    raw = {
        "resource": {
            "attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]
        },
        "scopeLogs": [
            {
                "logRecords": [
                    {
                        "body": {"stringValue": "nested record"},
                        "severityNumber": 24,
                    }
                ]
            }
        ],
    }
    sig = OTLPMapper.map_log_to_signal(raw)
    assert sig.service == "checkout"
    assert sig.description == "nested record"
