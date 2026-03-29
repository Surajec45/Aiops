import json
import time
import os
from datetime import datetime
from kafka import KafkaProducer

def produce_mock_telemetry():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "otel-telemetry"
    
    print(f"Simulator attempting to connect to Kafka at {bootstrap_servers}...")
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(7, 4, 0)
            )
            print("Successfully connected to Kafka!")
        except Exception as e:
            print(f"Kafka not available yet, retrying in 5s... ({e})")
            time.sleep(5)
    
    # Scenario: Redis Slowdown
    signals = [
        {
            "id": "sig-001",
            "timestamp": datetime.now().isoformat(),
            "service": "redis",
            "type": "metric_anomaly",
            "severity": 0.9,
            "description": "Redis GET latency increased from 2ms to 120ms",
            "metadata": {"latency_ms": 120}
        },
        {
            "id": "sig-002",
            "timestamp": datetime.now().isoformat(),
            "service": "payment-service",
            "type": "trace_error",
            "severity": 0.7,
            "description": "Timeout error in database/redis call",
            "metadata": {"operation": "get_user_info"}
        }
    ]
    
    for signal in signals:
        producer.send(topic, signal)
        print(f"Sent signal to {topic}: {signal['description']}")
    
    producer.flush()
    print("All mock signals sent. Simulator finishing.")

if __name__ == "__main__":
    print("Starting Telemetry Simulator...")
    # Add a small delay to ensure Kafka's internal state is fully stable
    time.sleep(5)
    produce_mock_telemetry()
