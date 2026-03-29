import json
import os
import threading
import time
from kafka import KafkaConsumer
from orchestrator import RCAOrchestrator
from schemas.signals import IncidentContext, StructuredSignal
from datetime import datetime
from context_builder import ContextBuilder
from utils.neo4j_client import Neo4jClient

class IncidentConsumer:
    def __init__(self, orchestrator: RCAOrchestrator):
        self.orchestrator = orchestrator
        self.neo4j = Neo4jClient()
        self.context_builder = ContextBuilder(self.neo4j)
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = "otel-telemetry" # Consuming raw telemetry to identify incidents
        self.consumer = None

    def start(self):
        thread = threading.Thread(target=self._run, daemon=True)
        thread.start()

    def _run(self):
        print(f"RCA Engine attempting to connect to Kafka at {self.bootstrap_servers}...")
        connected = False
        while not connected:
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id='rca-engine-group',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    api_version=(7, 4, 0) # Matching cp-kafka:7.4.0
                )
                connected = True
                print(f"Successfully connected to Kafka!")
            except Exception as e:
                print(f"Kafka not available yet, retrying in 5s... ({e})")
                time.sleep(5)
        
        print(f"RCA Engine listening on topic: {self.topic}")
        
        for message in self.consumer:
            data = message.value
            print(f"RCA Engine received signal from {data.get('service')}: {data.get('type')}")
            # Simple logic: if severity > 0.8, treat as an incident trigger
            if data.get("severity", 0) > 0.8:
                self._process_incident(data)

    def _process_incident(self, trigger_signal: dict):
        service_name = trigger_signal.get('service', 'unknown')
        print(f"Triggering RCA for incident in service: {service_name}")
        
        # Ensure the service node exists in Neo4j to avoid schema warnings
        try:
            with self.neo4j.driver.session() as session:
                session.run("MERGE (:Service {name: $name})", name=service_name)
        except Exception as e:
            print(f"Warning: Failed to ensure service node exists: {e}")

        context = self.context_builder.build_context(trigger_signal)
        result = self.orchestrator.run_workflow(context)
        print(f"RCA Result: {result.root_cause} (Confidence: {result.confidence})")
