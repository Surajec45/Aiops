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
from utils.otlp_mapper import OTLPMapper

class IncidentConsumer:
    def __init__(self, orchestrator: RCAOrchestrator):
        self.orchestrator = orchestrator
        self.neo4j = Neo4jClient()
        self.context_builder = ContextBuilder(self.neo4j)
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = "otel-logs" # Consuming logs from collector
        self.consumer = None
        self.active_incidents = {} # Tracking {service_name: last_trigger_time}
        self.dedupe_window = 600 # 10 minutes cooldown

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
            raw_data = message.value
            try:
                # Map OTLP JSON to our StructuredSignal schema
                signal = OTLPMapper.map_log_to_signal(raw_data)
                print(f"RCA Engine received signal from {signal.service}: {signal.type} (Severity: {signal.severity:.2f})")
                
                # If severity > 0.8, trigger RCA
                if signal.severity > 0.8:
                    now = time.time()
                    last_trigger = self.active_incidents.get(signal.service, 0)
                    
                    if now - last_trigger > self.dedupe_window:
                        self.active_incidents[signal.service] = now
                        self._process_incident(signal)
                    else:
                        print(f"Skipping redundant trigger for {signal.service} (Within cooldown)")
            except Exception as e:
                print(f"Error processing log: {e}")

    def _process_incident(self, signal: StructuredSignal):
        service_name = signal.service
        print(f"Triggering RCA for incident in service: {service_name}")
        
        # Ensure the service node exists in Neo4j
        try:
            self.neo4j.update_dependency(service_name, service_name) # Ensure self-node
        except Exception as e:
            print(f"Warning: Failed to ensure service node exists: {e}")

        context = self.context_builder.build_context(signal)
        result = self.orchestrator.run_workflow(context)
        print(f"RCA Result: {result.root_cause} (Confidence: {result.confidence})")
