import os
import json
import time
from kafka import KafkaConsumer
from neo4j import GraphDatabase

class TopologyDiscovery:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.topic = "otel-traces"
        
        # Neo4j config
        uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "password")
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
        self.consumer = None
        self._connect_kafka()

    def _connect_kafka(self):
        while True:
            try:
                print(f"Topology Discovery attempting to connect to Kafka at {self.bootstrap_servers}...")
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id='topology-discovery-group',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                print("Successfully connected to Kafka!")
                break
            except Exception as e:
                print(f"Kafka connection failed: {e}. Retrying in 5s...")
                time.sleep(5)

    def run(self):
        print(f"Topology Discovery listening on topic: {self.topic}")
        for message in self.consumer:
            trace_data = message.value
            self._process_trace(trace_data)

    def _process_trace(self, trace_data):
        """
        Parses OTLP JSON format to find parent-child relationships between services.
        """
        # OTLP JSON structure: resourceSpans -> scopeSpans -> spans
        resource_spans = trace_data.get("resourceSpans", [])
        for rs in resource_spans:
            resource_attrs = rs.get("resource", {}).get("attributes", [])
            service_name = "unknown"
            for attr in resource_attrs:
                if attr.get("key") == "service.name":
                    service_name = attr.get("value", {}).get("stringValue", "unknown")
            
            scope_spans = rs.get("scopeSpans", [])
            for ss in scope_spans:
                spans = ss.get("spans", [])
                for span in spans:
                    parent_span_id = span.get("parentSpanId")
                    if parent_span_id:
                        # In a real trace, the parent might be in a different message or the same
                        # For simple topology discovery, we focus on the fact that THIS service (child) 
                        # is being called, and if it's an entry point, we might find the caller.
                        # Complex version: store spans in a window and match parentSpanId -> spanId.
                        # Simple version for AIOps platform demo: 
                        # We use the 'spanKind' or specific attributes to identify 'Client' calls 
                        # where the 'peer.service' is explicitly defined.
                        
                        attributes = span.get("attributes", [])
                        peer_service = None
                        for attr in attributes:
                            if attr.get("key") in ["peer.service", "db.system", "rpc.service"]:
                                peer_service = attr.get("value", {}).get("stringValue")
                        
                        if peer_service:
                            self._update_neo4j(service_name, peer_service)

    def _update_neo4j(self, parent, child):
        query = """
        MERGE (p:Service {name: $parent})
        MERGE (c:Service {name: $child})
        MERGE (p)-[r:DEPENDS_ON]->(c)
        SET r.last_seen = timestamp()
        """
        try:
            with self.driver.session() as session:
                session.run(query, parent=parent, child=child)
                print(f"Topology Updated: {parent} -> {child}")
        except Exception as e:
            print(f"Neo4j Update Error: {e}")

if __name__ == "__main__":
    discovery = TopologyDiscovery()
    discovery.run()
