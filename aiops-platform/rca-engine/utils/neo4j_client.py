import os

from neo4j import GraphDatabase
from shared.subgraph_cypher import SERVICE_SUBGRAPH_ALL_CYPHER

class Neo4jClient:
    def __init__(self):
        uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "password")
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        # Pre-seed for demo if needed
        self.initialize_demo_graph()

    def initialize_demo_graph(self):
        """Pre-seeds the graph with a demo topology to avoid 'label not found' errors."""
        query = """
        MERGE (fe:Service {name: 'frontend'})
        MERGE (api:Service {name: 'api-gateway'})
        MERGE (pay:Service {name: 'payment-service'})
        MERGE (red:Service {name: 'redis'})
        MERGE (db:Service {name: 'postgres'})
        
        MERGE (fe)-[:DEPENDS_ON]->(api)
        MERGE (api)-[:DEPENDS_ON]->(pay)
        MERGE (pay)-[:DEPENDS_ON]->(red)
        MERGE (pay)-[:DEPENDS_ON]->(db)
        """
        try:
            with self.driver.session() as session:
                session.run(query)
                print("Neo4j Demo Graph Initialized (Service nodes and DEPENDS_ON edges)")
        except Exception as e:
            print(f"Neo4j Initialization Warning: {e}")

    def close(self):
        self.driver.close()

    def get_topology(self, focal_service: str, depth: int = 2) -> dict:
        """
        Retrieves the dependency graph centered around a focal service.
        """
        try:
            with self.driver.session() as session:
                result = session.run(
                    SERVICE_SUBGRAPH_ALL_CYPHER, service=focal_service, depth=depth
                )
                record = result.single()
                if record and record["nodes"]:
                    return {"nodes": record["nodes"], "edges": record["edges"]}
        except Exception as e:
            print(f"Neo4j Query Error: {e}")
        
        # Fallback for demo/missing data
        return {"nodes": [{"id": focal_service, "name": focal_service}], "edges": []}

    def update_dependency(self, parent: str, child: str):
        """
        Upserts a dependency relationship discovered from traces.
        """
        query = """
        MERGE (p:Service {name: $parent})
        MERGE (c:Service {name: $child})
        MERGE (p)-[r:DEPENDS_ON]->(c)
        SET r.last_seen = timestamp()
        """
        with self.driver.session() as session:
            session.run(query, parent=parent, child=child)
