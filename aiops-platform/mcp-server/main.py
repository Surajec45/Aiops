import os
import json
from datetime import datetime
from typing import List, Optional
from mcp.server.fastmcp import FastMCP
from neo4j import GraphDatabase

# 1. Initialize FastMCP Server
mcp = FastMCP("AIOps Platform Tools")

# 2. Neo4j Configuration
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

def get_neo4j_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# 3. Define Tools

@mcp.tool()
def get_service_topology(service_name: str, depth: int = 1) -> str:
    """
    Retrieves the immediate dependencies of a specified service from the graph database.
    Use this to 'walk' the graph and discover upstream or downstream services.
    """
    query = """
    OPTIONAL MATCH (s:Service {name: $service})
    WITH s
    WHERE s IS NOT NULL
    CALL apoc.path.subgraphAll(s, {
        maxDepth: $depth,
        relationshipFilter: 'DEPENDS_ON>'
    })
    YIELD nodes, relationships
    RETURN [n in nodes | {id: elementId(n), name: n.name}] as nodes,
           [r in relationships | {source: elementId(startNode(r)), target: elementId(endNode(r))}] as edges
    """
    try:
        driver = get_neo4j_driver()
        with driver.session() as session:
            result = session.run(query, service=service_name, depth=depth)
            record = result.single()
            if record and record["nodes"]:
                return json.dumps({"nodes": record["nodes"], "edges": record["edges"]}, indent=2)
    except Exception as e:
        return f"Error querying topology: {str(e)}"
    finally:
        if 'driver' in locals():
            driver.close()
    
    return json.dumps({"nodes": [{"id": service_name, "name": service_name}], "edges": []})

@mcp.tool()
def get_service_telemetry(service_name: str, window_minutes: int = 15) -> str:
    """
    Retrieves active telemetry signals (anomalies, logs, metrics) for a specific service.
    This helps in verifying if a suspected dependency is actually exhibiting issues.
    """
    # Simulated telemetry logic based on previous ContextBuilder implementation
    # In production, this would query Prometheus, Tempo, or a Signals DB.
    signals = []
    
    # Simulate an anomaly for the requested service to satisfy the agent's investigation
    # In a real scenario, this would be real data.
    signals.append({
        "id": f"sig-{service_name}-{int(datetime.now().timestamp())}",
        "timestamp": datetime.now().isoformat(),
        "service": service_name,
        "type": "metric_anomaly",
        "severity": 0.8,
        "description": f"Service '{service_name}' exhibiting elevated latency/error rate in the last {window_minutes}m."
    })
    
    # Specific demo logic for 'redis' to show multi-hop reasoning
    if service_name == "redis":
        signals.append({
            "id": "sig-redis-002",
            "timestamp": datetime.now().isoformat(),
            "service": "redis",
            "type": "log_pattern",
            "severity": 0.9,
            "description": "Redis 'maxmemory' limit reached; evictions occurring."
        })
        
    return json.dumps(signals, indent=2)

if __name__ == "__main__":
    # If transport is specified as SSE, we run as a web server
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    if transport == "sse":
        import uvicorn
        from mcp.server.fastmcp import FastMCP
        # FastMCP.run() automatically handles SSE if transport="sse" is passed 
        # but sometimes manual uvicorn is preferred for Docker.
        # However, FastMCP is designed to be simple:
        mcp.run(transport="sse", host="0.0.0.0", port=8000)
    else:
        mcp.run()
