"""Canonical APOC subgraph query for Service topology.

Keep in sync across:
- rca-engine (Neo4jClient.get_topology)
- mcp-server (get_service_topology)
"""

SERVICE_SUBGRAPH_ALL_CYPHER = """
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
