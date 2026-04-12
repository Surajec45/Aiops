"""
Flink Dependency Discovery Job
================================
Source : otel-traces  (OTLP JSON)
Sink   : Neo4j  (MERGE Service nodes + DEPENDS_ON edges)

Algorithm
---------
1. Parse OTLP trace envelopes.
2. Extract (caller_service, callee_service) pairs from:
   - peer.service attribute on CLIENT spans
   - db.system / rpc.service attributes
3. Batch MERGE into Neo4j every 5 seconds to avoid per-span writes.

Run:
    flink run -py dependency_discovery.py
"""

import json
import os
import time
from datetime import datetime, timezone

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    OffsetsInitializer,
)
from pyflink.datastream.functions import RichSinkFunction
from pyflink.datastream.window import Time

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

MERGE_QUERY = """
UNWIND $edges AS edge
MERGE (p:Service {name: edge.parent})
MERGE (c:Service {name: edge.child})
MERGE (p)-[r:DEPENDS_ON]->(c)
ON CREATE SET r.first_seen = timestamp()
SET r.last_seen = timestamp(), r.call_count = coalesce(r.call_count, 0) + edge.count
"""


def parse_edges(raw_json: str):
    """Yields (parent_service, child_service) from CLIENT spans with peer attributes."""
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError:
        return

    for rs in data.get("resourceSpans", []):
        service = "unknown"
        for attr in rs.get("resource", {}).get("attributes", []):
            if attr.get("key") == "service.name":
                service = attr["value"].get("stringValue", "unknown")

        for ss in rs.get("scopeSpans", []):
            for span in ss.get("spans", []):
                # SpanKind 3 = CLIENT
                span_kind = span.get("kind", 0)
                if span_kind != 3:
                    continue
                attrs = {
                    a["key"]: a.get("value", {}).get("stringValue", "")
                    for a in span.get("attributes", [])
                }
                peer = (
                    attrs.get("peer.service")
                    or attrs.get("db.system")
                    or attrs.get("rpc.service")
                )
                if peer and peer != service:
                    yield (service, peer)


class Neo4jBatchSink(RichSinkFunction):
    """Accumulates edges and flushes to Neo4j in batches."""

    def __init__(self, batch_size: int = 100):
        self._batch_size = batch_size
        self._buffer: dict[tuple, int] = {}
        self._driver = None
        self._last_flush = time.time()

    def open(self, runtime_context):
        from neo4j import GraphDatabase
        self._driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    def invoke(self, value, context):
        parent, child = value
        key = (parent, child)
        self._buffer[key] = self._buffer.get(key, 0) + 1

        now = time.time()
        if len(self._buffer) >= self._batch_size or (now - self._last_flush) >= 5:
            self._flush()

    def _flush(self):
        if not self._buffer:
            return
        edges = [{"parent": p, "child": c, "count": cnt} for (p, c), cnt in self._buffer.items()]
        try:
            with self._driver.session() as session:
                session.run(MERGE_QUERY, edges=edges)
        except Exception as exc:
            print(f"Neo4j flush error: {exc}")
        self._buffer.clear()
        self._last_flush = time.time()

    def close(self):
        self._flush()
        if self._driver:
            self._driver.close()


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # single parallelism for Neo4j sink simplicity

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("otel-traces")
        .set_group_id("flink-dependency-discovery")
        .set_starting_offsets(OffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "otel-traces-dep-source",
    )

    edges = raw.flat_map(
        lambda s: list(parse_edges(s)),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()]),
    )

    edges.add_sink(Neo4jBatchSink(batch_size=200))
    env.execute("Dependency Discovery")


if __name__ == "__main__":
    run()
