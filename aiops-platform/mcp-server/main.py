"""
MCP Server — AIOps Platform Tooling Hub
=========================================
Provides tools for the RCA planner agent to dynamically explore:
  - Service topology (Neo4j)
  - Live telemetry: metrics (Prometheus), logs (Loki), recent signals (Kafka)
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from typing import List

import httpx
from mcp.server.fastmcp import FastMCP
from neo4j import GraphDatabase

import path_setup
path_setup.ensure_platform_path()

from shared.subgraph_cypher import SERVICE_SUBGRAPH_ALL_CYPHER

mcp = FastMCP("AIOps Platform Tools")

# ── config ────────────────────────────────────────────────────────────────────
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
LOKI_URL = os.getenv("LOKI_URL", "http://loki:3100")

# Single shared driver — Neo4j driver is thread-safe and manages its own connection pool
_neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def _neo4j():
    return _neo4j_driver


# ── tool: topology ────────────────────────────────────────────────────────────

@mcp.tool()
def get_service_topology(service_name: str, depth: int = 2) -> str:
    """
    Retrieves the dependency graph for a service from Neo4j.
    Returns upstream and downstream services up to `depth` hops.
    Use this to walk the graph and discover which services may be affected.
    """
    try:
        driver = _neo4j()
        with driver.session() as session:
            result = session.run(
                SERVICE_SUBGRAPH_ALL_CYPHER, service=service_name, depth=depth
            )
            record = result.single()
            if record and record["nodes"]:
                return json.dumps(
                    {"nodes": record["nodes"], "edges": record["edges"]}, indent=2
                )
    except Exception as e:
        return json.dumps({"error": f"Neo4j query failed: {e}"})

    return json.dumps({"nodes": [{"id": service_name, "name": service_name}], "edges": []})


# ── tool: metrics via Prometheus ──────────────────────────────────────────────

@mcp.tool()
def get_service_metrics(service_name: str, window_minutes: int = 15) -> str:
    """
    Queries Prometheus for key health metrics of a service over the last `window_minutes`.
    Returns error rate, request rate, and p99 latency if available.
    Use this to verify whether a suspected service is actually degraded.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=window_minutes)
    step = "60s"

    queries = {
        "error_rate_per_min": (
            f'sum(rate(http_server_requests_seconds_count{{service="{service_name}",status=~"5.."}}[1m]))'
        ),
        "request_rate_per_min": (
            f'sum(rate(http_server_requests_seconds_count{{service="{service_name}"}}[1m]))'
        ),
        "p99_latency_seconds": (
            f'histogram_quantile(0.99, sum(rate(http_server_requests_seconds_bucket{{service="{service_name}"}}[1m])) by (le))'
        ),
        "cpu_usage": (
            f'avg(rate(process_cpu_seconds_total{{service="{service_name}"}}[1m]))'
        ),
    }

    results: dict = {"service": service_name, "window_minutes": window_minutes, "metrics": {}}

    try:
        with httpx.Client(timeout=10) as client:
            for metric_name, promql in queries.items():
                resp = client.get(
                    f"{PROMETHEUS_URL}/api/v1/query_range",
                    params={
                        "query": promql,
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                        "step": step,
                    },
                )
                if resp.status_code == 200:
                    data = resp.json().get("data", {}).get("result", [])
                    if data:
                        # Return last value and simple trend (first vs last)
                        values = data[0].get("values", [])
                        if values:
                            first_val = float(values[0][1]) if values[0][1] != "NaN" else None
                            last_val = float(values[-1][1]) if values[-1][1] != "NaN" else None
                            results["metrics"][metric_name] = {
                                "current": last_val,
                                "baseline": first_val,
                                "trend": (
                                    "increasing" if last_val and first_val and last_val > first_val * 1.2
                                    else "stable" if last_val and first_val
                                    else "unknown"
                                ),
                            }
                        else:
                            results["metrics"][metric_name] = {"current": None, "trend": "no_data"}
                    else:
                        results["metrics"][metric_name] = {"current": None, "trend": "no_data"}
    except Exception as e:
        results["error"] = f"Prometheus query failed: {e}"

    return json.dumps(results, indent=2)


# ── tool: logs via Loki ───────────────────────────────────────────────────────

@mcp.tool()
def get_service_logs(service_name: str, window_minutes: int = 15, limit: int = 20) -> str:
    """
    Queries Loki for recent error/warning logs from a service.
    Returns the most recent log lines with their severity.
    Use this to find specific error messages that explain an anomaly.
    """
    end_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    start_ns = int((datetime.now(timezone.utc) - timedelta(minutes=window_minutes)).timestamp() * 1e9)

    # LogQL: filter by service label, show errors and warnings
    logql = f'{{service_name="{service_name}"}} |= "" | json | level =~ "error|warn|ERROR|WARN"'

    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(
                f"{LOKI_URL}/loki/api/v1/query_range",
                params={
                    "query": logql,
                    "start": start_ns,
                    "end": end_ns,
                    "limit": limit,
                    "direction": "backward",
                },
            )
            if resp.status_code == 200:
                data = resp.json().get("data", {}).get("result", [])
                log_lines = []
                for stream in data:
                    for ts_ns, line in stream.get("values", []):
                        log_lines.append({
                            "timestamp": datetime.fromtimestamp(int(ts_ns) / 1e9, tz=timezone.utc).isoformat(),
                            "message": line[:300],
                        })
                log_lines.sort(key=lambda x: x["timestamp"], reverse=True)
                return json.dumps({
                    "service": service_name,
                    "window_minutes": window_minutes,
                    "log_count": len(log_lines),
                    "logs": log_lines[:limit],
                }, indent=2)
            else:
                return json.dumps({"error": f"Loki returned {resp.status_code}: {resp.text[:200]}"})
    except Exception as e:
        return json.dumps({"error": f"Loki query failed: {e}"})


# ── tool: combined telemetry summary ─────────────────────────────────────────

@mcp.tool()
def get_service_telemetry(service_name: str, window_minutes: int = 15) -> str:
    """
    Returns a combined telemetry summary for a service: metrics health + recent error logs.
    This is the primary tool for verifying whether a suspected dependency is degraded.
    """
    metrics_json = get_service_metrics(service_name, window_minutes)
    logs_json = get_service_logs(service_name, window_minutes, limit=10)

    metrics = json.loads(metrics_json)
    logs = json.loads(logs_json)

    # Derive a health score
    health_issues: List[str] = []
    m = metrics.get("metrics", {})

    error_rate = (m.get("error_rate_per_min") or {}).get("current")
    if error_rate and error_rate > 0.1:
        health_issues.append(f"High error rate: {error_rate:.3f} req/s")

    p99 = (m.get("p99_latency_seconds") or {}).get("current")
    if p99 and p99 > 1.0:
        health_issues.append(f"High p99 latency: {p99:.2f}s")

    cpu = (m.get("cpu_usage") or {}).get("current")
    if cpu and cpu > 0.8:
        health_issues.append(f"High CPU usage: {cpu:.1%}")

    log_count = logs.get("log_count", 0)
    if log_count > 5:
        health_issues.append(f"{log_count} recent error/warn log lines")

    summary = {
        "service": service_name,
        "window_minutes": window_minutes,
        "health_status": "degraded" if health_issues else "healthy",
        "health_issues": health_issues,
        "metrics": m,
        "recent_error_logs": logs.get("logs", [])[:5],
    }

    return json.dumps(summary, indent=2)


# ── tool: upstream impact analysis ───────────────────────────────────────────

@mcp.tool()
def get_upstream_impact(service_name: str) -> str:
    """
    Finds all services that depend on `service_name` (upstream callers).
    Use this to understand the blast radius of a degraded service.
    """
    try:
        driver = _neo4j()
        with driver.session() as session:
            result = session.run(
                """
                MATCH (upstream:Service)-[:DEPENDS_ON*1..3]->(s:Service {name: $service})
                RETURN collect(DISTINCT upstream.name) AS upstream_services
                """,
                service=service_name,
            )
            record = result.single()
            upstream = record["upstream_services"] if record else []
            return json.dumps({
                "service": service_name,
                "upstream_callers": upstream,
                "blast_radius": len(upstream),
            }, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Neo4j query failed: {e}"})


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    if transport == "sse":
        mcp.run(transport="sse", host="0.0.0.0", port=8000)
    else:
        mcp.run()
