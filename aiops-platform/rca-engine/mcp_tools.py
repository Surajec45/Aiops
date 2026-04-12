"""
MCP tool loader for the RCA planner agent.

Uses the MCP Python SDK correctly:
  - session.list_tools()  → discover available tools
  - session.call_tool()   → invoke a tool by name

Each tool is wrapped as a LangChain StructuredTool so the LLM can call it
via bind_tools(). A new SSE connection is opened per call — FastMCP/SSE
does not support multiplexing on a single connection.
"""

from __future__ import annotations

import json
import os
from typing import Any, List

from langchain_core.tools import StructuredTool
from mcp import ClientSession
from mcp.client.sse import sse_client
from pydantic import create_model

MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://mcp-server:8000/sse")

# Module-level cache — populated once on first call, reused across planner iterations
_TOOLS: List[StructuredTool] = []


async def _list_tools() -> list[dict]:
    """Fetches the tool manifest from the MCP server using the correct SDK method."""
    async with sse_client(MCP_SERVER_URL) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            # list_tools() is the correct MCP SDK method — NOT call_tool("tools/list")
            response = await session.list_tools()
            return [
                {
                    "name": t.name,
                    "description": t.description or "",
                    "parameters": t.inputSchema if hasattr(t, "inputSchema") else {},
                }
                for t in response.tools
            ]


def _build_langchain_tool(schema: dict) -> StructuredTool:
    """
    Converts an MCP tool schema into a LangChain StructuredTool.
    The tool's async function opens a fresh SSE connection per invocation.

    NOTE: tool_name is captured as a default argument to avoid the classic
    Python closure-over-loop-variable bug where all closures share the same
    binding and end up calling the last tool's name.
    """
    tool_name = schema["name"]
    props: dict = schema.get("parameters", {}).get("properties", {})
    required: set = set(schema.get("parameters", {}).get("required", []))

    fields: dict[str, Any] = {}
    for param_name, prop in props.items():
        typ = prop.get("type", "string")
        py_type: type = str
        if typ == "integer":
            py_type = int
        elif typ == "number":
            py_type = float
        elif typ == "boolean":
            py_type = bool
        default = ... if param_name in required else None
        fields[param_name] = (py_type, default)

    ArgsModel = create_model(f"{tool_name.replace('_', ' ').title().replace(' ', '')}Args", **fields)

    # Capture tool_name in the default arg to avoid closure-over-variable bug
    async def _invoke(_tool_name: str = tool_name, **kwargs: Any) -> str:
        async with sse_client(MCP_SERVER_URL) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(_tool_name, arguments=kwargs)
                parts = [
                    block.text
                    for block in result.content
                    if hasattr(block, "text")
                ]
                return "\n".join(parts) if parts else "{}"

    return StructuredTool.from_function(
        coroutine=_invoke,
        name=tool_name,
        description=schema["description"],
        args_schema=ArgsModel,
    )


async def get_mcp_tools() -> List[StructuredTool]:
    """
    Returns the cached list of LangChain tools backed by the MCP server.
    Fetches and builds them on first call.
    """
    global _TOOLS
    if not _TOOLS:
        schemas = await _list_tools()
        _TOOLS = [_build_langchain_tool(s) for s in schemas]
        tool_names = [t.name for t in _TOOLS]
        print(f"[mcp_tools] Loaded {len(_TOOLS)} tools from MCP server: {tool_names}")
    return _TOOLS


async def reset_mcp_tools() -> None:
    """Force re-fetch of tool schemas (e.g. after MCP server restart)."""
    global _TOOLS
    _TOOLS = []
