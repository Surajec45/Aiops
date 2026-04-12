from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode

from graph.nodes import (
    analyzer_node,
    planner_node,
    should_continue,
    should_retry,
    summarizer_node,
    verifier_node,
)
from graph.state import RCAState
from mcp_tools import get_mcp_tools


async def create_rca_graph(checkpointer: PostgresSaver):
    builder = StateGraph(RCAState)

    tools = await get_mcp_tools()
    tool_node = ToolNode(tools)

    builder.add_node("planner", planner_node)
    builder.add_node("tools", tool_node)
    builder.add_node("summarizer", summarizer_node)
    builder.add_node("analyze", analyzer_node)
    builder.add_node("verifier", verifier_node)

    builder.add_edge(START, "planner")

    # planner decides what to do next
    builder.add_conditional_edges(
        "planner",
        should_continue,
        {
            "tools": "tools",
            "summarizer": "summarizer",
            "analyze": "analyze",
        },
    )

    # after tools → back to planner
    builder.add_edge("tools", "planner")

    # after summarizer → back to planner (with compressed history)
    builder.add_edge("summarizer", "planner")

    builder.add_conditional_edges(
        "analyze",
        should_retry,
        {
            "planner": "planner",
            "verifier": "verifier",
        },
    )
    builder.add_edge("verifier", END)

    return builder.compile(checkpointer=checkpointer)
