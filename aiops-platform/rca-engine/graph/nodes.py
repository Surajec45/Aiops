"""
RCA LangGraph nodes.

Flow:
  planner → should_continue ──► tools ──► planner  (tool-call loop)
                             ├──► summarizer ──► planner  (when history is too long)
                             └──► analyze ──► verifier ──► END

Summarizer triggers when len(messages) > SUMMARIZE_THRESHOLD. It condenses
the full investigation history into a single compact HumanMessage so the
planner always has full context without hitting token limits.
"""

from __future__ import annotations

import json
import os
import re

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from graph.state import RCAState, SUMMARIZE_THRESHOLD
from llm_config import get_llm, get_verifier_llm
from mcp_tools import get_mcp_tools

MAX_EXPLORATION_STEPS = int(os.getenv("RCA_MAX_EXPLORATION_STEPS", "10"))


# ── planner ───────────────────────────────────────────────────────────────────

async def planner_node(state: RCAState) -> dict:
    llm = get_llm()
    tools = await get_mcp_tools()
    llm_with_tools = llm.bind_tools(tools)

    context = state["context"]
    messages = state.get("messages", [])
    exploration_count = state.get("exploration_count", 0)

    # Build the initial prompt only on the very first entry (empty message list)
    if not messages:
        signal_summary = "\n".join(
            f"  [{s.type.value.upper()}] severity={s.severity:.2f} — {s.description}"
            for s in context.signals
        )
        topology_hint = ""
        if context.topology_snapshot:
            services = context.topology_snapshot.get("services", [])
            if services:
                topology_hint = f"\nKNOWN TOPOLOGY (pre-fetched): {', '.join(services)}"

        remaining = MAX_EXPLORATION_STEPS - exploration_count
        system_prompt = (
            "## ROLE\n"
            "You are an elite SRE Agent performing root cause analysis on a live incident.\n"
            "You have access to real-time telemetry tools. Use them aggressively.\n\n"
            "## TOOLS — call any of these as many times as needed:\n"
            "- get_service_topology(service_name, depth=2)\n"
            "    → dependency graph from Neo4j; walk upstream/downstream\n"
            "- get_service_metrics(service_name, window_minutes=15)\n"
            "    → Prometheus: error rate, request rate, p99 latency, CPU per service\n"
            "- get_service_logs(service_name, window_minutes=15, limit=20)\n"
            "    → Loki: recent ERROR/WARN log lines for a service\n"
            "- get_service_telemetry(service_name, window_minutes=15)\n"
            "    → combined metrics + logs health summary (use for quick triage)\n"
            "- get_upstream_impact(service_name)\n"
            "    → which services call this one (blast radius)\n\n"
            "## INVESTIGATION STRATEGY\n"
            "1. You already have pre-correlated signals — use them as starting evidence.\n"
            "2. Call get_service_topology on the primary service to map its dependencies.\n"
            "3. For each suspicious dependency, call get_service_metrics AND get_service_logs\n"
            "   separately — metrics show WHAT is wrong, logs show WHY.\n"
            "4. Drill deeper: if redis looks bad, check its metrics with a longer window.\n"
            "5. Call get_upstream_impact on the suspected root cause to confirm blast radius.\n"
            "6. Keep calling tools until you have enough evidence. Do NOT conclude early.\n\n"
            f"## BUDGET: {remaining} tool call(s) remaining.\n\n"
            "## OUTPUT\n"
            "When you have enough evidence, stop calling tools and output ONLY this JSON:\n"
            "```json\n"
            '{"hypotheses": [\n'
            '  {"statement": "Root cause in one sentence", "confidence": 0.95,\n'
            '   "evidence": ["metric: redis p99=4.2s", "log: maxmemory eviction x12", ...]}\n'
            "]}\n"
            "```\n"
        )
        user_msg = (
            f"INCIDENT ID  : {context.incident_id}\n"
            f"PRIMARY SVC  : {context.primary_affected_service}\n"
            f"SIGNALS ({len(context.signals)}):\n{signal_summary}"
            f"{topology_hint}\n\n"
            "Start your investigation. Call tools first, conclude last."
        )
        messages = [SystemMessage(content=system_prompt), HumanMessage(content=user_msg)]

    response = await llm_with_tools.ainvoke(messages)

    new_count = exploration_count
    if getattr(response, "tool_calls", None):
        new_count += 1

    return {
        "messages": [response],
        "exploration_count": new_count,
    }


# ── summarizer ────────────────────────────────────────────────────────────────

async def summarizer_node(state: RCAState) -> dict:
    """
    Condenses the full investigation message history into a compact summary.

    Replaces the entire message list with:
      [original SystemMessage, HumanMessage(summary)]

    This preserves the LLM's role/instructions and all key findings while
    keeping the context window manageable for long investigations.
    """
    llm = get_llm()
    messages = state["messages"]

    # Separate the system message from the rest
    system_msg = next((m for m in messages if isinstance(m, SystemMessage)), None)

    # Build a readable transcript of what has happened so far
    transcript_parts = []
    for msg in messages:
        if isinstance(msg, SystemMessage):
            continue
        elif isinstance(msg, HumanMessage):
            transcript_parts.append(f"[USER] {msg.content}")
        elif isinstance(msg, AIMessage):
            if getattr(msg, "tool_calls", None):
                for tc in msg.tool_calls:
                    args = json.dumps(tc.get("args", {}), indent=None)
                    transcript_parts.append(f"[TOOL CALL] {tc['name']}({args})")
            elif msg.content:
                transcript_parts.append(f"[AGENT] {msg.content}")
        else:
            # ToolMessage — the result returned by a tool
            content = getattr(msg, "content", "")
            if content:
                transcript_parts.append(f"[TOOL RESULT] {str(content)[:600]}")

    transcript = "\n".join(transcript_parts)

    summarize_prompt = [
        SystemMessage(content=(
            "You are summarizing an ongoing SRE incident investigation. "
            "Produce a dense, factual summary that preserves:\n"
            "- All tool calls made and their key findings (metric values, log lines, topology)\n"
            "- Services confirmed healthy vs degraded\n"
            "- Current leading hypothesis (if any)\n"
            "- What still needs to be investigated\n\n"
            "Be concise but lose no evidence. The investigator will continue from this summary."
        )),
        HumanMessage(content=f"Summarize this investigation so far:\n\n{transcript}"),
    ]

    try:
        response = await llm.ainvoke(summarize_prompt)
        summary_text = response.content
    except Exception as e:
        # Fallback: keep the last few messages rather than crashing
        summary_text = f"[Summary unavailable: {e}]\n\nLast findings:\n{transcript[-1000:]}"

    summary_msg = HumanMessage(content=f"## INVESTIGATION SUMMARY (history compressed)\n\n{summary_text}")

    # Replace the full message list: system prompt + compact summary
    new_messages = []
    if system_msg:
        new_messages.append(system_msg)
    new_messages.append(summary_msg)

    return {
        # Overwrite messages entirely — use a replace reducer trick:
        # returning the full new list as the value for the Annotated reducer
        # won't work directly, so we store it via a sentinel approach.
        # Instead we clear and rebuild by returning the replacement list.
        "messages": new_messages,
        "needs_summarization": False,
    }


# ── routing ───────────────────────────────────────────────────────────────────

def should_continue(state: RCAState) -> str:
    """
    Routing after planner_node:
      1. Hard cap hit → analyze
      2. Message history too long → summarizer (then back to planner)
      3. LLM made tool calls → tools (then back to planner)
      4. LLM produced final text → analyze
    """
    last_message = state["messages"][-1]
    exploration_count = state.get("exploration_count", 0)
    message_count = len(state["messages"])

    # 1. Hard cap
    if exploration_count >= MAX_EXPLORATION_STEPS:
        return "analyze"

    # 2. History too long — summarize before next planner call
    if message_count > SUMMARIZE_THRESHOLD:
        return "summarizer"

    # 3. Tool calls pending
    if getattr(last_message, "tool_calls", None):
        return "tools"

    # 4. Final text response
    return "analyze"


# ── analyzer ──────────────────────────────────────────────────────────────────

def analyzer_node(state: RCAState) -> dict:
    """
    Extracts structured hypotheses from the planner's final text response.
    """
    messages = state["messages"]
    last_text = ""
    for msg in reversed(messages):
        content = getattr(msg, "content", "")
        if isinstance(content, str) and content.strip():
            last_text = content
            break

    hypotheses = []
    try:
        if "```json" in last_text:
            content = last_text.split("```json")[1].split("```")[0].strip()
            data = json.loads(content)
            hypotheses = data.get("hypotheses", [])
        elif "```" in last_text:
            content = last_text.split("```")[1].split("```")[0].strip()
            data = json.loads(content)
            hypotheses = data.get("hypotheses", [])
        else:
            match = re.search(r'\{.*?"hypotheses".*?\}', last_text, re.DOTALL)
            if match:
                data = json.loads(match.group())
                hypotheses = data.get("hypotheses", [])
    except (json.JSONDecodeError, ValueError, IndexError):
        pass

    if hypotheses:
        best = max(hypotheses, key=lambda h: h.get("confidence", 0.0))
        result = {
            "is_confirmed": True,
            "confidence": best.get("confidence", 0.9),
            "evidence": best.get("evidence", ["Matched via dynamic exploration"]),
            "winning_hypothesis": best,
            "all_hypotheses": hypotheses,
        }
    else:
        result = {
            "is_confirmed": False,
            "confidence": 0.0,
            "evidence": [],
            "winning_hypothesis": {},
            "all_hypotheses": [],
        }

    return {"verification_results": result, "hypotheses": hypotheses}


# ── verifier ──────────────────────────────────────────────────────────────────

async def verifier_node(state: RCAState) -> dict:
    """
    Writes the final incident report using the winning hypothesis + evidence.
    """
    llm = get_verifier_llm()
    context = state["context"]
    results = state.get("verification_results", {})
    winning = results.get("winning_hypothesis", {})

    tool_evidence = []
    for msg in state.get("messages", []):
        if getattr(msg, "type", "") == "tool" or msg.__class__.__name__ == "ToolMessage":
            content = getattr(msg, "content", "")
            if content and len(content) < 2000:
                tool_evidence.append(content[:500])

    system_prompt = (
        "## ROLE\n"
        "You are a senior On-Call Incident Commander writing a post-incident root cause report.\n"
        "Be specific, technical, and actionable. Reference actual metric values and log lines.\n\n"
        "## FORMAT\n"
        "1. Root Cause (1 sentence)\n"
        "2. Evidence (bullet points with specific values)\n"
        "3. Impact (which services were affected)\n"
        "4. Suggested Remediation (concrete steps)"
    )

    evidence_block = "\n".join(f"  - {e}" for e in results.get("evidence", []))
    tool_block = "\n".join(f"  {t}" for t in tool_evidence[:5])

    user_msg = (
        f"INCIDENT    : {context.incident_id}\n"
        f"SERVICE     : {context.primary_affected_service}\n"
        f"HYPOTHESIS  : {winning.get('statement', 'N/A')}\n"
        f"CONFIDENCE  : {results.get('confidence', 0.0):.0%}\n"
        f"EVIDENCE    :\n{evidence_block}\n"
        f"TOOL RESULTS (sample):\n{tool_block}"
    )

    try:
        response = await llm.ainvoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_msg),
        ])
        return {"final_explanation": response.content}
    except Exception as e:
        return {"final_explanation": f"Analysis complete. Report generation error: {e}"}
