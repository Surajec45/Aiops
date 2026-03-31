import os
import json
from typing import List, Dict, TypedDict, Optional
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
from langgraph.pregel import RetryPolicy
from schemas.signals import IncidentContext, StructuredSignal

# 1. Define State
class RCAState(TypedDict):
    context: IncidentContext     # Fixed input (passed by reference, no data duplication overhead)
    hypotheses: List[dict]       # Overwritten by Planner on each iteration
    verification_results: dict   # Overwritten by Analyzer on each iteration
    retry_count: int             # Incremented by Analyzer
    final_explanation: str       # Set by Verifier at the end

# 2. Setup LLM
def get_llm():
    api_key = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("LLM_BASE_URL")
    model = os.getenv("LLM_MODEL") or "gpt-4o"
    return ChatOpenAI(
        model=model,
        api_key=api_key,
        base_url=base_url if base_url else None,
        model_kwargs={"response_format": {"type": "json_object"}} if "llama" not in model.lower() else {}
    )

def get_verifier_llm():
    api_key = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("LLM_BASE_URL")
    model = os.getenv("LLM_MODEL") or "gpt-4o"
    return ChatOpenAI(
        model=model,
        api_key=api_key,
        base_url=base_url if base_url else None
    )

# ──────────────────────────────────────────────────────────────────────────────
# 3. NODE: Planner
# ──────────────────────────────────────────────────────────────────────────────
def planner_node(state: RCAState) -> dict:
    llm = get_llm()
    context = state["context"]
    retry_count = state.get("retry_count", 0)
    prev_results = state.get("verification_results", {})

    # ── System Prompt ─────────────────────────────────────────────────────────
    system_prompt = (
        "## ROLE\n"
        "You are an elite Site Reliability Engineer (SRE) and AIOps specialist with deep expertise in "
        "distributed systems, microservices failure patterns, and telemetry analysis. "
        "Your job is to reason over incident evidence and produce testable root-cause hypotheses "
        "for a deterministic verification engine downstream.\n\n"

        "## TASK\n"
        "Given a service topology graph and active telemetry signals, generate a ranked list of "
        "specific, testable root-cause hypotheses. Every hypothesis must directly reference a service "
        "and signal types that exist in the provided input.\n\n"

        "## HARD CONSTRAINTS\n"
        "1. Hypotheses MUST be specific — name the exact service and failure mode.\n"
        "   CORRECT: 'Connection pool exhaustion in redis due to concurrent GET spikes'\n"
        "   WRONG  : 'Database is slow'\n"
        "2. `test_criteria` MUST only list SignalTypes present in the Active Signals for that service.\n"
        "   Valid values: metric_anomaly | log_pattern | trace_error | dependency_change | failure_risk\n"
        "3. `target_service` MUST exactly match a node ID from the topology — no aliases.\n"
        "4. Return ONLY a valid JSON object. No markdown fences, no prose outside JSON.\n"
        "5. Generate 2–4 hypotheses ordered by likelihood (most likely first).\n\n"

        "## RETRY PROTOCOL — CRITICAL (applies only when RETRY FEEDBACK section appears)\n"
        "- The hypotheses listed under RETRY FEEDBACK were tested by the Analyzer and REJECTED.\n"
        "- You MUST NOT reproduce or paraphrase any rejected hypothesis.\n"
        "- The Analyzer is fully deterministic: identical `target_service` + `test_criteria` = identical failure.\n"
        "- Reinterpret the evidence: try a different service, a different signal type, "
        "or an upstream/downstream causal chain not yet explored.\n\n"

        "## OUTPUT SCHEMA\n"
        "{\n"
        '  "hypotheses": [\n'
        "    {\n"
        '      "id": "H1",\n'
        '      "statement": "<precise one-sentence description of the failure mode and affected service>",\n'
        '      "target_service": "<exact node ID from the topology graph>",\n'
        '      "reasoning": "<2-3 sentences explaining why these signals point to this conclusion>",\n'
        '      "test_criteria": ["<signal_type_matching_active_signals>"]\n'
        "    }\n"
        "  ]\n"
        "}"
    )

    # ── User Message ──────────────────────────────────────────────────────────
    user_msg = (
        "## INCIDENT CONTEXT\n"
        f"- Incident ID     : {context.incident_id}\n"
        f"- Primary Service : {context.primary_affected_service}\n"
        f"- Analysis Window : {getattr(context, 'time_window_minutes', 'N/A')} minutes\n\n"
        "## SERVICE TOPOLOGY\n"
        "Use the exact node IDs from this graph as `target_service` values.\n"
        f"{json.dumps(context.topology_snapshot, indent=2)}\n\n"
        "## ACTIVE TELEMETRY SIGNALS\n"
        "Only the signal types present here are valid for `test_criteria`.\n"
        f"{json.dumps([s.dict() for s in context.signals], indent=2, default=str)}\n"
    )

    # ── Retry Feedback (only injected when looping back) ──────────────────────
    if retry_count > 0:
        prev_hypotheses = state.get("hypotheses", [])
        failed_block = "\n".join([
            f"  [{h.get('id','?')}] {h.get('statement','?')}\n"
            f"       target={h.get('target_service','?')} | criteria={h.get('test_criteria',[])}"
            for h in prev_hypotheses
        ])
        user_msg += (
            f"\n## RETRY FEEDBACK — Attempt {retry_count} of 2\n"
            "The following hypotheses were submitted to the Analyzer and REJECTED (confidence too low).\n\n"
            "### Rejected Hypotheses\n"
            f"{failed_block}\n\n"
            "### Analyzer Outcome\n"
            f"- Confidence : {prev_results.get('confidence', 0.0)} (must exceed 0.5 to confirm)\n"
            f"- Confirmed  : {prev_results.get('is_confirmed', False)}\n"
            f"- Partial evidence found : {json.dumps(prev_results.get('evidence', []))}\n\n"
            "### Instructions for This Retry\n"
            "1. Do NOT reproduce or paraphrase any rejected hypothesis.\n"
            "2. Explore a DIFFERENT service node or a DIFFERENT signal type combination.\n"
            "3. Consider upstream causes (what service feeds into the primary?) or downstream effects.\n"
            "4. Treat the partial evidence as a symptom — trace it back to a root cause.\n"
        )

    user_msg += "\n## YOUR RESPONSE\nReturn ONLY the JSON object matching the OUTPUT SCHEMA. No other text.\n"

    # ── LLM Call ──────────────────────────────────────────────────────────────
    try:
        response = llm.invoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_msg)
        ])
        content = response.content
        print(f"[Planner] Raw output: {content}")

        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()

        data = json.loads(content)
        return {"hypotheses": data.get("hypotheses", [])}
    except Exception as e:
        print(f"[Planner] LLM Error: {e}")
        return {"hypotheses": []}


# ──────────────────────────────────────────────────────────────────────────────
# 4. NODE: Analyzer (deterministic — no LLM)
# ──────────────────────────────────────────────────────────────────────────────
def analyzer_node(state: RCAState) -> dict:
    hypotheses = state.get("hypotheses", [])
    signals = state["context"].signals
    current_retry = state.get("retry_count", 0)

    # Severity weight map: float severity → confidence contribution
    # severity is stored as float 0.0–1.0 in StructuredSignal
    def severity_weight(sig) -> float:
        sev = sig.severity
        if isinstance(sev, float):
            if sev >= 0.8:
                return 1.0
            elif sev >= 0.5:
                return 0.66
            else:
                return 0.33
        # Fallback for enum-style severity
        val = sev.value if hasattr(sev, "value") else int(sev)
        return {1: 0.33, 2: 0.66, 3: 1.0}.get(val, 0.33)

    best = {"is_confirmed": False, "confidence": 0.0, "evidence": []}

    for hypothesis in hypotheses:
        target = hypothesis.get("target_service", "").lower()
        criteria = hypothesis.get("test_criteria", [])
        criteria_scores = {c.lower(): 0.0 for c in criteria}
        matches = []

        for sig in signals:
            sig_service = sig.service.lower()
            sig_type = sig.type.value.lower() if hasattr(sig.type, "value") else str(sig.type).lower()

            if sig_service == target and sig_type in criteria_scores:
                matches.append(sig.description)
                criteria_scores[sig_type] = max(criteria_scores[sig_type], severity_weight(sig))

        score = sum(criteria_scores.values()) / len(criteria) if criteria else 0.0

        if score > best["confidence"]:
            best = {
                "is_confirmed": score > 0.5,
                "confidence": round(score, 2),
                "evidence": matches,
                "winning_hypothesis": hypothesis
            }

    print(f"[Analyzer] Best confidence: {best['confidence']} | Confirmed: {best['is_confirmed']}")
    return {
        "verification_results": best,
        "retry_count": current_retry + 1
    }


# ──────────────────────────────────────────────────────────────────────────────
# 5. NODE: Verifier
# ──────────────────────────────────────────────────────────────────────────────
def verifier_node(state: RCAState) -> dict:
    llm = get_verifier_llm()
    context = state["context"]
    results = state.get("verification_results", {})
    winning = results.get("winning_hypothesis", {})
    confirmed = results.get("is_confirmed", False)

    # ── System Prompt ─────────────────────────────────────────────────────────
    system_prompt = (
        "## ROLE\n"
        "You are a senior On-Call Incident Commander writing a post-incident root cause report. "
        "Your audience is a technical SRE team. You communicate clearly, concisely, and with "
        "precise technical language. You never speculate beyond the evidence provided.\n\n"

        "## TASK\n"
        "Synthesise the results of an automated Root Cause Analysis (RCA) into a structured, "
        "human-readable incident report. You will receive:\n"
        "- The incident ID and affected service\n"
        "- The confirmed (or best-effort) root cause hypothesis\n"
        "- The confidence score produced by the deterministic Analyzer\n"
        "- The raw evidence that matched the hypothesis\n\n"

        "## INSTRUCTIONS\n"
        "1. Write a concise Root Cause Summary (1–2 sentences).\n"
        "2. Explain the causal chain — how the root cause propagated to affect the primary service.\n"
        "3. List the supporting evidence in plain English (one bullet per piece of evidence).\n"
        "4. If confidence < 0.5, clearly state this is a best-effort diagnosis (inconclusive).\n"
        "5. Do NOT invent evidence. Only reference what is explicitly provided.\n"
        "6. Use clear section headers: ## Root Cause, ## Causal Chain, ## Evidence, ## Confidence.\n\n"

        "## OUTPUT FORMAT\n"
        "Plain markdown text. No JSON. Write for a human reader, not a machine."
    )

    # ── User Message ──────────────────────────────────────────────────────────
    user_msg = (
        "## INCIDENT DETAILS\n"
        f"- Incident ID       : {context.incident_id}\n"
        f"- Primary Service   : {context.primary_affected_service}\n"
        f"- Analysis Status   : {'CONFIRMED' if confirmed else 'INCONCLUSIVE (best-effort)'}\n\n"

        "## ROOT CAUSE HYPOTHESIS\n"
        f"- Statement    : {winning.get('statement', 'No hypothesis confirmed')}\n"
        f"- Target       : {winning.get('target_service', 'N/A')}\n"
        f"- Reasoning    : {winning.get('reasoning', 'N/A')}\n"
        f"- Test Criteria: {winning.get('test_criteria', [])}\n\n"

        "## ANALYZER RESULTS\n"
        f"- Confidence Score : {results.get('confidence', 0.0)} / 1.0 (threshold: 0.5)\n"
        f"- Confirmed        : {confirmed}\n\n"

        "## RAW EVIDENCE (signals that matched the hypothesis)\n"
        + "\n".join(f"- {e}" for e in results.get("evidence", ["No matching signals found"])) +
        "\n\n## YOUR TASK\n"
        "Write the structured incident report now using the format described in your instructions."
    )

    # ── LLM Call ──────────────────────────────────────────────────────────────
    try:
        response = llm.invoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_msg)
        ])
        return {"final_explanation": response.content}
    except Exception as e:
        print(f"[Verifier] LLM Error: {e}")
        return {"final_explanation": "Analysis complete. See verification_results for raw evidence."}


# ──────────────────────────────────────────────────────────────────────────────
# 6. Routing
# ──────────────────────────────────────────────────────────────────────────────
def check_retry(state: RCAState) -> str:
    results = state.get("verification_results", {})
    retry_count = state.get("retry_count", 0)
    MAX_RETRIES = 2  # initial pass + up to 2 semantic retries

    if results.get("is_confirmed", False):
        print(f"[Router] Hypothesis confirmed (confidence={results.get('confidence')}). → Verifier")
        return "verifier"
    elif retry_count >= MAX_RETRIES:
        print(f"[Router] Max retries ({MAX_RETRIES}) reached. Proceeding with best-effort. → Verifier")
        return "verifier"
    else:
        print(f"[Router] Confidence too low ({results.get('confidence', 0.0)}). → Retry Planner")
        return "planner"


# ──────────────────────────────────────────────────────────────────────────────
# 7. Build Graph
# ──────────────────────────────────────────────────────────────────────────────
def create_rca_graph():
    builder = StateGraph(RCAState)

    # Layer 1 — Infrastructure retry (API timeouts, rate limits, network errors)
    llm_retry = RetryPolicy(max_attempts=3)

    builder.add_node("planner", planner_node, retry=llm_retry)
    builder.add_node("analyzer", analyzer_node)           # deterministic — no retry needed
    builder.add_node("verifier", verifier_node, retry=llm_retry)

    builder.add_edge(START, "planner")
    builder.add_edge("planner", "analyzer")

    # Layer 2 — Semantic retry (low confidence → jump back to Planner)
    builder.add_conditional_edges("analyzer", check_retry, {
        "planner": "planner",
        "verifier": "verifier"
    })

    builder.add_edge("verifier", END)

    # Checkpointer — snapshots state after every node so crashes can resume
    # from the last successful checkpoint instead of restarting from scratch.
    # Swap MemorySaver → SqliteSaver / RedisSaver for cross-process persistence.
    checkpointer = MemorySaver()
    return builder.compile(checkpointer=checkpointer)
