import os
import json
from typing import List, Dict, TypedDict, Optional
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, START, END
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
        # Ensure we request JSON structure reliably
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

# 3. Define Nodes
def planner_node(state: RCAState) -> dict:
    llm = get_llm()
    context = state["context"]
    retry_count = state.get("retry_count", 0)
    prev_results = state.get("verification_results", {})
    
    system_prompt = """
    You are a Senior SRE and AIOps expert. Analyze the provided service topology and telemetry signals.
    Identify the most likely root cause hypotheses.
    
    Constraints:
    - Hypotheses must be specific (e.g., 'Connection pool exhaustion in [Service]' not 'Database issue').
    - Only look for SignalTypes that ARE PRESENT in the 'Active Signals'.
    - Valid SignalTypes: metric_anomaly, log_pattern, trace_error, dependency_change, failure_risk.
    - Output MUST be valid JSON with a "hypotheses" array.
    """
    
    user_msg = f"""
    Topology: {json.dumps(context.topology_snapshot)}
    Active Signals: {json.dumps([s.dict() for s in context.signals], default=str)}
    Affected Primary Service: {context.primary_affected_service}
    """
    
    if retry_count > 0:
        user_msg += f"\n\nNOTE: This is a retry attempt ({retry_count}). Previous verification failed:\n{json.dumps(prev_results)}\nPlease propose alternative hypotheses based on the signals."
        
    user_msg += """
    Output format:
    {
      "hypotheses": [
        {
            "id": "H1",
            "statement": "Detailed description of what is happening",
            "target_service": "The service name exactly as it appears in topology",
            "reasoning": "Why the current signals lead to this conclusion",
            "test_criteria": ["type_of_signal_to_look_for_as_proof"]
        }
      ]
    }
    """
    
    try:
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_msg)
        ]
        response = llm.invoke(messages)
        content = response.content
        print(f"Planner raw output: {content}")
        
        # Parse JSON
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
             content = content.split("```")[1].split("```")[0].strip()
        
        data = json.loads(content)
        hypotheses = data.get("hypotheses", [])
        return {"hypotheses": hypotheses} # Updates state dict
    except Exception as e:
        print(f"LLM Error in Planner Node: {e}")
        return {"hypotheses": []}

def analyzer_node(state: RCAState) -> dict:
    hypotheses = state.get("hypotheses", [])
    signals = state["context"].signals
    current_retry = state.get("retry_count", 0)
    
    weights = {1: 0.33, 2: 0.66, 3: 1.0}
    best_hypothesis_result = {"is_confirmed": False, "confidence": 0.0, "evidence": []}
    
    for hypothesis in hypotheses:
        target = hypothesis.get("target_service", "").lower()
        criteria = hypothesis.get("test_criteria", [])
        
        matches = []
        criteria_scores = {c.lower(): 0.0 for c in criteria}
        
        for sig in signals:
            sig_service = sig.service.lower()
            sig_type = sig.type.value.lower() if hasattr(sig.type, 'value') else str(sig.type).lower()
            
            if sig_service == target and sig_type in criteria_scores:
                matches.append(sig.description)
                severity_val = sig.severity.value if hasattr(sig.severity, 'value') else int(sig.severity)
                weight = weights.get(severity_val, 0.33)
                criteria_scores[sig_type] = max(criteria_scores[sig_type], weight)
        
        normalized_score = 0.0
        if criteria:
            normalized_score = sum(criteria_scores.values()) / len(criteria)
            
        is_confirmed = normalized_score > 0.5
        
        # If this hypothesis is stronger than the previous best, keep it
        if normalized_score > best_hypothesis_result["confidence"]:
             best_hypothesis_result = {
                 "is_confirmed": is_confirmed,
                 "confidence": round(normalized_score, 2),
                 "evidence": matches,
                 "winning_hypothesis": hypothesis
             }
             
    # If we didn't confirm any, and we haven't found a better one
    return {
        "verification_results": best_hypothesis_result,
        "retry_count": current_retry + 1
    }

def verifier_node(state: RCAState) -> dict:
    llm = get_verifier_llm()
    context = state["context"]
    results = state.get("verification_results", {})
    winning_hypothesis = results.get("winning_hypothesis", {})
    
    system_prompt = "Explain the root cause analysis findings to a human operator. Be concise and evidence-based."
    
    user_msg = f"""
    Incident: {context.incident_id} on {context.primary_affected_service}
    Analysis Results (Confirmed: {results.get("is_confirmed")}): {json.dumps(winning_hypothesis)}
    Evidence: {json.dumps(results.get("evidence", []))}
    
    Provide:
    1. Root Cause Summary
    2. Explanation of the chain of causality
    3. Human-readable evidence list
    """
    
    try:
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_msg)
        ]
        response = llm.invoke(messages)
        return {"final_explanation": response.content}
    except Exception:
        return {"final_explanation": "Analysis complete. See evidence for details."}

# 4. Define Edges & Routing
def check_retry(state: RCAState) -> str:
    results = state.get("verification_results", {})
    retry_count = state.get("retry_count", 0)
    MAX_RETRIES = 2 # e.g. allowing initial pass + 2 retries
    
    if results.get("is_confirmed", False):
        return "verifier"
    elif retry_count >= MAX_RETRIES:
         # Max retries reached, just proceed to verifier with what we have
        return "verifier"
    else:
        return "planner"

# 5. Build Graph
def create_rca_graph():
    builder = StateGraph(RCAState)
    
    # Add nodes
    builder.add_node("planner", planner_node)
    builder.add_node("analyzer", analyzer_node)
    builder.add_node("verifier", verifier_node)
    
    # Add edges
    builder.add_edge(START, "planner")
    builder.add_edge("planner", "analyzer")
    
    # Conditional routing after analysis
    builder.add_conditional_edges("analyzer", check_retry, {
        "planner": "planner",
        "verifier": "verifier"
    })
    
    builder.add_edge("verifier", END)
    
    # Compile
    return builder.compile()
