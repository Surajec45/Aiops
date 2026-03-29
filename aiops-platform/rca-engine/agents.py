import os
import json
from typing import List, Dict
from openai import OpenAI
from schemas.signals import IncidentContext, StructuredSignal

class AgentBase:
    def __init__(self, model=None):
        api_key = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("LLM_BASE_URL") # Optional: defaults to OpenAI's if None
        
        self.client = OpenAI(
            api_key=api_key,
            base_url=base_url
        )
        
        # Use provided model, or env var, or fallback
        self.model = model or os.getenv("LLM_MODEL") or "gpt-4o"

class PlannerAgent(AgentBase):
    def generate_hypotheses(self, context: IncidentContext) -> List[dict]:
        """
        Generates candidate hypotheses using an LLM based on generic context.
        Returns a list of dicts: {"hypothesis": "...", "required_signal_types": [...]}
        """
        system_prompt = """
        You are a Senior SRE and AIOps expert. Analyze the provided service topology and telemetry signals.
        Identify the most likely root cause hypotheses.
        
        Constraints:
        - Do not be vague. 
        - Hypotheses must be specific (e.g., 'Connection pool exhaustion in [Service]' not 'Database issue').
        - For each hypothesis, specify which SignalTypes would confirm it.
        - Valid SignalTypes are: metric_anomaly, log_pattern, trace_error, dependency_change, failure_risk.
        - Return valid JSON only.
        """
        
        user_msg = f"""
        Topology (JSON): {json.dumps(context.topology_snapshot)}
        Active Signals (Evidence): {json.dumps([s.dict() for s in context.signals], default=str)}
        Affected Primary Service: {context.primary_affected_service}
        
        Instructions:
        1. Look at the 'Active Signals' to see what evidence is actually available.
        2. For each hypothesis, only include SignalTypes in 'test_criteria' that ARE PRESENT in the 'Active Signals' for that 'target_service'. 
        3. If you suggest a hypothesis that has no signals associated with it in the 'Active Signals' list, it will be rejected by the Analyzer.
        
        Output format:
        {{
          "hypotheses": [
            {{
                "id": "H1",
                "statement": "Detailed description of what is happening",
                "target_service": "The service name (exactly as it appears in topology)",
                "reasoning": "Why the current signals lead to this conclusion",
                "test_criteria": ["type_of_signal_to_look_for_as_proof"]
            }}
          ]
        }}
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_msg}
                ],
                response_format={"type": "json_object"} if "llama" not in self.model.lower() else None
            )
            content = response.choices[0].message.content
            print(f"Planner raw output: {content}")
            
            # Robust JSON extraction (in case of markdown blocks)
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                 content = content.split("```")[1].split("```")[0].strip()
            
            data = json.loads(content)
            return data.get("hypotheses", [])
        except Exception as e:
            # Fallback for demo if API fails or key missing
            print(f"LLM Error in Planner: {e}")
            return []

class AnalyzerAgent:
    """
    Deterministic verification engine. 
    Matches hypothesis target_service and test_criteria against StructuredSignals.
    """
    def verify(self, hypothesis: dict, signals: List[StructuredSignal]) -> Dict:
        target = hypothesis.get("target_service", "").lower()
        criteria = hypothesis.get("test_criteria", [])
        
        matches = []
        # Weighted matrix: LOW (1)=0.33, MEDIUM (2)=0.66, HIGH (3)=1.0
        weights = {1: 0.33, 2: 0.66, 3: 1.0}
        
        # Track the highest severity score for each individually requested criteria
        criteria_scores = {c.lower(): 0.0 for c in criteria}
        
        for sig in signals:
            sig_service = sig.service.lower()
            sig_type = sig.type.value.lower() if hasattr(sig.type, 'value') else str(sig.type).lower()
            
            if sig_service == target and sig_type in criteria_scores:
                matches.append(sig.description)
                
                # Safely extract integer value from severity enum
                severity_val = sig.severity.value if hasattr(sig.severity, 'value') else int(sig.severity)
                # Map raw severity to a percentage weight
                weight = weights.get(severity_val, 0.33)
                # If multiple matches exist for same criteria, keep the strongest one
                criteria_scores[sig_type] = max(criteria_scores[sig_type], weight)
        
        # Calculate final confidence via Weighted Average Matrix
        if not criteria:
            normalized_score = 0.0
        else:
            normalized_score = sum(criteria_scores.values()) / len(criteria)
            
        return {
            "is_confirmed": normalized_score > 0.5,
            "confidence": round(normalized_score, 2),
            "evidence": matches
        }

class VerifierAgent(AgentBase):
    def finalize(self, context: IncidentContext, results: List[dict]) -> dict:
        """
        Synthesizes the verified hypotheses into a final explanation.
        """
        system_prompt = "Explain the root cause analysis findings to a human operator. Be concise and evidence-based."
        
        user_msg = f"""
        Incident: {context.incident_id} on {context.primary_affected_service}
        Analysis Results: {json.dumps(results)}
        
        Provide:
        1. Root Cause Summary
        2. Explanation of the chain of causality
        3. Human-readable evidence list
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_msg}
                ]
            )
            return {"explanation": response.choices[0].message.content}
        except Exception:
            return {"explanation": "Analysis complete. See evidence for details."}
