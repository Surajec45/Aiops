from agents import PlannerAgent, AnalyzerAgent, VerifierAgent
from schemas.signals import IncidentContext, RCAConclusion
from typing import List

class RCAOrchestrator:
    def __init__(self):
        # In a real system, these would be initialized with appropriate config/keys
        self.planner = PlannerAgent()
        self.analyzer = AnalyzerAgent()
        self.verifier = VerifierAgent()

    def run_workflow(self, context: IncidentContext) -> RCAConclusion:
        # 1. PLAN: Generate candidates via LLM
        hypotheses = self.planner.generate_hypotheses(context)
        
        # 2. ANALYZE: Deterministically verify each candidate against generic signals
        verified_results = []
        print(f"Orchestrator: Analysis phase started for {len(hypotheses)} hypotheses.")
        for h in hypotheses:
            analysis = self.analyzer.verify(h, context.signals)
            print(f"Hypothesis '{h.get('statement')}' verification: {analysis['is_confirmed']} (Confidence: {analysis['confidence']})")
            if analysis["is_confirmed"]:
                verified_results.append({
                    "hypothesis": h["statement"],
                    "confidence": analysis["confidence"],
                    "evidence": analysis["evidence"],
                    "reasoning": h.get("reasoning", "")
                })
        print(f"Orchestrator: Analysis phase complete. Verified results: {len(verified_results)}")
        
        # 3. VERIFY: Synthesize final report via LLM
        if not verified_results:
             return RCAConclusion(
                incident_id=context.incident_id,
                root_cause="Inconclusive",
                confidence=0.0,
                evidence=[],
                explanation="None of the generated hypotheses could be confirmed with the available signals."
            )

        # Pick the most confident result
        best_result = max(verified_results, key=lambda x: x['confidence'])
        
        # Final polish
        synthesis = self.verifier.finalize(context, [best_result])
        
        return RCAConclusion(
            incident_id=context.incident_id,
            root_cause=best_result["hypothesis"],
            confidence=best_result["confidence"],
            evidence=best_result["evidence"],
            explanation=synthesis["explanation"]
        )
