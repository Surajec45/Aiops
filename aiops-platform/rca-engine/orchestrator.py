from agents import create_rca_graph
from schemas.signals import IncidentContext, RCAConclusion

class RCAOrchestrator:
    def __init__(self):
        # Compile and store the LangGraph application
        self.graph = create_rca_graph()

    def run_workflow(self, context: IncidentContext) -> RCAConclusion:
        print(f"Orchestrator: Initiating LangGraph RCA Workflow for Incident {context.incident_id}")

        # thread_id scopes the checkpointer snapshot to THIS incident.
        # If the graph crashes and is re-invoked with the same thread_id,
        # LangGraph resumes from the last successful node instead of restarting.
        config = {"configurable": {"thread_id": context.incident_id}}

        # Invoke the graph with the initial state + checkpoint config
        final_state = self.graph.invoke({
            "context": context,
            "hypotheses": [],
            "verification_results": {},
            "retry_count": 0,
            "final_explanation": ""
        }, config=config)
        
        results = final_state.get("verification_results", {})
        
        # If verification totally failed across all retries
        if not results or not results.get("is_confirmed", False):
             return RCAConclusion(
                incident_id=context.incident_id,
                root_cause="Inconclusive",
                confidence=0.0,
                evidence=[],
                explanation="None of the generated hypotheses could be confirmed with the available signals even after maximum retries."
            )

        # Get the winner
        winning_hypothesis = results.get("winning_hypothesis", {})
        
        return RCAConclusion(
            incident_id=context.incident_id,
            root_cause=winning_hypothesis.get("statement", "Unknown"),
            confidence=results.get("confidence", 0.0),
            evidence=results.get("evidence", []),
            explanation=final_state.get("final_explanation", "Analysis complete.")
        )
