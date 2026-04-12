import os

from langgraph.checkpoint.postgres import PostgresSaver
from psycopg_pool import AsyncConnectionPool

from graph.builder import create_rca_graph
from schemas.signals import IncidentContext, RCAConclusion

# Postgres Connection Details
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://postgres:password@postgres-state:5432/langgraph_state")


class RCAOrchestrator:
    def __init__(self):
        self.graph = None
        self.pool = None
        self.checkpointer = None

    async def _ensure_initialized(self):
        """Initializes Postgres pool, checkpointer, and graph if needed."""
        if self.pool is None:
            print(f"Orchestrator: Connecting to Postgres at {POSTGRES_URL}")
            # min_size=1, max_size=10 is a reasonable default
            self.pool = AsyncConnectionPool(conninfo=POSTGRES_URL, max_size=10, open=False)
            await self.pool.open()
            
        if self.checkpointer is None:
            self.checkpointer = PostgresSaver(self.pool)
            # Ensure the necessary tables exist in the database
            await self.checkpointer.setup()
            
        if self.graph is None:
            self.graph = await create_rca_graph(self.checkpointer)

    async def run_workflow(self, context: IncidentContext) -> RCAConclusion:
        print(f"Orchestrator: Initiating Persistent LangGraph RCA Workflow for Incident {context.incident_id}")
        await self._ensure_initialized()

        # thread_id ensures this investigation state is persisted in Postgres
        config = {"configurable": {"thread_id": context.incident_id}}

        # Invoke the graph
        final_state = await self.graph.ainvoke({
            "context": context,
            "messages": [],
            "hypotheses": [],
            "verification_results": {},
            "retry_count": 0,
            "exploration_count": 0,
            "final_explanation": ""
        }, config=config)
        
        results = final_state.get("verification_results", {})
        
        if not results or not results.get("is_confirmed", False):
             return RCAConclusion(
                incident_id=context.incident_id,
                root_cause="Inconclusive",
                confidence=0.0,
                evidence=[],
                explanation="None of the generated hypotheses could be confirmed with the available signals."
            )

        winning_hypothesis = results.get("winning_hypothesis", {})
        
        return RCAConclusion(
            incident_id=context.incident_id,
            root_cause=winning_hypothesis.get("statement", "Unknown"),
            confidence=results.get("confidence", 0.0),
            evidence=results.get("evidence", []),
            explanation=final_state.get("final_explanation", "Analysis complete.")
        )

    async def close(self):
        """Cleanup connection pool on shutdown."""
        if self.pool:
            await self.pool.close()
