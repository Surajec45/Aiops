from fastapi import FastAPI
from schemas.signals import IncidentContext, RCAConclusion
from orchestrator import RCAOrchestrator
from consumer import IncidentConsumer
import uvicorn
import os

app = FastAPI(title="Agentic AIOps RCA Engine")
orchestrator = RCAOrchestrator()
consumer = IncidentConsumer(orchestrator)

@app.on_event("startup")
async def startup_event():
    print("FastAPI Startup: Triggering Incident Consumer...")
    consumer.start()

@app.post("/analyze", response_model=RCAConclusion)
async def analyze_incident(context: IncidentContext):
    """
    Triggers the Agentic RCA Workflow:
    1. Planner Agent: Suggests hypotheses.
    2. Analyzer Agent: Deterministic check of signals.
    3. Verifier Agent: Logical validation & Scoring.
    """
    result = orchestrator.run_workflow(context)
    return result

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
