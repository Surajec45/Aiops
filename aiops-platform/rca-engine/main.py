import asyncio

import path_setup

path_setup.ensure_platform_path()

from fastapi import FastAPI
import uvicorn

from consumer import IncidentConsumer
from orchestrator import RCAOrchestrator
from schemas.signals import IncidentContext, RCAConclusion

app = FastAPI(title="Agentic AIOps RCA Engine")
orchestrator = RCAOrchestrator()
consumer = IncidentConsumer(orchestrator)


@app.on_event("startup")
async def startup_event():
    print("FastAPI Startup: Triggering Incident Consumer...")
    consumer.start(asyncio.get_running_loop())


@app.post("/analyze", response_model=RCAConclusion)
async def analyze_incident(context: IncidentContext):
    """
    Triggers the Agentic RCA Workflow:
    1. Planner Agent: Suggests hypotheses.
    2. Analyzer Agent: Deterministic check of signals.
    3. Verifier Agent: Logical validation & Scoring.
    """
    result = await orchestrator.run_workflow(context)
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
