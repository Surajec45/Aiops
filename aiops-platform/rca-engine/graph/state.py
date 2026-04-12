import os
from typing import Annotated, List, TypedDict

from langchain_core.messages import BaseMessage, SystemMessage

from schemas.signals import IncidentContext

# Summarizer triggers when message count exceeds this threshold
SUMMARIZE_THRESHOLD = 20

# Max times the planner is retried when analyzer finds no valid hypotheses
MAX_RETRIES = int(os.getenv("RCA_MAX_RETRIES", "2"))


def _messages_reducer(current: List[BaseMessage], update: List[BaseMessage]) -> List[BaseMessage]:
    """
    Custom reducer for the messages field.

    Normal case (planner/tools appending):
      update is a list of new messages → append to current.

    Summarizer case (replacing the full history):
      update starts with a SystemMessage → treat as a full replacement,
      not an append. This lets summarizer_node overwrite the history with
      [SystemMessage, summary_HumanMessage] without the old messages leaking back.
    """
    if update and isinstance(update[0], SystemMessage):
        # Full replacement — summarizer compressed the history
        return list(update)
    # Normal append
    return current + list(update)


class RCAState(TypedDict):
    context: IncidentContext
    messages: Annotated[List[BaseMessage], _messages_reducer]
    hypotheses: List[dict]
    verification_results: dict
    retry_count: int
    exploration_count: int
    final_explanation: str
    # Cleared to False by summarizer_node after compression
    needs_summarization: bool
