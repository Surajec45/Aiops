import os
from typing import List, Optional, Tuple

from langchain_core.messages import BaseMessage
from langchain_openai import ChatOpenAI


def get_llm_api_settings() -> Tuple[str, Optional[str], Optional[str]]:
    api_key = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("LLM_BASE_URL") or None
    model = os.getenv("LLM_MODEL") or "gpt-4o"
    return model, api_key, base_url


def get_llm() -> ChatOpenAI:
    model, api_key, base_url = get_llm_api_settings()
    return ChatOpenAI(
        model=model,
        api_key=api_key,
        base_url=base_url,
        temperature=0,
    )


def get_verifier_llm() -> ChatOpenAI:
    model, api_key, base_url = get_llm_api_settings()
    return ChatOpenAI(
        model=model,
        api_key=api_key,
        base_url=base_url,
        temperature=0,
    )
