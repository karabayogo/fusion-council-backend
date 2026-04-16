"""Shared types for provider clients and domain logic."""

from dataclasses import dataclass
from typing import Optional, List, Dict, Any

from pydantic import BaseModel


# --- Provider dataclasses ---

@dataclass
class ProviderGenerateRequest:
    """Normalized request object passed to any model provider."""
    alias: str                # internal alias like "ollama/glm-5.1:cloud"
    provider: str             # "minimax_token_plan" or "ollama_cloud"
    provider_model: str       # provider-specific model name like "glm-5.1"
    system_prompt: Optional[str]
    user_prompt: str
    max_output_tokens: int    # default 3000
    temperature: float        # default 0.2


@dataclass
class ProviderGenerateResult:
    """Normalized result returned by any model provider."""
    success: bool
    raw_text: Optional[str]   # raw model output
    error_code: Optional[str]
    error_message: Optional[str]
    latency_ms: int           # time spent in the provider call
    input_tokens: Optional[int]
    output_tokens: Optional[int]


# --- API request/response models ---

class RunRequest(BaseModel):
    mode: str
    prompt: str
    system_prompt: Optional[str] = None
    requested_models: Optional[List[str]] = None
    temperature: float = 0.2
    max_output_tokens: int = 3000
    deadline_seconds: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None


class RunResponse(BaseModel):
    run_id: str
    status: str
    status_url: str
    events_url: str
    answers_url: str
    suggested_poll_interval_ms: int


class RespondRequest(BaseModel):
    mode: str
    prompt: str
    system_prompt: Optional[str] = None
    requested_models: Optional[List[str]] = None
    temperature: float = 0.2
    max_output_tokens: int = 3000
    deadline_seconds: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None
    wait_timeout_seconds: Optional[int] = None