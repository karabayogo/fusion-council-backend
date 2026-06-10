"""Shared types for provider clients and domain logic."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# --- Provider dataclasses ---

@dataclass
class ProviderGenerateRequest:
    """Normalized request object passed to any model provider."""
    alias: str                # internal alias like "ollama/glm-5.1:cloud"
    provider: str             # "minimax_token_plan", "ollama_cloud", "openai_codex", or "opencode_go"
    provider_model: str       # provider-specific model name like "glm-5.1"
    system_prompt: Optional[str]
    user_prompt: str
    max_output_tokens: int    # default 30000
    temperature: float        # default 0.2
    # Optional structured-output hints. Providers that don't support them can ignore.
    json_schema: Optional[dict] = None
    response_format: Optional[dict] = None
    # Optional per-model call timeout in seconds.  When unset, the caller's
    # default (typically 300s) is used.  This allows thinking models or slow
    # providers to get more time than the hard-coded default without making
    # the timeout a required parameter everywhere.
    timeout_seconds: Optional[int] = None


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

    def __iter__(self):
        """Preserve tuple-unpacking compatibility in worker orchestration.

        The worker historically unpacked provider results as
        ``success, text, ... = result``. Provider clients now return this
        dataclass consistently, so exposing the normalized tuple shape keeps
        all orchestration paths compatible without duplicating adapter code.
        """
        yield self.success
        yield self.raw_text
        yield self.error_code
        yield self.error_message
        yield self.latency_ms
        yield self.input_tokens
        yield self.output_tokens


# --- API request/response models ---

class RunRequest(BaseModel):
    mode: str
    prompt: str
    system_prompt: Optional[str] = None
    requested_models: Optional[List[str]] = None
    temperature: float = 0.2
    max_output_tokens: int = Field(default=30000, ge=1, le=30000)
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
    max_output_tokens: int = Field(default=30000, ge=1, le=30000)
    deadline_seconds: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None
    wait_timeout_seconds: Optional[int] = None
