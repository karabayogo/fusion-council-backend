"""Phase 2 end-to-end regression tests for structured verification calls."""

import json
from unittest.mock import patch

import pytest

from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds
from fusion_council_service.domain.run_repository import get_run, insert_run
from fusion_council_service.domain.types import ProviderGenerateResult
from fusion_council_service.domain.worker_loop import Worker


class _StructuredRegistry:
    def __init__(self, confidence: float):
        self.confidence = confidence
        self.calls = []

    def generate(self, request):
        self.calls.append(request)
        return ProviderGenerateResult(
            success=True,
            raw_text=json.dumps({"verdict": "pass", "confidence": self.confidence}),
            error_code=None,
            error_message=None,
            latency_ms=10,
            input_tokens=5,
            output_tokens=7,
        )


def _insert_run(tmp_db, run_id: str, mode: str, prompt: str) -> dict:
    insert_run(
        db=tmp_db,
        run_id=run_id,
        mode=mode,
        prompt=prompt,
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=800,
        deadline_seconds=300,
        deadline_at=utc_now_plus_seconds(300),
        owner_token_hash="testhash",
        metadata_json=json.dumps({}),
        requested_models_json=None,
        created_at=utc_now_iso(),
    )
    run = get_run(tmp_db, run_id)
    assert run is not None
    return run


@pytest.mark.asyncio
async def test_fusion_verification_uses_structured_output_path(tmp_db, model_catalog):
    run = _insert_run(tmp_db, "run_phase2_structured_fusion", "fusion", "How to diversify a portfolio?")
    registry = _StructuredRegistry(confidence=0.84)
    worker = Worker(db_path=":memory:", registry=registry, catalog=model_catalog)
    worker._db = tmp_db

    async def fake_provider(request, db, run_id, timeout_seconds=300):
        if request.user_prompt == run["prompt"]:
            return ProviderGenerateResult(True, f"candidate from {request.alias}", None, None, 10, 5, 7)
        if "Below are answers from multiple AI models" in request.user_prompt:
            return ProviderGenerateResult(True, "synthesized answer", None, None, 10, 5, 7)
        raise AssertionError("Verification should use invoke_structured_or_freetext, not _call_provider_async")

    with patch.object(worker, "_call_provider_async", side_effect=fake_provider):
        await worker._run_fusion(tmp_db, run)

    completed = get_run(tmp_db, run["run_id"])
    assert completed is not None
    assert completed["status"] == "succeeded"
    assert abs(float(completed["final_confidence"]) - 0.84) < 1e-9
    assert registry.calls, "Expected structured verification call via registry.generate"
    assert registry.calls[0].response_format is not None


@pytest.mark.asyncio
async def test_council_verification_uses_structured_output_path(tmp_db, model_catalog):
    run = _insert_run(tmp_db, "run_phase2_structured_council", "council", "How should I sequence retirement withdrawals?")
    registry = _StructuredRegistry(confidence=0.77)
    worker = Worker(db_path=":memory:", registry=registry, catalog=model_catalog)
    worker._db = tmp_db

    async def fake_provider(request, db, run_id, timeout_seconds=300):
        prompt = request.user_prompt
        if prompt == run["prompt"]:
            return ProviderGenerateResult(True, "Use a stable withdrawal framework.", None, None, 10, 5, 7)
        if "peer reviewer" in prompt:
            return ProviderGenerateResult(True, "Review: assumptions are acceptable.", None, None, 10, 5, 7)
        if "Council Chair" in prompt:
            return ProviderGenerateResult(True, "Final council synthesis answer.", None, None, 10, 5, 7)
        raise AssertionError("Verification should use invoke_structured_or_freetext, not _call_provider_async")

    with patch.object(worker, "_call_provider_async", side_effect=fake_provider):
        await worker._run_council(tmp_db, run)

    completed = get_run(tmp_db, run["run_id"])
    assert completed is not None
    assert completed["status"] == "succeeded"
    assert abs(float(completed["final_confidence"]) - 0.77) < 1e-9
    assert registry.calls, "Expected structured verification call via registry.generate"
    assert registry.calls[0].response_format is not None
