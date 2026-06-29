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
    # RCA-2: short verification now yields succeeded_degraded (the new policy
    # exposes the untrustworthy verdict visibly to operators instead of
    # pretending it was a normal success). The final_confidence=0.5 and
    # [INSUFFICIENT EVIDENCE] prefix are unchanged from PR #28.
    assert completed["status"] == "succeeded_degraded", (
        f"RCA-2: short verification must yield succeeded_degraded, "
        f"got {completed['status']!r}"
    )
    # _StructuredRegistry emits 7 output_tokens — same E2 short-output guard
    # that the council path uses (after _apply_verification_result() was
    # factored out in PR #28). The guard correctly rejects the verdict and
    # pins final_confidence=0.5. The structured-output path is still
    # exercised end-to-end (registry.calls[0].response_format is set).
    assert abs(float(completed["final_confidence"]) - 0.5) < 1e-9, (
        f"E2 guard should pin final_confidence=0.5 for short verification; "
        f"got {completed['final_confidence']}"
    )
    assert "[INSUFFICIENT EVIDENCE" in (completed.get("final_answer") or ""), (
        "E2 guard should prepend [INSUFFICIENT EVIDENCE] to final_answer for short verification"
    )
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

    # _StructuredRegistry emits 7 output_tokens — that triggers the E2 guard
    # (MIN_VERIFICATION_TOKENS=50). The guard correctly rejects the verdict
    # and pins final_confidence=0.5, which is the documented E2 fix behavior
    # (regression of run_c908a00b1c834b8eb9ebe2b4). The test now asserts the
    # guard fires, NOT the broken old behavior. The structured-output path is
    # still exercised end-to-end (registry.calls[0].response_format is set) — we
    # just no longer assert a fake 0.77 confidence was preserved through it.
    with patch.object(worker, "_call_provider_async", side_effect=fake_provider):
        await worker._run_council(tmp_db, run)

    completed = get_run(tmp_db, run["run_id"])
    assert completed is not None
    # RCA-2: short verification now yields succeeded_degraded (consistent
    # with the fusion path; see comment above for full policy).
    assert completed["status"] == "succeeded_degraded", (
        f"RCA-2: short verification must yield succeeded_degraded, "
        f"got {completed['status']!r}"
    )
    # E2 guard: short verification output (7 tokens) is rejected → confidence=0.5.
    assert abs(float(completed["final_confidence"]) - 0.5) < 1e-9, (
        f"E2 guard should pin final_confidence=0.5 for short verification; "
        f"got {completed['final_confidence']}"
    )
    assert "[INSUFFICIENT EVIDENCE" in (completed.get("final_answer") or ""), (
        "E2 guard should prepend [INSUFFICIENT EVIDENCE] to final_answer for short verification"
    )
    assert registry.calls, "Expected structured verification call via registry.generate"
    assert registry.calls[0].response_format is not None
