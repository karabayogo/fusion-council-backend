"""Phase 4 regression tests for memory-context prompt injection and worker wiring."""

import json
from unittest.mock import MagicMock, patch

import pytest

from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds
from fusion_council_service.domain.run_repository import get_run, insert_run
from fusion_council_service.domain.types import ProviderGenerateResult
from fusion_council_service.domain.worker_loop import Worker


def _insert_run(tmp_db, run_id: str, mode: str, prompt: str, deadline_seconds: int = 300):
    insert_run(
        db=tmp_db,
        run_id=run_id,
        mode=mode,
        prompt=prompt,
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=1200,
        deadline_seconds=deadline_seconds,
        deadline_at=utc_now_plus_seconds(deadline_seconds),
        owner_token_hash="testhash",
        metadata_json=json.dumps({}),
        requested_models_json=None,
        created_at=utc_now_iso(),
    )


def _make_worker(tmp_db, model_catalog):
    worker = Worker(
        db_path=":memory:",
        registry=MagicMock(),
        catalog=model_catalog,
        poll_interval_ms=50,
        heartbeat_interval_ms=5000,
        stale_run_threshold_seconds=30,
    )
    worker._db = tmp_db
    return worker


def test_build_fusion_prompt_accepts_memory_context_and_injects_header():
    from fusion_council_service.domain.scoring import build_fusion_prompt

    prompt = build_fusion_prompt(
        "What should I do?",
        [{"alias": "m1", "normalized_answer": "Do A", "status": "succeeded"}],
        memory_context="[lesson] cite assumptions first",
    )

    assert "Past Council Lessons" in prompt
    assert "cite assumptions first" in prompt


def test_build_council_synthesis_prompt_accepts_memory_context_and_injects_header():
    from fusion_council_service.domain.scoring import build_council_synthesis_prompt

    prompt = build_council_synthesis_prompt(
        "What should I do?",
        [{"alias": "m1", "normalized_answer": "Do A"}],
        [{"alias": "r1", "normalized_answer": "Looks fine"}],
        None,
        memory_context="[lesson] quantify tradeoffs",
    )

    assert "Past Council Lessons" in prompt
    assert "quantify tradeoffs" in prompt


@pytest.mark.asyncio
async def test_worker_fusion_injects_memory_context_before_synthesis(tmp_db, model_catalog):
    run_id = "run_phase4_fusion"
    _insert_run(tmp_db, run_id, "fusion", "How should I allocate portfolio risk?")
    worker = _make_worker(tmp_db, model_catalog)
    run = get_run(tmp_db, run_id)
    assert run is not None

    observed = {}

    def fake_fusion_prompt(prompt, candidates, memory_context=""):
        observed["memory_context"] = memory_context
        return f"SYNTH PROMPT\n{memory_context}"

    async def fake_provider(request, *_args, **_kwargs):
        text = request.user_prompt.lower()
        if "verification agent" in text:
            return ProviderGenerateResult(
                success=True,
                raw_text=json.dumps({"verdict": "confirm", "confidence": 0.88}),
                error_code=None,
                error_message=None,
                latency_ms=5,
                input_tokens=5,
                output_tokens=6,
            )
        if text.startswith("synth prompt"):
            return ProviderGenerateResult(
                success=True,
                raw_text="Synthesized answer",
                error_code=None,
                error_message=None,
                latency_ms=5,
                input_tokens=5,
                output_tokens=6,
            )
        return ProviderGenerateResult(
            success=True,
            raw_text=f"Generation answer {request.alias}",
            error_code=None,
            error_message=None,
            latency_ms=5,
            input_tokens=5,
            output_tokens=6,
        )

    with patch("fusion_council_service.domain.worker_loop.get_memory_context", return_value="MEMCTX-FUSION"), \
         patch("fusion_council_service.domain.worker_loop.build_fusion_prompt", side_effect=fake_fusion_prompt), \
         patch.object(worker, "_call_provider_async", side_effect=fake_provider):
        await worker._run_fusion(tmp_db, run)

    assert observed.get("memory_context") == "MEMCTX-FUSION"


@pytest.mark.asyncio
async def test_worker_council_early_synthesis_injects_memory_context(tmp_db, model_catalog):
    run_id = "run_phase4_council_early"
    _insert_run(tmp_db, run_id, "council", "How should I sequence retirement drawdown?")
    worker = _make_worker(tmp_db, model_catalog)
    run = get_run(tmp_db, run_id)
    assert run is not None

    observed = {}

    def fake_council_prompt(prompt, first_opinions, peer_reviews, debate_candidates=None, memory_context=""):
        observed["memory_context"] = memory_context
        return f"COUNCIL SYNTH\n{memory_context}"

    async def fake_provider(request, *_args, **_kwargs):
        text = request.user_prompt.lower()
        if text.startswith("council synth"):
            return ProviderGenerateResult(
                success=True,
                raw_text="Synthesis output",
                error_code=None,
                error_message=None,
                latency_ms=7,
                input_tokens=5,
                output_tokens=6,
            )
        return ProviderGenerateResult(
            success=True,
            raw_text="First opinion same answer",
            error_code=None,
            error_message=None,
            latency_ms=7,
            input_tokens=5,
            output_tokens=6,
        )

    with patch.object(worker, "_check_deadline", side_effect=[None, "council_skip_peer_review"]), \
         patch("fusion_council_service.domain.worker_loop.get_memory_context", return_value="MEMCTX-COUNCIL-EARLY"), \
         patch("fusion_council_service.domain.worker_loop.build_council_synthesis_prompt", side_effect=fake_council_prompt), \
         patch.object(worker, "_call_provider_async", side_effect=fake_provider):
        await worker._run_council(tmp_db, run)

    assert observed.get("memory_context") == "MEMCTX-COUNCIL-EARLY"


@pytest.mark.asyncio
async def test_worker_council_full_synthesis_injects_memory_context(tmp_db, model_catalog):
    run_id = "run_phase4_council_full"
    _insert_run(tmp_db, run_id, "council", "How should I hedge inflation?")
    worker = _make_worker(tmp_db, model_catalog)
    run = get_run(tmp_db, run_id)
    assert run is not None

    observed = {}

    def fake_council_prompt(prompt, first_opinions, peer_reviews, debate_candidates=None, memory_context=""):
        observed["memory_context"] = memory_context
        return f"COUNCIL SYNTH FULL\n{memory_context}"

    async def fake_provider(request, *_args, **_kwargs):
        text = request.user_prompt.lower()
        if "verification agent" in text:
            return ProviderGenerateResult(
                success=True,
                raw_text=json.dumps({"verdict": "confirm", "confidence": 0.75}),
                error_code=None,
                error_message=None,
                latency_ms=8,
                input_tokens=4,
                output_tokens=5,
            )
        if text.startswith("council synth full"):
            return ProviderGenerateResult(
                success=True,
                raw_text="Full-path synthesis answer",
                error_code=None,
                error_message=None,
                latency_ms=8,
                input_tokens=4,
                output_tokens=5,
            )
        return ProviderGenerateResult(
            success=True,
            raw_text="First opinion same answer",
            error_code=None,
            error_message=None,
            latency_ms=8,
            input_tokens=4,
            output_tokens=5,
        )

    with patch.object(worker, "_check_deadline", return_value=None), \
         patch("fusion_council_service.domain.worker_loop.get_memory_context", return_value="MEMCTX-COUNCIL-FULL"), \
         patch("fusion_council_service.domain.worker_loop.build_council_synthesis_prompt", side_effect=fake_council_prompt), \
         patch.object(worker, "_call_provider_async", side_effect=fake_provider):
        await worker._run_council(tmp_db, run)

    assert observed.get("memory_context") == "MEMCTX-COUNCIL-FULL"
