"""Fusion Council worker — polls DB, claims runs, executes model orchestration."""

import asyncio
import json
import signal
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds
from fusion_council_service.db import open_db_connection, initialize_schema
from fusion_council_service.domain import budget, event_emitter, scoring
from fusion_council_service.domain.budget import compute_budget, should_degrade, select_models_for_mode
from fusion_council_service.domain.candidate_repository import insert_candidate, update_candidate_result
from fusion_council_service.domain.event_emitter import (
    emit_candidate_completed, emit_candidate_failed, emit_fallback_promoted,
    emit_heartbeat, emit_run_completed, emit_run_failed, emit_run_finalizing,
    emit_run_started, emit_run_succeeded_degraded, emit_stage_progress, emit_stage_started,
)
from fusion_council_service.domain.event_repository import get_next_seq
from fusion_council_service.domain.run_repository import claim_next_run, get_run, update_run_status
from fusion_council_service.domain.scoring import (
    build_council_synthesis_prompt, build_debate_prompt, build_fusion_prompt,
    build_peer_review_prompt, build_verification_prompt, compute_pairwise_agreement,
    select_best_candidate,
)
from fusion_council_service.ids import new_candidate_id
from fusion_council_service.logging_utils import get_logger
from fusion_council_service.model_catalog import ModelCatalog
from fusion_council_service.providers.registry import ProviderRegistry

logger = get_logger("fusion_council_service.worker_loop")

# Thread pool for blocking provider calls
_executor = ThreadPoolExecutor(max_workers=10)


def _run_provider_sync(
    registry: ProviderRegistry,
    request,
) -> tuple:
    """Run a provider call in a thread (sync wrapper)."""
    result = registry.generate(request)
    return (result.success, result.raw_text, result.error_code, result.error_message,
            result.latency_ms, result.input_tokens, result.output_tokens)


class Worker:
    """Background worker that polls for and executes runs."""

    def __init__(
        self,
        db_path: str,
        registry: ProviderRegistry,
        catalog: ModelCatalog,
        poll_interval_ms: int = 1000,
        heartbeat_interval_ms: int = 5000,
    ):
        self._db_path = db_path
        self._registry = registry
        self._catalog = catalog
        self._poll_interval_s = poll_interval_ms / 1000.0
        self._heartbeat_interval_s = heartbeat_interval_ms / 1000.0
        self._running = False
        self._db: Optional[sqlite3.Connection] = None
        self._worker_id = f"worker-{int(time.time())}"

    def _get_db(self) -> sqlite3.Connection:
        if self._db is None:
            self._db = open_db_connection(self._db_path)
            initialize_schema(self._db)
        return self._db

    def _update_heartbeat(self, run_id: str) -> None:
        db = self._get_db()
        update_run_status(db, run_id, "running", last_heartbeat_at=utc_now_iso())

    def _progress_percent(self, models_completed: int, models_planned: int) -> float:
        if models_planned == 0:
            return 0.0
        return min(100.0, (models_completed / models_planned) * 100.0)

    def _elapsed_seconds(self, run: dict) -> float:
        """Seconds elapsed since the run started."""
        started = run.get("started_at") or run.get("created_at")
        if not started:
            return 0.0
        from fusion_council_service.clock import parse_iso
        try:
            start_dt = parse_iso(started)
            return (time.time() - start_dt.timestamp())
        except Exception:
            return 0.0

    def _check_deadline(self, run: dict) -> Optional[str]:
        """Check if deadline pressure should trigger degradation.
        Returns degradation reason or None.
        """
        elapsed = self._elapsed_seconds(run)
        total = run.get("deadline_seconds", 60)
        return should_degrade(run["mode"], elapsed, total)

    async def _finalize_degraded(self, db: sqlite3.Connection, run_id: str, mode: str,
                                   reason: str, best_text: str, confidence: float = 0.5) -> None:
        """Finalize a run as succeeded_degraded due to deadline pressure."""
        logger.info(f"Finalizing as succeeded_degraded: {reason}", run_id=run_id)
        now = utc_now_iso()
        db.execute(
            "UPDATE runs SET status='succeeded_degraded', finished_at=?, final_answer=?, "
            "final_confidence=?, degraded_reason=? WHERE run_id=?",
            (now, best_text, confidence, reason, run_id),
        )
        db.commit()
        emit_run_succeeded_degraded(db, run_id, best_text, reason, confidence=confidence)
        update_run_status(db, run_id, "succeeded_degraded", final_answer=best_text,
                          final_confidence=confidence, degraded_reason=reason)

    def _try_fallback(self, db: sqlite3.Connection, run: dict, failed_alias: str) -> Optional[dict]:
        """Try to promote a fallback model for a failed primary.
        Returns the fallback model dict or None.
        """
        mode = run["mode"]
        from fusion_council_service.model_catalog import FUSION_FALLBACK_QUEUE, COUNCIL_FALLBACK_QUEUE
        fallback_queue = COUNCIL_FALLBACK_QUEUE if mode == "council" else FUSION_FALLBACK_QUEUE
        for alias in fallback_queue:
            m = self._catalog.get(alias)
            if m and m.get("enabled", False):
                emit_fallback_promoted(db, run["run_id"], alias, failed_alias)
                logger.info(f"Fallback promoted: {alias} replacing {failed_alias}", run_id=run["run_id"])
                return m
        return None

    async def _call_provider_async(self, request, db: sqlite3.Connection, run_id: str):
        """Call provider in thread pool and return result."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            _executor, _run_provider_sync, self._registry, request,
        )

    async def _run_single(self, db: sqlite3.Connection, run: dict) -> None:
        """Execute a single-mode run."""
        from fusion_council_service.domain.types import ProviderGenerateRequest

        run_id = run["run_id"]
        logger.info("Starting single run", run_id=run_id)

        models = select_models_for_mode("single", self._catalog)
        if not models:
            await self._fail_run(db, run_id, "NO_MODELS", "No enabled models for single mode")
            return

        model = models[0]
        emit_stage_started(db, run_id, "generation", [model["alias"]])

        request = ProviderGenerateRequest(
            alias=model["alias"],
            provider=model["provider"],
            provider_model=model["provider_model"],
            system_prompt=run.get("system_prompt"),
            user_prompt=run["prompt"],
            max_output_tokens=run["max_output_tokens"],
            temperature=run["temperature"],
        )

        success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = await self._call_provider_async(request, db, run_id)
        candidate_id = new_candidate_id()

        if success:
            insert_candidate(db, run_id, candidate_id, model["alias"], model["provider"],
                             model["provider_model"], "generation", "succeeded", utc_now_iso())
            update_candidate_result(db, candidate_id, "succeeded", raw_answer=raw_text,
                                    latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok)
            emit_candidate_completed(db, run_id, candidate_id, model["alias"], "generation")

            # Emit completion
            db.execute(
                "UPDATE runs SET status='succeeded', finished_at=?, final_answer=? WHERE run_id=?",
                (utc_now_iso(), raw_text, run_id),
            )
            db.commit()
            emit_run_completed(db, run_id, raw_text)
            update_run_status(db, run_id, "succeeded")
        else:
            insert_candidate(db, run_id, candidate_id, model["alias"], model["provider"],
                             model["provider_model"], "generation", "failed", utc_now_iso())
            update_candidate_result(db, candidate_id, "failed", error_code=err_code, error_message=err_msg)
            emit_candidate_failed(db, run_id, candidate_id, model["alias"], "generation", err_msg or err_code)
            # Try fallback for single mode
            fallback = self._try_fallback(db, run, model["alias"])
            if fallback:
                fallback_req = ProviderGenerateRequest(
                    alias=fallback["alias"], provider=fallback["provider"],
                    provider_model=fallback["provider_model"],
                    system_prompt=run.get("system_prompt"),
                    user_prompt=run["prompt"],
                    max_output_tokens=run["max_output_tokens"],
                    temperature=run["temperature"],
                )
                fb_candidate_id = new_candidate_id()
                fb_ok, fb_txt, fb_ec, fb_em, fb_lat, fb_in, fb_out = await self._call_provider_async(fallback_req, db, run_id)
                if fb_ok:
                    insert_candidate(db, run_id, fb_candidate_id, fallback["alias"], fallback["provider"],
                                     fallback["provider_model"], "generation", "succeeded", utc_now_iso())
                    update_candidate_result(db, fb_candidate_id, "succeeded", raw_answer=fb_txt,
                                            latency_ms=fb_lat, input_tokens=fb_in, output_tokens=fb_out)
                    emit_candidate_completed(db, run_id, fb_candidate_id, fallback["alias"], "generation")
                    db.execute(
                        "UPDATE runs SET status='succeeded', finished_at=?, final_answer=? WHERE run_id=?",
                        (utc_now_iso(), fb_txt, run_id),
                    )
                    db.commit()
                    emit_run_completed(db, run_id, fb_txt)
                    update_run_status(db, run_id, "succeeded")
                    return
            await self._fail_run(db, run_id, err_code or "PROVIDER_FAILED", err_msg or "Single model failed")

    async def _run_fusion(self, db: sqlite3.Connection, run: dict) -> None:
        """Execute a fusion-mode run."""
        from fusion_council_service.domain.types import ProviderGenerateRequest

        run_id = run["run_id"]
        logger.info("Starting fusion run", run_id=run_id)

        models = select_models_for_mode("fusion", self._catalog)
        if len(models) < 2:
            await self._fail_run(db, run_id, "NO_MODELS", "Need at least 2 models for fusion")
            return

        deadline_s = run["deadline_seconds"]
        run_budget = compute_budget("fusion", deadline_s)
        stage_budgets = {s.stage: s for s in run_budget.stages}

        # Stage 1: generation — all models in parallel
        emit_stage_started(db, run_id, "generation", [m["alias"] for m in models])

        gen_candidates = []
        pending_calls = []
        for model in models:
            request = ProviderGenerateRequest(
                alias=model["alias"], provider=model["provider"],
                provider_model=model["provider_model"],
                system_prompt=run.get("system_prompt"),
                user_prompt=run["prompt"],
                max_output_tokens=run["max_output_tokens"],
                temperature=run["temperature"],
            )
            pending_calls.append((model, request))

        # Execute in parallel with semaphore to cap concurrency
        sem = asyncio.Semaphore(3)
        async def call_with_sem(model, req):
            async with sem:
                return model, await self._call_provider_async(req, db, run_id)

        results = await asyncio.gather(*[call_with_sem(m, r) for m, r in pending_calls])

        for (model, request), result in zip(pending_calls, results):
            success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = result
            cand_id = new_candidate_id()
            if success:
                insert_candidate(db, run_id, cand_id, model["alias"], model["provider"],
                                 model["provider_model"], "generation", "succeeded", utc_now_iso())
                update_candidate_result(db, cand_id, "succeeded", raw_answer=raw_text,
                                        latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok)
                emit_candidate_completed(db, run_id, cand_id, model["alias"], "generation")
                gen_candidates.append(get_run(db, run_id) or {})  # refresh
                # Reload candidate from DB
                cursor = db.execute("SELECT * FROM run_candidates WHERE candidate_id=?", (cand_id,))
                gen_candidates[-1] = dict(cursor.fetchone())
            else:
                insert_candidate(db, run_id, cand_id, model["alias"], model["provider"],
                                 model["provider_model"], "generation", "failed", utc_now_iso())
                update_candidate_result(db, cand_id, "failed", error_code=err_code, error_message=err_msg)
                emit_candidate_failed(db, run_id, cand_id, model["alias"], "generation", err_msg or err_code)

        succeeded = [c for c in gen_candidates if c.get("status") == "succeeded"]

        # Deadline check after generation stage
        degradation = self._check_deadline(run)
        if degradation and succeeded:
            best = select_best_candidate(succeeded)
            best_text = best.get("raw_answer", "") if best else ""
            await self._finalize_degraded(db, run_id, "fusion", degradation, best_text)
            return

        # Fallback promotion: if quorum not met, try fallbacks before failing
        if len(succeeded) < 2:
            failed_aliases = [c.get("alias", "") for c in gen_candidates if c.get("status") == "failed"]
            for failed_alias in failed_aliases:
                fallback = self._try_fallback(db, run, failed_alias)
                if fallback:
                    fallback_req = ProviderGenerateRequest(
                        alias=fallback["alias"], provider=fallback["provider"],
                        provider_model=fallback["provider_model"],
                        system_prompt=run.get("system_prompt"),
                        user_prompt=run["prompt"],
                        max_output_tokens=run["max_output_tokens"],
                        temperature=run["temperature"],
                    )
                    cand_id = new_candidate_id()
                    fb_success, fb_text, fb_ec, fb_em, fb_lat, fb_in, fb_out = await self._call_provider_async(fallback_req, db, run_id)
                    if fb_success:
                        insert_candidate(db, run_id, cand_id, fallback["alias"], fallback["provider"],
                                         fallback["provider_model"], "generation", "succeeded", utc_now_iso())
                        update_candidate_result(db, cand_id, "succeeded", raw_answer=fb_text,
                                                latency_ms=fb_lat, input_tokens=fb_in, output_tokens=fb_out)
                        emit_candidate_completed(db, run_id, cand_id, fallback["alias"], "generation")
                        cursor = db.execute("SELECT * FROM run_candidates WHERE candidate_id=?", (cand_id,))
                        succeeded.append(dict(cursor.fetchone()))
                    if len(succeeded) >= 2:
                        break

        if len(succeeded) < 2:
            # Quorum not met even after fallbacks
            db.execute("UPDATE runs SET status='failed', error_code='FUSION_QUORUM_NOT_MET', finished_at=? WHERE run_id=?",
                       (utc_now_iso(), run_id))
            db.commit()
            emit_run_failed(db, run_id, "FUSION_QUORUM_NOT_MET", f"Only {len(succeeded)}/3 models succeeded")
            update_run_status(db, run_id, "failed", error_code="FUSION_QUORUM_NOT_MET")
            return

        # Stage 2: synthesis
        # Deadline check before synthesis
        degradation = self._check_deadline(run)
        if degradation:
            best = select_best_candidate(succeeded)
            best_text = best.get("raw_answer", "") if best else ""
            await self._finalize_degraded(db, run_id, "fusion", degradation, best_text)
            return

        emit_stage_started(db, run_id, "synthesis", [])
        synth_models = select_models_for_mode("fusion", self._catalog)
        synth_model = synth_models[0] if synth_models else models[0]
        synthesis_prompt = build_fusion_prompt(run["prompt"], succeeded)
        request = ProviderGenerateRequest(
            alias=synth_model["alias"], provider=synth_model["provider"],
            provider_model=synth_model["provider_model"],
            system_prompt=run.get("system_prompt"),
            user_prompt=synthesis_prompt,
            max_output_tokens=run["max_output_tokens"],
            temperature=run["temperature"],
        )
        cand_id = new_candidate_id()
        success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = await self._call_provider_async(request, db, run_id)
        if success:
            insert_candidate(db, run_id, cand_id, synth_model["alias"], synth_model["provider"],
                             synth_model["provider_model"], "synthesis", "succeeded", utc_now_iso())
            update_candidate_result(db, cand_id, "succeeded", raw_answer=raw_text,
                                    latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok)
            emit_candidate_completed(db, run_id, cand_id, synth_model["alias"], "synthesis")
            synthesis_text = raw_text
        else:
            insert_candidate(db, run_id, cand_id, synth_model["alias"], synth_model["provider"],
                             synth_model["provider_model"], "synthesis", "failed", utc_now_iso())
            update_candidate_result(db, cand_id, "failed", error_code=err_code, error_message=err_msg)
            synthesis_text = succeeded[0].get("raw_answer", "") if succeeded else "No answer available."

        # Stage 3: verification
        # Deadline check — skip verification if under deadline pressure
        degradation = self._check_deadline(run)
        if degradation:
            # Deadline imminent — finalize with synthesis answer as succeeded_degraded
            await self._finalize_degraded(db, run_id, "fusion", degradation, synthesis_text, confidence=0.5)
            return

        emit_stage_started(db, run_id, "verification", [])
        verification_prompt = build_verification_prompt(run["prompt"], synthesis_text)
        verif_models = select_models_for_mode("fusion", self._catalog)
        verif_model = verif_models[1] if len(verif_models) > 1 else models[0]
        request = ProviderGenerateRequest(
            alias=verif_model["alias"], provider=verif_model["provider"],
            provider_model=verif_model["provider_model"],
            system_prompt=None, user_prompt=verification_prompt,
            max_output_tokens=500, temperature=0.1,
        )
        cand_id = new_candidate_id()
        success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = await self._call_provider_async(request, db, run_id)
        if success:
            insert_candidate(db, run_id, cand_id, verif_model["alias"], verif_model["provider"],
                             verif_model["provider_model"], "verification", "succeeded", utc_now_iso())
            update_candidate_result(db, cand_id, "succeeded", raw_answer=raw_text,
                                    latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok)
            # Parse verification verdict
            confidence = 0.5
            try:
                v_data = json.loads(raw_text)
                confidence = v_data.get("confidence", 0.5)
                if v_data.get("verdict") == "abstain":
                    final_answer = f"[INSUFFICIENT EVIDENCE — confidence: {confidence}]\n{synthesis_text}"
                else:
                    final_answer = synthesis_text
            except Exception:
                final_answer = synthesis_text
        else:
            final_answer = synthesis_text  # Fallback if verification fails

        db.execute("UPDATE runs SET status='succeeded', finished_at=?, final_answer=?, final_confidence=? WHERE run_id=?",
                   (utc_now_iso(), final_answer, confidence, run_id))
        db.commit()
        emit_run_completed(db, run_id, final_answer, confidence=confidence)
        update_run_status(db, run_id, "succeeded", final_answer=final_answer, final_confidence=confidence)

    async def _run_council(self, db: sqlite3.Connection, run: dict) -> None:
        """Execute a council-mode run."""
        from fusion_council_service.domain.types import ProviderGenerateRequest

        run_id = run["run_id"]
        logger.info("Starting council run", run_id=run_id)

        models = select_models_for_mode("council", self._catalog)
        if len(models) < 3:
            await self._fail_run(db, run_id, "NO_MODELS", "Need at least 3 models for council")
            return

        deadline_s = run["deadline_seconds"]

        # Stage 1: first opinions (all models in parallel)
        emit_stage_started(db, run_id, "first_opinion", [m["alias"] for m in models])

        sem = asyncio.Semaphore(3)
        async def call_model(model):
            async with sem:
                request = ProviderGenerateRequest(
                    alias=model["alias"], provider=model["provider"],
                    provider_model=model["provider_model"],
                    system_prompt=run.get("system_prompt"),
                    user_prompt=run["prompt"],
                    max_output_tokens=run["max_output_tokens"],
                    temperature=run["temperature"],
                )
                return model, await self._call_provider_async(request, db, run_id)

        first_results = await asyncio.gather(*[call_model(m) for m in models])

        first_opinions = []
        for (model, request), result in zip([(m, None) for m in models], first_results):
            success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = result
            cand_id = new_candidate_id()
            m = model
            if success:
                insert_candidate(db, run_id, cand_id, m["alias"], m["provider"],
                                 m["provider_model"], "first_opinion", "succeeded", utc_now_iso())
                update_candidate_result(db, cand_id, "succeeded", raw_answer=raw_text,
                                        latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok)
                emit_candidate_completed(db, run_id, cand_id, m["alias"], "first_opinion")
                cursor = db.execute("SELECT * FROM run_candidates WHERE candidate_id=?", (cand_id,))
                first_opinions.append(dict(cursor.fetchone()))
            else:
                insert_candidate(db, run_id, cand_id, m["alias"], m["provider"],
                                 m["provider_model"], "first_opinion", "failed", utc_now_iso())
                update_candidate_result(db, cand_id, "failed", error_code=err_code, error_message=err_msg)
                emit_candidate_failed(db, run_id, cand_id, m["alias"], "first_opinion", err_msg or err_code)

        succeeded_opinions = [c for c in first_opinions if c.get("status") == "succeeded"]

        # Deadline check after first opinions
        degradation = self._check_deadline(run)
        if degradation and succeeded_opinions:
            best = select_best_candidate(succeeded_opinions)
            best_text = best.get("raw_answer", "") if best else ""
            await self._finalize_degraded(db, run_id, "council", degradation, best_text)
            return

        # Fallback promotion: if council quorum not met, try fallbacks
        if len(succeeded_opinions) < 2:
            failed_aliases = [c.get("alias", "") for c in first_opinions if c.get("status") == "failed"]
            for failed_alias in failed_aliases:
                fallback = self._try_fallback(db, run, failed_alias)
                if fallback:
                    fallback_req = ProviderGenerateRequest(
                        alias=fallback["alias"], provider=fallback["provider"],
                        provider_model=fallback["provider_model"],
                        system_prompt=run.get("system_prompt"),
                        user_prompt=run["prompt"],
                        max_output_tokens=run["max_output_tokens"],
                        temperature=run["temperature"],
                    )
                    cand_id = new_candidate_id()
                    fb_ok, fb_txt, fb_ec, fb_em, fb_lat, fb_in, fb_out = await self._call_provider_async(fallback_req, db, run_id)
                    if fb_ok:
                        insert_candidate(db, run_id, cand_id, fallback["alias"], fallback["provider"],
                                         fallback["provider_model"], "first_opinion", "succeeded", utc_now_iso())
                        update_candidate_result(db, cand_id, "succeeded", raw_answer=fb_txt,
                                                latency_ms=fb_lat, input_tokens=fb_in, output_tokens=fb_out)
                        emit_candidate_completed(db, run_id, cand_id, fallback["alias"], "first_opinion")
                        cursor = db.execute("SELECT * FROM run_candidates WHERE candidate_id=?", (cand_id,))
                        succeeded_opinions.append(dict(cursor.fetchone()))
                    if len(succeeded_opinions) >= 2:
                        break

        if len(succeeded_opinions) < 2:
            db.execute("UPDATE runs SET status='failed', error_code='COUNCIL_QUORUM_NOT_MET', finished_at=? WHERE run_id=?",
                       (utc_now_iso(), run_id))
            db.commit()
            emit_run_failed(db, run_id, "COUNCIL_QUORUM_NOT_MET", f"Only {len(succeeded_opinions)}/3 opinions succeeded")
            update_run_status(db, run_id, "failed", error_code="COUNCIL_QUORUM_NOT_MET")
            return

        # Deadline check — skip peer review if under heavy pressure
        degradation = self._check_deadline(run)
        if degradation and "skip_peer" in (degradation or ""):
            # Skip peer reviews and debate, go straight to synthesis
            emit_stage_started(db, run_id, "synthesis", [])
            synth_prompt = build_council_synthesis_prompt(run["prompt"], succeeded_opinions, [], None)
            synth_model = models[0]
            request = ProviderGenerateRequest(
                alias=synth_model["alias"], provider=synth_model["provider"],
                provider_model=synth_model["provider_model"],
                system_prompt=None, user_prompt=synth_prompt,
                max_output_tokens=run["max_output_tokens"], temperature=0.2,
            )
            cand_id = new_candidate_id()
            success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = await self._call_provider_async(request, db, run_id)
            if success:
                insert_candidate(db, run_id, cand_id, synth_model["alias"], synth_model["provider"],
                                 synth_model["provider_model"], "synthesis", "succeeded", utc_now_iso())
                update_candidate_result(db, cand_id, "succeeded", raw_answer=raw_text,
                                        latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok)
                emit_candidate_completed(db, run_id, cand_id, synth_model["alias"], "synthesis")
                synthesis_text = raw_text
            else:
                best = select_best_candidate(succeeded_opinions)
                synthesis_text = best.get("raw_answer", "") if best else "Council synthesis failed."
            await self._finalize_degraded(db, run_id, "council", degradation, synthesis_text)
            return

        # Stage 2: peer reviews
        emit_stage_started(db, run_id, "peer_review", [])
        review_tasks = []
        for opinion_cand in succeeded_opinions:
            reviewer = models[0]  # Use first model as reviewer (simplified)
            review_prompt = build_peer_review_prompt(run["prompt"], opinion_cand.get("raw_answer", ""), reviewer["alias"])
            request = ProviderGenerateRequest(
                alias=reviewer["alias"], provider=reviewer["provider"],
                provider_model=reviewer["provider_model"],
                system_prompt=None, user_prompt=review_prompt,
                max_output_tokens=run["max_output_tokens"], temperature=0.1,
            )
            review_tasks.append((reviewer, request))

        async def call_review(model, req):
            async with sem:
                return model, await self._call_provider_async(req, db, run_id)

        review_results = await asyncio.gather(*[call_review(m, r) for m, r in review_tasks])

        peer_reviews = []
        for (model, request), result in zip(review_tasks, review_results):
            success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = result
            cand_id = new_candidate_id()
            if success:
                insert_candidate(db, run_id, cand_id, model["alias"], model["provider"],
                                 model["provider_model"], "peer_review", "succeeded", utc_now_iso())
                update_candidate_result(db, cand_id, "succeeded", raw_answer=raw_text,
                                        latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok)
                emit_candidate_completed(db, run_id, cand_id, model["alias"], "peer_review")
                cursor = db.execute("SELECT * FROM run_candidates WHERE candidate_id=?", (cand_id,))
                peer_reviews.append(dict(cursor.fetchone()))
            else:
                insert_candidate(db, run_id, cand_id, model["alias"], model["provider"],
                                 model["provider_model"], "peer_review", "failed", utc_now_iso())
                update_candidate_result(db, cand_id, "failed", error_code=err_code, error_message=err_msg)

        # Stage 3: debate (conditionally)
        # Deadline check — skip debate if under deadline pressure
        degradation = self._check_deadline(run)
        if degradation and "skip_debate" in (degradation or ""):
            debate_triggered = False
        else:
            debate_cands = []
            agreement = compute_pairwise_agreement(succeeded_opinions)
            debate_triggered = agreement < 0.55

        if debate_triggered:
            emit_stage_started(db, run_id, "debate", [])
            debate_prompt = build_debate_prompt(run["prompt"], succeeded_opinions)
            debate_model = models[0]
            request = ProviderGenerateRequest(
                alias=debate_model["alias"], provider=debate_model["provider"],
                provider_model=debate_model["provider_model"],
                system_prompt=None, user_prompt=debate_prompt,
                max_output_tokens=run["max_output_tokens"], temperature=0.2,
            )
            cand_id = new_candidate_id()
            success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = await self._call_provider_async(request, db, run_id)
            if success:
                insert_candidate(db, run_id, cand_id, debate_model["alias"], debate_model["provider"],
                                 debate_model["provider_model"], "debate", "succeeded", utc_now_iso())
                update_candidate_result(db, cand_id, "succeeded", raw_answer=raw_text,
                                        latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok)
                emit_candidate_completed(db, run_id, cand_id, debate_model["alias"], "debate")
                cursor = db.execute("SELECT * FROM run_candidates WHERE candidate_id=?", (cand_id,))
                debate_cands.append(dict(cursor.fetchone()))
            else:
                insert_candidate(db, run_id, cand_id, debate_model["alias"], debate_model["provider"],
                                 debate_model["provider_model"], "debate", "failed", utc_now_iso())
                update_candidate_result(db, cand_id, "failed", error_code=err_code, error_message=err_msg)

        # Stage 4: synthesis
        emit_stage_started(db, run_id, "synthesis", [])
        synth_prompt = build_council_synthesis_prompt(run["prompt"], succeeded_opinions, peer_reviews, debate_cands if debate_cands else None)
        synth_model = models[0]
        request = ProviderGenerateRequest(
            alias=synth_model["alias"], provider=synth_model["provider"],
            provider_model=synth_model["provider_model"],
            system_prompt=None, user_prompt=synth_prompt,
            max_output_tokens=run["max_output_tokens"], temperature=0.2,
        )
        cand_id = new_candidate_id()
        success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = await self._call_provider_async(request, db, run_id)
        if success:
            insert_candidate(db, run_id, cand_id, synth_model["alias"], synth_model["provider"],
                             synth_model["provider_model"], "synthesis", "succeeded", utc_now_iso())
            update_candidate_result(db, cand_id, "succeeded", raw_answer=raw_text,
                                    latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok)
            emit_candidate_completed(db, run_id, cand_id, synth_model["alias"], "synthesis")
            synthesis_text = raw_text
        else:
            best = select_best_candidate(succeeded_opinions)
            synthesis_text = best.get("raw_answer", "") if best else "Council synthesis failed."

        # Stage 5: verification
        # Deadline check — skip verification if deadline imminent
        degradation = self._check_deadline(run)
        if degradation:
            await self._finalize_degraded(db, run_id, "council", degradation, synthesis_text, confidence=0.5)
            return

        emit_stage_started(db, run_id, "verification", [])
        verif_prompt = build_verification_prompt(run["prompt"], synthesis_text)
        verif_model = models[1] if len(models) > 1 else models[0]
        request = ProviderGenerateRequest(
            alias=verif_model["alias"], provider=verif_model["provider"],
            provider_model=verif_model["provider_model"],
            system_prompt=None, user_prompt=verif_prompt,
            max_output_tokens=500, temperature=0.1,
        )
        cand_id = new_candidate_id()
        confidence = 0.5
        success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = await self._call_provider_async(request, db, run_id)
        if success:
            insert_candidate(db, run_id, cand_id, verif_model["alias"], verif_model["provider"],
                             verif_model["provider_model"], "verification", "succeeded", utc_now_iso())
            update_candidate_result(db, cand_id, "succeeded", raw_answer=raw_text,
                                    latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok)
            try:
                v_data = json.loads(raw_text)
                confidence = v_data.get("confidence", 0.5)
                if v_data.get("verdict") == "abstain":
                    synthesis_text = f"[INSUFFICIENT EVIDENCE — confidence: {confidence}]\n{synthesis_text}"
            except Exception:
                pass

        db.execute("UPDATE runs SET status='succeeded', finished_at=?, final_answer=?, final_confidence=? WHERE run_id=?",
                   (utc_now_iso(), synthesis_text, confidence, run_id))
        db.commit()
        emit_run_completed(db, run_id, synthesis_text, confidence=confidence)
        update_run_status(db, run_id, "succeeded", final_answer=synthesis_text, final_confidence=confidence)

    async def _fail_run(self, db: sqlite3.Connection, run_id: str, error_code: str, error_message: str) -> None:
        db.execute("UPDATE runs SET status='failed', error_code=?, error_message=?, finished_at=? WHERE run_id=?",
                   (error_code, error_message, utc_now_iso(), run_id))
        db.commit()
        emit_run_failed(db, run_id, error_code, error_message)
        update_run_status(db, run_id, "failed", error_code=error_code, error_message=error_message)

    async def _execute_run(self, run: dict) -> None:
        """Main entry point for executing a single run."""
        db = self._get_db()
        run_id = run["run_id"]
        mode = run["mode"]

        logger.info(f"Worker claiming run {run_id}", run_id=run_id)
        update_run_status(db, run_id, "running", started_at=utc_now_iso(), last_heartbeat_at=utc_now_iso())
        emit_run_started(db, run_id, mode)

        try:
            if mode == "single":
                await self._run_single(db, run)
            elif mode == "fusion":
                await self._run_fusion(db, run)
            elif mode == "council":
                await self._run_council(db, run)
            else:
                await self._fail_run(db, run_id, "INVALID_MODE", f"Unknown mode: {mode}")
        except Exception as e:
            logger.error(f"Worker exception in run {run_id}: {e}", run_id=run_id)
            await self._fail_run(db, run_id, "WORKER_EXCEPTION", str(e))

    async def _heartbeat_loop(self, run_id: str, stage: str) -> None:
        """Background heartbeat emitter while a run is active."""
        db = self._get_db()
        while self._running:
            await asyncio.sleep(self._heartbeat_interval_s)
            try:
                self._update_heartbeat(run_id)
                emit_heartbeat(db, run_id, stage)
            except Exception as e:
                logger.warning(f"Heartbeat error: {e}", run_id=run_id)

    async def run_async(self) -> None:
        """Main worker loop."""
        logger.info("Worker loop starting")
        self._running = True

        while self._running:
            db = self._get_db()
            try:
                # Try to claim a run
                run = claim_next_run(db)
                if run:
                    asyncio.create_task(self._execute_run(run))
                else:
                    await asyncio.sleep(self._poll_interval_s)
            except Exception as e:
                logger.error(f"Worker poll error: {e}")
                await asyncio.sleep(5)

    def run(self) -> None:
        """Synchronous entry point."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.run_async())
        finally:
            loop.close()

    def stop(self) -> None:
        self._running = False
        logger.info("Worker loop stopping")