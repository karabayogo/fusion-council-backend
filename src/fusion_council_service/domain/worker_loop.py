"""Fusion Council worker — polls DB, claims runs, executes model orchestration."""

import asyncio
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from pydantic import BaseModel

from fusion_council_service.clock import utc_now_iso
from fusion_council_service.db import new_session, initialize_schema, execute_sql, execute_sql_all, commit_tx
from fusion_council_service.domain.budget import compute_budget, should_degrade, select_models_for_mode
from fusion_council_service.domain.candidate_repository import get_candidate, insert_candidate, update_candidate_result
from fusion_council_service.domain.decision_log import get_memory_context, log_pending_decision
from fusion_council_service.domain.event_emitter import (
    emit_candidate_completed, emit_candidate_failed, emit_fallback_promoted,
    emit_heartbeat, emit_run_completed, emit_run_failed, emit_run_started, emit_run_succeeded_degraded, emit_stage_started,
)
from fusion_council_service.domain.model_selection import select_healthy_stage_model, update_health_for_candidate
from fusion_council_service.domain.orchestration import (
    LangGraphEngine,
    LegacyEngine,
    OrchestrationEngineRouter,
    parse_langgraph_modes,
)
from fusion_council_service.domain.run_repository import claim_next_run, reset_stale_running_runs, update_run_status
from fusion_council_service.domain.scoring import (
    build_council_synthesis_prompt, build_debate_prompt, build_fusion_prompt,
    build_peer_review_prompt, build_verification_prompt, compute_pairwise_agreement,
    select_best_candidate,
)
from fusion_council_service.domain.structured_output import invoke_structured_or_freetext
from fusion_council_service.domain.timeout_result import build_timeout_result  # W4: timeout result dedup
from fusion_council_service.domain.types import ProviderGenerateRequest, ProviderGenerateResult
from fusion_council_service.ids import new_candidate_id
from fusion_council_service.logging_utils import get_logger
from fusion_council_service.model_catalog import ModelCatalog
from fusion_council_service.providers.registry import ProviderRegistry

# LangGraph-aware stale run recovery (scans run_orchestration_state, not runs.status).
# Lazy-imported inside _recover_langgraph_stale_runs() to avoid import-time asyncpg dependency.
logger = get_logger("fusion_council_service.worker_loop")

def _apply_verification_result(
    *,
    db: object,
    cand_id: str,
    candidate: dict,
    raw_text: str,
    synthesis_text: str,
    current_confidence: float,
    verif_alias: str,
    run_id: str,
) -> tuple[float, str]:
    """Apply a verification candidate result with the E2 short-output guard.

    Returns (final_confidence, final_synthesis_text). Both council and fusion
    paths funnel through here so the guard is consistent — never accept a
    <MIN_VERIFICATION_TOKENS output as a verdict, never let a non-answer
    depress the synthesis. If the output is suspiciously short, we mark the
    candidate with error_code=VERIFICATION_TOO_SHORT, pin confidence=0.5, and
    prepend [INSUFFICIENT EVIDENCE] to the synthesis. Otherwise we honor
    the parsed verdict (abstain still prefixes the synthesis).

    This is the E2 fix factored out of the inline council code so fusion
    mode gets the same protection (PR #26 only patched the council path).
    """
    verif_out_tokens = int(candidate.get("output_tokens") or 0)
    if verif_out_tokens < MIN_VERIFICATION_TOKENS:
        logger.warning(
            f"verification output too short ({verif_out_tokens} tokens < {MIN_VERIFICATION_TOKENS}); "
            f"rejecting verdict from {verif_alias}, keeping confidence=0.5",
            run_id=run_id,
        )
        execute_sql(
            db,
            "UPDATE run_candidates SET error_code=:ec, error_message=:em WHERE candidate_id=:cid",
            {
                "ec": "VERIFICATION_TOO_SHORT",
                "em": f"output_tokens={verif_out_tokens} < MIN_VERIFICATION_TOKENS={MIN_VERIFICATION_TOKENS}",
                "cid": cand_id,
            },
        )
        commit_tx(db)
        return (
            0.5,
            f"[INSUFFICIENT EVIDENCE — confidence: 0.5] (verification rejected: {verif_out_tokens} tokens)\n{synthesis_text}",
        )
    try:
        parsed = _VerificationPayload.model_validate_json(raw_text)
        confidence = float(parsed.confidence)
        if parsed.verdict.strip().lower() == "abstain":
            return (
                confidence,
                f"[INSUFFICIENT EVIDENCE — confidence: {confidence}]\n{synthesis_text}",
            )
        return (confidence, synthesis_text)
    except Exception:
        return (current_confidence, synthesis_text)


# Thread pool for blocking provider calls
_executor = ThreadPoolExecutor(max_workers=10)


# Last-resort floor for the provider-call wall-clock timeout. Per the
# systematic-debugging skill "MiniMax Thinking Model" entry, thinking
# models on long chains can legitimately take 5-10 minutes. The 600s
# default matches the per-model `timeout_seconds: 600` we set for every
# frontier thinking model in config/models.yaml. A hardcoded 300 here
# was the root cause of run_c908a00b1c834b8eb9ebe2b4 M2.7 debate
# `PROVIDER_TIMEOUT` (2026-06-01) — many call sites in this file
# constructed ProviderGenerateRequest without timeout_seconds, and the
# 300s default in _call_provider_async silently overrode the catalog
# 600s. Never lower this without auditing every call site.
DEFAULT_PROVIDER_TIMEOUT_SECONDS = 600

# Minimum acceptable verification output (in tokens). Below this we treat
# the verification as a non-answer (model returned empty/garbage/structured-
# output failure) and fall back to confidence=0.5 + INSUFFICIENT_EVIDENCE
# prefix instead of accepting a low-confidence verdict. Root cause: run
# c908a00b1c834b8eb9ebe2b4 (2026-06-01) — kimi-k2.6 returned 19 tokens in
# 2s as "verification" candidate, model_validate_json succeeded with a
# 0.45 confidence verdict, and the run was marked succeeded with that
# low-confidence synthesis. We must catch empty/short structured outputs
# at the verification stage and never let a non-answer depress confidence
# silently. The 50-token floor is well below any legitimate verification
# verdict (which is always 200+ tokens of justification) but well above
# a structured-output-failure stub.
MIN_VERIFICATION_TOKENS = 50


def _resolve_effective_timeout(
    request: ProviderGenerateRequest,
    caller_timeout: Optional[int] = None,
) -> int:
    """Resolve the effective wall-clock timeout for a provider call.

    Precedence (highest first):
      1. caller_timeout (explicit override from call site)
      2. request.timeout_seconds (per-model catalog value)
      3. DEFAULT_PROVIDER_TIMEOUT_SECONDS (last-resort floor)

    The previous code used `request.timeout_seconds if request.timeout_seconds else 300`
    which silently fell back to 300s when the request didn't carry the field
    — exactly the E1 bug. Use this helper from all call sites so the
    floor is consistent and visible.
    """
    if caller_timeout is not None and caller_timeout > 0:
        return caller_timeout
    if request.timeout_seconds is not None and request.timeout_seconds > 0:
        return request.timeout_seconds
    return DEFAULT_PROVIDER_TIMEOUT_SECONDS


def build_provider_request(
    model_entry: dict,
    *,
    user_prompt: str,
    system_prompt: Optional[str] = None,
    max_output_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
    extra_overrides: Optional[dict] = None,
) -> ProviderGenerateRequest:
    """Build a ProviderGenerateRequest from a catalog entry with timeout_seconds populated.

    This is the ONLY canonical way to construct a request in the worker.
    Using it from every call site makes the E1 bug impossible to reintroduce:
    every request automatically carries `timeout_seconds` from the catalog.

    Args:
        model_entry: Catalog row (must have alias, provider, provider_model,
            and optionally timeout_seconds).
        user_prompt: The user-facing prompt.
        system_prompt: Optional system prompt.
        max_output_tokens: Caller's desired max tokens (defaults to 4096
            if None — matches the historical inline default).
        temperature: Defaults to 0.2 (run default).
        extra_overrides: For callers that need to override individual fields
            beyond timeout_seconds (rare — prefer extending the signature).
    """
    fields = {
        "alias": model_entry["alias"],
        "provider": model_entry["provider"],
        "provider_model": model_entry["provider_model"],
        "user_prompt": user_prompt,
        "system_prompt": system_prompt,
        "max_output_tokens": max_output_tokens if max_output_tokens is not None else 4096,
        "temperature": temperature if temperature is not None else 0.2,
        "timeout_seconds": model_entry.get("timeout_seconds"),
    }
    if extra_overrides:
        fields.update(extra_overrides)
    return ProviderGenerateRequest(**fields)


class _VerificationPayload(BaseModel):
    verdict: str
    confidence: float


def _run_provider_sync(
    registry: ProviderRegistry,
    request,
) -> tuple:
    """Run a provider call in a thread (sync wrapper)."""
    result = registry.generate(request)
    return (result.success, result.raw_text, result.error_code, result.error_message,
            result.latency_ms, result.input_tokens, result.output_tokens)


def _safe_log_pending_decision(db, run: dict, mode: str, final_answer: str) -> None:
    """Best-effort decision logging; never break successful run completion."""
    try:
        log_pending_decision(db, run["run_id"], run["prompt"], mode, final_answer)
    except Exception as exc:
        logger.warning(f"decision_log write skipped: {exc}", run_id=run.get("run_id"))


class Worker:
    """Background worker that polls for and executes runs."""

    def __init__(
        self,
        db_path: str = "",
        db_url: str = "",
        registry: ProviderRegistry = None,
        catalog: ModelCatalog = None,
        poll_interval_ms: int = 1000,
        heartbeat_interval_ms: int = 5000,
        stale_run_threshold_seconds: int = 30,
        orchestrator_engine: str = "legacy",
        orchestrator_langgraph_modes: str = "",
        langgraph_thread_namespace: str = "fusion-council",
        langgraph_engine_version: str = "v1",
    ):
        self._db_path = db_path
        self._db_url = db_url
        self._registry = registry
        self._catalog = catalog
        self._poll_interval_s = poll_interval_ms / 1000.0
        self._heartbeat_interval_s = heartbeat_interval_ms / 1000.0
        self._stale_run_threshold_seconds = stale_run_threshold_seconds
        self._running = False
        self._db = None
        self._current_run_task: Optional[asyncio.Task] = None
        self._worker_id = f"worker-{int(time.time())}"
        self._router = OrchestrationEngineRouter(
            orchestrator_engine=orchestrator_engine,
            langgraph_modes=parse_langgraph_modes(orchestrator_langgraph_modes),
            legacy_engine=LegacyEngine(),
            langgraph_engine=LangGraphEngine(
                thread_namespace=langgraph_thread_namespace,
                engine_version=langgraph_engine_version,
            ),
        )

    @property
    def catalog(self) -> ModelCatalog:
        """Public accessor for the model catalog.

        Required so LangGraph orchestration nodes can access the catalog via
        ``worker.catalog`` (they cannot access the private ``_catalog``).
        Without this property, any council/fusion run through LangGraph crashes
        with ``AttributeError: 'Worker' object has no attribute 'catalog'``
        (root cause of PEER_CATALOG_ERROR in run_14898b9d836340a2a6c50bf0).
        """
        return self._catalog

    def _get_db(self):
        if self._db is None:
            self._db = new_session()
            initialize_schema(self._db)
        return self._db

    def _reset_db(self) -> None:
        """Close and reset the cached database connection.

        Called when a connection error poisons the
        connection state. A fresh connection will be opened on the next _get_db call.
        """
        if self._db is not None:
            try:
                self._db.close()
            except Exception:
                pass
            self._db = None

    def _recover_stale_runs(self) -> None:
        """Reset any runs stuck in 'running' status past the stale threshold."""
        db = self._get_db()
        recovered = reset_stale_running_runs(db, self._stale_run_threshold_seconds)
        if recovered > 0:
            logger.info(f"Recovered {recovered} stale run(s) stuck in 'running' status")

    async def _recover_langgraph_stale_runs(self) -> None:
        """
        LangGraph-aware stale run recovery — scans run_orchestration_state for
        runs stuck in orchestration_status='resumed' past the stale threshold.

        This is the LangGraph equivalent of _recover_stale_runs(). The legacy
        method only touches runs.status; this one recovers orphans in the
        LangGraph checkpoint layer.

        Called on worker idle cycles (alongside legacy recovery) and on startup
        after the AsyncPostgresSaver is confirmed available.
        """
        try:
            from fusion_council_service.startup import _recover_stale_runs as startup_recover
            # Settings() requires API keys — construct with minimal env for DB URL only
            from fusion_council_service.config import Settings
            settings = Settings(
                SERVICE_API_KEYS="",
                SERVICE_ADMIN_API_KEYS="",
            )
            recovered = await startup_recover(settings)
            if recovered > 0:
                logger.info(
                    f"LangGraph recovery: {recovered} stale run(s) re-queued from "
                    f"run_orchestration_state"
                )
        except Exception as exc:
            logger.warning(f"LangGraph stale run recovery skipped: {exc}")

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

    async def _finalize_degraded(self, db: object, run_id: str, mode: str,
                                   reason: str, best_text: str, confidence: float = 0.5) -> None:
        """Finalize a run as succeeded_degraded due to deadline pressure."""
        logger.info(f"Finalizing as succeeded_degraded: {reason}", run_id=run_id)
        update_run_status(
            db,
            run_id,
            "succeeded_degraded",
            final_answer=best_text,
            final_confidence=confidence,
            degraded_reason=reason,
        )
        emit_run_succeeded_degraded(db, run_id, best_text, reason, confidence=confidence)

    def _attempted_model_identities(self, db: object, run_id: str) -> tuple[set[str], set[tuple[str, str]]]:
        """Return aliases and upstream provider/model pairs already attempted for a run."""
        rows = execute_sql_all(
            db,
            """
            SELECT alias, provider, provider_model
            FROM run_candidates
            WHERE run_id = :run_id
            """,
            {"run_id": run_id},
        )
        aliases: set[str] = set()
        upstream_pairs: set[tuple[str, str]] = set()
        for row in rows:
            alias = row.get("alias")
            provider = row.get("provider")
            provider_model = row.get("provider_model")
            if alias:
                aliases.add(alias)
            if provider and provider_model:
                upstream_pairs.add((provider, provider_model))
        return aliases, upstream_pairs

    def _failed_model_identities(self, db: object, run_id: str) -> tuple[set[str], set[tuple[str, str]]]:
        """Return aliases and upstream provider/model pairs that failed for a run."""
        rows = execute_sql_all(
            db,
            """
            SELECT alias, provider, provider_model
            FROM run_candidates
            WHERE run_id = :run_id AND status = 'failed'
            """,
            {"run_id": run_id},
        )
        aliases: set[str] = set()
        upstream_pairs: set[tuple[str, str]] = set()
        for row in rows:
            alias = row.get("alias")
            provider = row.get("provider")
            provider_model = row.get("provider_model")
            if alias:
                aliases.add(alias)
            if provider and provider_model:
                upstream_pairs.add((provider, provider_model))
        return aliases, upstream_pairs

    def _failed_providers(self, db: object, run_id: str) -> set[str]:
        """Return providers where ALL their models have failed for this run.

        When a provider returns a systemic error (e.g. 401 Unauthorized, 503),
        every model on that provider will fail. Rather than trying each model
        one-by-one and burning deadline budget, detect providers that are
        completely down and skip them entirely in fallback selection.

        A provider is considered failed when every enabled model in the catalog
        for that provider has a failed candidate for this run.
        """
        # Get all enabled models grouped by provider
        provider_models: dict[str, list[dict]] = {}
        for model in self._catalog.enabled_models():
            provider = model.get("provider", "")
            if provider:
                provider_models.setdefault(provider, []).append(model)

        # Get failed candidates for this run
        failed_aliases, failed_pairs = self._failed_model_identities(db, run_id)
        failed_providers: set[str] = set()

        for provider, models in provider_models.items():
            if not models:
                continue
            # Check if ALL models for this provider have failed
            all_failed = all(
                m["alias"] in failed_aliases or (provider, m.get("provider_model", "")) in failed_pairs
                for m in models
            )
            if all_failed:
                failed_providers.add(provider)

        return failed_providers

    def _select_stage_model(
        self,
        db: object,
        run_id: str,
        role_order: list[str],
        *,
        avoid_aliases: Optional[set[str]] = None,
    ) -> Optional[dict]:
        """Select a healthy model for a later council stage."""
        return select_healthy_stage_model(
            db=db,
            catalog=self._catalog,
            run_id=run_id,
            role_order=role_order,
            avoid_aliases=avoid_aliases,
        )

    def _emit_stage_started(self, db: object, run_id: str, stage: str, models: list[str] = None) -> None:
        update_run_status(
            db,
            run_id,
            "running",
            current_stage=stage,
            current_stage_message=f"Running {stage.replace('_', ' ')}",
        )
        emit_stage_started(db, run_id, stage, models or [])

    def _record_stage_candidate_result(
        self,
        db: object,
        run_id: str,
        cand_id: str,
        model: dict,
        stage: str,
        provider_result,
    ) -> Optional[dict]:
        """Persist success or failure for a non-first-opinion stage call."""
        success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = provider_result
        status = "succeeded" if success else "failed"
        insert_candidate(
            db, run_id, cand_id, model["alias"], model["provider"],
            model["provider_model"], stage, status, utc_now_iso(),
        )
        update_health_for_candidate(
            db, model["provider"], model["provider_model"], success, lat_ms,
        )
        if success:
            update_candidate_result(
                db, cand_id, "succeeded", normalized_answer=raw_text,
                latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok,
            )
            emit_candidate_completed(db, run_id, cand_id, model["alias"], stage)
            return get_candidate(db, cand_id) or {}
        update_candidate_result(db, cand_id, "failed", error_code=err_code, error_message=err_msg)
        emit_candidate_failed(db, run_id, cand_id, model["alias"], stage, err_msg or err_code or "provider failed")
        return None

    def _try_fallback(self, db: object, run: dict, failed_alias: str) -> Optional[dict]:
        """Try to promote a fallback model for a failed primary.

        A fallback must be a genuinely new upstream attempt for this run. Reusing
        the same alias or the same (provider, provider_model) pair causes noisy
        duplicate candidates, burns deadline budget, and does not improve quorum
        diversity when an upstream provider/model is unhealthy.

        When an entire provider is down (e.g. 401 on all its models), skip ALL
        models from that provider rather than trying them one-by-one and burning
        the run's deadline budget. See _failed_providers for detection logic.
        """
        mode = run["mode"]
        active_models = select_models_for_mode(mode, self._catalog)
        blocked_aliases = {m["alias"] for m in active_models}
        blocked_pairs = {(m.get("provider", ""), m.get("provider_model", "")) for m in active_models}
        attempted_aliases, attempted_pairs = self._attempted_model_identities(db, run["run_id"])
        blocked_aliases.update(attempted_aliases)
        blocked_pairs.update(attempted_pairs)
        blocked_aliases.add(failed_alias)

        # Skip entire providers where ALL models have failed (systemic provider
        # outage like 401/503). Without this, the fallback tries each model on
        # the dead provider one-by-one, burning 5+ minutes per model.
        failed_providers = self._failed_providers(db, run["run_id"])

        for model in self._catalog.enabled_models():
            alias = model["alias"]
            provider = model.get("provider", "")
            pair = (provider, model.get("provider_model", ""))
            if alias in blocked_aliases or pair in blocked_pairs:
                continue
            if provider in failed_providers:
                logger.info(
                    f"Skipping fallback {alias}: provider {provider} is fully down "
                    f"(all {len([m for m in self._catalog.enabled_models() if m.get('provider') == provider])} models failed)",
                    run_id=run["run_id"],
                )
                continue
            emit_fallback_promoted(db, run["run_id"], alias, failed_alias)
            logger.info(f"Fallback promoted: {alias} replacing {failed_alias}", run_id=run["run_id"])
            return model
        logger.warning(
            f"No unused fallback available for {failed_alias}; all aliases/upstream pairs already attempted",
            run_id=run["run_id"],
        )
        return None

    async def _call_provider_async(self, request, db: object, run_id: str,
                                    timeout_seconds: Optional[int] = None):
        """Call provider in thread pool and return result.

        Wraps the blocking call with a timeout. If the provider call exceeds
        the effective timeout, returns a failed ProviderGenerateResult with
        error_code='PROVIDER_TIMEOUT'.

        Non-timeout failures (HTTP 4xx/5xx, auth errors, network errors) are
        classified by the provider and returned with their real error code
        (e.g. HTTP_500, AUTH_FAILED). The "Provider call timed out" log message
        only appears for actual asyncio.TimeoutError — not for HTTP errors.

        Timeout precedence (see _resolve_effective_timeout):
          1. explicit timeout_seconds kwarg (caller override)
          2. request.timeout_seconds (per-model catalog value)
          3. DEFAULT_PROVIDER_TIMEOUT_SECONDS (last-resort floor, 600s)

        The previous hardcoded `timeout_seconds: int = 300` default silently
        overrode the catalog's 600s for any call site that constructed a
        ProviderGenerateRequest without `timeout_seconds` set — the E1 root
        cause for run_c908a00b1c834b8eb9ebe2b4. All call sites now go through
        build_provider_request() which always populates the field.
        """
        loop = asyncio.get_event_loop()
        coro = loop.run_in_executor(
            _executor, _run_provider_sync, self._registry, request,
        )
        effective_timeout = _resolve_effective_timeout(request, timeout_seconds)
        try:
            return await asyncio.wait_for(coro, timeout=effective_timeout)
        except asyncio.TimeoutError:
            logger.warning(
                f"Provider call timed out after {effective_timeout}s for {request.alias}",
                run_id=run_id,
            )
            return build_timeout_result(effective_timeout, run_id)  # W4: dedup

    async def _call_structured_provider_async(
        self,
        request: ProviderGenerateRequest,
        response_model: type[BaseModel],
        db: object,
        run_id: str,
        timeout_seconds: Optional[int] = None,
        max_retries: int = 2,
    ) -> ProviderGenerateResult:
        """Call provider with structured-output fallback utility in thread pool."""
        loop = asyncio.get_event_loop()
        effective_timeout = _resolve_effective_timeout(request, timeout_seconds)

        def _invoke() -> ProviderGenerateResult:
            return invoke_structured_or_freetext(
                request=request,
                registry=self._registry,
                response_model=response_model,
                max_retries=max_retries,
            )

        try:
            result = await asyncio.wait_for(loop.run_in_executor(_executor, _invoke), timeout=effective_timeout)
            if isinstance(result, ProviderGenerateResult):
                return result
            if isinstance(result, (tuple, list)) and len(result) == 7:
                return ProviderGenerateResult(
                    success=bool(result[0]),
                    raw_text=result[1],
                    error_code=result[2],
                    error_message=result[3],
                    latency_ms=int(result[4] or 0),
                    input_tokens=result[5],
                    output_tokens=result[6],
                )
            # Compatibility fallback for mock-heavy tests and any provider that
            # returns an unexpected shape under structured invocation.
            logger.warning(
                "Structured provider returned unexpected result type; falling back to legacy provider path",
                run_id=run_id,
                result_type=type(result).__name__,
            )
            legacy = await self._call_provider_async(request, db, run_id, timeout_seconds=timeout_seconds)
            if isinstance(legacy, ProviderGenerateResult):
                return legacy
            if isinstance(legacy, (tuple, list)) and len(legacy) == 7:
                return ProviderGenerateResult(
                    success=bool(legacy[0]),
                    raw_text=legacy[1],
                    error_code=legacy[2],
                    error_message=legacy[3],
                    latency_ms=int(legacy[4] or 0),
                    input_tokens=legacy[5],
                    output_tokens=legacy[6],
                )
            return ProviderGenerateResult(
                success=False,
                raw_text=None,
                error_code="PROVIDER_RESULT_INVALID",
                error_message=f"Unexpected provider result type: {type(legacy).__name__}",
                latency_ms=0,
                input_tokens=None,
                output_tokens=None,
            )
        except asyncio.TimeoutError:
            logger.warning(
                f"Structured provider call timed out after {timeout_seconds}s for {request.alias}",
                run_id=run_id,
            )
            return build_timeout_result(effective_timeout, run_id)  # W4: dedup

    async def _run_single(self, db: object, run: dict) -> None:
        """Execute a single-mode run."""

        run_id = run["run_id"]
        logger.info("Starting single run", run_id=run_id)

        models = select_models_for_mode("single", self._catalog)
        if not models:
            await self._fail_run(db, run_id, "NO_MODELS", "No enabled models for single mode")
            return

        model = models[0]
        emit_stage_started(db, run_id, "generation", [model["alias"]])

        request = build_provider_request(
            model,
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
            update_candidate_result(db, candidate_id, "succeeded", normalized_answer=raw_text,
                                    latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok)
            emit_candidate_completed(db, run_id, candidate_id, model["alias"], "generation")

            # Emit completion
            update_run_status(db, run_id, "succeeded", final_answer=raw_text)
            emit_run_completed(db, run_id, raw_text)
            _safe_log_pending_decision(db, run, "single", raw_text)
        else:
            insert_candidate(db, run_id, candidate_id, model["alias"], model["provider"],
                             model["provider_model"], "generation", "failed", utc_now_iso())
            update_candidate_result(db, candidate_id, "failed", error_code=err_code, error_message=err_msg)
            emit_candidate_failed(db, run_id, candidate_id, model["alias"], "generation", err_msg or err_code)
            # Try fallback for single mode
            fallback = self._try_fallback(db, run, model["alias"])
            if fallback:
                fallback_req = build_provider_request(
                    fallback,
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
                    update_candidate_result(db, fb_candidate_id, "succeeded", normalized_answer=fb_txt,
                                            latency_ms=fb_lat, input_tokens=fb_in, output_tokens=fb_out)
                    emit_candidate_completed(db, run_id, fb_candidate_id, fallback["alias"], "generation")
                    update_run_status(db, run_id, "succeeded", final_answer=fb_txt)
                    emit_run_completed(db, run_id, fb_txt)
                    _safe_log_pending_decision(db, run, "single", fb_txt)
                    return
            await self._fail_run(db, run_id, err_code or "PROVIDER_FAILED", err_msg or "Single model failed")

    async def _run_fusion(self, db: object, run: dict) -> None:
        """Execute a fusion-mode run."""

        run_id = run["run_id"]
        logger.info("Starting fusion run", run_id=run_id)

        models = select_models_for_mode("fusion", self._catalog)
        if len(models) < 2:
            await self._fail_run(db, run_id, "NO_MODELS", "Need at least 2 models for fusion")
            return

        deadline_s = run["deadline_seconds"]
        run_budget = compute_budget("fusion", deadline_s)
        _stage_budgets = {s.stage: s for s in run_budget.stages}

        # Stage 1: generation — all models in parallel
        emit_stage_started(db, run_id, "generation", [m["alias"] for m in models])

        gen_candidates = []
        pending_calls = []
        for model in models:
            request = build_provider_request(
                model,
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
            _m, provider_result = result
            success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = provider_result
            cand_id = new_candidate_id()
            if success:
                insert_candidate(db, run_id, cand_id, model["alias"], model["provider"],
                                 model["provider_model"], "generation", "succeeded", utc_now_iso())
                update_health_for_candidate(
                    db, model["provider"], model["provider_model"], True, float(lat_ms) if lat_ms else None,
                )
                update_candidate_result(db, cand_id, "succeeded", normalized_answer=raw_text,
                                        latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok)
                emit_candidate_completed(db, run_id, cand_id, model["alias"], "generation")
                gen_candidates.append(get_candidate(db, cand_id) or {})
            else:
                insert_candidate(db, run_id, cand_id, model["alias"], model["provider"],
                                 model["provider_model"], "generation", "failed", utc_now_iso())
                update_health_for_candidate(
                    db, model["provider"], model["provider_model"], False, float(lat_ms) if lat_ms else None,
                )
                update_candidate_result(db, cand_id, "failed", error_code=err_code, error_message=err_msg)
                emit_candidate_failed(db, run_id, cand_id, model["alias"], "generation", err_msg or err_code)

        succeeded = [c for c in gen_candidates if c.get("status") == "succeeded"]

        # Deadline check after generation stage
        degradation = self._check_deadline(run)
        if degradation and succeeded:
            best = select_best_candidate(succeeded)
            best_text = best.get("normalized_answer", "") if best else ""
            await self._finalize_degraded(db, run_id, "fusion", degradation, best_text)
            return

        # Fallback promotion: if quorum not met, try fallbacks before failing
        if len(succeeded) < 2:
            failed_aliases = [c.get("alias", "") for c in gen_candidates if c.get("status") == "failed"]
            for failed_alias in failed_aliases:
                fallback = self._try_fallback(db, run, failed_alias)
                if fallback:
                    fallback_req = build_provider_request(
                        fallback,
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
                        update_health_for_candidate(
                            db, fallback["provider"], fallback["provider_model"], True, float(fb_lat) if fb_lat else None,
                        )
                        update_candidate_result(db, cand_id, "succeeded", normalized_answer=fb_text,
                                                latency_ms=fb_lat, input_tokens=fb_in, output_tokens=fb_out)
                        emit_candidate_completed(db, run_id, cand_id, fallback["alias"], "generation")
                        succeeded.append(get_candidate(db, cand_id) or {})
                    if len(succeeded) >= 2:
                        break

        if len(succeeded) < 2:
            # Quorum not met even after fallbacks
            update_run_status(db, run_id, "failed", error_code="FUSION_QUORUM_NOT_MET")
            emit_run_failed(db, run_id, "FUSION_QUORUM_NOT_MET", f"Only {len(succeeded)}/3 models succeeded")
            return

        # Stage 2: synthesis
        # Deadline check before synthesis
        degradation = self._check_deadline(run)
        if degradation:
            best = select_best_candidate(succeeded)
            best_text = best.get("normalized_answer", "") if best else ""
            await self._finalize_degraded(db, run_id, "fusion", degradation, best_text)
            return

        self._emit_stage_started(db, run_id, "synthesis", [])
        synth_models = select_models_for_mode("fusion", self._catalog)
        synth_model = synth_models[0] if synth_models else models[0]
        memory_context = get_memory_context(db, run["prompt"], "fusion", n_same=3, n_cross=2)
        synthesis_prompt = build_fusion_prompt(run["prompt"], succeeded, memory_context=memory_context)
        request = build_provider_request(
            synth_model,
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
            update_health_for_candidate(
                db, synth_model["provider"], synth_model["provider_model"], True, float(lat_ms) if lat_ms else None,
            )
            update_candidate_result(db, cand_id, "succeeded", normalized_answer=raw_text,
                                    latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok)
            emit_candidate_completed(db, run_id, cand_id, synth_model["alias"], "synthesis")
            synthesis_text = raw_text
        else:
            insert_candidate(db, run_id, cand_id, synth_model["alias"], synth_model["provider"],
                             synth_model["provider_model"], "synthesis", "failed", utc_now_iso())
            update_health_for_candidate(
                db, synth_model["provider"], synth_model["provider_model"], False, float(lat_ms) if lat_ms else None,
            )
            update_candidate_result(db, cand_id, "failed", error_code=err_code, error_message=err_msg)
            synthesis_text = succeeded[0].get("normalized_answer", "") if succeeded else "No answer available."

        # Stage 3: verification
        # Deadline check — skip verification if under deadline pressure
        degradation = self._check_deadline(run)
        if degradation:
            # Deadline imminent — finalize with synthesis answer as succeeded_degraded
            await self._finalize_degraded(db, run_id, "fusion", degradation, synthesis_text, confidence=0.5)
            return

        self._emit_stage_started(db, run_id, "verification", [])
        verification_prompt = build_verification_prompt(run["prompt"], synthesis_text)
        verif_models = select_models_for_mode("fusion", self._catalog)
        verif_model = verif_models[1] if len(verif_models) > 1 else models[0]
        request = build_provider_request(
            verif_model,
            system_prompt=None,
            user_prompt=verification_prompt,
            max_output_tokens=500,
            temperature=0.1,
        )
        cand_id = new_candidate_id()
        confidence = 0.5
        final_answer = synthesis_text
        provider_result = await self._call_structured_provider_async(
            request,
            _VerificationPayload,
            db,
            run_id,
        )
        candidate = self._record_stage_candidate_result(
            db, run_id, cand_id, verif_model, "verification", provider_result
        )
        if candidate:
            raw_text = candidate.get("normalized_answer", "")
            confidence, final_answer = _apply_verification_result(
                db=db,
                cand_id=cand_id,
                candidate=candidate,
                raw_text=raw_text,
                synthesis_text=synthesis_text,
                current_confidence=confidence,
                verif_alias=verif_model["alias"],
                run_id=run_id,
            )

        update_run_status(
            db,
            run_id,
            "succeeded",
            final_answer=final_answer,
            final_confidence=confidence,
        )
        emit_run_completed(db, run_id, final_answer, confidence=confidence)
        _safe_log_pending_decision(db, run, "fusion", final_answer)

    async def _run_council(self, db: object, run: dict) -> None:
        """Execute a council-mode run."""

        run_id = run["run_id"]
        logger.info("Starting council run", run_id=run_id)

        models = select_models_for_mode("council", self._catalog)
        if len(models) < 3:
            await self._fail_run(db, run_id, "NO_MODELS", "Need at least 3 models for council")
            return

        deadline_s = run["deadline_seconds"]

        # Stage 1: first opinions (all models in parallel)
        self._emit_stage_started(db, run_id, "first_opinion", [m["alias"] for m in models])

        sem = asyncio.Semaphore(3)
        async def call_model(model):
            async with sem:
                request = build_provider_request(
                    model,
                    system_prompt=run.get("system_prompt"),
                    user_prompt=run["prompt"],
                    max_output_tokens=run["max_output_tokens"],
                    temperature=run["temperature"],
                )
                return model, await self._call_provider_async(request, db, run_id)

        first_results = await asyncio.gather(*[call_model(m) for m in models])

        first_opinions = []
        for (model, request), result in zip([(m, None) for m in models], first_results):
            _m, provider_result = result
            success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = provider_result
            cand_id = new_candidate_id()
            m = model
            if success:
                insert_candidate(db, run_id, cand_id, m["alias"], m["provider"],
                                 m["provider_model"], "first_opinion", "succeeded", utc_now_iso())
                update_health_for_candidate(
                    db, m["provider"], m["provider_model"], True, float(lat_ms) if lat_ms else None,
                )
                update_candidate_result(db, cand_id, "succeeded", normalized_answer=raw_text,
                                        latency_ms=lat_ms, input_tokens=in_tok, output_tokens=out_tok)
                emit_candidate_completed(db, run_id, cand_id, m["alias"], "first_opinion")
                first_opinions.append(get_candidate(db, cand_id) or {})
            else:
                insert_candidate(db, run_id, cand_id, m["alias"], m["provider"],
                                 m["provider_model"], "first_opinion", "failed", utc_now_iso())
                update_health_for_candidate(
                    db, m["provider"], m["provider_model"], False, float(lat_ms) if lat_ms else None,
                )
                update_candidate_result(db, cand_id, "failed", error_code=err_code, error_message=err_msg)
                emit_candidate_failed(db, run_id, cand_id, m["alias"], "first_opinion", err_msg or err_code)
                first_opinions.append(get_candidate(db, cand_id) or {})

        succeeded_opinions = [c for c in first_opinions if c.get("status") == "succeeded"]

        # Deadline check after first opinions
        degradation = self._check_deadline(run)
        if degradation and succeeded_opinions:
            best = select_best_candidate(succeeded_opinions)
            best_text = best.get("normalized_answer", "") if best else ""
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
                        update_health_for_candidate(
                            db, fallback["provider"], fallback["provider_model"], True, float(fb_lat) if fb_lat else None,
                        )
                        update_candidate_result(db, cand_id, "succeeded", normalized_answer=fb_txt,
                                                latency_ms=fb_lat, input_tokens=fb_in, output_tokens=fb_out)
                        emit_candidate_completed(db, run_id, cand_id, fallback["alias"], "first_opinion")
                        succeeded_opinions.append(get_candidate(db, cand_id) or {})
                    if len(succeeded_opinions) >= 2:
                        break

        if len(succeeded_opinions) < 2:
            update_run_status(db, run_id, "failed", error_code="COUNCIL_QUORUM_NOT_MET")
            emit_run_failed(db, run_id, "COUNCIL_QUORUM_NOT_MET", f"Only {len(succeeded_opinions)}/3 opinions succeeded")
            return

        # Deadline check — skip peer review if under heavy pressure
        degradation = self._check_deadline(run)
        if degradation and "skip_peer" in (degradation or ""):
            # Skip peer reviews and debate, go straight to synthesis
            synth_model = self._select_stage_model(
                db, run_id, ["synthesis", "backup", "reviewer", "primary", "creative", "verification"]
            )
            self._emit_stage_started(db, run_id, "synthesis", [synth_model["alias"]] if synth_model else [])
            memory_context = get_memory_context(db, run["prompt"], "council", n_same=3, n_cross=2)
            synth_prompt = build_council_synthesis_prompt(run["prompt"], succeeded_opinions, [], None, memory_context=memory_context)
            if synth_model is None:
                best = select_best_candidate(succeeded_opinions)
                synthesis_text = best.get("normalized_answer", "") if best else "Council synthesis failed."
            else:
                request = build_provider_request(
                    synth_model,
                    system_prompt=None,
                    user_prompt=synth_prompt,
                    max_output_tokens=run["max_output_tokens"],
                    temperature=0.2,
                )
                cand_id = new_candidate_id()
                provider_result = await self._call_provider_async(request, db, run_id)
                candidate = self._record_stage_candidate_result(db, run_id, cand_id, synth_model, "synthesis", provider_result)
                if candidate:
                    synthesis_text = candidate.get("normalized_answer", "")
                else:
                    best = select_best_candidate(succeeded_opinions)
                    synthesis_text = best.get("normalized_answer", "") if best else "Council synthesis failed."
            await self._finalize_degraded(db, run_id, "council", degradation, synthesis_text)
            return

        # Stage 2: peer reviews
# Stage 2: peer reviews
        # Build task list first so we know selected models before emitting stage.started
        review_tasks = []
        for opinion_cand in succeeded_opinions:
            reviewer = self._select_stage_model(
                db,
                run_id,
                ["reviewer", "backup", "verification", "synthesis", "primary", "creative"],
                avoid_aliases={opinion_cand.get("alias", "")},
            )
            if reviewer is None:
                logger.warning("No healthy peer-review model available; skipping review", run_id=run_id)
                continue
            review_prompt = build_peer_review_prompt(run["prompt"], opinion_cand.get("normalized_answer", ""), reviewer["alias"])
            request = build_provider_request(
                reviewer,
                system_prompt=None,
                user_prompt=review_prompt,
                max_output_tokens=run["max_output_tokens"],
                temperature=0.1,
            )
            review_tasks.append((reviewer, request))

        selected_reviewers = [m["alias"] for m, _ in review_tasks] if review_tasks else []
        self._emit_stage_started(db, run_id, "peer_review", selected_reviewers)

        async def call_review(model, req):
            async with sem:
                return model, await self._call_provider_async(req, db, run_id)

        review_results = await asyncio.gather(*[call_review(m, r) for m, r in review_tasks]) if review_tasks else []

        peer_reviews = []
        for (model, request), result in zip(review_tasks, review_results):
            _m, provider_result = result
            cand_id = new_candidate_id()
            candidate = self._record_stage_candidate_result(db, run_id, cand_id, model, "peer_review", provider_result)
            if candidate:
                peer_reviews.append(candidate)

        # Stage 3: debate (conditionally)
        # Deadline check — skip debate if under deadline pressure
        debate_cands = []
        degradation = self._check_deadline(run)
        if degradation and "skip_debate" in (degradation or ""):
            debate_triggered = False
        else:
            debate_cands = []
            agreement = compute_pairwise_agreement(succeeded_opinions)
            debate_triggered = agreement < 0.55

        if debate_triggered:
            self._emit_stage_started(db, run_id, "debate", [])
            debate_prompt = build_debate_prompt(run["prompt"], succeeded_opinions)
            debate_model = self._select_stage_model(
                db, run_id, ["creative", "backup", "reviewer", "primary", "synthesis", "verification"]
            )
            if debate_model is None:
                logger.warning("No healthy debate model available; skipping debate", run_id=run_id)
            else:
                request = build_provider_request(
                    debate_model,
                    system_prompt=None,
                    user_prompt=debate_prompt,
                    max_output_tokens=run["max_output_tokens"],
                    temperature=0.2,
                )
                cand_id = new_candidate_id()
                provider_result = await self._call_provider_async(request, db, run_id)
                candidate = self._record_stage_candidate_result(db, run_id, cand_id, debate_model, "debate", provider_result)
                if candidate:
                    debate_cands.append(candidate)

        # Stage 4: synthesis
        synth_model = self._select_stage_model(
            db, run_id, ["synthesis", "backup", "reviewer", "primary", "creative", "verification"]
        )
        self._emit_stage_started(db, run_id, "synthesis", [synth_model["alias"]] if synth_model else [])
        memory_context = get_memory_context(db, run["prompt"], "council", n_same=3, n_cross=2)
        synth_prompt = build_council_synthesis_prompt(
            run["prompt"], succeeded_opinions, peer_reviews,
            debate_cands if debate_cands else None,
            memory_context=memory_context,
        )
        if synth_model is None:
            logger.warning("No healthy synthesis model available; returning best council opinion", run_id=run_id)
            best = select_best_candidate(succeeded_opinions)
            synthesis_text = best.get("normalized_answer", "") if best else "Council synthesis failed."
        else:
            request = build_provider_request(
                synth_model,
                system_prompt=None,
                user_prompt=synth_prompt,
                max_output_tokens=run["max_output_tokens"],
                temperature=0.2,
            )
            cand_id = new_candidate_id()
            provider_result = await self._call_provider_async(request, db, run_id)
            candidate = self._record_stage_candidate_result(db, run_id, cand_id, synth_model, "synthesis", provider_result)
            if candidate:
                synthesis_text = candidate.get("normalized_answer", "")
            else:
                best = select_best_candidate(succeeded_opinions)
                synthesis_text = best.get("normalized_answer", "") if best else "Council synthesis failed."

        # Stage 5: verification
        # Deadline check — skip verification if deadline imminent
        degradation = self._check_deadline(run)
        if degradation:
            await self._finalize_degraded(db, run_id, "council", degradation, synthesis_text, confidence=0.5)
            return

        verif_model = self._select_stage_model(
            db, run_id, ["verification", "reviewer", "backup", "synthesis", "primary", "creative"]
        )
        self._emit_stage_started(db, run_id, "verification", [verif_model["alias"]] if verif_model else [])
        verif_prompt = build_verification_prompt(run["prompt"], synthesis_text)
        confidence = 0.5
        if verif_model is None:
            logger.warning("No healthy verification model available; completing without verification", run_id=run_id)
        else:
            request = build_provider_request(
                verif_model,
                system_prompt=None,
                user_prompt=verif_prompt,
                max_output_tokens=500,
                temperature=0.1,
            )
            cand_id = new_candidate_id()
            provider_result = await self._call_structured_provider_async(
                request,
                _VerificationPayload,
                db,
                run_id,
            )
            candidate = self._record_stage_candidate_result(db, run_id, cand_id, verif_model, "verification", provider_result)
            if candidate:
                raw_text = candidate.get("normalized_answer", "")
                confidence, synthesis_text = _apply_verification_result(
                    db=db,
                    cand_id=cand_id,
                    candidate=candidate,
                    raw_text=raw_text,
                    synthesis_text=synthesis_text,
                    current_confidence=confidence,
                    verif_alias=verif_model["alias"],
                    run_id=run_id,
                )

        terminal_count = len(execute_sql_all(db, "SELECT candidate_id FROM run_candidates WHERE run_id = :run_id AND status = 'succeeded'", {"run_id": run_id}))
        failed_count = len(execute_sql_all(db, "SELECT candidate_id FROM run_candidates WHERE run_id = :run_id AND status = 'failed'", {"run_id": run_id}))
        update_run_status(
            db,
            run_id,
            "succeeded",
            final_answer=synthesis_text,
            final_confidence=confidence,
            models_completed=terminal_count,
            models_failed=failed_count,
        )
        emit_run_completed(db, run_id, synthesis_text, confidence=confidence)
        _safe_log_pending_decision(db, run, "council", synthesis_text)

    async def _fail_run(self, db: object, run_id: str, error_code: str, error_message: str) -> None:
        update_run_status(db, run_id, "failed", error_code=error_code, error_message=error_message)
        emit_run_failed(db, run_id, error_code, error_message)

    async def _execute_run(self, run: dict) -> None:
        """Main entry point for executing a single run."""
        import os as _os

        db = self._get_db()
        run_id = run["run_id"]
        mode = run["mode"]

        # Sentinel file for graceful shutdown (preStop hook waits for this)
        run_active_sentinel = "/tmp/run-active"
        try:
            _os.makedirs("/tmp", exist_ok=True)
            with open(run_active_sentinel, "w") as f:
                f.write(run_id)
        except Exception:
            pass

        logger.info(f"Worker claiming run {run_id}", run_id=run_id)
        update_run_status(db, run_id, "running", started_at=utc_now_iso(), last_heartbeat_at=utc_now_iso())
        emit_run_started(db, run_id, mode)

        heartbeat_task = asyncio.create_task(self._heartbeat_loop(run_id, mode))

        try:
            if mode not in {"single", "fusion", "council"}:
                await self._fail_run(db, run_id, "INVALID_MODE", f"Unknown mode: {mode}")
            else:
                await self._router.execute(db=db, run=run, worker_ctx={"worker": self})
        except sqlite3.OperationalError as e:
            logger.error(
                f"SQLite operational error in run {run_id}: {e}. Resetting connection.",
                run_id=run_id,
            )
            self._reset_db()
            # Re-open a fresh connection to mark the run as failed
            fresh_db = self._get_db()
            await self._fail_run(fresh_db, run_id, "DB_LOCKED", str(e))
        except Exception as e:
            logger.error(f"Worker exception in run {run_id}: {e}", run_id=run_id)
            await self._fail_run(db, run_id, "WORKER_EXCEPTION", str(e))
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            try:
                _os.remove(run_active_sentinel)
            except FileNotFoundError:
                pass
            except Exception:
                pass

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
            # Check for graceful shutdown request (preStop hook touches this file)
            import os as _os
            if _os.path.exists("/tmp/shutdown-requested"):
                logger.info("Shutdown requested via sentinel file, stopping gracefully")
                self._running = False
                break

            db = self._get_db()
            try:
                # Try to claim a run
                run = claim_next_run(db)
                if run:
                    self._current_run_task = asyncio.create_task(self._execute_run(run))
                    await self._current_run_task
                else:
                    # When idle, recover stale runs (safe — no active processing)
                    self._recover_stale_runs()
                    await self._recover_langgraph_stale_runs()
                    await asyncio.sleep(self._poll_interval_s)
            except sqlite3.OperationalError as e:
                logger.error(f"SQLite operational error in poll loop: {e}. Resetting connection.")
                self._reset_db()
                await asyncio.sleep(5)
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

    async def run_mode_legacy(self, db: object, run: dict, mode: str) -> None:
        if mode == "single":
            await self._run_single(db, run)
        elif mode == "fusion":
            await self._run_fusion(db, run)
        elif mode == "council":
            await self._run_council(db, run)
        else:
            raise ValueError(f"Unknown mode: {mode}")
