"""LangGraph orchestration engine — Phase 4 checkpointing + Phase 5 fusion implementation."""
from __future__ import annotations

from typing import TYPE_CHECKING

from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
    check_engine_version_compatible,
    get_or_create_thread_id,
)
from fusion_council_service.domain.orchestration.orchestration_state import (
    OrchestrationState,
)
from fusion_council_service.domain.orchestration.orchestration_state_repository import (
    get_orchestration_state,
    upsert_orchestration_state,
)
from fusion_council_service.logging_utils import get_logger
from fusion_council_service.startup import get_checkpoint_saver

if TYPE_CHECKING:
    import asyncpg

logger = get_logger("fusion_council_service.orchestration.langgraph_engine")

# Module-level compiled graph singletons — built once, reused across all invocations.
_cached_graph_single: "StateGraph" | None = None
_cached_graph_fusion: "StateGraph" | None = None
_cached_graph_council: "StateGraph" | None = None


def _build_single_graph() -> "StateGraph":
    """
    Build and compile the LangGraph StateGraph for single-mode orchestration.

    Linear sequence: START -> node_prepare_run -> node_generation_call
                                   -> node_generation_persist -> node_finalize_success

    The graph is compiled once at module load and reused for all invocations.
    """
    from langgraph.graph import StateGraph, START

    from fusion_council_service.domain.orchestration.orchestration_nodes_single import (
        node_finalize_failure,
        node_finalize_success,
        node_generation_call,
        node_generation_persist,
        node_prepare_run,
    )

    builder = StateGraph(OrchestrationState)

    builder.add_node("node_prepare_run", node_prepare_run)
    builder.add_node("node_generation_call", node_generation_call)
    builder.add_node("node_generation_persist", node_generation_persist)
    builder.add_node("node_finalize_success", node_finalize_success)
    builder.add_node("node_finalize_failure", node_finalize_failure)

    # START -> first node (mandatory — raises ValueError if missing)
    builder.add_edge(START, "node_prepare_run")

    # Linear sequence edges
    builder.add_edge("node_prepare_run", "node_generation_call")
    builder.add_edge("node_generation_call", "node_generation_persist")
    builder.add_edge("node_generation_persist", "node_finalize_success")

    compiled = builder.compile()

    logger.info("LangGraph StateGraph (single) compiled successfully")
    return compiled


def _build_fusion_graph() -> "StateGraph":
    """
    Build and compile the LangGraph StateGraph for fusion-mode orchestration.

    Sequence: START -> node_prepare_fusion -> node_generation_parallel
                                  -> node_synthesis_call -> node_synthesis_persist
                                  -> node_verification_call -> node_verification_persist
                                  -> node_finalize_fusion_success
                                  (or node_finalize_fusion_failure on error path)

    The graph is compiled once at module load and reused for all invocations.
    """
    from langgraph.graph import StateGraph, START

    from fusion_council_service.domain.orchestration.orchestration_nodes_fusion import (
        node_finalize_fusion_failure,
        node_finalize_fusion_success,
        node_generation_parallel,
        node_prepare_fusion,
        node_synthesis_call,
        node_synthesis_persist,
        node_verification_call,
        node_verification_persist,
    )

    builder = StateGraph(OrchestrationState)

    builder.add_node("node_prepare_fusion", node_prepare_fusion)
    builder.add_node("node_generation_parallel", node_generation_parallel)
    builder.add_node("node_synthesis_call", node_synthesis_call)
    builder.add_node("node_synthesis_persist", node_synthesis_persist)
    builder.add_node("node_verification_call", node_verification_call)
    builder.add_node("node_verification_persist", node_verification_persist)
    builder.add_node("node_finalize_fusion_success", node_finalize_fusion_success)
    builder.add_node("node_finalize_fusion_failure", node_finalize_fusion_failure)

    # START -> first node
    builder.add_edge(START, "node_prepare_fusion")

    # Linear sequence edges
    builder.add_edge("node_prepare_fusion", "node_generation_parallel")
    builder.add_edge("node_generation_parallel", "node_synthesis_call")
    builder.add_edge("node_synthesis_call", "node_synthesis_persist")
    builder.add_edge("node_synthesis_persist", "node_verification_call")
    builder.add_edge("node_verification_call", "node_verification_persist")
    builder.add_edge("node_verification_persist", "node_finalize_fusion_success")

    compiled = builder.compile()

    logger.info("LangGraph StateGraph (fusion) compiled successfully")
    return compiled


def _graph_single() -> "StateGraph":
    """Return the compiled single-mode graph singleton, building it on first call."""
    global _cached_graph_single
    if _cached_graph_single is None:
        _cached_graph_single = _build_single_graph()
    return _cached_graph_single


def _graph_fusion() -> "StateGraph":
    """Return the compiled fusion-mode graph singleton, building it on first call."""
    global _cached_graph_fusion
    if _cached_graph_fusion is None:
        _cached_graph_fusion = _build_fusion_graph()
    return _cached_graph_fusion


def _build_council_graph() -> "StateGraph":
    """
    Build and compile the LangGraph StateGraph for council-mode orchestration.

    Sequence: START -> node_prepare_council -> node_first_opinion_parallel
                                  -> node_first_opinion_persist
                                  -> node_synthesis_call -> node_synthesis_persist
                                  -> node_verification_call -> node_verification_persist
                                  -> node_finalize_council_success
                                  (or node_finalize_council_failure on error path)

    Peer review and debate nodes are NOT included in the LangGraph path because
    they are conditional (only run when agreement < 0.55). The worker_loop.py
    handles those stages outside the graph, then re-enters the graph at
    node_synthesis_call.

    The graph is compiled once at module load and reused for all invocations.
    """
    from langgraph.graph import StateGraph, START

    from fusion_council_service.domain.orchestration.orchestration_nodes_council import (
        node_finalize_council_failure,
        node_finalize_council_success,
        node_first_opinion_parallel,
        node_first_opinion_persist,
        node_prepare_council,
        node_synthesis_call,
        node_synthesis_persist,
        node_verification_call,
        node_verification_persist,
    )

    builder = StateGraph(OrchestrationState)

    builder.add_node("node_prepare_council", node_prepare_council)
    builder.add_node("node_first_opinion_parallel", node_first_opinion_parallel)
    builder.add_node("node_first_opinion_persist", node_first_opinion_persist)
    builder.add_node("node_synthesis_call", node_synthesis_call)
    builder.add_node("node_synthesis_persist", node_synthesis_persist)
    builder.add_node("node_verification_call", node_verification_call)
    builder.add_node("node_verification_persist", node_verification_persist)
    builder.add_node("node_finalize_council_success", node_finalize_council_success)
    builder.add_node("node_finalize_council_failure", node_finalize_council_failure)

    # START -> first node
    builder.add_edge(START, "node_prepare_council")

    # Linear sequence edges (peer_review and debate handled outside graph in worker_loop)
    builder.add_edge("node_prepare_council", "node_first_opinion_parallel")
    builder.add_edge("node_first_opinion_parallel", "node_first_opinion_persist")
    builder.add_edge("node_first_opinion_persist", "node_synthesis_call")
    builder.add_edge("node_synthesis_call", "node_synthesis_persist")
    builder.add_edge("node_synthesis_persist", "node_verification_call")
    builder.add_edge("node_verification_call", "node_verification_persist")
    builder.add_edge("node_verification_persist", "node_finalize_council_success")

    compiled = builder.compile()

    return compiled


def _graph_council() -> "StateGraph":
    """Return the compiled council-mode graph singleton, building it on first call."""
    global _cached_graph_council
    if _cached_graph_council is None:
        _cached_graph_council = _build_council_graph()
    return _cached_graph_council


class LangGraphEngine:
    """
    LangGraph orchestration engine with checkpointing support.

    Implements Option B: coarse-grained nodes that wrap run_mode_legacy() as a
    single step per mode. Checkpoint granularity is at the mode level.

    The legacy engine is completely untouched. The LangGraph path is a parallel
    routing target selected via ORCHESTRATOR_ENGINE=langgraph in the router.
    """

    def __init__(self, thread_namespace: str = "fusion-council", engine_version: str = "v1"):
        self._thread_namespace = thread_namespace
        self._engine_version = engine_version

    def _thread_id(self, run_id: str, mode: str) -> str:
        return f"{self._thread_namespace}:{mode}:{run_id}"

    async def _invoke_graph(
        self,
        db: "asyncpg.Connection",
        run: dict,
        worker_ctx: dict,
        mode: str,
        thread_id: str,
        langgraph_config: dict,
        is_resume: bool,
    ) -> OrchestrationState:
        """
        Invoke (or resume) the compiled StateGraph and persist orchestration state.

        After graph execution, upserts the final orchestration_status to the
        run_orchestration_state table. The graph itself is idempotent — replaying
        a completed run will hit terminal-node guards and no-op.

        Args:
            db: asyncpg connection
            run: run record dict
            worker_ctx: worker context (used for legacy engine fallback)
            mode: run mode (single/fusion/council)
            thread_id: LangGraph thread identifier
            langgraph_config: {"thread_id": ..., "checkpoint_namespace": ...}
            is_resume: True if replaying from checkpoint

        Returns:
            Final OrchestrationState after graph execution
        """

        run_id = run["run_id"]
        # Import lazily to avoid hard dependency on langgraph.checkpoint.postgres
        # when the module is imported in test environments without libpq.
        from fusion_council_service.startup import get_checkpoint_saver
        saver = get_checkpoint_saver()

        # Build the initial OrchestrationState for this run
        initial_state: OrchestrationState = {
            "run_id": run_id,
            "mode": mode,
            "engine": "langgraph",
            "engine_version": self._engine_version,
            "thread_id": thread_id,
            "checkpoint_namespace": langgraph_config.get("checkpoint_namespace", f"mode={mode}"),
            "resume_count": 1 if is_resume else 0,
            "current_stage": "",
            "candidate_ids": [],
            "current_candidate_id": None,
            "final_answer": None,
            "final_confidence": None,
            "error_code": None,
            "error_message": None,
            "updated_at": None,
            "raw_response": None,
            "candidate_summaries": None,
            "computed_final_answer": None,
            "computed_final_confidence": None,
        }

        # Select the correct graph based on mode
        if mode == "single":
            graph = _graph_single()
        elif mode == "fusion":
            graph = _graph_fusion()
        elif mode == "council":
            graph = _graph_council()
        else:
            # For modes without a dedicated graph, use single (fallback)
            graph = _graph_single()

        # Inject worker into config so nodes can access provider registry, catalog, DB.
        # LangGraph passes RunnableConfig as the second argument to every node function.
        worker = worker_ctx.get("worker")
        if worker is not None:
            langgraph_config.setdefault("configurable", {})["worker"] = worker

        if is_resume and saver is not None:
            # Resume path: replay from last checkpoint
            saved = await saver.aget(langgraph_config)
            if saved is None:
                # Checkpoint gone — fall back to fresh invoke
                logger.warning(
                    f"Checkpoint missing for resume run_id={run_id}, performing fresh invoke"
                )
                raw_result = await graph.ainvoke(
                    initial_state,
                    langgraph_config,
                )
            else:
                # Replay from saved state
                raw_result = await graph.ainvoke(
                    None,  # type: ignore[arg-type]  # None = replay from checkpoint
                    langgraph_config,
                )
        else:
            # Fresh run
            raw_result = await graph.ainvoke(initial_state, langgraph_config)

        result: OrchestrationState
        if isinstance(raw_result, dict):
            result = raw_result
        else:
            result = initial_state

        # Mark terminal status based on graph outcome
        if result.get("current_stage", "").startswith("finalize_failure"):
            terminal_status = "failed"
            terminal_checkpoint = f"{mode}:failed"
        else:
            terminal_status = "succeeded"
            terminal_checkpoint = f"{mode}:completed"

        upsert_orchestration_state(
            db,
            run_id=run_id,
            thread_id=thread_id,
            orchestrator_engine="langgraph",
            orchestrator_mode=mode,
            engine_version=self._engine_version,
            orchestration_status=terminal_status,
            last_checkpoint_id=terminal_checkpoint,
        )

        return result

    async def run_single(self, db: "asyncpg.Connection", run: dict, worker_ctx: dict) -> None:
        await self._run_mode(db, run, worker_ctx, "single")

    async def run_fusion(self, db: "asyncpg.Connection", run: dict, worker_ctx: dict) -> None:
        await self._run_mode(db, run, worker_ctx, "fusion")

    async def run_council(self, db: "asyncpg.Connection", run: dict, worker_ctx: dict) -> None:
        await self._run_mode(db, run, worker_ctx, "council")

    async def _run_mode(
        self,
        db: "asyncpg.Connection",
        run: dict,
        worker_ctx: dict,
        mode: str,
    ) -> None:
        """
        Execute a run in the given mode using the LangGraph checkpointing engine.

        1. Get or create thread_id via get_or_create_thread_id (handles resume vs fresh)
        2. Check if run is already completed/succeeded — if so, no-op
        3. Check if run is failed — mark orchestration_state as failed, no-op
        4. Otherwise invoke the graph (fresh or resume from checkpoint)
        5. Update orchestration_state with final status

        Option B: all actual work (model calls, DB writes) is delegated to
        run_mode_legacy() which is called by the caller after the graph returns.
        This node just handles checkpoint sequencing.
        """


        run_id = run["run_id"]
        thread_id = self._thread_id(run_id, mode)

        # Determine resume vs fresh — consult the orchestration_state table
        previous = get_orchestration_state(db, run_id)
        resume_increment = (
            previous is not None
            and previous.get("orchestration_status") not in {"succeeded", "failed", "abandoned"}
        )

        # Upsert initial orchestration state
        upsert_orchestration_state(
            db,
            run_id=run_id,
            thread_id=thread_id,
            orchestrator_engine="langgraph",
            orchestrator_mode=mode,
            engine_version=self._engine_version,
            orchestration_status="running",
            last_checkpoint_id=f"{mode}:start",
            resume_count_increment=resume_increment,
        )

        # Check if the run itself is already completed — no-op
        current_run = run  # run is already fetched by caller
        if current_run and current_run.get("status") in {"succeeded", "succeeded_degraded", "failed"}:
            upsert_orchestration_state(
                db,
                run_id=run_id,
                thread_id=thread_id,
                orchestrator_engine="langgraph",
                orchestrator_mode=mode,
                engine_version=self._engine_version,
                orchestration_status="succeeded",
                last_checkpoint_id=f"{mode}:noop_terminal",
            )
            return

        saver = None
        try:
            from fusion_council_service.startup import get_checkpoint_saver
            saver = get_checkpoint_saver()
        except Exception:
            pass

        if saver is None:
            logger.warning(
                "LangGraph checkpointer unavailable; continuing without persisted graph checkpoints",
                run_id=run_id,
            )

        # Get thread_id and is_resume from checkpoint table
        langgraph_config, is_resume = await get_or_create_thread_id(db, run_id, mode)

        # Check engine version compatibility for resume
        if is_resume and previous is not None:
            stored_version = previous.get("engine_version")
            if stored_version and stored_version != self._engine_version:
                check_engine_version_compatible(stored_version, self._engine_version)

        try:
            # Invoke (or resume) the graph
            result = await self._invoke_graph(
                db, run, worker_ctx, mode, thread_id, langgraph_config, is_resume
            )

            # Delegate actual work to legacy engine (Option B — coarse-grained wrapper)
            # The caller (worker_loop.py) will call run_mode_legacy() after this returns.
            # We set the stage so the caller knows what to do.
            if result.get("current_stage", "").startswith("finalize_failure"):
                # Graph signaled failure — do not call legacy engine
                logger.error(
                    f"LangGraph engine aborted: error_code={result.get('error_code')}",
                    run_id=run_id,
                    mode=mode,
                )
                upsert_orchestration_state(
                    db,
                    run_id=run_id,
                    thread_id=thread_id,
                    orchestrator_engine="langgraph",
                    orchestrator_mode=mode,
                    engine_version=self._engine_version,
                    orchestration_status="failed",
                    last_checkpoint_id=f"{mode}:failed",
                    last_error_code=result.get("error_code", "LANGGRAPH_ENGINE_ERROR"),
                    last_error_message=result.get(
                        "error_message", "Graph execution failed — see logs"
                    ),
                )
                return

        except Exception as exc:
            logger.error(
                f"LangGraph engine mode failed: {exc}",
                run_id=run_id,
                mode=mode,
            )
            upsert_orchestration_state(
                db,
                run_id=run_id,
                thread_id=thread_id,
                orchestrator_engine="langgraph",
                orchestrator_mode=mode,
                engine_version=self._engine_version,
                orchestration_status="failed",
                last_checkpoint_id=f"{mode}:failed",
                last_error_code="LANGGRAPH_MODE_EXCEPTION",
                last_error_message=str(exc),
            )
            raise
