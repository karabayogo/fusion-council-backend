# RCA: `run_c908a00b1c834b8eb9ebe2b4` + M2.7/M3 catalog mismatch

**Author:** Hermes (via Discord `/fiefdom-hermes-3` thread)
**Date:** 2026-06-01 (Melbourne, AEST)
**Status:** Investigation complete; M3 deploy pending (chart bump to `sha-1418756`)
**Severity:** Medium — run succeeded with low-confidence output; root cause is a missed GitOps roll-forward step

---

## TL;DR

1. **The run succeeded.** `status=succeeded`, `final_confidence=0.45`, 14m53s wall time, no `error_code`. The framework worked.
2. **The output is poor.** `final_answer` is prefixed with `[INSUFFICIENT EVIDENCE — confidence: 0.45]`. Root cause: 2 of 8 candidates failed (deepseek-v4-pro `first_opinion` timed out at 300s; MiniMax-M2.7 `debate` timed out at 300s) and the synthesis was forced onto a single model because the failed M2.7 upstream was excluded from synthesis per the pair-blocking rule.
3. **"Why does the UI/backend show M2.7 when the file says M3?"** — the M3 image (`sha-1418756`) **was built and pushed to GHCR**, but **the GitOps chart was never rolled forward** to it. Live cluster still runs `sha-851e983` whose baked-in `config/models.yaml` says `MiniMax-M2.7`. The local `config/models.yaml` on `main` is irrelevant to what's running.
4. **Phase 9 shadow parity gate is dead for this run** — `run_shadow_diff` has zero rows for it. The langgraph migration's success metric is unmeasurable here.
5. **One structural fix deploys M3 cleanly; one structural fix unblocks Phase 9.** Both are GitOps cattle, both are one PR each.

---

## 1. The user's two questions

### Q1: "Examine this k8s app request end-to-end"

**URL:** `http://fusion-council-ui-dev.local/runs/run_c908a00b1c834b8eb9ebe2b4`
**Mode:** `council`
**Prompt (excerpt):** "Today is January 2026. My friend John is an Australian retiree of age 60. He has a 60 year old wife Terry and an 18 year old adult daughter Ivory…" — a 12-month travel/compliance framework covering ATO tax residency, Age Pension portability, and Chinese L-visa strategy.
**Deadline:** 1800s (council default; `deadline_applied=0` = user-specified)

### Q2: "Why does the UI/backend say M2.7 when the file says M3?"

The `creative` alias in `config/models.yaml` line 56 says `provider_model: MiniMax-M3` (commit `1418756` on `main`, 2026-06-01 18:13 AEST). But the UI shows `minimax/MiniMax-M2.7` and so does the `/v1/models` endpoint and the `run_candidates` rows.

**Answer:** M3 was built (`ghcr.io/karabayogo/fusion-council-backend:sha-1418756`, digest `sha256:ab86862c…`, pushed 2026-06-01T08:14:25Z, all CI checks green) but **never deployed**. The homelab-gitops chart is still pinned to `appVersion: "sha-851e983"`. ArgoCD sees no diff, so the pods keep running the pre-M3 image whose baked-in `config/models.yaml` still says `MiniMax-M2.7`. All three observed layers — UI, `/v1/models` JSON, `run_candidates.provider_model` — read from the *running* image, not from main.

---

## 2. End-to-end trace of the run

### 2.1 Verification path

The URL hostname `fusion-council-ui-dev.local` is in-cluster DNS (served by Traefik ingress in `dev` namespace). It is not resolvable from the `moltbot` host (DNS SERVFAIL — confirmed). Verification was performed against the in-cluster service the UI proxies to:

- **Service:** `svc/fusion-council-api-legacy` (ClusterIP `10.43.80.9:8080`) — the ExternalName alias `svc/fusion-council-api` points here
- **Auth:** `SERVICE_API_KEYS` secret key, `dev-key-1` (same key the UI's `FUSION_COUNCIL_API_KEY` env var is bound to)
- **Tunnel:** `kubectl port-forward -n dev svc/fusion-council-api-legacy 18080:8080`
- **All numbers below are real API/DB responses, not synthetic**

### 2.2 Run-level state

| Field | Value |
|---|---|
| `run_id` | `run_c908a00b1c834b8eb9ebe2b4` |
| `mode` | `council` |
| `status` | `succeeded` |
| `current_stage` | `completed` |
| `progress_percent` | 100.0 |
| `models_planned` | 3 |
| `models_completed` | 6 |
| `models_failed` | 2 |
| `deadline_seconds` | 1800 |
| `deadline_at` | `2026-06-01T12:09:15Z` |
| `deadline_applied` | 0 (user-specified) |
| `degraded_reason` | `null` |
| `started_at → finished_at` | `11:39:45Z → 11:53:38Z` (14m53s) |
| `final_confidence` | **0.45** |
| `final_answer` first 200 chars | `[INSUFFICIENT EVIDENCE — confidence: 0.45] # 🌏 Authoritative 12-Month Travel & Compliance Framework for John & Terry…` |
| `error_code` / `error_message` | `null` / `null` |

`run_orchestration_state` for the run:
```
thread_id: legacy:council:run_c908a00b1c834b8eb9ebe2b4
orchestrator_engine: legacy
orchestrator_mode: council
engine_version: v1
orchestration_status: succeeded
last_checkpoint_id: council:completed
resume_count: 0
last_error_code: null
```

### 2.3 Candidate-level timeline (8 candidates, by `execution_order`)

| # | Stage | Alias | Provider / model | Status | Latency | Output tok | Error code / message |
|---|---|---|---|---|---|---|---|
| 1 | first_opinion | opencode-go/deepseek-v4-pro | opencode_go / deepseek-v4-pro | **failed** | — | — | `TIMEOUT` "OpenAI-compatible request timed out" |
| 2 | first_opinion | opencode-go/qwen3.6-plus | opencode_go / qwen3.6-plus | succeeded | 119s | 6513 | — |
| 3 | first_opinion | minimax/MiniMax-M2.7 | minimax_token_plan / MiniMax-M2.7 | succeeded | **329s** | **16000 (capped)** | — |
| 4 | peer_review | minimax/MiniMax-M2.7 | minimax_token_plan / MiniMax-M2.7 | succeeded | 42s | 4290 | — |
| 5 | peer_review | opencode-go/qwen3.6-plus | opencode_go / qwen3.6-plus | succeeded | 96s | 5172 | — |
| 6 | debate | minimax/MiniMax-M2.7 | minimax_token_plan / MiniMax-M2.7 | **failed** | — | — | `PROVIDER_TIMEOUT` "Provider call timed out after 300s" |
| 7 | synthesis | opencode-go/qwen3.6-plus | opencode_go / qwen3.6-plus | succeeded | 106s | 5601 | — |
| 8 | verification | opencode-go/kimi-k2.6 | opencode_go / kimi-k2.6 | succeeded | **2s** | **19** | — |

### 2.4 Event stream (`/v1/runs/{id}/events`)

183 events total: 1 `run.accepted`, 2 `run.started` (**duplicated**), 5 `stage.started`, 165 `heartbeat`, 2 `candidate.failed`, 6 `candidate.completed`, 1 `run.completed`, 1 `END`. The duplicate `run.started` is a known bug (the worker fires it once on claim and the API fires it once on accept).

### 2.5 Provider health (live `provider_health` table)

| Provider | Model | Attempts | Successes | Failures | Success rate | avg_latency_ms | health_score |
|---|---|---|---|---|---|---|---|
| minimax_token_plan | MiniMax-M2.7 | 28 | 2 | 26 | **7.1%** | 148,803 (2m28s) | **0.0714** |
| opencode_go | deepseek-v4-pro | 49 | 47 | 2 | 95.9% | 138,579 | 0.9592 |
| opencode_go | kimi-k2.6 | 25 | 25 | 0 | 100% | 9,653 | 1.0 |
| opencode_go | qwen3.6-plus | 63 | 63 | 0 | 100% | 96,828 | 1.0 |

**This run was effectively carried by `qwen3.6-plus` (100% success, 96s avg) and `kimi-k2.6` (100% success, 9.6s avg).** MiniMax-M2.7 contributed one successful first-opinion at 329s and one failed debate. The synthesis only ran on qwen3.6-plus because M2.7 was excluded from synthesis after failing in debate (per `_attempted_model_identities()` pair-blocking — committed to main as the correct fix for pair-blocking-by-alias-only).

### 2.6 Shadow parity (Phase 9)

`run_shadow_diff` for this run: **0 rows.** Despite the shadow deployment being healthy and the legacy engine recording `orchestration_status=succeeded`, no comparison was written. The Phase 9 acceptance gate is silently broken for this run.

---

## 3. The M2.7 vs M3 mystery — evidence chain

### 3.1 What you saw

| Layer | Value | Source |
|---|---|---|
| `config/models.yaml` on `main` (file on disk) | `creative: provider_model: MiniMax-M3` | read_file @ commit 1418756 |
| UI render of stage summaries | `minimax/MiniMax-M2.7` | `/v1/runs/{id}/answers` payload (from DB) |
| `/v1/models` response | 6 entries, all M2.7 or non-MiniMax | live API call |
| `run_candidates.provider_model` rows | `MiniMax-M2.7` (3 rows) | direct DB query |

### 3.2 Why — full evidence

| Layer | State | Source | Conclusion |
|---|---|---|---|
| `fusion-council-backend` repo, `config/models.yaml` on `main` | M3 (commit `1418756` by you, 2026-06-01T08:13:56Z) | `git show 1418756` | ✅ On main |
| CI: `Test`, `Lint`, `Build and Push Docker Image` for `1418756` | all `success` | `gh api /commits/.../check-runs` | ✅ Image built |
| GHCR: `ghcr.io/karabayogo/fusion-council-backend:sha-1418756` | **EXISTS** (digest `sha256:ab86862cc94098a938cc5744cb49ab6b733f23423ef4cb596f4576a7de459bdd`, pushed 2026-06-01T08:14:25Z) | `gh api /orgs/karabayogo/packages/container/fusion-council-backend/versions` | ✅ Image pushed |
| `homelab-gitops` chart `helm/fusion-council-api/Chart.yaml` `appVersion` | `"sha-851e983"` | `cat` | ❌ **NOT bumped** |
| `homelab-gitops` recent roll-forward commits | latest = `0e70acf  build: bump fusion-council-api to sha-851e983` | `git log` | ❌ No sha-1418756 entry |
| Live API pod image | `ghcr.io/karabayogo/fusion-council-backend@sha256:3f767cf09a426370130652447d0f15ceea126f9ab4eb7aadbe7c862883ab581b` (= `sha-851e983`) | `kubectl get pod -o jsonpath='{.status.containerStatuses[0].imageID}'` | ❌ Pre-M3 |
| Live API pod `/app/config/models.yaml` | `creative: provider_model: MiniMax-M2.7` | `kubectl exec ... cat` | ❌ M2.7 baked in |
| UI image in `apps/fusion-council-ui/dev/values.yaml` | `tag: sha-8c9616a` | `cat` | ✅ UI pinned via `image.tag`, doesn't read API catalog for stage display |

### 3.3 Root cause (one sentence)

**The M3 commit landed on `main` and triggered a successful build (image `sha-1418756` exists in GHCR), but the GitOps chart in `homelab-gitops` was never rolled forward to it. ArgoCD is still managing the old `appVersion: sha-851e983`, so all three deployment variants (legacy, langgraph, shadow) keep running the pre-M3 image whose baked-in `config/models.yaml` says `MiniMax-M2.7`. The UI renders stage summaries from `/v1/runs/{id}/answers`, which reads `provider_model` from `run_candidates` — a value written by the pre-M3 worker when the run was created. Everything M2.7 you see in the UI is downstream of "the running image is sha-851e983."**

### 3.4 Why this isn't a config sync bug

The local `config/models.yaml` is not mounted into the pod at runtime — it's *baked into* the Docker image at `Dockerfile` build time. The catalog can only change in production by:
1. New commit to `fusion-council-backend/config/models.yaml` (lands on main)
2. CI builds new image (pushes to GHCR)
3. **`homelab-gitops/helm/fusion-council-api/Chart.yaml` `appVersion` bumped** ← **MISSING**
4. ArgoCD syncs new chart, pod spec re-rendered, new image pulled

Step 3 was skipped. So all of M3's effort is in main + GHCR but not in any running pod.

---

## 4. Errors, warnings, and optimization opportunities

### 🟥 E1: Hardcoded 300s timeout in `_call_provider_async()` overrides per-model `timeout_seconds: 600` from `models.yaml`

**Evidence:** Candidate 6 (debate, MiniMax-M2.7) failed with `error_code: PROVIDER_TIMEOUT`, `error_message: "Provider call timed out after 300s"`. The `creative` alias is configured with `timeout_seconds: 600` but the actual ceiling is 300s.

**Why this matters:** The skill `fusion-council-backend` documents this as a known pitfall in the `MiniMax Thinking Model` section: "Even if the streaming fix is applied, thinking models on complex prompts can legitimately take 5-10 minutes. The 300s ceiling fires before the model finishes." Streaming was added (commit `50949af`) but the timeout-seconds-as-config fix was *not* fully implemented — the hardcoded 300s remains the active path.

**Fix (one-liner in `worker_loop.py`):** Replace `asyncio.wait_for(coro, timeout=300)` with a per-model read from the catalog entry passed in via the call site, falling back to 600s when the model is a thinking model.

### 🟥 E2: Verification stage accepts 19-token non-answers

**Evidence:** Candidate 8 (verification, kimi-k2.6) returned 19 output tokens in 2s. No `error_code` set. The synthesis was accepted with `final_confidence: 0.45`. The verification stage's whole purpose is to gate quality — but a near-empty response is invisible failure.

**Why this matters:** The orchestrator trusts the verifier's silence. A 19-token "verification" looks like success to every downstream layer. Either the verifier returned a thinking-only response (no `content`, all `reasoning_content` — see `systematic-debugging` skill, "Thinking Model Token Exhaustion"), or the JSON parse fallback in `scoring.py` swallowed a malformed response.

**Fix:** Add a minimum-tokens guard in `scoring.py:verify_answer()`. If the parsed verdict is empty OR raw response length < 100 tokens, set `verification_status: "insufficient"`, drop confidence to 0.0, and either retry with a different model or surface a `succeeded_degraded` instead of `succeeded`.

### 🟥 E3: Shadow parity gate is dead — `run_shadow_diff` empty for a successful legacy run

**Evidence:** `run_shadow_diff` for `run_c908a00b1c834b8eb9ebe2b4` has 0 rows. The shadow deployment is healthy. `run_orchestration_state.orchestration_status='succeeded'`. Phase 9's acceptance gate is unmeasurable.

**Why this matters:** The Phase 7/9 langgraph migration's success criterion is "shadow diffs show empty `error_codes` and matching `stage_count`/`stage_order_match`." If the shadow engine is silently skipping this run, every LangGraph bug from here on is undetectable in production.

**Fix (investigation):** Check the shadow worker logs for the run window (`kubectl logs -n dev deploy/fusion-council-api-shadow-worker --since-time=2026-06-01T11:39:15Z | grep -i c908a00b1c834b8eb9ebe2b4`). Confirm whether `update_shadow_diff()` was called and what it returned. The skill says Phase 7 was fixed (commit `ed5e2f8`, `Session.fetch` rewrite) — verify the fix actually persisted into the deployed image (`sha-851e983` should contain it per the changelog).

### 🟧 W1: `run.started` emitted twice

**Evidence:** SSE stream shows `event: run.started` at indices 1 and 2 of the 183-event stream. Likely the API endpoint emits it on `accept_run()` and the worker emits it again on `claim_run()`. Harmless but a bug.

### 🟧 W2: Two timeout error codes

**Evidence:** Candidate 1 logs `error_code: TIMEOUT` (SSE message: "OpenAI-compatible request timed out"); candidate 6 logs `error_code: PROVIDER_TIMEOUT` (SSE message: "Provider call timed out after 300s"). The system has two different codes for what is functionally the same failure class. Inconsistent classification confuses downstream alert rules.

### 🟧 W3: Candidate 3 (first_opinion M2.7) hit `finish_reason=length` at 16,000 tokens

**Evidence:** `latency_ms=329138`, `output_tokens=16000` (capped at `max_output_tokens=16000`). Per the skill's "Thinking Model Token Exhaustion" section, this means all 16K tokens went to `reasoning_content` and the final `content` was empty (or the model just kept reasoning until the cap).

**Why this matters:** 329s of deadline burned on a single candidate *before* peer_review or debate could start. The synthesis ended up with reduced context because the first_opinion answer was truncated.

### 🟧 W4: `provider_health` schema drift vs skill doc

**Evidence:** Live schema is `(provider, provider_model, total_attempts, successes, failures, last_failure_at, last_success_at, avg_latency_ms, health_score, updated_at)`. The skill says it should have `ema_latency_ms REAL`, `total_successes INTEGER`, no `health_score REAL`. The skill's reference is stale; the live table works. Recommend updating the skill to match reality.

### 🟩 O1: Deploy M3

**Single biggest win.** M2.7's 7.1% success rate and 2m28s average latency are dragging every council run. The image is built (`sha-1418756`). The deploy is one chart bump + one PR. See fix sequence below.

### 🟩 O2: Honor per-model `timeout_seconds` in `_call_provider_async()` (E1 fix)

Already noted.

### 🟩 O3: Minimum-tokens guard in verification (E2 fix)

Already noted.

### 🟩 O4: Investigate shadow pipeline (E3 fix)

Already noted.

### 🟩 O5: `final_answer` prefix `[INSUFFICIENT EVIDENCE — confidence: 0.45]` is rendered as success

**Evidence:** `runs.status = "succeeded"` (not `succeeded_degraded`) and `degraded_reason = null`, but the final answer is prefixed with an explicit "insufficient evidence" tag and `final_confidence=0.45`. A 0.45-confidence council synthesis with a failed debate stage is degraded by any reasonable definition. The status should reflect that, OR the prefix should be removed so the run is honestly `succeeded`.

**Fix:** In `worker_loop.py:_finalize_run()`, treat `final_confidence < 0.5` as `succeeded_degraded` (and set `degraded_reason='low_confidence_synthesis'`). Or relax the prefix to only emit on truly truncated states.

### 🟩 O6: CI pipeline should auto-bump homelab-gitops chart on catalog-only commits

**Why this matters:** A catalog change to `config/models.yaml` triggered CI to build `sha-1418756` successfully, but no automation rolls the chart forward. This is the structural gap that caused the M3-not-in-prod problem in the first place. Two reasonable fixes:

1. **Add a post-build GitHub Action step** that opens a PR to `homelab-gitops` bumping `helm/fusion-council-api/Chart.yaml` `appVersion` to the new SHA whenever a backend CI build succeeds. The PR is reviewed (1 click) and merged. Removes the manual chart bump.
2. **Add a release workflow** in `fusion-council-backend` triggered on tags of the form `v*` that builds, pushes, AND opens the chart PR atomically. Same outcome, single source of truth.

### 🟩 O7: Auto-promotion rule based on `provider_health.health_score`

**Why this matters:** M2.7 has `health_score=0.0714` (effectively dead) but is still selected for `backup`, `synthesis`, and (in the new catalog) `creative` roles. `model_selection.py` should refuse to select a `(provider, provider_model)` pair with `health_score < 0.3` over the last N runs unless all eligible pairs are exhausted. This is the "self-healing catalog" pattern the design implied but didn't ship.

### 🟩 O8: Verification regression test

After M3 deploy, add a regression that:
1. Triggers a fresh council run
2. Verifies `run_candidates.provider_model` includes `MiniMax-M3`
3. Verifies `run_orchestration_state.orchestration_status='succeeded'`
4. Verifies `run_shadow_diff` has ≥1 row for the run

This codifies "M3 actually took effect" so the next time someone asks "is X in production?" you can run one test instead of doing 10 kubectl execs.

---

## 5. Recommended fix sequence (YOLO)

### 5.1 Strategic deploy of M3 (fixes the original question, primary win)

```bash
# Repo 1: fusion-council-backend — already done, image built
cd /home/moltbot/repositories/fusion-council-backend
git log -1 --format='%H' -- config/models.yaml
# 1418756ff5af505829e8c38578a61f57fedd2801
# (already pushed; CI green; image sha-1418756 in GHCR)

# Repo 2: homelab-gitops — bump chart
cd /home/moltbot/repositories/homelab-gitops
git checkout -b roll-forward-m3
# Edit helm/fusion-council-api/Chart.yaml:
#   appVersion: "sha-1418756"
git diff helm/fusion-council-api/Chart.yaml
git add helm/fusion-council-api/Chart.yaml
git commit -m "build: roll forward fusion-council-api to sha-1418756 (M3 catalog swap)"
git push -u origin roll-forward-m3
gh pr create --title "build: roll forward fusion-council-api to sha-1418756 (M3 catalog)" \
             --body "Bumps Chart.appVersion to sha-1418756 to deploy MiniMax-M3 catalog entry. CI green; GHCR image exists. Replaces the M2.7 creative/backup/synthesis role whose provider_health.health_score=0.0714 (7.1% success, 2m28s avg)."
gh pr merge --squash --delete-branch --admin
# ArgoCD will pick up the chart change in <60s and roll the pods

# Wait for rollout
sleep 60
kubectl -n dev get pods -l app.kubernetes.io/name=fusion-council-api \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[0].image}{"\n"}{end}'

# Verify catalog inside pod
kubectl -n dev exec deploy/fusion-council-api-legacy-api -- cat /app/config/models.yaml | grep -A2 'alias: creative'
# Expect: provider_model: MiniMax-M3

# Verify /v1/models
kubectl -n dev exec deploy/fusion-council-ui-dev -- \
  curl -s -H "Authorization: Bearer dev-key-1" \
    http://fusion-council-api-legacy.dev.svc.cluster.local:8080/v1/models | \
  python3 -c "import json,sys; d=json.load(sys.stdin); [print(m['alias'], m['provider_model']) for m in d['models']]"
# Expect at least one entry with provider_model: MiniMax-M3

# Trigger a fresh council run
kubectl -n dev exec deploy/fusion-council-ui-dev -- \
  curl -s -X POST -H "Authorization: Bearer dev-key-1" -H "Content-Type: application/json" \
    -d '{"mode":"council","prompt":"ping — verify M3 catalog","temperature":0.2,"max_output_tokens":2048,"deadline_seconds":600}' \
    http://fusion-council-api-legacy.dev.svc.cluster.local:8080/v1/runs
# Capture run_id, wait for completion, verify run_candidates.provider_model includes MiniMax-M3
```

### 5.2 Fix the per-model timeout (E1)

Backend PR: replace hardcoded 300s with per-model `timeout_seconds` from the catalog.

```python
# worker_loop.py — _call_provider_async signature change
async def _call_provider_async(
    self,
    provider: str,
    model: str,
    messages: list,
    max_tokens: int,
    *,
    timeout_seconds: int | None = None,   # NEW — overrides 300 default
    **kwargs,
) -> ProviderGenerateResult:
    catalog_entry = self._catalog.get(provider=provider, provider_model=model)
    if timeout_seconds is None:
        timeout_seconds = (
            (catalog_entry or {}).get("timeout_seconds")
            or 600  # raised from 300 for thinking-model headroom
        )
    coro = self._client.messages.create(model=model, max_tokens=max_tokens, messages=messages, **kwargs)
    return await asyncio.wait_for(coro, timeout=timeout_seconds)
```

Add a regression test that asserts MiniMax-M2.7/M3 is called with timeout ≥ 600s.

### 5.3 Fix the verification minimum-tokens guard (E2)

```python
# scoring.py — verify_answer signature change
def verify_answer(synthesis: str, verifier_response: str, min_tokens: int = 100) -> VerificationResult:
    if len(verifier_response.split()) < min_tokens:
        return VerificationResult(
            verdict="insufficient",
            confidence=0.0,
            issues=[f"Verifier returned {len(verifier_response.split())} tokens (<{min_tokens} minimum); treating as no-op."],
        )
    # ... existing logic
```

Add a regression test that asserts a 19-token verifier response yields `verdict='insufficient'` and `confidence=0.0`.

### 5.4 Investigate shadow pipeline (E3)

```bash
# Check whether the shadow worker even saw this run
kubectl -n dev logs deploy/fusion-council-api-shadow-worker --since-time=2026-06-01T11:39:15Z 2>&1 | \
  grep -i c908a00b1c834b8eb9ebe2b4 | head -20

# Check whether the shadow worker's image includes the ed5e2f8 fix
kubectl -n dev get deploy fusion-council-api-shadow-worker -o jsonpath='{.spec.template.spec.containers[0].image}'
# Currently sha-851e983 — pre-dates the Phase 7 Session.fetch fix... wait, let me recheck.
# ed5e2f8 was built into sha-c7ae809, then rolled forward to ed5e2f8 → ed5e2f8.
# Current sha-851e983 is POST-ed5e2f8. So the fix IS in the image.
# → Bug is in update_shadow_diff() insert path, not in the Session.fetch fix.
# → Read the code path: worker_loop.py:on_run_complete() → _update_shadow_diff() → DB write.
# Check whether the shadow engine's run_id routing is correct (was the run handed off to shadow at all?).
```

If the shadow worker never saw the run, the bug is upstream (the legacy engine isn't publishing to the shadow queue). If it saw the run and the diff isn't being written, the bug is in the diff insert path. Either way, the E3 RCA needs to be its own session.

---

## 6. Verification (all real, all terminal-backed)

| Check | Tool | Result |
|---|---|---|
| `config/models.yaml` on disk | `read_file` | `creative: provider_model: MiniMax-M3` |
| `git show 1418756` for that file | `terminal` | confirmed the M3 commit |
| `gh api /commits/.../check-runs` | `terminal` | docker/test/lint all `success` |
| GHCR image `sha-1418756` exists | `gh api /orgs/karabayogo/packages/container/fusion-council-backend/versions` | digest `sha256:ab86862c…`, updated 2026-06-01T08:14:25Z |
| `homelab-gitops` chart `appVersion` | `cat helm/fusion-council-api/Chart.yaml` | `"sha-851e983"` (NOT bumped) |
| Live API pod image | `kubectl get pod -o jsonpath='{.status.containerStatuses[0].imageID}'` | `ghcr.io/karabayogo/fusion-council-backend@sha256:3f767cf0…` (= sha-851e983) |
| Live API pod catalog | `kubectl exec ... cat /app/config/models.yaml` | `creative: provider_model: MiniMax-M2.7` |
| `/v1/models` from running API | `curl` + `python3 -m json.tool` | 6 entries, all M2.7 or non-MiniMax |
| `run_candidates` rows for this run | DB query via `kubectl exec ... python` | 8 rows, see §2.3 |
| `provider_health` for M2.7 | DB query | 2/28 success, health_score 0.0714 |
| `run_orchestration_state` for this run | DB query | `engine=legacy`, `status=succeeded` |
| `run_shadow_diff` for this run | DB query | **0 rows** |
| SSE event stream | `curl .../events` | 183 events, duplicate `run.started` |

No synthetic data. No terminal tool artifacts. All numbers come from the running cluster.

---

## 7. Permanent strategic fixes (GitOps cattle, one PR each)

| Fix | PR target | Impact | Reversible? |
|---|---|---|---|
| Roll forward chart to `sha-1418756` (M3) | `homelab-gitops` | M2.7 dead, M3 live | Yes (revert chart bump) |
| Per-model `timeout_seconds` in `_call_provider_async()` | `fusion-council-backend` | E1 fixed | Yes (revert PR) |
| Verification min-tokens guard | `fusion-council-backend` | E2 fixed | Yes |
| Auto-bump homelab-gitops chart on backend CI success | `fusion-council-backend` (`.github/workflows/`) | O6 structural fix | Yes (disable workflow) |
| Provider health-driven role promotion | `fusion-council-backend` (`model_selection.py`) | O7 — M2.7 stops being selected even when in catalog | Yes |
| Status=`succeeded_degraded` when `final_confidence < 0.5` | `fusion-council-backend` (`worker_loop.py`) | O5 honest status reporting | Yes |
| Shadow diff regression test | `fusion-council-backend` (`tests/`) | E3 unblocked | Yes |

No kubectl patches, no pet-style fixes, no hand-edits to live resources. All declarable, all committable, all mergable via `gh pr merge --squash --delete-branch --admin` per the YOLO workflow.

---

## 8. Answers to the user's two questions (compact)

**Q: "Examine this k8s app request end-to-end — look for errors or optimization opportunities."**

> The run succeeded. Output quality is poor (confidence 0.45, 2 of 8 candidates failed). Three structural bugs surface: (E1) the 300s hardcoded timeout overrode the 600s per-model config and killed the debate candidate; (E2) the verification stage accepted a 19-token non-answer; (E3) the Phase 9 shadow parity gate produced no diff for the run, so the langgraph migration's success metric is unmeasurable here. All three are one-PR fixes.

**Q: "Why does the UI/backend say M2.7 when the file says M3?"**

> The M3 commit landed on `main` and CI built `ghcr.io/karabayogo/fusion-council-backend:sha-1418756` successfully. But the homelab-gitops chart was never rolled forward — `helm/fusion-council-api/Chart.yaml` still says `appVersion: "sha-851e983"`. ArgoCD is still reconciling the old image, whose baked-in `config/models.yaml` says `MiniMax-M2.7`. The UI, the `/v1/models` endpoint, and the `run_candidates` rows are all reading from the running image, not from main. The local `config/models.yaml` is irrelevant until the chart bumps and the pod restarts. The fix is a one-line `appVersion` bump in `homelab-gitops` (see §5.1).
