# Phase 9 — LangGraph Cutover Runbook

## Objective

Cut over fusion-council-backend from the legacy orchestration engine to the LangGraph engine, using the Shadow deployment as the validation gate. The cutover is data-driven: langgraph promotion is gated on shadow parity metrics collected over at least 7 days of shadow traffic.

---

## Prerequisites

- [ ] Shadow deployment (`fusion-council-api-dev-shadow`) is deployed and healthy in the `dev` namespace
- [ ] `run_shadow_diff` table is accumulating rows (shadowValidate cron has run at least once)
- [ ] All three engines (legacy, langgraph, shadow) are built from the same Git SHA on `main`
- [ ] PostgreSQL is the active database (not SQLite) for production deployments
- [ ] LangGraph checkpoint retention CronJob is deployed and working

---

## Environments

| Environment | ArgoCD Application | Engine | Notes |
|---|---|---|---|
| `dev` | `fusion-council-api-dev-langgraph` | `langgraph` → will become production path after cutover |
| `dev` | `fusion-council-api-dev-legacy` | `legacy` | Source of truth (old) |
| `dev` | `fusion-council-api-dev-shadow` | `shadow` | Validation / parity gate |
| ~~`pp`~~ | KILLED per 2026-05-30 decision | — | pp namespace permanently off-limits |
| ~~`prod`~~ | KILLED per 2026-05-30 decision | — | prod namespace permanently off-limits |

**Cutover is a single-env `dev` operation**: Flip `ORCHESTRATOR_ENGINE` from `shadow` to `langgraph` in the `dev-langgraph` values after shadow parity passes. No pp/prod promotion needed.

---
## 2026-05-30 Shadow Bug Fix (RCA)

**Bug**: All `run_shadow_diff` rows show `SHADOW_LANGGRAPH_EXCEPTION: 'Session' object has no attribute 'fetch'`. Shadow validation produces zero valid comparisons.

**Root Cause**: `get_or_create_thread_id()` in `orchestration_checkpoint.py` used `await conn.fetch()` (asyncpg API), but the DB handle passed from `worker_loop.py` → `router.execute()` → `langgraph_engine._run_mode()` is a SQLAlchemy sync `Session` (returned by `new_session()` for PostgreSQL). The `Session` object does not have `.fetch()`.

**Fix** (commit: pending): Rewrote `get_or_create_thread_id()` to use `execute_sql()`/`execute_sql_one()` from `db.py` — sync, works with both SQLAlchemy `Session` and `sqlite3.Connection`. Added TDD regression tests (`test_orchestration_checkpoint_sync.py`, 5 tests + updated `test_orchestration_checkpoint.py`, 13 tests).

---

## Shadow Parity Gate Criteria

Promotion from legacy to langgraph is governed by the `shadow_validate` script. All three criteria must pass simultaneously:

```
total_runs           >= 100   (enough data for statistical confidence)
stage_parity_rate    >= 0.95  (95% of runs show identical stage ordering)
terminal_corruption_count = 0 (langgraph never produces a bad final answer when legacy did not)
```

The `shadow_validate` script runs as a CronJob at `0 3 * * *` (03:00 UTC) and writes a JSON report to stdout.

### Reading the Gate Report

```bash
# View the most recent shadow-validate job result
kubectl logs job/fusion-council-api-shadow-shadow-validate -n dev --tail=50

# Or describe for exit code
kubectl describe job/fusion-council-api-shadow-shadow-validate -n dev
```

A passing report looks like:

```json
{
  "overall": "PASS",
  "failures": [],
  "metrics": {
    "total_runs": 847,
    "stage_parity_rate": 0.987,
    "terminal_corruption_count": 0
  }
}
```

---

## Cutover Stages

### Stage 9.1 — Shadow Validation Baseline

**Goal**: Confirm shadow traffic is flowing and the parity gate is reachable.

1. Deploy the current `main` SHA to all three dev deployments.
2. Verify `run_shadow_diff` rows are being written (check for recent `logged_at` values):

   ```bash
   kubectl exec -n dev deploy/postgres-dev-postgresql-0 -- psql -U app -d appdb -c \
     "SELECT COUNT(*) FROM run_shadow_diff WHERE logged_at > NOW() - INTERVAL '1 hour';"
   ```

3. Wait at least 7 days of shadow traffic before considering gate evaluation.
4. Run the shadow-validate cronjob manually to check current status:

   ```bash
   kubectl create job --from=cronjob/fusion-council-api-shadow-shadow-validate shadow-validate-manual -n dev
   ```

5. Review the gate report. If any failure, diagnose before proceeding.

**Exit criterion**: Shadow CronJob completes with `overall=PASS`.

---

### ~~Stage 9.2 — LangGraph Promotion to pp~~ ❌ KILLED 2026-05-30

> pp namespace permanently off-limits. Cutover is dev-only.

### ~~Stage 9.3 — LangGraph Promotion to prod~~ ❌ KILLED 2026-05-30

> prod namespace permanently off-limits. Cutover is dev-only.

### Stage 9.2 (replacement) — Dev LangGraph Promotion

**Goal**: Cut over the dev-langgraph deployment from shadow to production langgraph engine.

1. Confirm shadow parity gate has passed on `dev` (Stage 9.1).
2. In `homelab-gitops`, update `dev/values-langgraph.yaml`:
   ```yaml
   commonEnv:
     ORCHESTRATOR_ENGINE: "langgraph"    # was "shadow"
     ORCHESTRATOR_LANGGRAPH_MODES: "single,fusion,council"
   ```
3. Commit and push. ArgoCD syncs automatically.
4. Verify the langgraph worker logs show `orchestrator_engine=langgraph`.
5. Verify `run_orchestration_state` shows `orchestrator_engine='langgraph'` for new runs.
6. Monitor for 30 minutes — verify smoke CronJob healthy.

**Exit criterion**: Smoke test passes with langgraph engine confirmed.

### Stage 9.3 — Legacy Decommission

**Goal**: Remove the legacy engine after confirmed langgraph stability.

**Do not proceed until**:

- LangGraph has been in production for at least 7 days
- Shadow parity gate has been passing for that period
- Zero `terminal_corruption_count` incidents
- Smoke test CronJob green on all environments

**Actions**:

1. Update ArgoCD applications to disable legacy:

   ```bash
   # Disable legacy deployments (remove or set replicaCount=0)
   kubectl scale deployment fusion-council-api-legacy -n dev --replicas=0
   kubectl scale deployment fusion-council-api-legacy -n pp --replicas=0
   kubectl scale deployment fusion-council-api-legacy -n prod --replicas=0
   ```

2. Remove legacy-specific values overrides from Helm values files:
   - `dev/values-legacy.yaml` → can be deleted or archived
   - `pp/values-legacy.yaml` → same
   - `prod/values-legacy.yaml` → same

3. Remove legacy ArgoCD Applications:

   ```bash
   kubectl delete -f k8s-workbench/argocd/apps/fusion-council-api-dev-legacy.yaml
   kubectl delete -f k8s-workbench/argocd/apps/fusion-council-api-pp-legacy.yaml  # if exists
   kubectl delete -f k8s-workbench/argocd/apps/fusion-council-api-prod-legacy.yaml  # if exists
   ```

4. Verify zero legacy pods:

   ```bash
   kubectl get pods -A | grep fusion-council-api | grep legacy
   # should return nothing
   ```

5. Commit and push hash changes to k8s-workbench.

---

## Rollback

If langgraph behaves badly at any stage:

### Emergency Rollback to Legacy (prod)

```bash
# Fastest path: scale langgraph to 0, scale legacy back to 1
kubectl scale deployment fusion-council-api-langgraph -n prod --replicas=0
kubectl scale deployment fusion-council-api-legacy -n prod --replicas=1

# If legacy wasn't deployed, use Helm rollback
helm rollback fusion-council-api-legacy -n prod
```

### Image Rollback

If the issue is a specific bad image SHA:

```bash
# Find the last good SHA from GitHub Actions history
# Edit the Helm values to pin the previous image tag
# Example using a previous sha commit:
# dev.yaml:  image.tag: "sha-6fe9226"  (last known good)
# Then commit and push; ArgoCD syncs within 3 minutes.
```

---

## Verification Checklist

After cutover, run through this checklist:

- [ ] Prod smoke test CronJob passes (check last 3 runs in k8s-workbench)
- [ ] `fusion_council_run_total{engine="langgraph"}` Prometheus metric is incrementing
- [ ] `/v1/runs` endpoint returns runs with `orchestrator_engine=langgraph` in DB
- [ ] `/readyz` returns 200 for all API pods
- [ ] Zero `CrashLoopBackOff` pods in prod
- [ ] `decision_log` table is receiving new entries from live runs
- [ ] Shadow deployment continues to run (parity monitoring ongoing)
- [ ] No spike in `run.failed` status count
- [ ] Checkpoint retention CronJob is running on schedule (check `kubectl get jobs`)

---

## Key Contacts

| Role | Contact | Notes |
|---|---|---|
| App owner | @moltbot | Primary on-call |
| k8s-infrastructure | OpenClaw cron `fusion-council-api-smoke` | Alerts to `#kai-ops` |
| Prometheus alerts | `K8sPodNotReady`, `K8sDeploymentReplicasMismatch` | Route to `#kai-ops` |
| Vault secrets | Secret path: `secret/data/fusion-council/dev` | ESO syncs hourly |

---

## Troubleshooting

### LangGraph pod CrashLoopBackOff

**Symptom**: `kubectl get pods -n prod | grep langgraph` shows crashloop.

**Check**:

1. `kubectl logs <pod> -n prod` — look for `AsyncPostgresSaver` setup errors
2. `LANGGRAPH_CHECKPOINT_DB_URL` env var is correctly set
3. PostgreSQL is reachable from the pod:

   ```bash
   kubectl exec -it <pod> -n prod -- python3 -c \
     "import asyncpg; import asyncio; asyncio.run(asyncpg.connect('<DB_URL>'))"
   ```

### Shadow gate failing `stage_parity_rate`

**Root cause**: LangGraph is generating a different stage ordering than legacy for the same prompt pattern.

**Diagnosis**:

```bash
# Check which runs are failing parity
kubectl exec -n dev deploy/postgres-dev-postgresql-0 -- psql -U app -d appdb -c "
SELECT run_id, stage_count, stage_order_match, diff_summary
FROM run_shadow_diff
WHERE logged_at > NOW() - INTERVAL '24 hours'
  AND stage_order_match = false
LIMIT 10;"
```

Common fix: tune the model selection or scoring thresholds in `model_selection.py`.

### Shadow gate failing `terminal_corruption_count > 0`

**Root cause**: LangGraph produced a final answer when legacy produced none, or vice versa.

**Action**: This is a hard FAIL. Do not promote. Investigate the specific run_id and roll back prod if already promoted.
