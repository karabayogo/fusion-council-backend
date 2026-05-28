# Phase 10 — Long-Term Governance

## Overview

Phase 10 defines the ongoing operational model for fusion-council-backend after the LangGraph engine cutover (Phase 9). It covers repository ownership, operational responsibilities, upgrade procedures, on-call runbooks, and the feedback loop that drives model selection improvement.

---

## Repository Inventory & Ownership

### fusion-council-backend

**Location**: `https://github.com/karabayogo/fusion-council-backend`

| Aspect | Owner | Notes |
|---|---|---|
| Source code, Dockerfile | @moltbot | Primary author |
| CI/CD (docker-publish.yml) | @moltbot | Auto-build on main push |
| Model catalog (`config/models.yaml`) | @moltbot | Quarterly review |
| Orchestration engines | @moltbot | Legacy + LangGraph |

### k8s-workbench

**Location**: `https://github.com/karabayogo/k8s-workbench`

| Aspect | Owner | Notes |
|---|---|---|
| ArgoCD Applications | @moltbot | Fusion Council App definitions |
| Dev Helm values | @moltbot | Image tag management |
| Promotion workflow | @moltbot | dev → pp → prod |

### homelab-gitops

**Location**: `https://github.com/karabayogo/homelab-gitops`

| Aspect | Owner | Notes |
|---|---|---|
| Helm chart (fusion-council-api) | @moltbot | Chart templates |
| Per-env values (dev/pp/prod) | @moltbot | Environment configs |
| Backup CronJob | @moltbot | Nightly DB backup |
| Smoke test CronJob | @moltbot | Daily smoke on all envs |
| Checkpoint retention CronJob | @moltbot | LangGraph checkpoint pruning |
| Shadow validation CronJob | @moltbot | Shadow parity gate |

---

## Branching & Release Model

```
main (protected)
  └── sha-<short-sha>        — auto-built by CI on every push
  └── v<semver>              — version tag, also auto-built
```

### Release Tags

| Tag format | Trigger | Build target |
|---|---|---|
| `sha-<7-char-sha>` | Every push to `main` | dev |
| `v<major>.<minor>.<patch>` | Git tag pushed to `main` | prod |

Version tags should follow [Semantic Versioning](https://semver.org/). Cut a release tag when:

- A significant model catalog change is deployed
- A schema migration is included
- A security fix is shipped
- A milestone feature is promoted to prod

### Release Checklist

Before tagging `vX.Y.Z`:

- [ ] All smoke tests pass (dev, pp, prod)
- [ ] Shadow parity gate is passing on `dev` (if langgraph is active)
- [ ] No pending Postgres schema migrations with `pending=true` in `schema_migrations`
- [ ] Changelog entry written at `docs/changelog/YYYY-MM-DD-<short-desc>.md`
- [ ] Docker image verified pullable from GHCR

---

## Changelog & Documentation Policy

### Changelog Entries

Each meaningful change must have a changelog entry. Format:

**File**: `docs/changelog/YYYY-MM-DD-<imperative-desc>.md`

```markdown
# <YYYY-MM-DD> — <Short descriptive title>

## What changed

- <Bullet describing the change>
- <Another bullet>

## Why

<One-sentence explanation of the motivation or issue fixed>

## How to migrate / roll back

<Any action required by operators, or "no action required">

## Related

- PR #<number>
- Phase <N> milestone
```

### Existing Changelog Files

| File | Status |
|---|---|
| `docs/changelog/2026-05-17-epic-f-raw-answer-deprecation.md` | ✅ Active |

### When to write a changelog entry

- New model added to `models.yaml`
- New API endpoint added
- Schema change (new table, column, migration)
- Breaking change to API response contract
- New orchestration engine mode
- Operational procedure change (new CronJob, alert added/changed)
- Security fix

### Docs requiring updates on change

| Document | Trigger to update |
|---|---|
| `docs/deployment.md` | CI/CD changes, environment changes, Helm chart changes |
| `docs/OPERATIONS.md` | Alerts, monitoring, known issues, CronJobs |
| `docs/Phase-9-Cutover-Runbook.md` | When cutover stages are completed |
| `docs/Phase-10-Long-Term-Governance.md` | This file — any governance procedure change |

---

## Prometheus Monitoring & Alert Routing

The service exposes metrics to Prometheus via the `ServiceMonitor` template in the Helm chart.

### Key Metrics

| Metric | Meaning |
|---|---|
| `fusion_council_run_total{engine, mode}` | Total runs by engine and mode |
| `fusion_council_run_errors_total{engine, error_code}` | Run errors |
| `fusion_council_candidate_latency_ms{alias, stage}` | Per-model latency histogram |
| `fusion_council_candidate_health_score{alias}` | Health score per model/provider pair |
| `fusion_council_checkpoint_rows_deleted{retention_days}` | Checkpoint retention job output |
| `fusion_council_shadow_parity_passed{lookback_hours}` | Shadow validation gate result |

### Active Alerts

| Alert | Fire condition | Routing |
|---|---|---|
| `K8sPodCrashLoopBackOff` | Pod restart loop detected | `#kai-ops` Discord |
| `K8sPodHighRestartCount` | >10 restarts in 10m | `#kai-ops` Discord |
| `K8sPodNotReady` | Pod not Ready for >2m | `#kai-ops` Discord |
| `K8sDeploymentReplicasMismatch` | Desired replicas not met | `#kai-ops` Discord |
| `K8sServiceHasNoEndpoints` | Service has no backing pods | `#kai-ops` Discord |
| `FusionCouncilRunErrorRate` | >5% error rate in 5m | `#kai-ops` Discord |

All Prometheus alerts route to `#kai-ops` via Alertmanager. The webhook is stored in Vault at `secret/observability/alertmanager-discord` and synced every 6 hours.

### Adding a new alert

1. Add PrometheusRule to the Helm chart under `templates/prometheusrules.yaml`:
   ```yaml
   apiVersion: monitoring.coreos.com/v1
   kind: PrometheusRule
   metadata:
     name: fusion-council-api-custom-alerts
     labels:
       prometheus: prometheus
   spec:
     groups:
       - name: fusion-council-api
         rules:
           - alert: FusionCouncilCustomAlert
             expr: fusion_council_run_errors_total > 0.05 * fusion_council_run_total
             for: 5m
             labels:
               severity: warning
             annotations:
               summary: "Custom alert"
   ```
2. Commit to `homelab-gitops`
3. ArgoCD syncs within 3 minutes
4. Verify rule appears in Prometheus UI

---

## Database Migrations

Schema changes are managed through the `apply_schema_migrations` function in `db.py`. The function is called on every startup and API init.

### Writing a new migration

1. Add a function `_migration_<version>_<desc>(db)` to `db.py`:
   ```python
   def _migration_YYYYMMDD_desc(db):
       columns = _table_columns(db, "my_table")
       if "new_column" not in columns:
           execute_sql(db, "ALTER TABLE my_table ADD COLUMN new_column TEXT")
           commit_tx(db)
   ```
2. Add the tuple to the `migrations` list in `apply_schema_migrations`:
   ```python
   ("YYYYMMDD_desc", _migration_YYYYMMDD_desc),
   ```
3. Write a changelog entry

### Migration rules

- Always use `IF NOT EXISTS` or conditional column checks — migrations must be **idempotent**
- Acquire PostgreSQL advisory lock before applying using `pg_advisory_lock(hashtext('fusion_council_schema_migrations'))`
- Backfill NULLable columns with safe defaults before marking migration complete
- Never drop a column in the same migration that stops using it — always two-phase (deprecate → drop)

---

## Model Catalog Operations

### Adding a new model

1. Edit `config/models.yaml`:
   ```yaml
   - alias: provider/model-alias
     provider: provider_key  # must match provider registry
     provider_model: actual-provider-model-name
     enabled: true
     family: family-name
     tier: frontier  # or "mid", "backup"
     role_bias: primary  # primary | reviewer | synthesis | verification | backup | creative
     timeout_seconds: 300
   ```
2. Write a changelog entry
3. Create a test run to validate:

```bash
curl -X POST http://localhost:8080/v1/runs \
  -H "Authorization: Bearer <key>" \
  -H "Content-Type: application/json" \
  -d '{"mode":"single","prompt":"What is 1+1?","requested_models":["provider/model-alias"]}'
```

4. Verify the model runs in council/fusion modes too

### Removing/disabling a model

1. Set `enabled: false` in `config/models.yaml`
2. Verify no active runs depend on it by checking `run_candidates`:
   ```bash
   kubectl exec -n dev deploy/postgres-dev-postgresql-0 -- psql -U app -d appdb -c \
     "SELECT alias, COUNT(*) FROM run_candidates WHERE alias='provider/model-alias' GROUP BY alias;"
   ```
3. Wait for in-flight runs to complete before fully removing the entry
4. Write a changelog entry

### Provider key rotation

1. Vault: Update the secret at `secret/data/fusion-council/<env>`
2. ESO: Within 1 hour, ExternalSecret syncs automatically. Force refresh:
   ```bash
   kubectl annotate externalsecret fusion-council-secrets -n dev force-sync=$(date +%s) --overwrite
   ```
3. Restart pods to pick up new env var:
   ```bash
   kubectl rollout restart deployment/fusion-council-api-api -n dev
   kubectl rollout restart deployment/fusion-council-api-worker -n dev
   kubectl rollout restart deployment/fusion-council-api-api -n prod
   kubectl rollout restart deployment/fusion-council-api-worker -n prod
   ```
4. Verify with smoke test

---

## Upgrade Procedures

### Docker image upgrade (routine)

1. CI automatically builds on every push to `main`
2. CI commits the new `sha-<sha>` tag to `k8s-workbench` values files
3. ArgoCD detects the drift and syncs the new image automatically within 3 minutes
4. Manual verification:
   ```bash
   kubectl rollout status deployment/fusion-council-api-api -n dev
   kubectl get pods -n dev -l app=fusion-council-api-api
   ```

### Helm chart upgrade (homelab-gitops)

1. Update the Helm chart version or values in `homelab-gitops`
2. Commit and push
3. ArgoCD syncs automatically
4. Monitor: `kubectl get events -n dev --field-selector reason=Synced`

### Version tag promotion

1. Tag the commit in GitHub: `git tag v0.2.0 && git push origin v0.2.0`
2. CI builds `ghcr.io/karabayogo/fusion-council-backend:v0.2.0`
3. Update `k8s-workbench/values/fusion-council-api/prod.yaml` to use the semver tag instead of `sha-...`
4. Commit and push — ArgoCD syncs

---

## Nightly / Periodic Jobs

| Job | Schedule | Purpose | Script |
|---|---|---|---|
| Smoke test | `15 7 * * *` (07:15 UTC) | Daily health check, alerts on failure | CronJob `*-smoke` |
| Shadow validation | `0 3 * * *` (03:00 UTC) | LangGraph parity gate | `shadow_validate.py` |
| Checkpoint retention | `0 4 * * *` (04:00 UTC) | Prune LangGraph checkpoints older than 7 days | `checkpoint_retention.py` |
| DB backup | `0 2 * * *` (02:00 UTC) | Nightly DB backup | CronJob `*-backup` |

### Monitoring job health

```bash
# List recent jobs and their status
kubectl get jobs -n dev --sort-by='.status.startTime' | tail -10

# View logs for a specific job run
kubectl logs job/<job-name> -n dev --tail=30

# View a CronJob's last scheduled run time
kubectl get cronjob -n dev <cron-name> -o jsonpath='{.status.lastScheduleTime}'
```

### Job failure response

If a CronJob fails repeatedly:

1. Describe the job for events: `kubectl describe job/<job-name> -n dev`
2. Check pod logs: `kubectl logs job/<job-name>-xxx -n dev`
3. Common causes:
   - `checkpoint_retention`: DATABASE_URL env var missing or wrong; Postgres unreachable
   - `shadow_validate`: DATABASE_URL wrong; `run_shadow_diff` table empty
   - `smoke`: API pod not responding; auth key wrong
4. Fix and re-run manually to confirm:
   ```bash
   kubectl create job --from=cronjob/fusion-council-api-<name>-<job> test-run -n dev
   ```

---

## Open-Source / Third-Party Dependencies

| Dependency | Version | Usage | Policy |
|---|---|---|---|
| FastAPI | `>=0.100` | HTTP framework | Pin in pyproject.toml |
| LangGraph | from pyproject.toml | Orchestration engine | Review release notes before major versions |
| OpenTelemetry | from pyproject.toml | Distributed tracing | Keep minor releases current |
| Pydantic | from pyproject.toml | Settings/validation | Keep minor releases current |
| Anthropic SDK | from pyproject.toml | MiniMax provider | Review API deprecations |
| asyncpg | from pyproject.toml | PostgreSQL async driver | Keep minor releases current |

Dependency update procedure:

1. Run `uv pip compile --upgrade pyproject.toml` to check for new versions
2. Run the full test suite: `make test`
3. Run smoke test locally: `make smoke`
4. Deploy to dev, let smoke CronJob run at 07:15 UTC next day
5. If green, promote to pp then prod

---

## Chaos & Disaster Recovery

### Pod-level failures

- `CrashLoopBackOff` → Kubernetes automatically restarts up to `restartPolicy`
- Persistent crashloop → check logs, fix image or config, push fix
- Worker `CrashLoopBackOff` → check DB connectivity and provider credentials

### Database failures

- **PostgreSQL down**: API pods return 503 via `/readyz` failing. Worker enters standby.
  - Recovery: PostgreSQL auto-recovers; worker resumes polling on its own.
- **SQLite (dev only)**: Corruption → restore from backup CronJob snapshot.
  ```bash
  # Find latest backup
  kubectl exec -n dev statefulset/fusion-council-api-langgraph -- ls /backups/
  # Restore
  kubectl cp /path/to/backup.sqlite <pod>:/app/data/fusion_council.db
  ```

### Complete cluster failure

1. Wait for k3s to recover (self-hosted, usually within minutes)
2. ArgoCD re-syncs all Applications automatically with `selfHeal: true`
3. Monitor: `kubectl get pods -A | grep fusion-council`
4. If ArgoCD doesn't self-heal within 10 minutes, manually resync:
   ```bash
   argocd app sync fusion-council-api-dev-langgraph
   argocd app sync fusion-council-api-prod-langgraph
   ```

### Vault ESO sync failure

If `ExternalSecret` fails to sync passwords:

```bash
# Force a sync
kubectl annotate externalsecret fusion-council-secrets -n dev \
  force-sync=$(date +%s) --overwrite

# If that fails, delete and recreate
kubectl delete externalsecret fusion-council-secrets -n dev
# ESO recreates within 5 minutes
```

---

## On-Call Reference

### First response checklist

1. Check which component is failing: API, worker, or database?
2. Check current Prometheus alerts: look for `K8sPodNotReady`, `K8sPodCrashLoopBackOff` in `#kai-ops`
3. Get pod status: `kubectl get pods -n <env> -l app=fusion-council-api`
4. Get logs: `kubectl logs -n <env> deployment/fusion-council-api-api --tail=50`
5. Check run database state: `kubectl exec -n <env> deploy/postgres-<env>-postgresql-0 -- psql -U app -d appdb -c "SELECT run_id, status, error_code FROM runs WHERE status='failed' ORDER BY created_at DESC LIMIT 10;"`
6. Check decision log: same approach, `SELECT * FROM decision_log WHERE pending=1 ORDER BY created_at DESC LIMIT 5;`

### Escalation

| Scenario | Contact |
|---|---|
| Prometheus alert firing | OpenClaw `observability-alert-relay` cron posts to `#kai-ops` |
| Smoke test failing (prod) | OpenClaw `fusion-council-api-smoke` alerts to `#kai-ops` |
| Database backup missing | Check backup CronJob in `kubectl get cronjob -n prod` |
| LangGraph engine bug | Roll back to legacy via `kubectl scale` (see Phase 9 rollback) |
| Vault secret missing | Reload ESO: `kubectl annotate externalsecret` |

---

## Open TODO Items

These are known operational gaps that should be addressed in future iterations:

| Item | Priority | Notes |
|---|---|---|
| K8sContainerWaiting alert for ImagePullBackOff | High | ImagePullBackOff doesn't trigger existing alerts |
| Backup CronJob images use broken `sha-REPLACE_ME` placeholder | High | Both `pp` and `prod` namespaces have broken backup images |
| Decision log rotation not automated | Medium | `rotate_decision_log()` called per-request but not on a cron |
| No Prometheus `fusion_council_*` recording rules | Medium | Raw metrics without aggregated recording rules are less efficient |
| No `healthy models available` alert | Medium | Alert when no model in catalog has `health_score > 0.5` |

---

## Feedback Loop — Decision Log & Outcome Tracking

The `decision_log` table enables outcome-driven model selection improvement.

### Workflow

1. **Outcome submission**: Consumer calls `PATCH /v1/runs/{run_id}/outcome` with a `rating` (helpful/not_helpful/partial) and optional `outcome_raw` float.
2. **Outcome resolution**: `reflection` is generated; the entry is marked `pending=0`, `resolved_at` set.
3. **Quarterly review**: @moltbot reviews the resolution rate and score distributions:
   ```bash
   kubectl exec -n prod deploy/postgres-prod-postgresql-0 -- psql -U app -d appdb -c "
   SELECT rating, COUNT(*), AVG(outcome_raw) as avg_score
   FROM decision_log
   WHERE resolved_at > NOW() - INTERVAL '90 days'
   GROUP BY rating;"
   ```
4. **Action**: Models with >30% `not_helpful` rating are downgraded in the catalog or have their `role_bias` adjusted.

### Reflection role

The `REFLECTION_ROLE_ALIAS` setting controls which model generates reflection for each run. Currently set to `minimax/MiniMax-M2.7`. Change via `APP_ENV=production` in Helm values if a better model becomes available.
