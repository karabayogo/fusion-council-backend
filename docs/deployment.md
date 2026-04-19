# Deployment Guide — Fusion Council Service

## Overview

The Fusion Council Service is deployed to a self-hosted k3s cluster via **GitOps** using ArgoCD. Container images are built by GitHub Actions and the image tag is automatically committed to the `k8s-workbench` repository, which ArgoCD monitors and syncs to the cluster.

```
fusion-council-backend  ──build──▶  ghcr.io/karabayogo/fusion-council-backend:<sha>
                                        │
                                        │  docker-publish.yml commits tag to k8s-workbench
                                        ▼
karabayogo/k8s-workbench  ──ArgoCD sync──▶  k3s cluster
                                            ├── dev   (https://fusion-council-dev.local)
                                            ├── pp    (https://fusion-council-pp.local)
                                            └── prod  (cluster-3)
```

---

## Environments

| Environment | ArgoCD Application | Helm Values | Namespace |
|---|---|---|---|
| `dev` | `fusion-council-api-dev` | `values/fusion-council-api/dev.yaml` | `dev` |
| `pp` (staging) | `fusion-council-api-pp` | `values/fusion-council-api/pp.yaml` | `pp` |
| `prod` | `fusion-council-api-prod` | `values/fusion-council-api/prod.yaml` | `prod` |

---

## Repositories

| Repository | Purpose | Location |
|---|---|---|
| `fusion-council-backend` | Source code, Dockerfile, CI workflows | `karabayogo/fusion-council-backend` |
| `k8s-workbench` | Helm charts + ArgoCD Applications for our apps | `karabayogo/k8s-workbench` |
| `homelab-gitops` | GitOps for third-party/upstream apps (not this service) | `karabayogo/homelab-gitops` |

---

## Image Build Pipeline

### docker-publish.yml

Triggered on every push to `main` (or via `workflow_dispatch`).

Steps:

1. `actions/checkout@v5` — checks out the commit SHA to build
2. `docker/setup-buildx-action@v3` — sets up Docker buildx
3. `docker/login-action@v3` — authenticates to `ghcr.io`
4. `docker/metadata-action@v5` — generates tags (`sha-<sha>`, semver)
5. `docker/build-push-action@v5` — builds and pushes the image to `ghcr.io/karabayogo/fusion-council-backend:<tag>`
6. **Smoke test** — loads the image and runs `--help` to verify it starts
7. **k8s-workbench update** — if on `main`, commits the new image tag to `k8s-workbench/values/fusion-council-api/{env}.yaml`
8. ArgoCD detects the commit and syncs the new image to the cluster

### Image Tag Strategy

Images are tagged by Git commit SHA for traceability:

```
ghcr.io/karabayogo/fusion-council-backend:sha-e0bf702
```

The full semver tag (e.g. `v0.1.0`) is applied only when a version tag is pushed to `main`.

---

## CI/CD Variables (Required Secrets)

Configure these in `Settings > Secrets and variables > Actions` on the `fusion-council-backend` repository:

| Secret | Purpose |
|---|---|
| `GITHUB_TOKEN` | Provided automatically by GitHub Actions |
| `K8S_WORKBENCH_PUSH_TOKEN` | PAT with `repo` scope to commit image tags to `k8s-workbench` |

> **Note:** `K8S_WORKBENCH_PUSH_TOKEN` is required for the `k8s-workbench` auto-commit step. Without it, the image is still built and pushed, but the tag update step is skipped.

---

## Helm Chart

Chart location: `k8s-workbench/helm/fusion-council-api/`
Values files: `k8s-workbench/values/fusion-council-api/{dev,pp,prod}.yaml`

### Key Values by Environment

**dev.yaml** (development):

```yaml
image:
  tag: "sha-<latest>"          # updated by CI on every main push

api:
  replicaCount: 1
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      cpu: 1000m
      memory: 1Gi

worker:
  replicaCount: 1

persistence:
  size: 5Gi
```

**pp.yaml** (staging):

```yaml
image:
  tag: "sha-<latest>"

api:
  replicaCount: 0               # API disabled in staging (worker only)
  resources:
    limits:
      cpu: 1500m
      memory: 1536Mi

worker:
  replicaCount: 0               # scaled down in pp

persistence:
  size: 8Gi

backup:
  enabled: true
```

### Persistence

All environments use a PVC for the SQLite database at `/app/data/fusion_council.db`.

- **dev**: 5Gi
- **pp**: 8Gi
- **prod**: (see `prod.yaml`)

> **Important:** SQLite must live on local storage, not NFS or SMB. The Helm chart enforces this.

---

## Environment Variables

Set via `commonEnv` in the Helm values, or via a Kubernetes Secret referenced in the deployment:

| Variable | Description | Example |
|---|---|---|
| `SERVICE_API_KEYS` | Comma-separated bearer tokens for callers | `dev-key-1,dev-key-2` |
| `SERVICE_ADMIN_API_KEYS` | Admin tokens (cancel, diagnostics) | `admin-key-1` |
| `MINIMAX_TOKEN_PLAN_API_KEY` | MiniMax Token Plan API key | — |
| `MINIMAX_ANTHROPIC_BASE_URL` | MiniMax Anthropic-compatible base URL | `https://api.minimax.io/anthropic` |
| `OLLAMA_API_KEY` | Ollama Cloud API key | — |
| `OLLAMA_BASE_URL` | Ollama Cloud base URL | `https://ollama.com` |
| `DATABASE_PATH` | SQLite DB path inside container | `/app/data/fusion_council.db` |
| `MODEL_CATALOG_PATH` | Path to models YAML | `/app/config/models.yaml` |
| `APP_ENV` | `development` / `staging` / `production` | `development` |
| `HOST` | Listen address | `0.0.0.0` |
| `PORT` | Listen port | `8080` |

---

## Health Endpoints

The running service exposes:

| Endpoint | Auth | Description |
|---|---|---|
| `GET /healthz` | None | Returns `{"ok": true}` — always available |
| `GET /readyz` | None | Checks DB, worker heartbeat, model keys — returns `503` if not ready |
| `GET /v1/models` | Bearer | Returns model catalog with enablement status |

---

## Deployment Lifecycle

### Adding a new environment (e.g. prod)

1. Create `k8s-workbench/values/fusion-council-api/prod.yaml` with production sizing
2. Create `k8s-workbench/argocd/apps/fusion-council-api-prod.yaml` referencing it
3. Apply the `Application` to ArgoCD: `kubectl apply -f argocd/apps/fusion-council-api-prod.yaml`
4. Add prod values to the promotion workflow

See `k8s-workbench/PROMOTION-RUNBOOK.md` for the full promotion process from dev → pp → prod.

### Triggering a manual deploy

Via `workflow_dispatch` on the `docker-publish.yml` workflow in GitHub Actions:

- Provides the `--load` flag to make the image available locally without pushing
- Useful for testing the Dockerfile locally before pushing

### Rolling back

If a bad image is deployed:

1. Find the last good commit SHA from GitHub Actions history
2. Manually revert the `tag` value in the relevant `values/fusion-council-api/{env}.yaml`
3. ArgoCD will detect the drift and sync to the previous image
4. Optionally: open a revert PR against `k8s-workbench`

---

## Container Resource Limits

| Component | CPU Request | CPU Limit | Memory Request | Memory Limit |
|---|---|---|---|---|
| API (dev) | 100m | 1000m | 256Mi | 1Gi |
| API (pp) | 200m | 1500m | 512Mi | 1536Mi |
| Worker (dev) | 250m | 2000m | 512Mi | 2Gi |
| Worker (pp) | 500m | 2500m | 1Gi | 3Gi |

> Tune these based on actual load. The worker is CPU-bound during model inference.

---

## Local Development

For local development outside the cluster, use `docker compose`:

```bash
# Create .env from template
cp .env.example .env
# Fill in your API keys

# Start the full stack
docker compose up --build

# Run only the API (for fast iteration)
docker compose up --build api
```

The `docker-compose.yml` starts both the API and worker containers, with a shared volume for the SQLite DB.
