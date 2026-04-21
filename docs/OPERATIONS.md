# Fusion Council Backend — Operations Guide

## Quick Reference

| Action | Command |
|--------|---------|
| Check pod status | `kubectl get pods -n dev -l app=fusion-council-api` |
| View API logs | `kubectl logs -n dev deployment/fusion-council-api-api --tail=50` |
| View worker logs | `kubectl logs -n dev deployment/fusion-council-api-worker --tail=50` |
| Test health | `curl http://fusion-council-api.dev.svc.cluster.local:8080/healthz` |
| Test ready | `curl http://fusion-council-api.dev.svc.cluster.local:8080/readyz` |
| Restart deployment | `kubectl rollout restart deployment/fusion-council-api-api -n dev` |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_PATH` | Yes | Path to SQLite DB (e.g., `/app/data/fusion_council.db`) |
| `SERVICE_API_KEYS` | Yes | Comma-separated list of API keys for auth |
| `SERVICE_ADMIN_API_KEYS` | No | Comma-separated list of admin API keys |
| `MINIMAX_TOKEN_PLAN_API_KEY` | Yes | MiniMax API token |
| `OLLAMA_API_KEY` | Yes | Ollama API token |
| `SKIP_PROVIDER_VALIDATION` | No | Set to `1` to skip provider validation at startup |

## Provider Validation

By default, the service validates MiniMax and Ollama connectivity at startup. If either fails,
the pod enters `CrashLoopBackOff` with an authentication error.

To bypass this (e.g., in CI, air-gapped environments, or during maintenance):

```bash
export SKIP_PROVIDER_VALIDATION=1
```

This allows the service to start even if upstream providers are unavailable.
The validation will be skipped, and the service will proceed to serve requests.
Runtime calls to providers will still fail if the credentials are invalid.

## Secret Management

Secrets flow: **Vault** → **ExternalSecret** → **K8s Secret** → **Pod**

The ExternalSecret reconciles every hour. To force a refresh:

```bash
kubectl annotate externalsecret fusion-council-secrets -n dev force-sync=$(date +%s) --overwrite
```

After updating Vault, restart the deployment to pick up new secrets:

```bash
kubectl rollout restart deployment/fusion-council-api-api -n dev
kubectl rollout restart deployment/fusion-council-api-worker -n dev
```

## Common Issues

### CrashLoopBackOff: MiniMax auth failed

**Symptom:** Pod logs show `anthropic.AuthenticationError: 401 - invalid api key`

**Causes:**
1. Vault secret `MINIMAX_TOKEN_PLAN_API_KEY` is stale
2. ExternalSecret hasn't synced yet
3. API key was rotated but not updated in Vault

**Fix:**
1. Update Vault with the new key
2. Force ExternalSecret refresh (see above)
3. Restart deployment

### CrashLoopBackOff: Ollama validation failed

**Symptom:** Pod logs show `Ollama /api/tags request failed`

**Causes:**
1. Ollama cloud is unreachable
2. `OLLAMA_API_KEY` is invalid

**Workaround:**
Set `SKIP_PROVIDER_VALIDATION=1` in the Helm values to skip validation at startup.

## CI/CD Pipeline

GitHub Actions workflow (`.github/workflows/docker-publish.yml`):

1. Builds Docker image on push to `main`
2. Pushes to `ghcr.io/karabayogo/fusion-council-backend:sha-<short-sha>`
3. Updates `k8s-workbench` with new image tag
4. ArgoCD syncs the change automatically

## Monitoring

The service exposes:

- `/healthz` — Liveness probe (always returns 200 if process is running)
- `/readyz` — Readiness probe (returns 503 until catalog is loaded)
- `/metrics` — Basic app metrics (env, model count)

## Rollback

To rollback to a previous image:

```bash
cd /home/moltbot/repositories/k8s-workbench
# Edit values/fusion-council-api/dev.yaml, change image.tag
git add values/fusion-council-api/dev.yaml
git commit -m "rollback: revert to sha-<old>"
git push
```

ArgoCD will sync the change within 3 minutes.
