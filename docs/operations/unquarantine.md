# Unquarantine Runbook

## When to unquarantine

A pair is quarantined when it has 3+ consecutive low-health updates
(`health_score < 0.3`). Unquarantine is appropriate when:

- The upstream provider has fixed the underlying issue
- You have new evidence the upstream is healthy (manual probe, or recent successful runs)
- The catalog still intends to use this upstream (verify in `config/models.yaml`)

## How to unquarantine

```bash
kubectl -n dev exec -it deploy/fusion-council-api-legacy-api -- \
  python -m fusion_council_service.scripts.unquarantine_cli \
    <provider> <provider_model> "reason in 1 line, e.g. PR #30 fixed kimi-k2.6 timeout"

# Example:
kubectl -n dev exec -it deploy/fusion-council-api-legacy-api -- \
  python -m fusion_council_service.scripts.unquarantine_cli \
    opencode_go qwen3.7-max "manual probe shows 200 OK on 2026-06-02"
```

## What gets recorded

- `provider_health.quarantined` → 0
- `provider_health.consecutive_low_health_count` → 0
- `provider_health.quarantined_at` and `quarantine_reason` → NULL
- One `provider_quarantine_events` row with `event_type='unquarantine'`, `reason=<your reason>`, `created_at=now`

## Who has the admin keys

The CLI runs inside the API pod using `DATABASE_URL` from the existing
ESO-managed secret. No additional credentials are required.
