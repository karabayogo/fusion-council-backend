#!/usr/bin/env bash
# sync-provider-keys.sh — sync active provider keys from ~/.hermes/.env → k8s cluster
#
# Reads the LAST uncommented OPENCODE_GO_API_KEY and MINIMAX_API_KEY from
# ~/.hermes/.env and applies them to all fusion-council-api deployments in
# the dev namespace.
#
# Idempotent — no-op when keys are already in sync.
# Exit 0 on success (whether or not changes were made), non-zero on error.
#
# Usage:
#   ./scripts/sync-provider-keys.sh            # dev namespace (default)
#   NAMESPACE=staging ./scripts/sync-provider-keys.sh
#
# Called by:
#   - E2E test fixture (sync_provider_keys)
#   - Hermes cron job (periodic sync)
#   - Manual invocation

set -euo pipefail

ENV_FILE="${HOME}/.hermes/.env"
NAMESPACE="${NAMESPACE:-dev}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "sync-provider-keys: ${ENV_FILE} not found — nothing to sync"
  exit 0
fi

# ── Extract last uncommented value for each key ──────────────────────────

extract_key() {
  local key="$1"
  grep "^${key}=" "$ENV_FILE" | tail -1 | cut -d= -f2-
}

OPENCODE_GO_KEY=$(extract_key "OPENCODE_GO_API_KEY")
MINIMAX_KEY=$(extract_key "MINIMAX_API_KEY")

if [[ -z "$OPENCODE_GO_KEY" && -z "$MINIMAX_KEY" ]]; then
  echo "sync-provider-keys: no active provider keys in ${ENV_FILE}"
  exit 0
fi

# ── Discover fusion-council deployments ──────────────────────────────────

mapfile -t DEPLOYS < <(
  kubectl get deploy -n "$NAMESPACE" -o json 2>/dev/null \
    | jq -r '.items[] | select(.metadata.name | contains("fusion-council-api")) | .metadata.name'
)

if [[ ${#DEPLOYS[@]} -eq 0 ]]; then
  echo "sync-provider-keys: no fusion-council-api deployments in ${NAMESPACE}"
  exit 0
fi

# ── Sync each key ────────────────────────────────────────────────────────

sync_key() {
  local key_name="$1" new_value="$2"
  [[ -z "$new_value" ]] && return 0

  # Read current value from first deployment
  local current
  current=$(kubectl get deploy -n "$NAMESPACE" "${DEPLOYS[0]}" \
    -o "jsonpath={.spec.template.spec.containers[0].env[?(@.name=='${key_name}')].value}" 2>/dev/null)

  if [[ "$current" == "$new_value" ]]; then
    echo "  ${key_name}: already in sync"
    return 0
  fi

  echo "  ${key_name}: updating (${current:0:12}... → ${new_value:0:12}...)"
  for dep in "${DEPLOYS[@]}"; do
    kubectl set env deployment "$dep" -n "$NAMESPACE" --overwrite \
      "${key_name}=${new_value}" >/dev/null
  done
  return 1  # signal that changes were made
}

CHANGED=0
sync_key "OPENCODE_GO_API_KEY" "$OPENCODE_GO_KEY" || CHANGED=1
sync_key "MINIMAX_API_KEY"     "$MINIMAX_KEY"     || CHANGED=1

if [[ $CHANGED -eq 1 ]]; then
  echo "  Waiting for rollout..."
  for dep in "${DEPLOYS[@]}"; do
    kubectl rollout status deployment "$dep" -n "$NAMESPACE" --timeout=120s >/dev/null
  done
  echo "sync-provider-keys: rollout complete"
else
  echo "sync-provider-keys: all keys in sync"
fi
