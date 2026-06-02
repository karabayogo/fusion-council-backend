-- fusion-council-service schema
-- Compatible with both PostgreSQL (production) and SQLite (tests/local).

CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  owner_token_hash TEXT NOT NULL,
  mode TEXT NOT NULL,
  prompt TEXT NOT NULL,
  system_prompt TEXT,
  requested_models_json TEXT,
  status TEXT NOT NULL,
  temperature REAL NOT NULL,
  max_output_tokens INTEGER NOT NULL,
  deadline_seconds INTEGER NOT NULL,
  deadline_at TEXT NOT NULL,
  deadline_applied INTEGER NOT NULL DEFAULT 0,
  degraded_reason TEXT,
  deadline_trigger_stage TEXT,
  metadata_json TEXT NOT NULL,
  current_stage TEXT,
  current_stage_message TEXT,
  progress_percent REAL,
  models_planned INTEGER NOT NULL DEFAULT 0,
  models_completed INTEGER NOT NULL DEFAULT 0,
  models_failed INTEGER NOT NULL DEFAULT 0,
  last_heartbeat_at TEXT,
  final_answer TEXT,
  final_summary TEXT,
  final_confidence REAL,
  verification_json TEXT,
  error_code TEXT,
  error_message TEXT,
  created_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT
);

CREATE TABLE IF NOT EXISTS run_events (
  event_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_run_events_run_seq ON run_events(run_id, seq);

CREATE TABLE IF NOT EXISTS run_candidates (
  candidate_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  alias TEXT NOT NULL,
  provider TEXT NOT NULL,
  provider_model TEXT NOT NULL,
  stage TEXT NOT NULL,
  status TEXT NOT NULL,
  execution_order INTEGER,
  latency_ms INTEGER,
  input_tokens INTEGER,
  output_tokens INTEGER,
  normalized_answer TEXT,
  score_json TEXT,
  error_code TEXT,
  error_message TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS schema_migrations (
  version TEXT PRIMARY KEY,
  applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS worker_state (
  worker_id TEXT PRIMARY KEY,
  last_heartbeat_at TEXT NOT NULL,
  current_run_id TEXT
);

CREATE TABLE IF NOT EXISTS model_catalog (
  alias TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  provider_model TEXT NOT NULL,
  family TEXT NOT NULL,
  tier TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  validated_at TEXT,
  validation_error TEXT
);

CREATE TABLE IF NOT EXISTS provider_health (
  provider TEXT NOT NULL,
  provider_model TEXT NOT NULL,
  total_attempts INTEGER NOT NULL DEFAULT 0,
  successes INTEGER NOT NULL DEFAULT 0,
  failures INTEGER NOT NULL DEFAULT 0,
  last_failure_at TEXT,
  last_success_at TEXT,
  avg_latency_ms REAL DEFAULT 0,
  health_score REAL DEFAULT 1.0,
  updated_at TEXT NOT NULL,
  consecutive_low_health_count INTEGER NOT NULL DEFAULT 0,  -- W2: durable quarantine state
  quarantined INTEGER NOT NULL DEFAULT 0,                   -- W2: durable quarantine state
  quarantined_at TEXT,                                      -- W2: durable quarantine state
  quarantine_reason TEXT,                                   -- W2: durable quarantine state
  PRIMARY KEY (provider, provider_model)
);

CREATE TABLE IF NOT EXISTS provider_quarantine_events (       -- W2: durable quarantine audit log
  id BIGSERIAL PRIMARY KEY,
  provider TEXT NOT NULL,
  provider_model TEXT NOT NULL,
  event_type TEXT NOT NULL,         -- quarantine | unquarantine
  reason TEXT NOT NULL,
  health_score REAL,
  consecutive_low_health_count INTEGER,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_provider_quarantine_events_pair
  ON provider_quarantine_events(provider, provider_model);

CREATE TABLE IF NOT EXISTS decision_log (
  run_id TEXT PRIMARY KEY,
  prompt_hash TEXT NOT NULL,
  prompt TEXT NOT NULL,
  mode TEXT NOT NULL,
  final_answer TEXT NOT NULL,
  rating TEXT,
  outcome_raw REAL,
  pending INTEGER NOT NULL DEFAULT 1,
  reflection TEXT,
  created_at TEXT NOT NULL,
  resolved_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_decision_log_pending ON decision_log(pending);
CREATE INDEX IF NOT EXISTS idx_decision_log_prompt_hash ON decision_log(prompt_hash);

CREATE TABLE IF NOT EXISTS run_orchestration_state (
  run_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  orchestrator_engine TEXT NOT NULL,
  orchestrator_mode TEXT NOT NULL,
  engine_version TEXT NOT NULL,
  orchestration_status TEXT NOT NULL,
  last_checkpoint_id TEXT,
  resume_count INTEGER NOT NULL DEFAULT 0,
  last_error_code TEXT,
  last_error_message TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_run_orch_state_engine_mode
  ON run_orchestration_state(orchestrator_engine, orchestrator_mode, orchestration_status);

CREATE TABLE IF NOT EXISTS run_shadow_diff (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  engine TEXT NOT NULL,
  final_status TEXT,
  final_answer_present INTEGER NOT NULL DEFAULT 0,
  stage_count INTEGER,
  stage_order_match INTEGER,
  candidate_counts TEXT,
  error_codes TEXT,
  diff_summary TEXT,
  logged_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_run_shadow_diff_run_id ON run_shadow_diff(run_id);
CREATE INDEX IF NOT EXISTS idx_run_shadow_diff_logged_at ON run_shadow_diff(logged_at);
