-- fusion-council-service schema
-- SQLite only. WAL mode. Do not add PostgreSQL in v1.

PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

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
  latency_ms INTEGER,
  input_tokens INTEGER,
  output_tokens INTEGER,
  raw_answer TEXT,
  normalized_answer TEXT,
  score_json TEXT,
  error_code TEXT,
  error_message TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES runs(run_id)
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