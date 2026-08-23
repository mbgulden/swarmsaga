"""
SwarmSaga Journal Database Schema and SQL Definitions.
"""

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS sagas (
    tx_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'EXECUTING', 'COMPENSATING', 'COMMITTED', 'ABORTED', 'QUARANTINED')),
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    metadata_json TEXT
);

CREATE TABLE IF NOT EXISTS saga_steps (
    step_id TEXT PRIMARY KEY,
    tx_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('READY', 'RUNNING', 'COMPLETED', 'COMPENSATING', 'COMPENSATED', 'FAILED', 'FAILED_QUARANTINE')),
    forward_payload_json TEXT,
    compensation_payload_json TEXT,
    is_pivot INTEGER NOT NULL DEFAULT 0,
    dependencies_json TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    completed_at REAL,
    error_message TEXT,
    FOREIGN KEY(tx_id) REFERENCES sagas(tx_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS saga_checkpoints (
    checkpoint_id TEXT PRIMARY KEY,
    tx_id TEXT NOT NULL,
    worktree_path TEXT,
    git_commit_sha TEXT,
    created_at REAL NOT NULL,
    FOREIGN KEY(tx_id) REFERENCES sagas(tx_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_steps_tx ON saga_steps(tx_id);
CREATE INDEX IF NOT EXISTS idx_sagas_state ON sagas(state);
"""