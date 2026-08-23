"""
Durable Write-Ahead Saga Journal Engine for SwarmSaga.
Provides SQLite WAL-backed atomic state logging and crash recovery.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from swarmsaga.journal.schema import SCHEMA_SQL


class JournalEngine:
    """
    SQLite WAL-backed persistent state logging for distributed sagas.
    """

    def __init__(self, db_path: Optional[str | Path] = None):
        if db_path is None:
            base_dir = Path.home() / ".swarmsaga"
            base_dir.mkdir(parents=True, exist_ok=True)
            self.db_path = base_dir / "saga_journal.db"
        else:
            self.db_path = Path(db_path)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._lock:
            conn = self._get_conn()
            try:
                conn.executescript(SCHEMA_SQL)
                conn.commit()
            finally:
                conn.close()

    def begin_saga(self, tx_id: str, agent_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        now = time.time()
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("""
                    INSERT INTO sagas (tx_id, agent_id, state, created_at, updated_at, metadata_json)
                    VALUES (?, ?, 'EXECUTING', ?, ?, ?)
                    ON CONFLICT(tx_id) DO UPDATE SET updated_at = excluded.updated_at, agent_id = excluded.agent_id;
                """, (tx_id, agent_id, now, now, json.dumps(metadata or {})))
                conn.commit()
            finally:
                conn.close()

    def log_step_start(
        self,
        step_id: str,
        tx_id: str,
        step_name: str,
        forward_payload: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None,
        is_pivot: bool = False
    ) -> None:
        now = time.time()
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("""
                    INSERT INTO saga_steps (
                        step_id, tx_id, step_name, state, forward_payload_json,
                        dependencies_json, is_pivot, created_at
                    ) VALUES (?, ?, ?, 'RUNNING', ?, ?, ?, ?)
                    ON CONFLICT(step_id) DO UPDATE SET
                        state = 'RUNNING',
                        forward_payload_json = excluded.forward_payload_json;
                """, (
                    step_id, tx_id, step_name,
                    json.dumps(forward_payload or {}),
                    json.dumps(dependencies or []),
                    1 if is_pivot else 0,
                    now
                ))
                conn.commit()
            finally:
                conn.close()

    def log_step_complete(
        self,
        step_id: str,
        compensation_payload: Optional[Dict[str, Any]] = None
    ) -> None:
        now = time.time()
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("""
                    UPDATE saga_steps
                    SET state = 'COMPLETED',
                        compensation_payload_json = ?,
                        completed_at = ?
                    WHERE step_id = ?;
                """, (json.dumps(compensation_payload or {}), now, step_id))
                conn.commit()
            finally:
                conn.close()

    def log_step_failed(self, step_id: str, error_message: str) -> None:
        now = time.time()
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("""
                    UPDATE saga_steps
                    SET state = 'FAILED',
                        error_message = ?,
                        completed_at = ?
                    WHERE step_id = ?;
                """, (error_message, now, step_id))
                conn.commit()
            finally:
                conn.close()

    def mark_compensating(self, tx_id: str) -> None:
        now = time.time()
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("""
                    UPDATE sagas
                    SET state = 'COMPENSATING', updated_at = ?
                    WHERE tx_id = ?;
                """, (now, tx_id))
                conn.commit()
            finally:
                conn.close()

    def mark_step_compensating(self, step_id: str) -> None:
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("UPDATE saga_steps SET state = 'COMPENSATING' WHERE step_id = ?;", (step_id,))
                conn.commit()
            finally:
                conn.close()

    def mark_step_compensated(self, step_id: str) -> None:
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("UPDATE saga_steps SET state = 'COMPENSATED' WHERE step_id = ?;", (step_id,))
                conn.commit()
            finally:
                conn.close()

    def mark_step_quarantined(self, step_id: str, error_message: str) -> None:
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("""
                    UPDATE saga_steps
                    SET state = 'FAILED_QUARANTINE', error_message = ?
                    WHERE step_id = ?;
                """, (error_message, step_id))
                conn.commit()
            finally:
                conn.close()

    def finalize_saga(self, tx_id: str, state: str) -> None:
        now = time.time()
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("""
                    UPDATE sagas
                    SET state = ?, updated_at = ?
                    WHERE tx_id = ?;
                """, (state, now, tx_id))
                conn.commit()
            finally:
                conn.close()

    def get_saga(self, tx_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = self._get_conn()
            try:
                row = conn.execute("SELECT * FROM sagas WHERE tx_id = ?;", (tx_id,)).fetchone()
                if not row:
                    return None
                return dict(row)
            finally:
                conn.close()

    def get_saga_steps(self, tx_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._get_conn()
            try:
                rows = conn.execute("SELECT * FROM saga_steps WHERE tx_id = ? ORDER BY created_at ASC;", (tx_id,)).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()

    def list_sagas(self, state: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._get_conn()
            try:
                if state:
                    rows = conn.execute("SELECT * FROM sagas WHERE state = ? ORDER BY created_at DESC;", (state,)).fetchall()
                else:
                    rows = conn.execute("SELECT * FROM sagas ORDER BY created_at DESC;").fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()

    def recover_dangling_sagas(self) -> List[str]:
        """Detect and return abandoned EXECUTING or COMPENSATING sagas after daemon restart."""
        with self._lock:
            conn = self._get_conn()
            try:
                rows = conn.execute("""
                    SELECT tx_id FROM sagas
                    WHERE state IN ('EXECUTING', 'COMPENSATING')
                    ORDER BY created_at ASC;
                """).fetchall()
                return [r["tx_id"] for r in rows]
            finally:
                conn.close()