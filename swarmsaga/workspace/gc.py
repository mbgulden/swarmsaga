"""
Saga Garbage Collection & Dead-Letter Queue (DLQ) Cleaner for SwarmSaga.
Cleans up orphaned ephemeral worktrees (.sagas/<tx_id>/) past configurable TTL (default 48h).
"""

from __future__ import annotations

import logging
import os
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional

from swarmsaga.journal.engine import JournalEngine
from swarmsaga.workspace.git_cow import GitWorktreeManager

logger = logging.getLogger("swarmsaga.gc")


class SagaGarbageCollector:
    """
    Sweeps stalled or resolved saga worktrees and reclaims disk space.
    """

    def __init__(self, journal: JournalEngine, repo_root: Optional[str | Path] = None, ttl_seconds: float = 172800.0):
        self.journal = journal
        self.repo_root = Path(repo_root or os.getcwd()).resolve()
        self.worktree_mgr = GitWorktreeManager(repo_root=self.repo_root)
        self.ttl_seconds = ttl_seconds

    def sweep_stale_worktrees(self) -> List[str]:
        """
        Scans .sagas/ directory and cleans up worktrees whose transactions
        are in COMMITTED, ABORTED, or QUARANTINED past TTL.
        """
        cleaned_tx_ids: List[str] = []
        sagas_dir = self.repo_root / ".sagas"
        if not sagas_dir.exists():
            return cleaned_tx_ids

        now = time.time()

        for entry in sagas_dir.iterdir():
            if entry.is_dir() and not entry.name.startswith("."):
                tx_id = entry.name
                saga = self.journal.get_saga(tx_id)

                should_clean = False
                if not saga:
                    # Orphaned directory without journal entry
                    should_clean = True
                else:
                    state = saga.get("state")
                    updated_at = saga.get("updated_at", 0)
                    age = now - updated_at

                    # Clean committed/aborted immediately or quarantined past TTL
                    if state in ["COMMITTED", "ABORTED"] and age > 300.0:  # 5 min grace period
                        should_clean = True
                    elif state in ["QUARANTINED", "ABORTED_WITH_DLQ"] and age > self.ttl_seconds:
                        should_clean = True

                if should_clean:
                    try:
                        self.worktree_mgr.cleanup_worktree(tx_id)
                        cleaned_tx_ids.append(tx_id)
                        logger.info("GC: Reclaimed worktree for saga %s", tx_id)
                    except Exception as exc:
                        logger.warning("GC: Failed to clean worktree %s: %s", tx_id, exc)

        return cleaned_tx_ids