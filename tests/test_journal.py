"""
Unit tests for JournalEngine WAL persistence and recovery.
"""

import tempfile
from pathlib import Path
from swarmsaga.journal.engine import JournalEngine


def test_journal_dangling_recovery():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "journal.db"
        j1 = JournalEngine(db_path=db_path)

        j1.begin_saga("tx_active_1", "agent_1")
        j1.begin_saga("tx_active_2", "agent_2")
        j1.finalize_saga("tx_active_1", "COMMITTED")

        # j1 crashes or restarts. Create j2 from same DB file
        j2 = JournalEngine(db_path=db_path)
        dangling = j2.recover_dangling_sagas()

        assert "tx_active_2" in dangling
        assert "tx_active_1" not in dangling