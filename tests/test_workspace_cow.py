"""
Unit tests for GitWorktreeManager Copy-on-Write isolation.
"""

import tempfile
from pathlib import Path
from swarmsaga.workspace.git_cow import GitWorktreeManager


def test_worktree_lifecycle_fallback():
    with tempfile.TemporaryDirectory() as tmpdir:
        mgr = GitWorktreeManager(repo_root=tmpdir)
        wt = mgr.create_worktree("tx_cow_1")

        assert wt.exists()
        test_file = wt / "modified.txt"
        test_file.write_text("mutation", encoding="utf-8")
        assert test_file.exists()

        mgr.cleanup_worktree("tx_cow_1")
        assert not (mgr.sagas_dir / "tx_cow_1").exists()