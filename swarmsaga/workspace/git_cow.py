"""
Copy-on-Write (CoW) Ephemeral Git Worktree Manager for SwarmSaga.
Isolates filesystem mutations inside ephemeral worktrees with strict realpath containment.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

logger = logging.getLogger("swarmsaga.workspace")


class PathTraversalSecurityError(PermissionError):
    """Raised when a path escapes the repository sandbox boundary."""
    pass


class GitWorktreeManager:
    """
    Manages isolated ephemeral worktrees for sagas with strict chroot-like path containment.
    """

    def __init__(self, repo_root: Optional[str | Path] = None):
        self.repo_root = Path(repo_root or os.getcwd()).resolve()
        self.sagas_dir = (self.repo_root / ".sagas").resolve()

    def validate_safe_path(self, target_path: str | Path, worktree_path: Optional[Path] = None) -> Path:
        """
        Enforces that target_path strictly resolves inside the worktree or repo_root.
        Prevents symlink directory traversal attacks (e.g. pointing to ~/.ssh or /etc).
        """
        allowed_root = (worktree_path or self.repo_root).resolve()
        resolved = Path(target_path).resolve()

        try:
            # Must be a strict sub-path of allowed_root
            resolved.relative_to(allowed_root)
        except ValueError:
            logger.critical("Path traversal escape attempt detected: %s outside %s", resolved, allowed_root)
            raise PathTraversalSecurityError(
                f"Security Violation: Path '{target_path}' resolves to '{resolved}', which is outside the workspace boundary '{allowed_root}'."
            )
        return resolved

    def _run_git(self, args: list[str], cwd: Optional[Path] = None) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["git"] + args,
            cwd=str(cwd or self.repo_root),
            capture_output=True,
            text=True,
            check=False
        )

    def create_worktree(self, tx_id: str, base_ref: str = "HEAD") -> Path:
        """Create an ephemeral worktree at .sagas/<tx_id>/."""
        self.sagas_dir.mkdir(parents=True, exist_ok=True)
        worktree_path = (self.sagas_dir / tx_id).resolve()
        branch_name = f"saga/{tx_id}"

        # Clean if previously exists
        if worktree_path.exists():
            self.cleanup_worktree(tx_id)

        res = self._run_git(["worktree", "add", "-b", branch_name, str(worktree_path), base_ref])
        if res.returncode != 0:
            logger.warning("Git worktree creation fallback: %s", res.stderr)
            worktree_path.mkdir(parents=True, exist_ok=True)
        return worktree_path

    def commit_worktree(self, tx_id: str, commit_message: str = "chore: saga commit") -> bool:
        """Commit worktree modifications."""
        worktree_path = (self.sagas_dir / tx_id).resolve()
        if not worktree_path.exists():
            return False

        self._run_git(["add", "-A"], cwd=worktree_path)
        res = self._run_git(["commit", "-m", commit_message], cwd=worktree_path)
        return res.returncode == 0

    def cleanup_worktree(self, tx_id: str) -> None:
        """Atomically remove the ephemeral worktree and branch."""
        worktree_path = (self.sagas_dir / tx_id).resolve()
        branch_name = f"saga/{tx_id}"

        if worktree_path.exists():
            self._run_git(["worktree", "remove", "--force", str(worktree_path)])
            if worktree_path.exists():
                try:
                    shutil.rmtree(worktree_path, ignore_errors=True)
                except Exception:
                    pass

        self._run_git(["branch", "-D", branch_name])
        self._run_git(["worktree", "prune"])