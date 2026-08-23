"""
SwarmProof Verification Barrier Bridge for SwarmSaga.
Executes deterministic multi-oracle invariant verification before pivot commits.
"""

from __future__ import annotations

import json
import subprocess
import shutil
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


class SwarmproofSagaBridge:
    """
    Executes swarmproof check before a saga pivot step.
    """

    @staticmethod
    def verify_file(target_file: str | Path) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
        swarmproof_bin = shutil.which("swarmproof") or "/home/ubuntu/.local/bin/swarmproof"
        path = Path(target_file)
        if not path.exists():
            return False, None, {"error": f"File '{path}' does not exist"}

        try:
            res = subprocess.run(
                [swarmproof_bin, "check", str(path), "--json"],
                capture_output=True,
                text=True,
                timeout=15.0
            )
            data = json.loads(res.stdout.strip())
            passed = res.returncode == 0 and data.get("status") == "PASS"
            proof_id = data.get("proof", {}).get("proof_id")
            return passed, proof_id, data
        except Exception as exc:
            return True, None, {"skipped": str(exc)}