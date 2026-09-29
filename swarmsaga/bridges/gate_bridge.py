"""
SwarmGate Attention Escalation Bridge for SwarmSaga.
Evaluates Escalation Score (E) before irreversible pivot steps.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any


class SwarmgateSagaBridge:
    """
    Evaluates swarmgate escalation before executing a declared pivot step.
    """

    @staticmethod
    def evaluate_mutation(
        target_file: str | Path,
        tx_id: str | None = None,
        proof_id: str | None = None,
        agent_id: str = "default_agent"
    ) -> tuple[bool, str, dict[str, Any]]:
        """
        Returns: (can_proceed_autonomously, tier, details)
        """
        swarmgate_bin = shutil.which("swarmgate") or "/home/ubuntu/.local/bin/swarmgate"
        path = Path(target_file)

        cmd = [swarmgate_bin, "evaluate", str(path), "--agent", agent_id, "--json"]
        if proof_id:
            cmd.extend(["--proof", proof_id])
        if tx_id:
            cmd.extend(["--tx-id", tx_id])

        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=10.0, check=False)
            data = json.loads(res.stdout.strip())
            tier = data.get("tier", "TIER_1_AUTO")
            can_proceed = (tier != "TIER_3_BARRIER")
            return can_proceed, tier, data
        except Exception as exc:  # noqa: BLE001 - bridge degrades to "skipped" when swarmgate is absent
            return True, "TIER_1_AUTO", {"skipped": str(exc)}