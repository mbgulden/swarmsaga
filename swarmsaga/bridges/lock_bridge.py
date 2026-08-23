"""
SwarmLock Ecosystem Bridge for SwarmSaga.
Coordinates transaction-scoped lock acquisition and bulk atomic release.
"""

from __future__ import annotations

import json
import logging
import os
import socket
from typing import Any, Dict, Optional

logger = logging.getLogger("swarmsaga.bridges.lock")
SWARMLOCK_SOCK = "/tmp/swarmlock.sock"


def send_swarmlock_ipc(payload: Dict[str, Any], socket_path: str = SWARMLOCK_SOCK) -> Optional[Dict[str, Any]]:
    if not os.path.exists(socket_path):
        return None
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(2.0)
            client.connect(socket_path)
            client.sendall(json.dumps(payload).encode("utf-8") + b"\n")
            line = client.recv(8192)
            if line:
                return json.loads(line.decode("utf-8").strip())
    except Exception as exc:
        logger.debug("Swarmlock IPC call failed: %s", exc)
        return None
    return None


class SwarmlockSagaBridge:
    """
    Coordinates tx_id lease scoping and bulk release on saga completion or abort.
    """

    @staticmethod
    def release_all(tx_id: str, action: str = "COMMIT") -> bool:
        """Release all leases associated with this transaction ID."""
        res = send_swarmlock_ipc({
            "action": "RELEASE_ALL_BY_TX",
            "tx_id": tx_id,
            "commit": (action == "COMMIT")
        })
        return res is not None and res.get("status") == "SUCCESS"