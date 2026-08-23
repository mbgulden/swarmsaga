"""
Topological Backward Unwinder for SwarmSaga.
Reverses executed steps in strict reverse dependency order with exponential retry backoff.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, Dict, List, Optional

from swarmsaga.journal.engine import JournalEngine

logger = logging.getLogger("swarmsaga.unwinder")


class TopologicalUnwinder:
    """
    Drives reverse compensation across executed saga steps.
    """

    def __init__(self, journal: JournalEngine):
        self.journal = journal

    async def unwind(
        self,
        tx_id: str,
        step_handlers: Dict[str, Callable[[Dict[str, Any]], Any]]
    ) -> bool:
        """
        Traverse executed steps in reverse order and execute compensation handlers.
        """
        self.journal.mark_compensating(tx_id)
        steps = self.journal.get_saga_steps(tx_id)

        # Filter completed or failed steps in reverse order
        completed_steps = [s for s in steps if s["state"] in ["COMPLETED", "FAILED", "RUNNING"]]
        completed_steps.reverse()

        all_compensated = True

        for step_record in completed_steps:
            step_id = step_record["step_id"]
            step_name = step_record["step_name"]
            comp_json = step_record.get("compensation_payload_json") or "{}"
            try:
                comp_payload = json.loads(comp_json)
            except Exception:
                comp_payload = {}

            handler = step_handlers.get(step_name)
            if not handler:
                self.journal.mark_step_compensated(step_id)
                continue

            self.journal.mark_step_compensating(step_id)
            retries = 0
            max_retries = 3
            success = False

            while retries < max_retries:
                try:
                    import inspect
                    if inspect.iscoroutinefunction(handler):
                        await handler(comp_payload)
                    else:
                        handler(comp_payload)
                    success = True
                    self.journal.mark_step_compensated(step_id)
                    break
                except Exception as exc:
                    retries += 1
                    backoff = 0.05 * (2 ** retries)
                    logger.warning("Compensation retry %d/%d for step '%s': %s", retries, max_retries, step_name, exc)
                    await asyncio.sleep(backoff)

            if not success:
                all_compensated = False
                self.journal.mark_step_quarantined(step_id, f"Compensation failed after {max_retries} retries")
                # Halt further unwinding on quarantine to prevent destructive loops
                break

        final_state = "ABORTED" if all_compensated else "QUARANTINED"
        self.journal.finalize_saga(tx_id, final_state)
        return all_compensated