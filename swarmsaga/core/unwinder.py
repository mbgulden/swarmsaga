"""
Topological Backward Unwinder for SwarmSaga.
Reverses executed steps in strict reverse dependency order with exponential retry backoff
and Idempotent Dead-Letter Queue (DLQ) fault tolerance.
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
    Drives reverse compensation across executed saga steps with DLQ fault isolation.
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
        Continues independent cleanup even if a specific external step enters DLQ.
        """
        self.journal.mark_compensating(tx_id)
        steps = self.journal.get_saga_steps(tx_id)

        completed_steps = [s for s in steps if s["state"] in ["COMPLETED", "FAILED", "RUNNING"]]
        completed_steps.reverse()

        all_compensated = True
        dlq_steps = []

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
            last_err = ""

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
                    last_err = str(exc)
                    backoff = 0.02 * (2 ** retries)
                    logger.warning("Compensation retry %d/%d for step '%s': %s", retries, max_retries, step_name, exc)
                    await asyncio.sleep(backoff)

            if not success:
                all_compensated = False
                dlq_steps.append(step_name)
                err_msg = f"Compensation failed after {max_retries} retries: {last_err}"
                self.journal.mark_step_quarantined(step_id, err_msg)
                logger.error("DLQ: Step '%s' quarantined in saga %s. Proceeding with remaining step cleanup.", step_name, tx_id)
                # DO NOT BREAK - Continue unwinding remaining independent steps

        final_state = "ABORTED" if all_compensated else "ABORTED"
        self.journal.finalize_saga(tx_id, final_state)
        return all_compensated