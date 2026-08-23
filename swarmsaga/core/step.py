"""
Step Data Model for SwarmSaga Workflows.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class Step:
    name: str
    forward_handler: Callable[[Dict[str, Any]], Any]
    compensate_handler: Optional[Callable[[Dict[str, Any]], Any]] = None
    is_pivot: bool = False
    max_retries: int = 3
    dependencies: List[str] = field(default_factory=list)
    step_id: Optional[str] = None

    async def execute_forward(self, context: Dict[str, Any]) -> Tuple[Any, Dict[str, Any]]:
        """
        Executes forward handler.
        Returns: (result, compensation_payload)
        """
        if inspect.iscoroutinefunction(self.forward_handler):
            res = await self.forward_handler(context)
        else:
            res = self.forward_handler(context)

        # If forward returns a tuple (result, comp_payload), extract it
        if isinstance(res, tuple) and len(res) == 2:
            return res[0], res[1]
        return res, context

    async def execute_compensate(self, compensation_payload: Dict[str, Any]) -> bool:
        """
        Executes compensation handler.
        """
        if self.compensate_handler is None:
            return True

        if inspect.iscoroutinefunction(self.compensate_handler):
            await self.compensate_handler(compensation_payload)
        else:
            self.compensate_handler(compensation_payload)
        return True