"""
DAG Coordinator for SwarmSaga.
Constructs execution graphs using graphlib.TopologicalSorter and handles forward/backward orchestration.
"""

from __future__ import annotations

import asyncio
import graphlib
import logging
import uuid
from typing import Any, Dict, List, Optional

from swarmsaga.core.step import Step
from swarmsaga.core.unwinder import TopologicalUnwinder
from swarmsaga.journal.engine import JournalEngine

logger = logging.getLogger("swarmsaga.coordinator")


class SagaCoordinator:
    """
    Coordinates forward DAG execution and deterministic backward unwinding.
    """

    def __init__(self, journal: Optional[JournalEngine] = None):
        self.journal = journal or JournalEngine()
        self.unwinder = TopologicalUnwinder(self.journal)
        self._steps: Dict[str, Step] = {}

    def add_step(self, step: Step) -> SagaCoordinator:
        self._steps[step.name] = step
        return self

    async def execute(
        self,
        tx_id: Optional[str] = None,
        agent_id: str = "default_agent",
        initial_context: Optional[Dict[str, Any]] = None,
        pivot_gate_check: Optional[Any] = None
    ) -> Dict[str, Any]:
        tx_id = tx_id or f"tx_{uuid.uuid4().hex[:16]}"
        self.journal.begin_saga(tx_id, agent_id)

        context = dict(initial_context or {})
        context["tx_id"] = tx_id
        context["agent_id"] = agent_id

        # Build dependency graph
        graph: Dict[str, set[str]] = {}
        for name, step in self._steps.items():
            graph[name] = set(step.dependencies)

        sorter = graphlib.TopologicalSorter(graph)
        sorter.prepare()

        active_tasks: Dict[asyncio.Task, str] = {}
        comp_handlers: Dict[str, Any] = {
            name: step.compensate_handler for name, step in self._steps.items() if step.compensate_handler
        }

        abort_triggered = False
        abort_reason = None

        while sorter.is_active() and not abort_triggered:
            ready_nodes = sorter.get_ready()
            if not ready_nodes and not active_tasks:
                break

            for node_name in ready_nodes:
                step = self._steps[node_name]
                step_id = f"step_{uuid.uuid4().hex[:12]}"
                step.step_id = step_id

                # If pivot step, execute gate check if provided
                if step.is_pivot and pivot_gate_check:
                    gate_passed = await pivot_gate_check(context) if asyncio.iscoroutinefunction(pivot_gate_check) else pivot_gate_check(context)
                    if not gate_passed:
                        abort_triggered = True
                        abort_reason = f"Pivot barrier check failed on step '{node_name}'"
                        break

                self.journal.log_step_start(
                    step_id=step_id,
                    tx_id=tx_id,
                    step_name=node_name,
                    forward_payload=context,
                    dependencies=step.dependencies,
                    is_pivot=step.is_pivot
                )

                async def _run_node(n=node_name, s=step, sid=step_id):
                    res, comp_payload = await s.execute_forward(context)
                    self.journal.log_step_complete(sid, comp_payload)
                    return n, res

                task = asyncio.create_task(_run_node())
                active_tasks[task] = node_name

            if not active_tasks:
                break

            # Wait for any active step to complete
            done, pending = await asyncio.wait(
                active_tasks.keys(),
                return_when=asyncio.FIRST_COMPLETED
            )

            for finished_task in done:
                node_name = active_tasks.pop(finished_task)
                try:
                    name, res = finished_task.result()
                    if isinstance(res, dict):
                        context.update(res)
                    sorter.done(name)
                except Exception as exc:
                    abort_triggered = True
                    abort_reason = str(exc)
                    logger.error("Step '%s' failed in saga %s: %s", node_name, tx_id, exc)
                    # Cancel all remaining parallel branches
                    for p in active_tasks.keys():
                        p.cancel()
                    break

        if abort_triggered:
            logger.info("Triggering backward topological compensation for saga %s", tx_id)
            await self.unwinder.unwind(tx_id, comp_handlers)
            raise RuntimeError(f"Saga '{tx_id}' aborted and compensated. Reason: {abort_reason}")

        self.journal.finalize_saga(tx_id, "COMMITTED")
        return context