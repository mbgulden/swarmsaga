"""
Comprehensive lifecycle tests for SwarmSaga Forward Execution & Backward Compensation.
"""

import tempfile
from pathlib import Path
import pytest

from swarmsaga.core.coordinator import SagaCoordinator
from swarmsaga.core.step import Step
from swarmsaga.journal.engine import JournalEngine


@pytest.mark.asyncio
async def test_linear_saga_success():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "journal.db"
        journal = JournalEngine(db_path=db_path)
        coordinator = SagaCoordinator(journal=journal)

        execution_log = []

        def step_1_forward(ctx):
            execution_log.append("s1_forward")
            return {"s1_done": True}, {"s1_val": 42}

        def step_1_comp(payload):
            execution_log.append(f"s1_comp_{payload.get('s1_val')}")

        def step_2_forward(ctx):
            execution_log.append("s2_forward")
            return {"s2_done": True}, {"s2_val": 99}

        coordinator.add_step(Step("step_1", step_1_forward, step_1_comp))
        coordinator.add_step(Step("step_2", step_2_forward, dependencies=["step_1"]))

        ctx = await coordinator.execute(tx_id="tx_test_linear")
        assert ctx["s1_done"] is True
        assert ctx["s2_done"] is True
        assert execution_log == ["s1_forward", "s2_forward"]

        saga = journal.get_saga("tx_test_linear")
        assert saga["state"] == "COMMITTED"


@pytest.mark.asyncio
async def test_backward_topological_compensation_on_failure():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "journal.db"
        journal = JournalEngine(db_path=db_path)
        coordinator = SagaCoordinator(journal=journal)

        comp_log = []

        def step_a_forward(ctx):
            return {"a": 1}, {"comp_a": "undo_a"}

        def step_a_comp(payload):
            comp_log.append(payload["comp_a"])

        def step_b_forward(ctx):
            return {"b": 2}, {"comp_b": "undo_b"}

        def step_b_comp(payload):
            comp_log.append(payload["comp_b"])

        def step_c_failing(ctx):
            raise ValueError("Simulated database failure on step C")

        coordinator.add_step(Step("step_a", step_a_forward, step_a_comp))
        coordinator.add_step(Step("step_b", step_b_forward, step_b_comp, dependencies=["step_a"]))
        coordinator.add_step(Step("step_c", step_c_failing, dependencies=["step_b"]))

        with pytest.raises(RuntimeError) as exc_info:
            await coordinator.execute(tx_id="tx_test_fail")

        assert "Simulated database failure" in str(exc_info.value)
        # Reverse compensation must execute B before A
        assert comp_log == ["undo_b", "undo_a"]

        saga = journal.get_saga("tx_test_fail")
        assert saga["state"] == "ABORTED"


@pytest.mark.asyncio
async def test_pivot_barrier_gate_failure():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "journal.db"
        journal = JournalEngine(db_path=db_path)
        coordinator = SagaCoordinator(journal=journal)

        comp_log = []

        def prep_forward(ctx):
            return {"staged": True}, {"undo": "revert_prep"}

        def prep_comp(payload):
            comp_log.append(payload["undo"])

        def pivot_forward(ctx):
            return {"published": True}, {}

        coordinator.add_step(Step("prep", prep_forward, prep_comp))
        coordinator.add_step(Step("pivot_deploy", pivot_forward, is_pivot=True, dependencies=["prep"]))

        # Simulated gate failure (e.g. swarmgate Tier 3 reject or swarmproof syntax error)
        def failing_gate(ctx):
            return False

        with pytest.raises(RuntimeError) as exc_info:
            await coordinator.execute(tx_id="tx_pivot_fail", pivot_gate_check=failing_gate)

        assert "Pivot barrier check failed" in str(exc_info.value)
        assert comp_log == ["revert_prep"]