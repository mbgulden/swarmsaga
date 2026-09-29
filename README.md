# 🔀 SwarmSaga

[![CI](https://github.com/mbgulden/swarmsaga/actions/workflows/ci.yml/badge.svg)](https://github.com/mbgulden/swarmsaga/actions)
[![PyPI version](https://img.shields.io/badge/pypi-v0.1.0-blue.svg)](https://pypi.org/project/swarmsaga/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Distributed Saga & Compensating Transaction Hypervisor for Multi-Agent Swarms**  
> *Write-ahead journaling, forward DAG execution, and deterministic topological backward compensation — so a failed multi-agent workflow unwinds itself instead of leaving half-applied side effects behind.*

---

## 💡 Why SwarmSaga?

When a single agent (or a swarm of them) works across codebases, databases, containers, and external APIs, there is no `ROLLBACK`. A workflow that dies on step 7 of 12 leaves 6 applied mutations behind — half-written files, half-migrated rows, half-deployed configs.

**SwarmSaga** gives multi-agent workflows transactional semantics via the saga pattern:

- Define each unit of work as a **Step** with a forward handler *and* a compensation (undo) handler.
- The **SagaCoordinator** executes steps as a dependency DAG — independent steps run in parallel.
- If any step fails (or a pivot gate rejects), the **TopologicalUnwinder** walks the journaled steps in strict reverse dependency order and runs each compensation, with retries and a dead-letter quarantine for compensations that won't complete.
- A **SQLite write-ahead journal** records every saga and step state, so crashed or orphaned sagas are detected and recoverable after a restart.

---

## 🏛️ System Architecture

```
                    ┌──────────────────────────────────────────────┐
                    │              AI Agent / Swarm                │
                    │   (defines Steps + compensation handlers)    │
                    └──────────────────────┬───────────────────────┘
                                           │  add_step(...) × N
                                           ▼
                    ┌──────────────────────────────────────────────┐
                    │              SagaCoordinator                 │
                    │   - Forward DAG via graphlib.TopologicalSorter
                    │   - Parallel execution of dependency-ready   │
                    │     steps (asyncio)                          │
                    │   - Pivot gate checks before irreversible    │
                    │     steps (SwarmGate / SwarmProof bridges)   │
                    └──────┬───────────────────────────┬───────────┘
                           │ 1. journal every          │ 2. on failure
                           │    step start/complete    │
                           ▼                           ▼
              ┌────────────────────────┐   ┌────────────────────────┐
              │     JournalEngine      │   │  TopologicalUnwinder   │
              │  SQLite WAL journal    │   │  Reverse-dependency    │
              │  ~/.swarmsaga/         │   │  compensation walk:    │
              │  saga_journal.db       │   │  retries + DLQ         │
              └────────────────────────┘   └────────────────────────┘
```

---

## 📦 Installation

```bash
pip install swarmsaga
```

*Pure Python standard library. Zero runtime dependencies.*

For development (runs the test suite):

```bash
pip install -e ".[dev]"
pytest tests/
```

---

## 🚀 Quick Start (< 5 minutes)

```python
import asyncio
from swarmsaga import SagaCoordinator, Step

# --- forward + compensation handlers (sync or async) ---
def reserve_flight(ctx):
    booking_id = book_flight(ctx["destination"])          # your side effect
    return {"flight": booking_id}, {"flight": booking_id}  # (result, undo payload)

def cancel_flight(payload):
    refund_flight(payload["flight"])                      # undo side effect

def reserve_hotel(ctx):
    confirmation = book_hotel(ctx["destination"])
    return {"hotel": confirmation}, {"hotel": confirmation}

def cancel_hotel(payload):
    cancel_reservation(payload["hotel"])

def failing_payment_gateway(ctx):
    raise ValueError("payment gateway timeout")           # simulated failure

coordinator = SagaCoordinator()
coordinator.add_step(Step("flight", reserve_flight, cancel_flight))
coordinator.add_step(Step("hotel", reserve_hotel, cancel_hotel))
coordinator.add_step(Step("pay", failing_payment_gateway, dependencies=["flight", "hotel"]))

try:
    ctx = asyncio.run(coordinator.execute(agent_id="travel_agent"))
except RuntimeError as exc:
    print(exc)
    # Saga 'tx_...' aborted and compensated. Reason: payment gateway timeout
    # -> cancel_hotel and cancel_flight already ran, in reverse dependency order
```

The saga journal records the whole run — `COMMITTED` on success, `ABORTED` (or `QUARANTINED` if a compensation itself failed) on failure:

```python
from swarmsaga import JournalEngine

journal = JournalEngine()
saga = journal.get_saga("tx_...")          # {'tx_id', 'agent_id', 'state', ...}
steps = journal.get_saga_steps("tx_...")  # per-step states + compensation payloads
```

### Pivot steps & gates

Mark irreversible steps as pivots. The coordinator runs your gate check *before* the pivot executes — a rejection aborts and compensates everything staged so far:

```python
coordinator.add_step(Step("stage", stage_forward, stage_compensate))
coordinator.add_step(Step("deploy", deploy_forward, is_pivot=True, dependencies=["stage"]))

asyncio.run(coordinator.execute(pivot_gate_check=lambda ctx: human_approved(ctx)))
```

---

## 🖥️ CLI Usage

SwarmSaga ships a `swarmsaga` command for inspecting and recovering the journal:

```bash
# List recorded sagas (optionally filtered by state)
swarmsaga list
swarmsaga list --state ABORTED

# Inspect one transaction: agent, state, per-step timeline
swarmsaga inspect tx_9f3a1c2d4b5e6071

# Recover dangling EXECUTING/COMPENSATING sagas left by a crashed process
swarmsaga recover
swarmsaga recover --tx-id tx_9f3a1c2d4b5e6071
```

Saga states: `PENDING` · `EXECUTING` · `COMPENSATING` · `COMMITTED` · `ABORTED` · `QUARANTINED`

---

## 🗄️ Journal, Recovery & Workspaces

**Crash recovery.** The journal lives in SQLite WAL mode (default `~/.swarmsaga/saga_journal.db`; pass `db_path=` to relocate). After a restart, `recover_dangling_sagas()` finds transactions stuck in `EXECUTING` or `COMPENSATING` so they can be unwound — the `swarmsaga recover` CLI does exactly this.

**Copy-on-write workspaces.** `GitWorktreeManager` isolates filesystem mutations in ephemeral git worktrees under `.sagas/<tx_id>/` (branch `saga/<tx_id>`), with strict path-containment checks (`PathTraversalSecurityError` on escape attempts). Outside a git repo it degrades gracefully to a plain directory.

**Garbage collection.** `SagaGarbageCollector` sweeps worktrees whose sagas are `COMMITTED`/`ABORTED` past a 5-minute grace period, and `QUARANTINED` worktrees past the TTL (default 48h).

---

## 🌉 Ecosystem Bridges (optional)

`swarmsaga.bridges` wires sagas into the sibling swarm primitives. Every bridge is **fail-open**: if the peer tool isn't installed, the call degrades to a `{"skipped": ...}` detail instead of blocking the saga.

| Bridge | Peer tool | Role |
|---|---|---|
| `SwarmproofSagaBridge.verify_file(path)` | `swarmproof` CLI | Deterministic invariant verification before a pivot commit |
| `SwarmgateSagaBridge.evaluate_mutation(path, ...)` | `swarmgate` CLI | Escalation-tier evaluation before irreversible steps |
| `SwarmlockSagaBridge.release_all(tx_id, action)` | `swarmlock` daemon (`/tmp/swarmlock.sock`) | Bulk release of tx-scoped leases on commit/abort |

```python
from swarmsaga.bridges import SwarmproofSagaBridge

passed, proof_id, details = SwarmproofSagaBridge.verify_file("src/billing.py")
```

---

## 🔌 API Reference

| Symbol | Module | Purpose |
|---|---|---|
| `Step(name, forward_handler, compensate_handler, is_pivot, max_retries, dependencies)` | `swarmsaga.core.step` | One unit of work. Handlers may be sync or async. Forward may return `(result, compensation_payload)` or just `result`. |
| `SagaCoordinator(journal)` | `swarmsaga.core.coordinator` | `add_step(step)` → `await execute(tx_id, agent_id, initial_context, pivot_gate_check)`. Raises `RuntimeError` after compensating on failure. |
| `TopologicalUnwinder(journal)` | `swarmsaga.core.unwinder` | `await unwind(tx_id, step_handlers)`. Reverse-order compensation, 3 retries with exponential backoff, DLQ quarantine (`FAILED_QUARANTINE`) for unrecoverable steps. |
| `JournalEngine(db_path)` | `swarmsaga.journal.engine` | WAL journal: `begin_saga`, `log_step_start/complete/failed`, `mark_compensating`, `finalize_saga`, `get_saga`, `get_saga_steps`, `list_sagas`, `recover_dangling_sagas`. |
| `GitWorktreeManager(repo_root)` | `swarmsaga.workspace.git_cow` | `create_worktree`, `commit_worktree`, `cleanup_worktree`, `validate_safe_path`. |
| `SagaGarbageCollector(journal, repo_root, ttl_seconds)` | `swarmsaga.workspace.gc` | `sweep_stale_worktrees()` → list of reclaimed tx ids. |

Step states: `READY` · `RUNNING` · `COMPLETED` · `COMPENSATING` · `COMPENSATED` · `FAILED` · `FAILED_QUARANTINE`

---

## 🧪 Testing & CI

```bash
pip install -e ".[dev]"
pytest tests/ -v
ruff check .
python -m build && twine check dist/*
```

Every push and PR runs the full matrix on GitHub Actions: **3 OS × 4 Python versions (3.10–3.13)** — test suite, CLI smoke (`swarmsaga --help` / `swarmsaga --version`), wheel + sdist build, and `twine check`.

---

## 🗺️ Swarm Ecosystem

SwarmSaga is part of the **Swarm Primitives Ecosystem** for autonomous agent swarms, and slots into multi-agent orchestration stacks (such as [prismatic-engine](https://github.com/mbgulden/prismatic-engine)) wherever long-running, multi-step agent work needs transactional safety:

- 🔀 **SwarmSaga**: Distributed saga & compensating-transaction hypervisor (this repo).
- 🔒 **SwarmLock**: Tokenized, non-blocking distributed advisory locks.
- 🛡️ **SwarmProof**: Truth Oracle, evidence ledgers, and anti-hallucination gates.
- 🧭 **SwarmGate**: Attention escalation tiers for irreversible agent actions.
- ⏱️ **SwarmCron**: Native high-precision background cron scheduling.

---

## 📄 License
MIT © Michael Gulden
