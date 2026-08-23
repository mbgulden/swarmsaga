"""
SwarmSaga Command Line Interface (CLI).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

from swarmsaga.core.unwinder import TopologicalUnwinder
from swarmsaga.journal.engine import JournalEngine


def cmd_list(args: argparse.Namespace) -> int:
    journal = JournalEngine()
    sagas = journal.list_sagas(state=args.state)

    print("=" * 78)
    print(f" 📜 SWARMSAGA TRANSACTION JOURNAL  ({len(sagas)} record(s))")
    print("=" * 78)

    if not sagas:
        print("  (No sagas recorded matching criteria)")
    else:
        print(f"  {'TX ID':<22} {'AGENT':<16} {'STATE':<14} {'CREATED AT'}")
        print("  " + "-" * 74)
        for s in sagas:
            t_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(s["created_at"]))
            print(f"  {s['tx_id']:<22} {s['agent_id']:<16} {s['state']:<14} {t_str}")
    print("=" * 78)
    return 0


def cmd_inspect(args: argparse.Namespace) -> int:
    journal = JournalEngine()
    saga = journal.get_saga(args.tx_id)
    if not saga:
        print(f"Error: Saga transaction '{args.tx_id}' not found.")
        return 1

    steps = journal.get_saga_steps(args.tx_id)

    print("=" * 78)
    print(f" 🔍 SWARMSAGA INSPECTION: {saga['tx_id']}")
    print("=" * 78)
    print(f"  Agent ID:   {saga['agent_id']}")
    print(f"  State:      {saga['state']}")
    print(f"  Created:    {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(saga['created_at']))}")
    print(f"  Updated:    {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(saga['updated_at']))}")
    print("-" * 78)
    print("  Execution Steps:")

    for idx, st in enumerate(steps, 1):
        pivot_tag = " [PIVOT]" if st["is_pivot"] else ""
        print(f"   {idx}. [{st['state']:<10}] {st['step_name']}{pivot_tag} (ID: {st['step_id']})")
        if st.get("error_message"):
            print(f"       ❌ Error: {st['error_message']}")

    print("=" * 78)
    return 0


def cmd_recover(args: argparse.Namespace) -> int:
    journal = JournalEngine()
    dangling = journal.recover_dangling_sagas()

    if not dangling:
        print("🟢 No dangling sagas detected in journal. System clean!")
        return 0

    target_ids = [args.tx_id] if args.tx_id else dangling
    print(f"🔄 Recovering {len(target_ids)} orphaned sagas...")

    unwinder = TopologicalUnwinder(journal)
    for tx_id in target_ids:
        print(f"  - Unwinding {tx_id}...")
        # Execute default empty recovery unwind
        asyncio.run(unwinder.unwind(tx_id, {}))
        print(f"    ✅ Finalized state: {journal.get_saga(tx_id)['state']}")

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="swarmsaga",
        description="SwarmSaga: Distributed Saga & Compensating Transaction Hypervisor",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # list
    p_list = sub.add_parser("list", help="List saga transactions")
    p_list.add_argument("--state", choices=["PENDING", "EXECUTING", "COMPENSATING", "COMMITTED", "ABORTED", "QUARANTINED"], help="Filter by state")
    p_list.set_defaults(func=cmd_list)

    # inspect
    p_insp = sub.add_parser("inspect", help="Inspect a specific saga transaction")
    p_insp.add_argument("tx_id", help="Transaction ID")
    p_insp.set_defaults(func=cmd_inspect)

    # recover
    p_rec = sub.add_parser("recover", help="Recover dangling orphaned sagas")
    p_rec.add_argument("--tx-id", help="Specific transaction ID to recover")
    p_rec.set_defaults(func=cmd_recover)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()