#!/usr/bin/env python3
"""Bounded explicit-state checker for LikhaDB's Iceberg/WAL durability interlock."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, replace
from pathlib import Path
import sys


MAX_LSN = 3
BATCH_LIMIT = 2


@dataclass(frozen=True)
class Entry:
    lsn: int
    kind: str  # "create" or "insert"


@dataclass(frozen=True)
class State:
    entries: tuple[Entry, ...] = ()
    wal: tuple[int, ...] = ()
    staged: tuple[int, ...] = ()
    watermark: int = 0
    collection_created: bool = False
    batch: tuple[int, ...] = ()
    staging_result: str = "idle"  # idle, pending, success, failure


@dataclass(frozen=True)
class SourcePolicy:
    truncate_only_without_ddl: bool
    ddl_only_advances_watermark: bool


def repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "Cargo.toml").is_file() and (parent / "crates").is_dir():
            return parent
    raise RuntimeError("run from a LikhaDB checkout")


def read_policy(root: Path) -> SourcePolicy:
    flusher = (root / "crates/likhadb-lakehouse/src/iceberg_flusher.rs").read_text()
    staging = (root / "crates/likhadb-lakehouse/src/staging_io.rs").read_text()
    wal = (root / "crates/likhadb-persist/src/wal/mod.rs").read_text()
    recovery = (root / "crates/likhadb-lakehouse/src/iceberg_recovery.rs").read_text()

    required = {
        "batch bound": ".take(self.max_batch_entries)" in flusher,
        "DDL detection": "has_ddl = true;" in flusher,
        "watermark advancement": "guard.set_iceberg_watermark(max_lsn);" in flusher,
        "WAL truncation": "guard.truncate_wal_up_to(max_lsn)" in flusher,
        "atomic staging transaction": ".fast_append()" in staging
        and "STAGING_WATERMARK_PROP.to_string()" in staging
        and "tx.commit(catalog).await" in staging,
        "watermark drops buffered entries": "self.unflushed.retain(|(entry_lsn, _)| *entry_lsn > lsn);"
        in wal,
        "truncate retains newer LSNs": "if entry.lsn > watermark" in wal,
        "no-snapshot WAL fallback": "return Ok(wal);" in recovery
        and "WalManager::open(dir)?" in recovery,
    }
    missing = [name for name, present in required.items() if not present]
    if missing:
        print("SOURCE DRIFT: covered atomic steps changed: " + ", ".join(missing))
        raise SystemExit(3)

    guarded = "if !has_ddl {" in flusher
    deliberately_unguarded = "if true {" in flusher
    if not guarded and not deliberately_unguarded:
        print("SOURCE DRIFT: WAL truncation's DDL gate is not modeled")
        raise SystemExit(3)

    ddl_only = (
        "if batches.is_empty()" in flusher
        and "self.wal.write().await.set_iceberg_watermark(max_lsn);" in flusher
    )
    if not ddl_only:
        print("SOURCE DRIFT: DDL-only watermark behavior is not modeled")
        raise SystemExit(3)

    return SourcePolicy(guarded, ddl_only)


def entry_by_lsn(state: State, lsn: int) -> Entry:
    return next(entry for entry in state.entries if entry.lsn == lsn)


def transitions(state: State, policy: SourcePolicy):
    next_lsn = len(state.entries) + 1

    if state.staging_result == "idle" and next_lsn <= MAX_LSN:
        if not state.collection_created:
            entry = Entry(next_lsn, "create")
            yield "ack CreateCollection", replace(
                state,
                entries=state.entries + (entry,),
                wal=state.wal + (next_lsn,),
                collection_created=True,
            )
        else:
            entry = Entry(next_lsn, "insert")
            yield f"ack Insert at LSN {next_lsn}", replace(
                state,
                entries=state.entries + (entry,),
                wal=state.wal + (next_lsn,),
            )

    if state.staging_result == "idle":
        unflushed = tuple(
            entry.lsn for entry in state.entries if entry.lsn > state.watermark
        )[:BATCH_LIMIT]
        if unflushed:
            yield f"start flush of LSNs {list(unflushed)}", replace(
                state, batch=unflushed, staging_result="pending"
            )

    if state.staging_result == "pending":
        dml = tuple(
            lsn for lsn in state.batch if entry_by_lsn(state, lsn).kind == "insert"
        )
        if dml:
            yield "Iceberg staging commit succeeds", replace(
                state,
                staged=tuple(sorted(set(state.staged) | set(dml))),
                staging_result="success",
            )
            yield "Iceberg staging commit fails", replace(
                state, staging_result="failure"
            )
        else:
            yield "DDL-only batch has no staging rows", replace(
                state, staging_result="success"
            )

    if state.staging_result in {"success", "failure"}:
        if state.staging_result == "failure":
            yield "finish failed flush without advancing", replace(
                state, batch=(), staging_result="idle"
            )
        else:
            max_lsn = max(state.batch)
            has_ddl = any(
                entry_by_lsn(state, lsn).kind == "create" for lsn in state.batch
            )
            has_dml = any(
                entry_by_lsn(state, lsn).kind == "insert" for lsn in state.batch
            )
            new_watermark = max(state.watermark, max_lsn)
            truncate = has_dml and (
                not policy.truncate_only_without_ddl or not has_ddl
            )
            new_wal = (
                tuple(lsn for lsn in state.wal if lsn > max_lsn)
                if truncate
                else state.wal
            )
            action = f"advance watermark to {new_watermark}"
            if truncate:
                action += f" and truncate WAL through {max_lsn}"
            elif has_ddl:
                action += " but retain WAL because batch has DDL"
            yield action, replace(
                state,
                wal=new_wal,
                watermark=new_watermark,
                batch=(),
                staging_result="idle",
            )


def violation(state: State) -> str | None:
    if not state.entries:
        return None
    create = next((entry for entry in state.entries if entry.kind == "create"), None)
    if create is None:
        return None

    collection_recoverable = create.lsn in state.wal
    if not collection_recoverable:
        return (
            f"CreateCollection LSN {create.lsn} is absent from the WAL and no index "
            "snapshot or local checkpoint exists; WAL-only fallback cannot recreate "
            "the collection and returns before scanning staging"
        )

    for entry in state.entries:
        if entry.kind == "insert" and entry.lsn not in state.wal:
            return (
                f"Insert LSN {entry.lsn} is absent from the WAL; without an index "
                "snapshot or local checkpoint, recovery returns before reading its "
                "durable staging row"
            )
    return None


def format_state(state: State) -> str:
    return (
        f"wal={list(state.wal)} staged={list(state.staged)} "
        f"watermark={state.watermark} batch={list(state.batch)}"
    )


def check(policy: SourcePolicy) -> int:
    initial = State()
    queue = deque([initial])
    predecessor: dict[State, tuple[State, str] | None] = {initial: None}

    while queue:
        state = queue.popleft()
        problem = violation(state)
        if problem:
            trace: list[tuple[str, State]] = []
            cursor = state
            while predecessor[cursor] is not None:
                previous, action = predecessor[cursor]
                trace.append((action, cursor))
                cursor = previous
            trace.reverse()
            print("COUNTEREXAMPLE: acknowledged state is not crash-recoverable")
            print(problem)
            for index, (action, reached) in enumerate(trace, 1):
                print(f"  {index}. {action}")
                print(f"     {format_state(reached)}")
            print(f"Explored {len(predecessor)} distinct states before the shortest failure.")
            return 1

        for action, successor in transitions(state, policy):
            if successor not in predecessor:
                predecessor[successor] = (state, action)
                queue.append(successor)

    print(
        "PASS: every acknowledged entry is crash-recoverable within the bound "
        f"(states={len(predecessor)}, max_lsn={MAX_LSN}, batch_limit={BATCH_LIMIT})."
    )
    return 0


def main() -> int:
    policy = read_policy(repo_root())
    print(
        "Model policy: "
        f"truncate_only_without_ddl={policy.truncate_only_without_ddl}, "
        f"ddl_only_advances_watermark={policy.ddl_only_advances_watermark}"
    )
    return check(policy)


if __name__ == "__main__":
    sys.exit(main())
