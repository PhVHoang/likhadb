---
name: formally-verify-iceberg-wal
description: Exhaustively model-check LikhaDB's Iceberg flusher and WAL recovery interlock. Use when changing WAL watermarks, Iceberg staging, WAL truncation, DDL persistence, recovery, or flusher batching.
---

# Formally verify the Iceberg/WAL durability interlock

This skill checks the promise: **after any crash, every acknowledged collection definition and vector mutation is recoverable from either the WAL or durable Iceberg state.** Breaking it can make acknowledged vectors disappear after restart.

The checker is a dependency-free, explicit-state bounded model checker. TLA+ is the natural tool for event-ordering properties, but TLC is not installed in the development environment. The Python checker keeps the verification reproducible with the repository's existing `python3` and explores every transition order in its stated bound rather than sampling executions.

## Run

From the repository root:

```sh
python3 .agents/skills/formally-verify-iceberg-wal/scripts/check_iceberg_wal.py
```

Exit meanings:

- `0`: the invariant holds for every explored state.
- `1`: a real counterexample exists within the bound; read the shortest trace printed after `COUNTEREXAMPLE`.
- `3`: covered source no longer matches the modeled atomic steps; update the model before trusting a result.

The current synchronized code has a known counterexample, so exit `1` is the expected baseline until the product bug is fixed. Do not turn that into a pass by weakening the invariant or assuming an index snapshot exists.

## Property and bound

The checker explores breadth-first, so the first failure is a shortest trace. It covers:

- one collection;
- one `CreateCollection` DDL entry and up to two later `Insert` DML entries;
- LSNs `1..3`;
- flusher batches of at most two entries;
- every ordering of append, start-flush, staging success/failure, flush finalization, and crash;
- the supported early-life recovery state where neither an Iceberg index snapshot nor a local checkpoint exists; in this state `open_with_iceberg` takes its WAL-only fallback before scanning staging;
- atomic Iceberg commit of a staging row and its table watermark.

One collection and three entries are sufficient to expose the cross-tick truncation bug. The proof does not cover multiple collections, catalog transaction implementation, byte-level WAL corruption, filesystem or S3 behavior, snapshot serialization, deletes, or DDL types other than collection creation. A pass would establish only this bounded model under these assumptions.

## Reading a counterexample

Translate the trace literally into code terms. For example:

1. `WalManager::create_collection` appends DDL LSN 1.
2. `IcebergFlusher::flush_once` advances its in-memory watermark but retains the WAL because the batch has DDL.
3. A later insert is staged in another tick.
4. That DML-only tick calls `truncate_wal_up_to`, removing the earlier DDL.
5. A crash occurs before any index snapshot or local checkpoint exists; `open_with_iceberg` falls back to WAL-only recovery before scanning staging, but the collection definition and mutation WAL entries are gone.

A source-drift result is not a product bug. It means the code/model correspondence must be reviewed. Preserve the property, update atomic steps to match the new code, and rerun.

## Coverage record

Last synchronized product commit: `d91c37fdcf36c44c723681cf34cf932cdc24cf28`.

Covered code:

- `crates/likhadb-lakehouse/src/iceberg_flusher.rs`
  - `IcebergFlusher::flush_once`: batch capture, DDL detection, staging success/failure, watermark advancement, and WAL-truncation gate.
- `crates/likhadb-lakehouse/src/staging_io.rs`
  - `append_to_staging`: atomic data-file plus `last_wal_lsn` property commit.
- `crates/likhadb-persist/src/wal/mod.rs`
  - `WalManager::log_and_apply`, `set_iceberg_watermark`, `collect_unflushed`, and `truncate_wal_up_to`.
- `crates/likhadb-lakehouse/src/iceberg_recovery.rs`
  - `open_with_iceberg`: no-snapshot WAL fallback and staging/WAL recovery order.

Move the sync commit forward only after inspecting every covered function and confirming that the model still gives each indivisible code step one transition. Changes to any covered file require review even if the source-shape guard still passes.

## Updating the verification

When the behavior intentionally changes:

1. State the new durability rule in plain language before editing the model.
2. Map each new transition to a lock scope or atomic storage commit in the covered code.
3. Keep the no-index-snapshot/no-local-checkpoint recovery path unless product behavior removes it.
4. Run the current model and save its shortest counterexample.
5. Update the model, rerun, and require either a pass or a clearly documented new finding.
6. Make a scratch product-code mutation that removes or bypasses the new safety gate. Require the checker to fail, then restore the file and rerun.
7. Update the sync commit only after this sequence.

If a future version uses TLA+, read every result with `$statechart` when that skill is installed. Do not copy release-specific `$statechart` commands into this skill.
