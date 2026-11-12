# Synql prototype (Python + SQLite)

Two (or more) SQLite replicas, no coordination. Merge = CRDT join; integrity = computed state + compensation.

    pip install -r requirements.txt
    pytest -q                     # 97 tests
    python demo.py                # Fig 12 -> Fig 19 on two replicas
    streamlit run dashboard.py    # interactive demo

## Use it with plain SQL
```python
A = Replica("a.db", "A", contest_schema()); B = Replica("b.db", "B", contest_schema())
A.execute("INSERT INTO player(name) VALUES('Alice')")      # triggers capture -> CRDT metadata
B.execute("INSERT INTO player(name) VALUES('Alice')")
sync(A, B)                                                  # delta exchange; one Alice survives
with A.transaction():                                       # statements validated/replicated together
    A.execute("DELETE FROM contest WHERE id = 1")
```
User tables carry a *local* `id INTEGER PRIMARY KEY AUTOINCREMENT` (never replicated; differs per replica) and are
rewritten from the computed state after every local change or merge. The connection runs with `foreign_keys=OFF`
(SQLite's default), so Synql, not SQLite, decides FK behaviour: a deleted parent hides its CASCADE dependents
(without marking them deleted, as in the paper), RESTRICT violations and dangling references raise `IntegrityError`
and the user tables roll back to the replicated truth.

| Paper concept | Where |
|---|---|
| Labeled timestamps `c@replica` (total order) | `parse()`, `_tick()`, `_observe()` in `replica.py` |
| LWW register per field | `_merge()` |
| CLFlag (odd = deleted) | `_synql_tuple.flag`, merge = max |
| Delta sync | `_synql_log`, `push()` / `receive()`, `full_delta()` for 3+ replicas |
| Auto-increment ids not replicated | `_synql_map` + `_materialize()` |
| Computed state, steps 1-4 | `_compute()` |
| Compensation cases (i), (ii) | `_compensate()` (called by `insert/delete/update`) |
| SQL triggers capturing user changes | `_capture_triggers()`, `flush()` |
| RESTRICT = add-win, CASCADE = remove-win | `ForeignKey.on_delete` in `schema.py` |

## Comparison with the authors' repository (coast-team/synql, last commit 2023-02-28)
| Topic | Authors' repo | This prototype |
|---|---|---|
| Design | Earlier CRR-style design: `pull_from` writes undo entries at merge time (steps A-E in `_CONFLICT_RESOLUTION`) | Paper's design: merge only joins CRDTs, integrity comes from a deterministic computed state |
| CLFlag / computed state / local compensation (Fig 19-22) | **Not present** (no flag, no compensation code) | Implemented |
| `delta()` | Raises `NotImplementedError("Unfinished implementation")`; sync = attach the whole remote DB | Delta log per peer (`push`), plus `full_delta()` |
| SQL triggers | Yes, generated from the parsed schema; supports `rowid`, multi-column unique/FK, NULLs, ON UPDATE actions, SET NULL | Yes, but fixed per-relation; single unique key, single-column FKs, no NULLs, no ON UPDATE, no SET NULL |
| RESTRICT -> CASCADE chain (test `test_concur_del_fk_restrict_rec`) | Restores X for Z -RESTRICT-> Y -CASCADE-> X | Same, after the fix described below (`tests/test_repo_parity.py`) |

**Finding.** Read literally, step 2 of the paper restores only tuples referenced through abort (RESTRICT) links; then step 4
removes the restored tuple again if it has a CASCADE link to a deleted tuple, which loses the add-win insertion. The repo
test above shows the intended behaviour, so `_compute` treats a tuple referenced via RESTRICT as *required* and restores everything
it references, and `_compensate` follows the same rule. Worth raising with your supervisor/authors as an ambiguity in the paper.

## Remaining simplifications (state them in the report)
- One unique key per relation; single-column FKs; NOT NULL columns; no cyclic FKs; no ON UPDATE actions.
- `push()` relays only a replica's own deltas; use `full_delta()` for 3+ replicas.
- Compensation for FK *updates* is our reading of cases (i)/(ii).
- A user transaction is validated on exit (`transaction()`), not isolated while it runs.

## Future work from the progress report

### 1. Unifying `_synql_log` and `_synql_fklog` (authors' repo, `unified_log.patch`)
These two tables only exist in the authors' implementation, so the change is a patch against `coast-team/synql`
(`git apply unified_log.patch`, enable with `Config(unified_log=True)`). One physical table `_synql_unified_log`
(`val` for value changes, `foreign_row_ts/peer` for FK changes, same primary key and `(peer, ts)` index) replaces both;
`_synql_log` / `_synql_fklog` remain as read-only views so the rest of the engine is untouched. Pull copies one table
instead of two and the context update is one statement instead of two.

| Criterion | Result |
|---|---|
| Correctness | The authors' 39 tests pass in both modes (`SYNQL_UNIFIED=1 pytest test`); the helpers compare replica state through the views. |
| Code complexity | Dual-mode patch: +92/-4 lines in `crr.py` (compat views + two code paths). A replacement instead of an option would *remove* the duplicated table/index/insert/update definitions; not measured. |
| Sync efficiency | `experiments/bench_unified_log.py`: pull of 2 x n rows (n=2000: 1.18 s vs 1.16 s; n=8000: 18.3 s vs 19.2 s, median of 3). No clear difference, within noise. |
| Metadata storage | n=2000: identical file size (1,245,184 B); n=8000: 4,538,368 B vs 4,591,616 B (+1.2%) because every row carries the two nullable FK columns. |
| Side finding | `pull_from` time grows much faster than linearly (4x rows -> ~15x time) in both modes. |

Conclusion: unifying is behaviour-preserving and simplifies the schema, but brings no measurable sync or storage win here.

### 2. Transaction-aware CRDT grouping (this prototype)
- Every SQL transaction (`with r.transaction():`, or one statement in autocommit) gets a transaction id (labeled timestamp).
  Its operations travel as **one delta** (`{"txn": ..., "tuples": [...]}`) and are merged in one SQLite transaction,
  so a replica never exposes half of a transaction. Each tuple stores its creation transaction (grow-only, merge = set once).
- Optional policy `Replica(..., atomic_txn=True)`: if the integrity rules (unique conflict, dangling reference) keep only
  *some* of the tuples a transaction inserted, the rest are dropped too (fixpoint, recomputed deterministically; a fully
  lost transaction can revive if its blocker is itself dropped). The policy is our design choice; the paper does not define it.

`python experiments/txn_atomicity.py` (60 random runs, 3 replicas, 40 concurrent multi-statement transactions):

| Policy | Converged | Multi-tuple txns | Partially surviving txns | Tuples kept |
|---|---|---|---|---|
| default (grouped delivery only) | 60/60 | 622 | 80 | 66.4% |
| `atomic_txn=True` | 60/60 | 624 | 0 | 58.8% |

Consistency: no partial transactions with `atomic_txn`, at the price of discarding more tuples. Conflict resolution:
decisions now depend on transaction membership, not only on a tuple's own constraints. Convergence: unchanged (computed
state is still a function of the merged replicated state). The two runs do not execute identical workloads (operations
depend on each replica's local state), so the kept-% comparison is indicative only.
