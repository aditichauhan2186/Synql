"""Impact of transaction-aware grouping: default policy vs all-or-nothing (atomic_txn).
Run:  python experiments/txn_atomicity.py [seeds]"""
import os, random, sys, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from synql import Replica, contest_schema
from synql.workload import run_workload

seeds = int(sys.argv[1]) if len(sys.argv) > 1 else 60
print(f"{seeds} random runs, 3 replicas, 40 concurrent multi-statement transactions each\n")
print(f"{'policy':<10}{'converged':>10}{'multi-tuple txns':>18}{'partial txns':>14}{'tuples inserted':>17}{'tuples kept':>13}{'kept %':>8}")
for atomic in (False, True):
    conv = multi = partial = ins = kept = 0
    for seed in range(seeds):
        d = tempfile.mkdtemp()
        reps = [Replica(os.path.join(d, f"{x}.db"), x, contest_schema(), atomic_txn=atomic) for x in "ABC"]
        run_workload(random.Random(seed), reps)
        conv += reps[0].dump_state() == reps[1].dump_state() == reps[2].dump_state() and \
                reps[0].view() == reps[1].view() == reps[2].view()
        s = reps[0].txn_stats()
        multi += s["multi_tuple_transactions"]; partial += s["partial_transactions"]
        ins += s["tuples_inserted"]; kept += s["tuples_kept"]
    print(f"{'atomic' if atomic else 'default':<10}{f'{conv}/{seeds}':>10}{multi:>18}{partial:>14}{ins:>17}{kept:>13}{100*kept/ins:>7.1f}%")
