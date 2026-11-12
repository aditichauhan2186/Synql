"""Replays the paper's running example (Figures 12, 19) on two SQLite replicas."""
import tempfile, os
from synql import Replica, contest_schema, sync

d = tempfile.mkdtemp()
A = Replica(os.path.join(d, "A.db"), "A", contest_schema())
B = Replica(os.path.join(d, "B.db"), "B", contest_schema())
alice = A.insert("player", name="Alice")
c1 = A.insert("contest", name="C1")
A.insert("game", contest=c1)
sync(A, B)

show = lambda title: print(f"\n== {title}") or [print(f"  {r}: {x.rows(r)}") for x in (A,) for r in ("contest", "game", "enrolled")]
e = A.insert("enrolled", player=alice, contest=c1)   # replica A enrolls Alice ...
B.delete("contest", c1)                              # ... while B concurrently deletes the contest
sync(A, B)
show("after merge: contest restored (abort semantics)")
print("  contest delete-flag:", A.flag("contest", c1))
A.delete("enrolled", e)                              # compensated deletion
show("after deleting the enrolment: contest + game survive")
print("  contest delete-flag:", A.flag("contest", c1))
sync(A, B)
print("\nreplicas converged:", A.view() == B.view() and A.dump_state() == B.dump_state())
