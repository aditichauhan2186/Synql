import random
import pytest
from synql import Replica, contest_schema, sync
from synql.workload import run_workload


@pytest.fixture
def mk(tmp_path):
    return lambda rid, atomic=False: Replica(str(tmp_path / f"{rid}.db"), rid, contest_schema(), atomic_txn=atomic)


def players(r):
    return sorted(v[2][0][1] for v in r.view() if v[0] == "player")


def contests(r):
    return sorted(v[2][0][1] for v in r.view() if v[0] == "contest")


def test_transaction_is_one_delta_with_one_txn_id(mk):
    A = mk("A")
    with A.transaction():
        p = A.execute("INSERT INTO player(name) VALUES('Alice')").lastrowid
        c = A.execute("INSERT INTO contest(name) VALUES('C1')").lastrowid
        A.execute("INSERT INTO enrolled(player, contest) VALUES(?,?)", (p, c))
    log = A.local_log()
    assert len(log) == 1 and len(log[0]["tuples"]) == 3
    assert len({t["txn"] for t in log[0]["tuples"]}) == 1 == len({log[0]["txn"]})
    A.execute("INSERT INTO player(name) VALUES('Bea')")           # a lone statement is its own transaction
    A.execute("INSERT INTO player(name) VALUES('Cy')")
    assert len(A.local_log()) == 3
    assert len({d["txn"] for d in A.local_log()}) == 3


def test_transaction_becomes_visible_atomically(mk):
    A, B = mk("A"), mk("B")
    with A.transaction():
        p = A.execute("INSERT INTO player(name) VALUES('Alice')").lastrowid
        c = A.execute("INSERT INTO contest(name) VALUES('C1')").lastrowid
        A.execute("INSERT INTO enrolled(player, contest) VALUES(?,?)", (p, c))
    B.receive(A.local_log())                                      # the only delivery unit is the transaction
    assert [len(B.rows(t)) for t in ("player", "contest", "enrolled")] == [1, 1, 1]
    assert A.view() == B.view() and A.dump_state() == B.dump_state()


def scenario_unique(mk, atomic):
    """B's Bob (tid 2@B) is older than A's transaction, whose tuples get counters >= 5."""
    A, B = mk("A", atomic), mk("B", atomic)
    B.execute("INSERT INTO player(name) VALUES('Bob')")
    A.execute("INSERT INTO player(name) VALUES('t1')")             # advance A's clock
    A.execute("INSERT INTO player(name) VALUES('t2')")
    with A.transaction():
        A.execute("INSERT INTO player(name) VALUES('Bob')")
        A.execute("INSERT INTO contest(name) VALUES('C9')")
    sync(A, B)
    assert A.view() == B.view()
    return A


def test_default_policy_keeps_partial_transaction(mk):
    A = scenario_unique(mk, atomic=False)
    assert players(A).count("Bob") == 1 and contests(A) == ["C9"]    # A's Bob lost, C9 survives alone
    assert A.txn_stats()["partial_transactions"] == 1


def test_atomic_policy_drops_whole_transaction(mk):
    A = scenario_unique(mk, atomic=True)
    assert players(A).count("Bob") == 1 and contests(A) == []        # C9 dropped with the losing Bob
    assert A.txn_stats()["partial_transactions"] == 0


@pytest.mark.parametrize("atomic", [False, True])
def test_dangling_reference_loses_whole_transaction_when_atomic(mk, atomic):
    A, B = mk("A", atomic), mk("B", atomic)
    c1 = A.execute("INSERT INTO contest(name) VALUES('C1')").lastrowid
    sync(A, B)
    with A.transaction():
        A.execute("INSERT INTO game(contest) VALUES(?)", (c1,))
        A.execute("INSERT INTO player(name) VALUES('Zed')")
    B.execute("DELETE FROM contest WHERE id = 1")                 # CASCADE: the concurrent game must go
    sync(A, B)
    assert A.view() == B.view()
    assert ("Zed" in players(A)) == (not atomic)
    assert not [v for v in A.view() if v[0] == "game"]


def test_atomic_policy_lets_the_loser_of_a_dropped_winner_survive(mk):
    """A's transaction {Bob, C1}: its C1 loses against C's older C1, so the whole transaction is dropped.
    B's younger Bob, which had lost to A's Bob, is then free to exist (the fixpoint redoes step 3)."""
    A, B, C = mk("A", True), mk("B", True), mk("C", True)
    with A.transaction():                                            # counters 1..3
        A.execute("INSERT INTO player(name) VALUES('Bob')")
        A.execute("INSERT INTO contest(name) VALUES('C1')")
    C.execute("INSERT INTO contest(name) VALUES('C1')")              # tid 2@C < 3@A
    B.execute("INSERT INTO player(name) VALUES('x')")                # advance B's clock past A's Bob (2@A)
    B.execute("INSERT INTO player(name) VALUES('y')")
    B.execute("INSERT INTO player(name) VALUES('Bob')")
    for _ in range(2):
        for a, b in [(A, B), (A, C), (B, C)]:
            a.receive([b.full_delta()])
            b.receive([a.full_delta()])
    assert A.view() == B.view() == C.view()
    assert A.dump_state() == B.dump_state() == C.dump_state()
    assert contests(A) == ["C1"] and players(A).count("Bob") == 1
    bob = [v for v in A.view() if v[0] == "player" and v[2][0][1] == "Bob"][0]
    assert bob[1].endswith("@B")                                     # B's Bob, not the dropped A's one


@pytest.mark.parametrize("atomic", [False, True])
@pytest.mark.parametrize("seed", range(12))
def test_random_transactions_converge(tmp_path, seed, atomic):
    rnd = random.Random(seed)
    reps = [Replica(str(tmp_path / f"{x}.db"), x, contest_schema(), atomic_txn=atomic) for x in "ABC"]
    run_workload(rnd, reps)
    assert reps[0].dump_state() == reps[1].dump_state() == reps[2].dump_state()
    assert reps[0].view() == reps[1].view() == reps[2].view()
    if atomic:
        assert reps[0].txn_stats()["partial_transactions"] == 0
