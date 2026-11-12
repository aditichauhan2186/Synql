import itertools
import random
import pytest
from synql import Replica, IntegrityError, contest_schema, sync


@pytest.fixture
def mk(tmp_path):
    return lambda rid: Replica(str(tmp_path / f"{rid}.db"), rid, contest_schema())


def rels(replica, rel):
    return [v for v in replica.view() if v[0] == rel]


def base(mk):
    """A and B share: player Alice, contest C1, game on C1 (game -> contest is CASCADE)."""
    A, B = mk("A"), mk("B")
    alice = A.insert("player", name="Alice")
    c1 = A.insert("contest", name="C1")
    g = A.insert("game", contest=c1)
    sync(A, B)
    return A, B, alice, c1, g


# ------------------------------------------------------------- uniqueness
def test_unique_conflict_keeps_oldest(mk):
    A, B = mk("A"), mk("B")
    A.insert("player", name="Alice")
    B.insert("player", name="Alice")
    sync(A, B)
    assert A.view() == B.view()
    assert [p[1] for p in rels(A, "player")] == ["1@A"]          # 1@A < 1@B
    assert A.dump_state() == B.dump_state()                      # both tuples still in replicated state


def test_autoincrement_ids_are_local(mk):
    A, B = mk("A"), mk("B")
    A.insert("player", name="Alice")
    B.insert("player", name="Bea")
    sync(A, B)
    assert [(r["id"], r["name"]) for r in A.rows("player")] == [(1, "Alice"), (2, "Bea")]
    assert [(r["id"], r["name"]) for r in B.rows("player")] == [(1, "Bea"), (2, "Alice")]
    assert A.view() == B.view()


# --------------------------------------------------- referential integrity
def test_abort_semantics_add_wins(mk):                           # Figure 12
    A, B, alice, c1, g = base(mk)
    A.insert("enrolled", player=alice, contest=c1)
    B.delete("contest", c1)
    sync(A, B)
    assert A.view() == B.view()
    assert len(rels(A, "contest")) == 1 and len(rels(A, "game")) == 1 and len(rels(A, "enrolled")) == 1
    assert A.flag("contest", c1) == 1                            # still marked deleted in replicated state


def test_compensated_deletion(mk):                               # Figures 18-19
    A, B, alice, c1, g = base(mk)
    e = A.insert("enrolled", player=alice, contest=c1)
    B.delete("contest", c1)
    sync(A, B)
    A.delete("enrolled", e)
    assert A.flag("contest", c1) == 2                            # insertion of contest redone
    sync(A, B)
    assert A.view() == B.view()
    assert len(rels(B, "contest")) == 1 and len(rels(B, "game")) == 1 and not rels(B, "enrolled")


def test_no_compensation_when_game_already_deleted(mk):          # Figures 20-21
    A, B, alice, c1, g = base(mk)
    e = A.insert("enrolled", player=alice, contest=c1)
    B.delete("game", g)
    B.delete("contest", c1)
    sync(A, B)
    assert len(rels(A, "contest")) == 1 and not rels(A, "game")
    A.delete("enrolled", e)
    assert A.flag("contest", c1) == 1                            # no compensation
    sync(A, B)
    assert A.view() == B.view()
    assert not rels(A, "contest") and not rels(A, "game")


def test_compensation_on_insert_of_referencing_tuple(mk):        # Figure 22
    A, B, alice, c1, g = base(mk)
    A.insert("enrolled", player=alice, contest=c1)
    B.delete("contest", c1)
    sync(A, B)
    bob = A.insert("player", name="Bob")
    A.insert("enrolled", player=bob, contest=c1)
    assert A.flag("contest", c1) == 2


def test_cascade_remove_wins(mk):
    A, B = mk("A"), mk("B")
    c1 = A.insert("contest", name="C1")
    sync(A, B)
    A.insert("game", contest=c1)                                 # concurrently inserted dependent
    B.delete("contest", c1)
    sync(A, B)
    assert A.view() == B.view()
    assert not rels(A, "contest") and not rels(A, "game")


def test_local_constraints_still_enforced(mk):
    A, B, alice, c1, g = base(mk)
    A.insert("enrolled", player=alice, contest=c1)
    with pytest.raises(IntegrityError):
        A.delete("contest", c1)                                  # RESTRICT
    with pytest.raises(IntegrityError):
        A.insert("player", name="Alice")                         # unique
    with pytest.raises(IntegrityError):
        A.insert("enrolled", player="99@A", contest=c1)          # dangling FK


# ------------------------------------------------- convergence / SEC
def test_all_delivery_orders_converge(mk):                       # scenario of Figure 8
    O, A, B = mk("O"), mk("A"), mk("B")
    p1, p2 = O.insert("player", name="P1"), O.insert("player", name="P2")
    c1 = O.insert("contest", name="C1")
    sync(O, A)
    sync(O, B)
    e = A.insert("enrolled", player=p1, contest=c1)
    A.update("enrolled", e, "player", p2)
    B.delete("player", p1)
    sync(A, B)
    expected_view, expected_state = A.view(), A.dump_state()
    assert B.view() == expected_view
    assert ("enrolled", e, (("player", p2), ("contest", c1))) in expected_view
    assert not [p for p in rels(A, "player") if p[1] == p1]

    base_log = O.local_log()
    others = A.local_log() + B.local_log()
    for i, perm in enumerate(itertools.permutations(others)):
        R = mk(f"R{i}")
        R.receive(base_log)
        for d in perm:                                           # one delta at a time, any order
            R.receive([d])
        assert R.view() == expected_view
        assert R.dump_state() == expected_state


@pytest.mark.parametrize("seed", range(25))
def test_random_concurrent_ops_converge_and_keep_integrity(mk, seed):
    rnd = random.Random(seed)
    reps = [mk(x) for x in "ABC"]
    names = ["x", "y", "z"]

    def live(rep, rel):
        return [v[1] for v in rep.view() if v[0] == rel]

    def random_op(rep):
        rel = rnd.choice(["player", "contest", "game", "enrolled"])
        kind = rnd.choice(["insert", "insert", "delete", "update"])
        r = rep.schema[rel]
        try:
            if kind == "insert":
                vals = {}
                for f in r.fields:
                    fk = next((x for x in r.foreign_keys if x.field == f), None)
                    if fk:
                        c = live(rep, fk.ref)
                        if not c:
                            return
                        vals[f] = rnd.choice(c)
                    else:
                        vals[f] = rnd.choice(names)
                rep.insert(rel, **vals)
            else:
                c = live(rep, rel)
                if not c:
                    return
                t = rnd.choice(c)
                if kind == "delete":
                    rep.delete(rel, t)
                else:
                    f = rnd.choice(r.fields)
                    fk = next((x for x in r.foreign_keys if x.field == f), None)
                    if fk:
                        cand = live(rep, fk.ref)
                        if not cand:
                            return
                        rep.update(rel, t, f, rnd.choice(cand))
                    else:
                        rep.update(rel, t, f, rnd.choice(names))
        except IntegrityError:
            pass

    for _ in range(40):
        random_op(rnd.choice(reps))
        if rnd.random() < 0.2:
            a, b = rnd.sample(reps, 2)
            a.receive([b.full_delta()])

    for _ in range(2):                                           # final anti-entropy rounds
        for a, b in itertools.permutations(reps, 2):
            a.receive([b.full_delta()])

    assert reps[0].dump_state() == reps[1].dump_state() == reps[2].dump_state()
    assert reps[0].view() == reps[1].view() == reps[2].view()
    # uniqueness holds in the computed state
    for rel in ("player", "contest"):
        vals = [v[2] for v in reps[0].view() if v[0] == rel]
        assert len(vals) == len(set(vals))
