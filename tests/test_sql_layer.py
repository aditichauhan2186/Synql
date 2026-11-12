import itertools
import random
import pytest
from synql import Replica, IntegrityError, contest_schema, sync


@pytest.fixture
def mk(tmp_path):
    return lambda rid: Replica(str(tmp_path / f"{rid}.db"), rid, contest_schema())


def count(r, rel):
    return len(r.rows(rel))


def seeded(mk):
    """A and B share Alice, contest C1 and a game on C1, all created with plain SQL."""
    A, B = mk("A"), mk("B")
    A.execute("INSERT INTO player(name) VALUES('Alice')")
    A.execute("INSERT INTO contest(name) VALUES('C1')")
    A.execute("INSERT INTO game(contest) VALUES(1)")
    sync(A, B)
    return A, B


def test_sql_insert_is_replicated_with_local_ids(mk):
    A, B = mk("A"), mk("B")
    A.execute("INSERT INTO player(name) VALUES('Alice')")
    B.execute("INSERT INTO player(name) VALUES('Bea')")
    sync(A, B)
    assert [(r["id"], r["name"]) for r in A.rows("player")] == [(1, "Alice"), (2, "Bea")]
    assert [(r["id"], r["name"]) for r in B.rows("player")] == [(1, "Bea"), (2, "Alice")]
    assert A.view() == B.view()


def test_sql_unique_conflict_across_replicas(mk):
    A, B = mk("A"), mk("B")
    A.execute("INSERT INTO player(name) VALUES('Alice')")
    B.execute("INSERT INTO player(name) VALUES('Alice')")
    sync(A, B)
    assert count(A, "player") == count(B, "player") == 1
    assert A.view() == B.view()


def test_sql_unique_violation_is_local_error(mk):
    A, _ = seeded(mk)
    with pytest.raises(IntegrityError):
        A.execute("INSERT INTO player(name) VALUES('Alice')")
    assert count(A, "player") == 1


def test_sql_abort_semantics_and_compensated_delete(mk):          # Figures 12 and 19, via SQL
    A, B = seeded(mk)
    A.execute("INSERT INTO enrolled(player, contest) VALUES(1, 1)")
    B.execute("DELETE FROM contest WHERE id = 1")                  # no cascade needed in user table
    sync(A, B)
    assert A.view() == B.view()
    assert [count(A, r) for r in ("contest", "game", "enrolled")] == [1, 1, 1]
    c1 = A.find("contest", name="C1")
    assert A.flag("contest", c1) == 1
    A.execute("DELETE FROM enrolled")
    assert A.flag("contest", c1) == 2                              # compensation
    sync(A, B)
    assert A.view() == B.view()
    assert [count(B, r) for r in ("contest", "game", "enrolled")] == [1, 1, 0]


def test_sql_cascade_hides_dependents_without_marking_them(mk):
    A, _ = seeded(mk)
    game = A.find("game", contest=A.find("contest", name="C1"))
    A.execute("DELETE FROM contest WHERE id = 1")
    assert count(A, "contest") == 0 and count(A, "game") == 0     # removed by the computed state
    assert A.flag("game", game) == 0                              # but not marked deleted (paper, Fig 18)


def test_sql_explicit_delete_of_already_hidden_dependent(mk):
    A, B = seeded(mk)
    with A.transaction():
        A.execute("DELETE FROM contest WHERE id = 1")
        A.execute("DELETE FROM game WHERE id = 1")                 # app also deletes the dependent
    game = A.db.execute("SELECT tid FROM _synql_map WHERE rel='game'").fetchone()["tid"]
    assert A.flag("game", game) == 1
    sync(A, B)
    assert A.view() == B.view()


def test_sql_restrict_violation_rolls_back(mk):
    A, _ = seeded(mk)
    A.execute("INSERT INTO enrolled(player, contest) VALUES(1, 1)")
    with pytest.raises(IntegrityError):
        A.execute("DELETE FROM contest WHERE id = 1")
    assert count(A, "contest") == 1                                # user table restored


def test_sql_dangling_reference_rejected(mk):
    A, _ = seeded(mk)
    with pytest.raises(IntegrityError):
        A.execute("INSERT INTO enrolled(player, contest) VALUES(1, 99)")
    assert count(A, "enrolled") == 0


def test_sql_update_and_convergence(mk):
    A, B = seeded(mk)
    A.execute("INSERT INTO player(name) VALUES('Bea')")
    sync(A, B)
    A.execute("INSERT INTO enrolled(player, contest) VALUES(1, 1)")
    A.execute("UPDATE enrolled SET player = 2")
    B.execute("DELETE FROM player WHERE name = 'Alice'")
    sync(A, B)
    assert A.view() == B.view()
    assert A.query("SELECT p.name FROM enrolled e JOIN player p ON p.id = e.player") == [{"name": "Bea"}]


def test_sql_id_is_immutable(mk):
    A, _ = seeded(mk)
    with pytest.raises(Exception):
        A.execute("UPDATE player SET id = 7")


def test_sql_transaction_discarded_on_error(mk):
    A, _ = seeded(mk)
    with pytest.raises(RuntimeError):
        with A.transaction():
            A.execute("INSERT INTO player(name) VALUES('Zed')")
            raise RuntimeError("boom")
    assert count(A, "player") == 1


def test_sql_pending_changes_survive_reopen(tmp_path):
    path = str(tmp_path / "a.db")
    A = Replica(path, "A", contest_schema())
    A.execute("INSERT INTO player(name) VALUES('Alice')")
    A.db.execute("INSERT INTO player(name) VALUES('Crash')")       # captured but never flushed
    A.db.close()
    A2 = Replica(path, "A", contest_schema())
    assert [r["name"] for r in A2.rows("player")] == ["Alice", "Crash"]
    assert len(A2.dump_state()) == 2


@pytest.mark.parametrize("seed", range(15))
def test_sql_random_ops_converge(mk, seed):
    rnd = random.Random(seed)
    reps = [mk(x) for x in "ABC"]
    for _ in range(40):
        r = rnd.choice(reps)
        rel = rnd.choice(["player", "contest", "game", "enrolled"])
        ids = lambda t: [x["id"] for x in r.rows(t)]
        try:
            k = rnd.choice(["i", "i", "d", "u"])
            if k == "i":
                if rel in ("player", "contest"):
                    r.execute(f"INSERT INTO {rel}(name) VALUES(?)", (rnd.choice("xyz"),))
                elif rel == "game" and ids("contest"):
                    r.execute("INSERT INTO game(contest) VALUES(?)", (rnd.choice(ids("contest")),))
                elif rel == "enrolled" and ids("player") and ids("contest"):
                    r.execute("INSERT INTO enrolled(player, contest) VALUES(?,?)",
                              (rnd.choice(ids("player")), rnd.choice(ids("contest"))))
            elif k == "d" and ids(rel):
                r.execute(f"DELETE FROM {rel} WHERE id = ?", (rnd.choice(ids(rel)),))
            elif k == "u" and ids(rel):
                if rel in ("player", "contest"):
                    r.execute(f"UPDATE {rel} SET name = ? WHERE id = ?", (rnd.choice("xyz"), rnd.choice(ids(rel))))
                elif rel == "game" and ids("contest"):
                    r.execute("UPDATE game SET contest = ? WHERE id = ?", (rnd.choice(ids("contest")), rnd.choice(ids(rel))))
        except IntegrityError:
            pass
        if rnd.random() < 0.2:
            a, b = rnd.sample(reps, 2)
            a.receive([b.full_delta()])
    for _ in range(2):
        for a, b in itertools.permutations(reps, 2):
            a.receive([b.full_delta()])
    assert reps[0].dump_state() == reps[1].dump_state() == reps[2].dump_state()
    assert reps[0].view() == reps[1].view() == reps[2].view()
