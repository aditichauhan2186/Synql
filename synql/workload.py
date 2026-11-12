"""Random multi-statement transactions on several replicas (used by tests and experiments)."""
import itertools
from .replica import IntegrityError

NAMES = ["a", "b", "c"]          # small pool -> frequent unique-key conflicts


def _ids(r, rel):
    return [x["id"] for x in r.rows(rel)]


def random_transaction(rnd, r):
    """One application transaction (1-3 SQL statements touching related tables)."""
    kind = rnd.choice(["enrol", "contest_game", "player", "delete_contest", "enrol_existing"])
    try:
        with r.transaction():
            if kind == "enrol":                                     # player + contest + enrolment
                p = r.execute("INSERT INTO player(name) VALUES(?)", (rnd.choice(NAMES),)).lastrowid
                c = r.execute("INSERT INTO contest(name) VALUES(?)", (rnd.choice(NAMES),)).lastrowid
                r.execute("INSERT INTO enrolled(player, contest) VALUES(?,?)", (p, c))
            elif kind == "contest_game":                            # contest + its game
                c = r.execute("INSERT INTO contest(name) VALUES(?)", (rnd.choice(NAMES),)).lastrowid
                r.execute("INSERT INTO game(contest) VALUES(?)", (c,))
            elif kind == "player":
                r.execute("INSERT INTO player(name) VALUES(?)", (rnd.choice(NAMES),))
            elif kind == "delete_contest" and _ids(r, "contest"):
                r.execute("DELETE FROM contest WHERE id = ?", (rnd.choice(_ids(r, "contest")),))
            elif kind == "enrol_existing" and _ids(r, "contest") and _ids(r, "player"):
                c = rnd.choice(_ids(r, "contest"))
                r.execute("INSERT INTO enrolled(player, contest) VALUES(?,?)", (rnd.choice(_ids(r, "player")), c))
                r.execute("INSERT INTO game(contest) VALUES(?)", (c,))
    except IntegrityError:
        pass


def run_workload(rnd, reps, steps=40, merge_prob=0.2):
    """Concurrent transactions with occasional partial merges, then full anti-entropy."""
    for _ in range(steps):
        random_transaction(rnd, rnd.choice(reps))
        if rnd.random() < merge_prob:
            a, b = rnd.sample(reps, 2)
            a.receive([b.full_delta()])
    for _ in range(2):
        for a, b in itertools.permutations(reps, 2):
            a.receive([b.full_delta()])
