"""Ready-made scenarios (paper figures) for the dashboard. Each leaves A and B *unsynchronised*."""
from .replica import sync


def _base(A, B):
    A.execute("INSERT INTO player(name) VALUES('Alice')")
    A.execute("INSERT INTO contest(name) VALUES('C1')")
    A.execute("INSERT INTO game(contest) VALUES(1)")
    sync(A, B)


def unique_conflict(A, B):
    A.execute("INSERT INTO player(name) VALUES('Alice')")
    B.execute("INSERT INTO player(name) VALUES('Alice')")


def autoincrement(A, B):
    A.execute("INSERT INTO player(name) VALUES('Alice')")
    B.execute("INSERT INTO player(name) VALUES('Bea')")


def abort_semantics(A, B):
    _base(A, B)
    A.execute("INSERT INTO enrolled(player, contest) VALUES(1, 1)")
    B.execute("DELETE FROM contest WHERE id = 1")


def game_deleted_too(A, B):
    _base(A, B)
    A.execute("INSERT INTO enrolled(player, contest) VALUES(1, 1)")
    B.execute("DELETE FROM game WHERE id = 1")
    B.execute("DELETE FROM contest WHERE id = 1")


def cascade_remove_win(A, B):
    A.execute("INSERT INTO contest(name) VALUES('C1')")
    sync(A, B)
    A.execute("INSERT INTO game(contest) VALUES(1)")
    B.execute("DELETE FROM contest WHERE id = 1")


SCENARIOS = {
    "Unique-key conflict (Fig 1/2)": (unique_conflict,
        "Both replicas insert player 'Alice'. Sync: the oldest identifier is kept, deterministically."),
    "Local auto-increment ids (Fig 3)": (autoincrement,
        "Different players on each replica. Sync: both rows survive, local ids differ per replica."),
    "Abort semantics: enrol vs delete contest (Fig 12)": (abort_semantics,
        "A enrols Alice in C1 while B deletes C1. Sync -> the contest AND its game are restored. "
        "Then run on A: DELETE FROM enrolled;  (compensated deletion, Fig 19: flag goes 1 -> 2)."),
    "Game deleted at B too (Fig 20/21)": (game_deleted_too,
        "Like above but B also deleted the game. Sync -> contest restored, game stays deleted. "
        "Then run on A: DELETE FROM enrolled;  (no compensation: contest and game both go)."),
    "CASCADE remove-win": (cascade_remove_win,
        "A inserts a game on C1 while B deletes C1. Sync -> contest and the concurrent game are both removed."),
}
