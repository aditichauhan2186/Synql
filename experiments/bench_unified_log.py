import os, sys, time, tempfile, pathlib, statistics
sys.path.insert(0, os.environ.get("SYNQL_REPO", "synql_ref"))   # checkout of coast-team/synql with unified_log.patch applied
import pysqlite3 as sqlite3
from synql import crr

def run(unified, n, seed_rows=0):
    d = pathlib.Path(tempfile.mkdtemp())
    a = sqlite3.connect(d/"a.db"); b = sqlite3.connect(d/"b.db")
    a.execute("PRAGMA foreign_keys=ON")
    a.execute("CREATE TABLE parent(id integer PRIMARY KEY, name text UNIQUE NOT NULL, note text)")
    a.execute("CREATE TABLE child(id integer PRIMARY KEY, parent integer NOT NULL REFERENCES parent(id) ON DELETE CASCADE, label text)")
    conf = crr.Config(unified_log=unified)
    crr.init(a, replica_id=1, conf=conf); a.commit()
    crr.clone_to(a, b, replica_id=2)
    a.execute("BEGIN")
    for i in range(n):
        a.execute("INSERT INTO parent(name, note) VALUES(?,?)", (f"p{i}", "x"*20))
    for i in range(n):
        a.execute("INSERT INTO child(parent, label) VALUES(?,?)", (i+1, f"c{i}"))
    a.commit()
    # a few updates and deletes to also exercise the logs
    a.execute("UPDATE parent SET note='y' WHERE id % 10 = 0"); a.execute("UPDATE child SET label='z' WHERE id % 7 = 0")
    a.execute("DELETE FROM parent WHERE id % 50 = 0"); a.commit()
    b.execute("PRAGMA foreign_keys=ON")
    t0 = time.perf_counter(); crr.pull_from(b, d/"a.db"); b.commit(); dt = time.perf_counter()-t0
    tables = [r[0] for r in a.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '_synql_%'")]
    a.execute("VACUUM")
    size = {}
    for t in tables:
        try:
            size[t] = a.execute("SELECT sum(pgsize) FROM dbstat WHERE name IN (?, ?)", (t, "sqlite_autoindex_"+t+"_1")).fetchone()[0] or 0
        except Exception: size[t] = None
    meta_rows = {t: a.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0] for t in tables if t.startswith(("_synql_log","_synql_fklog","_synql_unified"))}
    total = os.path.getsize(d/"a.db")
    b_rows = b.execute("SELECT (SELECT count(*) FROM parent),(SELECT count(*) FROM child)").fetchone()
    a_rows = a.execute("SELECT (SELECT count(*) FROM parent),(SELECT count(*) FROM child)").fetchone()
    return dict(pull=dt, total=total, meta_rows=meta_rows, ok=(tuple(a_rows)==tuple(b_rows)), tables=tables)

if __name__ == "__main__":
    n = int(sys.argv[1])
    for unified in (False, True):
        r = [run(unified, n) for _ in range(3)]
        print("unified" if unified else "original", f"n={n}",
              "pull median %.3fs" % statistics.median(x["pull"] for x in r),
              "| db bytes", r[0]["total"], "| log rows", r[0]["meta_rows"], "| replicas equal:", r[0]["ok"],
              "| has unified table:", "_synql_unified_log" in r[0]["tables"], "| has _synql_log table:", "_synql_log" in r[0]["tables"])
