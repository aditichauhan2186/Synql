"""Synql prototype: replicated state (CRDTs) + computed state + compensation, on SQLite.

Replicated state, per tuple:   id (labeled timestamp "counter@replica"),
                               one LWW register per field, one CLFlag (odd = deleted).
Computed state:                deterministic function of the replicated state (4 steps of the paper).
Compensation:                  extra flag increments produced by *local* operations.
"""
import contextlib
import json
import sqlite3


class IntegrityError(Exception):
    pass


def parse(label):
    """Labeled timestamp 'c@r' -> (c, r); tuples compare lexicographically (total order)."""
    c, r = label.split("@")
    return int(c), r


def sync(a, b):
    """Bidirectional delta synchronisation of two replicas."""
    a.push(b)
    b.push(a)


class Replica:
    def __init__(self, path, rid, schema, atomic_txn=False):
        assert "@" not in rid
        self.rid, self.schema = rid, schema
        self.atomic_txn = atomic_txn   # all-or-nothing policy for the tuples a transaction inserted
        self._cur_txn = None           # id of the transaction being flushed
        self._batch_tuples = []        # operations of the transaction being flushed (one delta)
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=OFF")   # Synql enforces integrity itself (SQLite default)
        self._batch = False       # True while flush() applies captured SQL changes atomically
        self._deferred = False    # True inside `with replica.transaction()`
        self._init_tables()
        self.flush()              # recover SQL changes captured before a crash
        self._materialize()

    # ------------------------------------------------------------------ setup
    def _init_tables(self):
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS _synql_meta(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS _synql_tuple(rel TEXT, tid TEXT, flag INTEGER NOT NULL DEFAULT 0, txn TEXT,
                                                PRIMARY KEY(rel, tid));
        CREATE TABLE IF NOT EXISTS _synql_field(rel TEXT, tid TEXT, field TEXT, value TEXT, ts TEXT,
                                                PRIMARY KEY(rel, tid, field));
        CREATE TABLE IF NOT EXISTS _synql_map(rel TEXT, tid TEXT, local_id INTEGER, PRIMARY KEY(rel, tid));
        CREATE TABLE IF NOT EXISTS _synql_log(seq INTEGER PRIMARY KEY AUTOINCREMENT, delta TEXT);
        CREATE TABLE IF NOT EXISTS _synql_peer(peer TEXT PRIMARY KEY, sent INTEGER);
        CREATE TABLE IF NOT EXISTS _synql_capture(seq INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT,
                                                  rel TEXT, local_id INTEGER, data TEXT);
        INSERT OR IGNORE INTO _synql_meta VALUES('clock', '0');
        INSERT OR IGNORE INTO _synql_meta VALUES('capturing', '1');
        """)
        for rel in self.schema.ordered():
            self.db.execute(rel.ddl())
            for trg in self._capture_triggers(rel):
                self.db.execute(trg)

    @staticmethod
    def _capture_triggers(rel):
        """SQL triggers that record every application-level INSERT/UPDATE/DELETE of a user table.
        They are inert while Synql itself rewrites the table (meta 'capturing' = 0)."""
        n, on = rel.name, "WHEN (SELECT value FROM _synql_meta WHERE key='capturing')='1'"
        obj = lambda row: "json_object(" + ",".join(f"'{f}',{row}.\"{f}\"" for f in rel.fields) + ")"
        return [
            f'CREATE TRIGGER IF NOT EXISTS "_synql_i_{n}" AFTER INSERT ON "{n}" {on} BEGIN '
            f"INSERT INTO _synql_capture(op,rel,local_id,data) VALUES('I','{n}',NEW.id,{obj('NEW')}); END",
            f'CREATE TRIGGER IF NOT EXISTS "_synql_u_{n}" AFTER UPDATE ON "{n}" {on} BEGIN '
            f"INSERT INTO _synql_capture(op,rel,local_id,data) VALUES('U','{n}',OLD.id,{obj('NEW')}); END",
            f'CREATE TRIGGER IF NOT EXISTS "_synql_d_{n}" AFTER DELETE ON "{n}" {on} BEGIN '
            f"INSERT INTO _synql_capture(op,rel,local_id,data) VALUES('D','{n}',OLD.id,NULL); END",
            f'CREATE TRIGGER IF NOT EXISTS "_synql_id_{n}" BEFORE UPDATE OF id ON "{n}" '
            f"WHEN NEW.id <> OLD.id AND (SELECT value FROM _synql_meta WHERE key='capturing')='1' "
            f"BEGIN SELECT RAISE(ABORT, 'id is a local identifier and cannot be updated'); END",
        ]

    # ------------------------------------------------------------------ clock
    def _clock(self):
        return int(self.db.execute("SELECT value FROM _synql_meta WHERE key='clock'").fetchone()[0])

    def _tick(self):
        c = self._clock() + 1
        self.db.execute("UPDATE _synql_meta SET value=? WHERE key='clock'", (str(c),))
        return f"{c}@{self.rid}"

    def _observe(self, label):
        c = parse(label)[0]
        if c > self._clock():
            self.db.execute("UPDATE _synql_meta SET value=? WHERE key='clock'", (str(c),))

    # --------------------------------------------------- replicated state / merge
    def _state(self):
        """Replicated state as {(rel, tid): {"flag": n, "fields": {f: (value, ts)}}}.
        Tuples whose registers have not all arrived yet (out-of-order deltas) are ignored."""
        st = {}
        for r in self.db.execute("SELECT rel, tid, flag, txn FROM _synql_tuple"):
            st[(r["rel"], r["tid"])] = {"flag": r["flag"], "fields": {}, "txn": r["txn"]}
        for r in self.db.execute("SELECT rel, tid, field, value, ts FROM _synql_field"):
            st[(r["rel"], r["tid"])]["fields"][r["field"]] = (json.loads(r["value"]), r["ts"])
        return {k: t for k, t in st.items() if set(t["fields"]) == set(self.schema[k[0]].fields)}

    def _merge(self, delta):
        """Join a delta into the replicated state (idempotent, commutative, associative)."""
        d = self.db
        for t in delta["tuples"]:
            d.execute("INSERT OR IGNORE INTO _synql_tuple(rel, tid, flag) VALUES(?,?,0)", (t["rel"], t["tid"]))
            self._observe(t["tid"])
            if t.get("txn"):                                    # creation transaction: set once, grow-only
                self._observe(t["txn"])
                d.execute("UPDATE _synql_tuple SET txn=? WHERE rel=? AND tid=? AND txn IS NULL",
                          (t["txn"], t["rel"], t["tid"]))
            if t.get("flag") is not None:                       # CLFlag: max
                d.execute("UPDATE _synql_tuple SET flag=MAX(flag, ?) WHERE rel=? AND tid=?",
                          (t["flag"], t["rel"], t["tid"]))
            for f, (v, ts) in t["fields"].items():              # LWW register: latest timestamp wins
                self._observe(ts)
                row = d.execute("SELECT ts FROM _synql_field WHERE rel=? AND tid=? AND field=?",
                                (t["rel"], t["tid"], f)).fetchone()
                if row is None or parse(ts) > parse(row["ts"]):
                    d.execute("INSERT OR REPLACE INTO _synql_field VALUES(?,?,?,?,?)",
                              (t["rel"], t["tid"], f, json.dumps(v), ts))

    # ----------------------------------------------------------- computed state
    def _compute(self, state):
        """Computed state. With atomic_txn, a transaction whose inserted tuples were not all kept by the
        integrity rules (unique conflict, dangling reference) loses *all* of them (fixpoint, deterministic)."""
        if not self.atomic_txn:
            return self._compute_base(state)
        members = {}
        for k, t in state.items():
            if t["txn"] and t["flag"] % 2 == 0:
                members.setdefault(t["txn"], []).append(k)
        excluded = set()
        while True:
            S = self._compute_base(state, excluded)
            # only *partially* surviving transactions are cut down; a fully lost one may revive once the
            # tuple that blocked it (e.g. a unique-key winner) has itself been excluded
            newly = {k for ks in members.values()
                     if any(m in S for m in ks) and any(m not in S for m in ks) for k in ks}
            if not newly:
                return S
            excluded |= newly

    def _compute_base(self, state, excluded=frozenset()):
        """The four steps of the paper. Returns the set of (rel, tid) in the computed state."""
        val = lambda k, f: state[k]["fields"][f][0]
        # 1. remove tuples marked as deleted (odd flag)
        S = {k for k, t in state.items() if t["flag"] % 2 == 0 and k not in excluded}
        # 2. add tuples transitively referenced by a live tuple whose FK has abort (RESTRICT) semantics
        #    A tuple that is *required* (referenced via RESTRICT by a live or required tuple) must keep all
        #    its own references, CASCADE ones included, otherwise step 4 would drop it again:
        #    Z -RESTRICT-> Y -CASCADE-> X pulls back Y *and* X (as in the authors' repository tests).
        required = set()
        stack = [(k, False) for k in S]
        while stack:
            k, req = stack.pop()
            for fk in self.schema[k[0]].foreign_keys:
                if req or fk.on_delete == "RESTRICT":
                    ref = (fk.ref, val(k, fk.field))
                    if ref in state and ref not in excluded:
                        S.add(ref)
                        if ref not in required:
                            required.add(ref)
                            stack.append((ref, True))
        # 3. unique-key conflicts: keep the oldest identifier
        groups = {}
        for k in S:
            u = self.schema[k[0]].unique
            if u:
                groups.setdefault((k[0], tuple(val(k, f) for f in u)), []).append(k)
        for g in groups.values():
            if len(g) > 1:
                g.sort(key=lambda k: parse(k[1]))
                S -= set(g[1:])
        # 4. remove tuples that (transitively) reference a tuple absent from the computed state
        changed = True
        while changed:
            changed = False
            for k in list(S):
                if any((fk.ref, val(k, fk.field)) not in S for fk in self.schema[k[0]].foreign_keys):
                    S.discard(k)
                    changed = True
        return S

    # ------------------------------------------------------------- compensation
    def _live_referencer(self, state, key, exclude=None):
        for k, t in state.items():
            if k == exclude or t["flag"] % 2:
                continue
            for fk in self.schema[k[0]].foreign_keys:
                if fk.ref == key[0] and t["fields"][fk.field][0] == key[1]:
                    return True
        return False

    def _abort_targets(self, rel, values, all_fks=False):
        return [(fk.ref, values[fk.field]) for fk in self.schema[rel].foreign_keys
                if all_fks or fk.on_delete == "RESTRICT"]

    def _compensate(self, state, comp, targets, delta, only_if_referenced, exclude=None):
        """Redo the insertion (flag +1) of tuples that are deleted in the replicated state but
        were restored in the computed state, following abort FKs transitively.
        only_if_referenced=True: case (i) deletion/update of a FK -> only if still referenced by a
        non-deleted tuple.  False: case (ii) insertion/update to reference a restored tuple."""
        stack, seen = list(targets), set()
        while stack:
            k = stack.pop()
            if k in seen or k not in state:
                continue
            seen.add(k)
            t = state[k]
            if (t["flag"] % 2 == 1 and k in comp
                    and (not only_if_referenced or self._live_referencer(state, k, exclude))):
                t["flag"] += 1
                delta["tuples"].append({"rel": k[0], "tid": k[1], "fields": {}, "flag": t["flag"]})
            vals = {f: v for f, (v, _) in t["fields"].items()}
            stack.extend(self._abort_targets(k[0], vals, all_fks=True))   # visited tuples are required

    # --------------------------------------------------------------- local ops
    def _check(self, state, comp, rel, values, ignore=None):
        r = self.schema[rel]
        for fk in r.foreign_keys:
            if (fk.ref, values[fk.field]) not in comp:
                raise IntegrityError(f"{rel}.{fk.field}: referenced {fk.ref} tuple does not exist")
        if r.unique:
            uv = tuple(values[f] for f in r.unique)
            for k in comp:
                if k[0] == rel and k != ignore and \
                        tuple(state[k]["fields"][f][0] for f in r.unique) == uv:
                    raise IntegrityError(f"{rel}: unique key {r.unique}={uv} already exists")

    def _commit(self, delta):
        if self._batch:                      # inside flush(): merge now, log ONE delta for the transaction
            self._merge(delta)
            self._batch_tuples.extend(delta["tuples"])
            return
        self.db.execute("BEGIN")
        self._merge(delta)
        self.db.execute("INSERT INTO _synql_log(delta) VALUES(?)", (json.dumps(delta),))
        self.db.execute("COMMIT")
        self._materialize()

    def insert(self, rel, **values):
        r = self.schema[rel]
        if set(values) != set(r.fields):
            raise ValueError(f"{rel} expects fields {r.fields}")
        state = self._state()
        comp = self._compute(state)
        self._check(state, comp, rel, values)
        tid = self._tick()
        delta = {"tuples": [{"rel": rel, "tid": tid, "flag": None, "txn": self._cur_txn or tid,
                             "fields": {f: [v, tid] for f, v in values.items()}}]}
        self._compensate(state, comp, self._abort_targets(rel, values), delta, False)   # case (ii)
        self._commit(delta)
        return tid

    def delete(self, rel, tid, _cascaded_ok=False):
        key = (rel, tid)
        state = self._state()
        comp = self._compute(state)
        # _cascaded_ok: SQL layer, the app explicitly deletes a dependent that a CASCADE already hid
        if key not in comp and not (_cascaded_ok and key in state and state[key]["flag"] % 2 == 0):
            raise IntegrityError(f"{rel} {tid} does not exist")
        for k in comp:                                            # RESTRICT: abort the deletion
            for fk in self.schema[k[0]].foreign_keys:
                if fk.on_delete == "RESTRICT" and fk.ref == rel and state[k]["fields"][fk.field][0] == tid:
                    raise IntegrityError(f"{rel} {tid} is referenced by {k[0]} {k[1]} (RESTRICT)")
        t = state[key]
        if t["flag"] % 2 == 0:
            t["flag"] += 1                                        # CLFlag.enable
        delta = {"tuples": [{"rel": rel, "tid": tid, "fields": {}, "flag": t["flag"]}]}
        vals = {f: v for f, (v, _) in t["fields"].items()}
        self._compensate(state, comp, self._abort_targets(rel, vals), delta, True)      # case (i)
        self._commit(delta)

    def update(self, rel, tid, field, value):
        key = (rel, tid)
        r = self.schema[rel]
        if field not in r.fields:
            raise ValueError(f"{rel} has no field {field}")
        state = self._state()
        comp = self._compute(state)
        if key not in comp:
            raise IntegrityError(f"{rel} {tid} does not exist")
        vals = {f: v for f, (v, _) in state[key]["fields"].items()}
        old = vals[field]
        vals[field] = value
        self._check(state, comp, rel, vals, ignore=key)
        ts = self._tick()
        delta = {"tuples": [{"rel": rel, "tid": tid, "flag": None, "fields": {field: [value, ts]}}]}
        fk = next((f for f in r.foreign_keys if f.field == field), None)
        if fk and fk.on_delete == "RESTRICT" and old != value:
            state[key]["fields"][field] = (value, ts)
            self._compensate(state, comp, [(fk.ref, value)], delta, False)                     # new target
            self._compensate(state, comp, [(fk.ref, old)], delta, True, exclude=key)           # old target
        self._commit(delta)

    # ------------------------------------------------------------ synchronisation
    def local_log(self):
        return [json.loads(r["delta"]) for r in self.db.execute("SELECT delta FROM _synql_log ORDER BY seq")]

    def push(self, other):
        """Send the deltas not yet sent to `other` (only locally generated deltas are relayed)."""
        self.flush()
        row = self.db.execute("SELECT sent FROM _synql_peer WHERE peer=?", (other.rid,)).fetchone()
        sent = row["sent"] if row else 0
        rows = self.db.execute("SELECT seq, delta FROM _synql_log WHERE seq > ? ORDER BY seq", (sent,)).fetchall()
        if rows:
            other.receive([json.loads(r["delta"]) for r in rows])
            self.db.execute("INSERT OR REPLACE INTO _synql_peer VALUES(?,?)", (other.rid, rows[-1]["seq"]))

    def receive(self, deltas):
        self.flush()
        self.db.execute("BEGIN")
        for d in deltas:
            self._merge(d)
        self.db.execute("COMMIT")
        self._materialize()

    def full_delta(self):
        """Whole replicated state as one delta (for 3+ replicas / anti-entropy)."""
        tuples = {}
        for r in self.db.execute("SELECT rel, tid, flag, txn FROM _synql_tuple"):
            tuples[(r["rel"], r["tid"])] = {"rel": r["rel"], "tid": r["tid"], "flag": r["flag"],
                                            "txn": r["txn"], "fields": {}}
        for r in self.db.execute("SELECT rel, tid, field, value, ts FROM _synql_field"):
            tuples[(r["rel"], r["tid"])]["fields"][r["field"]] = [json.loads(r["value"]), r["ts"]]
        return {"tuples": list(tuples.values())}

    # ----------------------------------------------------- user-visible database
    def _materialize(self):
        """Rewrite the user tables from the computed state; local auto ids are never replicated."""
        state = self._state()
        S = self._compute(state)
        order = self.schema.ordered()
        amap = {(r["rel"], r["tid"]): r["local_id"] for r in self.db.execute("SELECT * FROM _synql_map")}
        nxt = {r.name: max([v for (rl, _), v in amap.items() if rl == r.name], default=0) for r in order}
        new_rows = []
        for r in order:
            for k in sorted((k for k in S if k[0] == r.name), key=lambda k: parse(k[1])):
                if k not in amap:
                    nxt[r.name] += 1
                    amap[k] = nxt[r.name]
                    new_rows.append((k[0], k[1], amap[k]))
        d = self.db
        d.execute("PRAGMA foreign_keys=OFF")        # SQLite default; we verify integrity ourselves below
        d.execute("BEGIN")
        try:
            d.execute("UPDATE _synql_meta SET value='0' WHERE key='capturing'")   # triggers inert
            d.executemany("INSERT INTO _synql_map VALUES(?,?,?)", new_rows)
            for r in reversed(order):
                d.execute(f'DELETE FROM "{r.name}"')
            for r in order:
                fks = {fk.field: fk for fk in r.foreign_keys}
                for k in sorted((k for k in S if k[0] == r.name), key=lambda k: parse(k[1])):
                    row = [amap[k]]
                    for f in r.fields:
                        v = state[k]["fields"][f][0]
                        row.append(amap[(fks[f].ref, v)] if f in fks else v)
                    d.execute(f'INSERT INTO "{r.name}" VALUES ({",".join("?" * len(row))})', row)
            d.execute("UPDATE _synql_meta SET value='1' WHERE key='capturing'")
            d.execute("COMMIT")
        except Exception:
            d.execute("ROLLBACK")
            raise
        bad = d.execute("PRAGMA foreign_key_check").fetchall()
        if bad:
            raise RuntimeError(f"referential integrity violated: {[tuple(b) for b in bad]}")

    # ------------------------------------------------------------------ SQL layer
    def execute(self, sql, params=()):
        """Run application SQL on the user tables; triggers capture the change, then it is replicated."""
        try:
            cur = self.db.execute(sql, params)
        except sqlite3.IntegrityError as e:          # e.g. UNIQUE enforced by the user table itself
            raise IntegrityError(str(e)) from e
        if not self._deferred:
            self.flush()
        return cur

    def query(self, sql, params=()):
        return [dict(r) for r in self.db.execute(sql, params).fetchall()]

    @contextlib.contextmanager
    def transaction(self):
        """Group several SQL statements; they are validated and replicated together on exit."""
        self._deferred = True
        try:
            yield self
        except BaseException:
            self._deferred = False
            self.db.execute("DELETE FROM _synql_capture")
            self._materialize()                       # discard the application's edits
            raise
        self._deferred = False
        self.flush()

    def flush(self):
        """Turn captured SQL changes into Synql operations (all or nothing)."""
        rows = self.db.execute("SELECT op, rel, local_id, data FROM _synql_capture ORDER BY seq").fetchall()
        if not rows:
            return
        inv = {(r["rel"], r["local_id"]): r["tid"]
               for r in self.db.execute("SELECT rel, tid, local_id FROM _synql_map")}

        def tid_of(rel, local_id):
            if (rel, local_id) not in inv:
                raise IntegrityError(f"{rel} row {local_id} does not exist")
            return inv[(rel, local_id)]

        def translate(rel, data):                     # FK columns hold local ids -> tuple ids
            fks = {fk.field: fk for fk in self.schema[rel].foreign_keys}
            return {f: (tid_of(fks[f].ref, v) if f in fks else v) for f, v in data.items()}

        self._batch = True
        self.db.execute("BEGIN")
        try:
            self._cur_txn = self._tick()              # transaction id (labeled timestamp)
            self._batch_tuples = []
            for r in rows:
                rel, op = r["rel"], r["op"]
                if op == "I":
                    tid = self.insert(rel, **translate(rel, json.loads(r["data"])))
                    self.db.execute("DELETE FROM _synql_map WHERE rel=? AND local_id=?", (rel, r["local_id"]))
                    self.db.execute("INSERT INTO _synql_map VALUES(?,?,?)", (rel, tid, r["local_id"]))
                    inv[(rel, r["local_id"])] = tid
                elif op == "U":
                    tid = tid_of(rel, r["local_id"])
                    new = translate(rel, json.loads(r["data"]))
                    cur = self._state()[(rel, tid)]["fields"]
                    for f in self.schema[rel].fields:
                        if new[f] != cur[f][0]:
                            self.update(rel, tid, f, new[f])
                else:
                    self.delete(rel, tid_of(rel, r["local_id"]), _cascaded_ok=True)
            if self._batch_tuples:                    # the whole transaction travels as one delta
                self.db.execute("INSERT INTO _synql_log(delta) VALUES(?)",
                                (json.dumps({"txn": self._cur_txn, "tuples": self._batch_tuples}),))
            self.db.execute("DELETE FROM _synql_capture")
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            self._batch = False
            self._cur_txn, self._batch_tuples = None, []
            self.db.execute("DELETE FROM _synql_capture")
            self._materialize()                       # user tables go back to the replicated truth
            raise
        self._batch = False
        self._cur_txn, self._batch_tuples = None, []
        self._materialize()

    # ------------------------------------------------------------------ inspection
    def replicated_rows(self):
        """Replicated state for display: every tuple, its CLFlag and whether it is in the computed state."""
        st = self._state()
        S = self._compute(st)
        return [{"relation": k[0], "id": k[1], "flag": t["flag"], "marked deleted": t["flag"] % 2 == 1,
                 "in computed state": k in S,
                 "fields": ", ".join(f"{f}={st[k]['fields'][f][0]}" for f in self.schema[k[0]].fields)}
                for k, t in sorted(st.items(), key=lambda kv: (kv[0][0], parse(kv[0][1])))]

    def rows(self, rel):
        """Rows of the user table (with this replica's local ids)."""
        return [dict(r) for r in self.db.execute(f'SELECT * FROM "{rel}" ORDER BY id')]

    def view(self):
        """Replica-independent view of the computed state."""
        st = self._state()
        return sorted((k[0], k[1], tuple((f, st[k]["fields"][f][0]) for f in self.schema[k[0]].fields))
                      for k in self._compute(st))

    def find(self, rel, **where):
        st = self._state()
        for k in sorted(self._compute(st), key=lambda k: parse(k[1])):
            if k[0] == rel and all(st[k]["fields"][f][0] == v for f, v in where.items()):
                return k[1]
        return None

    def flag(self, rel, tid):
        return self.db.execute("SELECT flag FROM _synql_tuple WHERE rel=? AND tid=?", (rel, tid)).fetchone()["flag"]

    def dump_state(self):
        """Canonical dump of the replicated state (to compare convergence)."""
        out = {}
        for r in self.db.execute("SELECT rel, tid, flag, txn FROM _synql_tuple"):
            out[(r["rel"], r["tid"])] = [r["flag"], {}, r["txn"]]
        for r in self.db.execute("SELECT rel, tid, field, value, ts FROM _synql_field"):
            out[(r["rel"], r["tid"])][1][r["field"]] = (json.loads(r["value"]), r["ts"])
        return sorted((k[0], k[1], v[0], sorted(v[1].items()), v[2]) for k, v in out.items())

    def txn_stats(self):
        """How transactions fared: tuples inserted (and not deleted later) vs kept in the computed state."""
        st = self._state()
        S = self._compute(st)
        by = {}
        for k, t in st.items():
            if t["txn"] and t["flag"] % 2 == 0:
                by.setdefault(t["txn"], []).append(k in S)
        multi = {x: v for x, v in by.items() if len(v) > 1}
        return {"transactions": len(by), "multi_tuple_transactions": len(multi),
                "partial_transactions": sum(1 for v in multi.values() if any(v) and not all(v)),
                "tuples_inserted": sum(len(v) for v in by.values()), "tuples_kept": sum(sum(v) for v in by.values())}
