"""Streamlit demo: two Synql replicas, plain SQL on each, manual sync, replicated vs computed state.
Run:  streamlit run dashboard.py"""
import os
import tempfile
import pandas as pd
import streamlit as st
from synql import Replica, IntegrityError, contest_schema, sync
from synql.scenarios import SCENARIOS

TABLES = ("player", "contest", "game", "enrolled")


def fresh():
    d = tempfile.mkdtemp(prefix="synql_")
    st.session_state.replicas = {x: Replica(os.path.join(d, f"{x}.db"), x, contest_schema()) for x in "AB"}
    st.session_state.log = []


def note(msg, error=False):
    st.session_state.log.insert(0, ("error" if error else "ok", msg))


st.set_page_config(page_title="Synql replicas", layout="wide")
if "replicas" not in st.session_state:
    fresh()
R = st.session_state.replicas

st.title("Synql: CRDT-replicated SQLite")
st.caption("Each replica is a SQLite file. Write plain SQL to the user tables; triggers capture the change, "
           "Synql replicates it and recomputes a state that satisfies UNIQUE and FOREIGN KEY constraints.")

with st.sidebar:
    st.header("Scenario")
    name = st.selectbox("Load a scenario from the paper", list(SCENARIOS), key="scenario")
    st.write(SCENARIOS[name][1])
    if st.button("Load scenario", key="load"):
        fresh()
        R = st.session_state.replicas
        SCENARIOS[name][0](R["A"], R["B"])
        note(f"Loaded: {name}")
    if st.button("Reset replicas", key="reset"):
        fresh()
        R = st.session_state.replicas
    st.divider()
    st.markdown("**Tables**  \n`player(name UNIQUE)`  \n`contest(name UNIQUE)`  \n"
                "`game(contest -> contest, CASCADE)`  \n`enrolled(player -> player, contest -> contest, RESTRICT)`")

c1, c2 = st.columns([1, 3])
with c1:
    target = st.radio("Run SQL on replica", ["A", "B"], horizontal=True, key="target")
with c2:
    sql = st.text_area("SQL (ids shown below are local to each replica)", key="sql", height=90,
                       value="INSERT INTO player(name) VALUES('Alice');")
if st.button("Run SQL", key="run"):
    try:
        for stmt in [s for s in sql.split(";") if s.strip()]:
            R[target].execute(stmt)
        note(f"[{target}] {sql.strip()}")
    except (IntegrityError, Exception) as e:
        note(f"[{target}] {type(e).__name__}: {e}", error=True)

b1, b2, b3, _ = st.columns([1, 1, 1, 3])
if b1.button("Sync A → B", key="a2b"):
    R["A"].push(R["B"]); note("Synced A → B")
if b2.button("Sync B → A", key="b2a"):
    R["B"].push(R["A"]); note("Synced B → A")
if b3.button("Sync both", key="both"):
    sync(R["A"], R["B"]); note("Synced both ways")

same_state = R["A"].dump_state() == R["B"].dump_state()
same_view = R["A"].view() == R["B"].view()
if same_state and same_view:
    st.success("Replicas converged: identical replicated state and identical computed state.")
else:
    st.warning("Replicas differ (changes not yet exchanged).")

cols = st.columns(2)
for col, rid in zip(cols, "AB"):
    with col:
        st.subheader(f"Replica {rid}")
        tab_user, tab_rep = st.tabs(["User tables (computed state)", "Replicated state (CRDT metadata)"])
        with tab_user:
            for t in TABLES:
                rows = R[rid].rows(t)
                st.markdown(f"`{t}`")
                st.dataframe(pd.DataFrame(rows) if rows else pd.DataFrame(columns=["id"]),
                             hide_index=True, width="stretch")
        with tab_rep:
            st.caption("Tuple id = labeled timestamp counter@replica. Odd flag = deleted. "
                       "A deleted tuple can still be in the computed state when it is restored (abort semantics).")
            rows = R[rid].replicated_rows()
            st.dataframe(pd.DataFrame(rows) if rows else pd.DataFrame(
                columns=["relation", "id", "flag", "marked deleted", "in computed state", "fields"]),
                hide_index=True, width="stretch")

st.subheader("Log")
for kind, msg in st.session_state.log[:12]:
    (st.error if kind == "error" else st.code)(msg)
