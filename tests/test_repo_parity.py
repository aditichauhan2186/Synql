"""Scenarios taken from the authors' repository (coast-team/synql, test/test_crr.py)."""
import pytest
from synql import Replica, Schema, Relation, ForeignKey, IntegrityError, sync


def chain_schema():       # Z -RESTRICT-> Y -CASCADE-> X
    return Schema(
        Relation("x", ("name",)),
        Relation("y", ("x",), foreign_keys=(ForeignKey("x", "x", "CASCADE"),)),
        Relation("z", ("y",), foreign_keys=(ForeignKey("y", "y", "RESTRICT"),)),
    )


@pytest.fixture
def mk(tmp_path):
    return lambda rid: Replica(str(tmp_path / f"{rid}.db"), rid, chain_schema())


def names(r):
    return sorted(v[0] for v in r.view())


def test_concur_del_fk_restrict_rec(mk):
    """test_concur_del_fk_restrict_rec: X deleted at A while B inserts Y(x) and Z(y).
    The RESTRICT reference from Z restores Y, which in turn needs X (CASCADE link)."""
    A, B = mk("A"), mk("B")
    x = A.insert("x", name="1")
    sync(A, B)
    A.delete("x", x)
    y = B.insert("y", x=x)
    B.insert("z", y=y)
    sync(A, B)
    assert A.view() == B.view()
    assert names(A) == ["x", "y", "z"]


def test_concur_del_fk_cascade(mk):
    A, B = mk("A"), mk("B")
    x = A.insert("x", name="1")
    sync(A, B)
    A.delete("x", x)
    B.insert("y", x=x)                  # no RESTRICT reference: the deletion wins (remove-win)
    sync(A, B)
    assert A.view() == B.view() and names(A) == []


def test_compensation_follows_restored_chain(mk):
    """Deleting z (the RESTRICT referrer) is compensated: the concurrent insertion of y must not be
    lost just because x was restored only through z (same idea as Figure 19 of the paper)."""
    A, B = mk("A"), mk("B")
    x = A.insert("x", name="1")
    sync(A, B)
    A.delete("x", x)
    y = B.insert("y", x=x)
    z = B.insert("z", y=y)
    sync(A, B)
    B.delete("z", z)
    sync(A, B)
    assert A.view() == B.view()
    assert names(A) == ["x", "y"]       # y (live) still references x, so x is redone; z is gone
