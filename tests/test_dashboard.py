import pytest

pytest.importorskip("streamlit")
from pathlib import Path
from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).resolve().parent.parent / "dashboard.py")


def run(at):
    at.run(timeout=30)
    assert not at.exception, [e.value for e in at.exception]
    return at


def click(at, key):
    at.button(key=key).click()
    return run(at)


def test_dashboard_fig12_flow():
    at = run(AppTest.from_file(APP, default_timeout=30))
    at.selectbox(key="scenario").select("Abort semantics: enrol vs delete contest (Fig 12)")
    click(at, "load")
    assert at.warning                                      # not converged yet
    click(at, "both")
    assert at.success                                      # converged after sync
    at.radio(key="target").set_value("A")
    at.text_area(key="sql").set_value("DELETE FROM enrolled;")
    click(at, "run")
    click(at, "both")
    assert at.success
    assert not at.error                                    # no SQL error in the log


def test_dashboard_reports_errors():
    at = run(AppTest.from_file(APP, default_timeout=30))
    at.text_area(key="sql").set_value("INSERT INTO enrolled(player, contest) VALUES(1, 1);")
    click(at, "run")
    assert at.error                                        # dangling reference rejected, shown in the log
