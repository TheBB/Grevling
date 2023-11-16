from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from grevling import Case

DATADIR = Path(__file__).parent / "data"
PATH = DATADIR / "run" / "migrate"
DBPATH = PATH / "grevlingdata" / "grevling.db"


@pytest.fixture()
def db(request):
    DBPATH.unlink(missing_ok=True)
    yield


def test_migrate(db):
    with Case(PATH):
        assert DBPATH.exists()

        con = sqlite3.connect(DBPATH)
        cur = con.cursor()

        res = cur.execute("SELECT * FROM dbinfo")
        assert list(res) == [(0, 0)]
