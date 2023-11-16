from __future__ import annotations

import json
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
    with Case(PATH) as case:
        assert DBPATH.exists()

        con = sqlite3.connect(DBPATH)
        cur = con.cursor()

        res = cur.execute("SELECT * FROM dbinfo")
        assert list(res) == [(0, 1)]

        ninstances = 0
        for logdir in case.storagepath.iterdir():
            if not logdir.is_dir():
                continue
            ninstances += 1
            with open(logdir / ".grevling" / "context.json", "r") as f:
                context = json.load(f)
            with open(logdir / ".grevling" / "captured.json", "r") as f:
                captured = json.load(f)
            with open(logdir / ".grevling" / "status.txt", "r") as f:
                status = f.read().strip()
            index = context["g_index"]
            res = next(
                cur.execute(
                    "SELECT id, logdir, context, captured, status FROM instance WHERE id = ?", (index,)
                )
            )
            assert res[0] == index
            assert res[1] == logdir.name
            assert json.loads(res[2]) == context
            assert json.loads(res[3]) == captured
            assert res[4].casefold() == status.casefold()

        assert cur.execute("SELECT COUNT (*) FROM instance").fetchone() == (ninstances,)
