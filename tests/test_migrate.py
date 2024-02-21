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
        assert list(res) == [(0, 2)]

        ninstances = 0
        for logdir in case.storagepath.iterdir():
            if not logdir.is_dir():
                continue
            ninstances += 1
            with (logdir / ".grevling" / "context.json").open() as f:
                context = json.load(f)
            with (logdir / ".grevling" / "captured.json").open() as f:
                captured = json.load(f)
            with (logdir / ".grevling" / "status.txt").open() as f:
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

        res = cur.execute("SELECT * FROM 'case'")
        assert list(res) == [
            (
                0,
                False,
                False,
            )
        ]
