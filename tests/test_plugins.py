from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from typing import Any

import click

from grevling import Case
from grevling.api import Plugin

from .common import cli_run

DATADIR = Path(__file__).parent / "data"


class MyPlugin(Plugin):
    def __init__(self, case: Case, settings: Any) -> None:
        assert settings is None

    def commands(self, ctx: click.Context) -> list[click.Command]:
        @click.command()
        @click.pass_context
        def test(ctx):
            cs: Case = ctx.obj["case"]
            with open(cs.storagepath / "test.json", "w") as f:
                json.dump({"success": True}, f)

        return [test]


class FakeModule:
    Plugin = MyPlugin


sys.modules["grevling_testplugin"] = FakeModule


class MyOtherPlugin(Plugin):
    def __init__(self, case: Case, settings: Any) -> None:
        assert settings == {"something": "something else"}

    def commands(self, ctx: click.Context) -> list[click.Command]:
        return []


class FakeOtherModule:
    Plugin = MyOtherPlugin


sys.modules["grevling_testplugin_with_settings"] = FakeOtherModule


def test_plugins():
    path = DATADIR / "run" / "plugins" / "grevling.gold"
    storagepath = path.parent / "grevlingdata"
    if storagepath.exists():
        shutil.rmtree(storagepath)

    cli_run([["plugin", "test"]])(path)

    with open(storagepath / "test.json", "r") as f:
        obj = json.load(f)
        assert obj["success"] is True
