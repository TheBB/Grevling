from __future__ import annotations

from click.testing import CliRunner

from grevling import Case
from grevling.__main__ import main


def api_run(pre=[], post=["collect"]):
    def runner(path):
        with Case(path) as case:
            case.clear_cache()
            for method in pre:
                getattr(case, method)()
            assert case.run()
            for method in post:
                getattr(case, method)()

    return runner


def cli_run(commands=["run", "collect"]):
    def runner(path):
        with Case(path) as case:
            case.clear_cache()
        r = CliRunner()
        for cmd in commands:
            args = [cmd] if isinstance(cmd, str) else cmd
            result = r.invoke(main, ["-c", str(path), *args])
            if result.exit_code != 0:
                print(result)
                print(result.stdout)
            assert result.exit_code == 0

    return runner
