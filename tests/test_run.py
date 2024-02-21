from __future__ import annotations

import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from time import time

import pandas as pd
import pytest

from grevling import Case
from grevling.parameters import Parameter
from grevling.workflow.local import LocalWorkflow

from .common import api_run, cli_run

DATADIR = Path(__file__).parent / "data"


def read_file(path: Path) -> str:
    with path.open() as f:
        return f.read()


def check_df(left, right):
    blacklist = {"g_started", "g_finished", "g_logdir", "g_sourcedir"}
    to_remove = [c for c in left.columns if c.startswith("g_walltime_") or c in blacklist]
    pd.testing.assert_frame_equal(left.drop(columns=to_remove).sort_index(axis=1), right.sort_index(axis=1))


@pytest.mark.parametrize("runner", [api_run(), cli_run()])
@pytest.mark.parametrize("suffix", [".yaml", ".gold"])
def test_echo(runner, suffix):
    path = DATADIR / "run" / "echo" / f"grevling{suffix}"
    runner(path)

    with Case(path) as case:
        data = case.load_dataframe()

    check_df(
        data,
        pd.DataFrame(
            index=pd.Index(range(9), dtype=int),
            data={
                "alpha": pd.array([1, 1, 1, 2, 2, 2, 3, 3, 3], dtype=pd.Int64Dtype()),
                "bravo": ["a", "b", "c", "a", "b", "c", "a", "b", "c"],
                "charlie": pd.array([1, 1, 1, 3, 3, 3, 5, 5, 5], dtype=pd.Int64Dtype()),
                "a": pd.array([1, 1, 1, 2, 2, 2, 3, 3, 3], dtype=pd.Int64Dtype()),
                "b": ["a", "b", "c", "a", "b", "c", "a", "b", "c"],
                "c": [1.0, 1.0, 1.0, 3.0, 3.0, 3.0, 5.0, 5.0, 5.0],
                "g_success": pd.array([True] * 9, dtype=pd.BooleanDtype()),
            },
        ),
    )


@pytest.mark.parametrize("runner", [api_run(), cli_run()])
@pytest.mark.parametrize("suffix", [".yaml", ".gold"])
def test_cat(runner, suffix):
    path = DATADIR / "run" / "cat" / f"grevling{suffix}"
    runner(path)

    with Case(path) as case:
        data = case.load_dataframe()

    check_df(
        data,
        pd.DataFrame(
            index=pd.Index(range(9), dtype=int),
            data={
                "alpha": pd.array([1, 1, 1, 2, 2, 2, 3, 3, 3], dtype=pd.Int64Dtype()),
                "bravo": ["a", "b", "c", "a", "b", "c", "a", "b", "c"],
                "charlie": pd.array([1, 1, 1, 3, 3, 3, 5, 5, 5], dtype=pd.Int64Dtype()),
                "a": pd.array([1, 1, 1, 2, 2, 2, 3, 3, 3], dtype=pd.Int64Dtype()),
                "b": ["a", "b", "c", "a", "b", "c", "a", "b", "c"],
                "c": [1.0, 1.0, 1.0, 3.0, 3.0, 3.0, 5.0, 5.0, 5.0],
                "a_auto": pd.array([1, 1, 1, 2, 2, 2, 3, 3, 3], dtype=pd.Int64Dtype()),
                "g_success": pd.array([True] * 9, dtype=pd.BooleanDtype()),
            },
        ),
    )


@pytest.mark.parametrize("runner", [api_run(post=[]), cli_run(commands=["run"])])
@pytest.mark.parametrize("suffix", [".yaml", ".gold"])
def test_files(runner, suffix):
    path = DATADIR / "run" / "files" / f"grevling{suffix}"
    runner(path)

    with Case(path) as case:
        storagepath = case.storagepath

    for a in range(1, 4):
        for b in "abc":
            path = storagepath / f"{a}-{b}"
            assert read_file(path / "template.txt") == f"a={a} b={b} c={2*a-1}\n"
            assert read_file(path / "other-template.txt") == f"a={a} b={b} c={2*a-1}\n"
            assert read_file(path / "non-template.txt") == "a=${alpha} b=${bravo} c=${charlie}\n"
            assert read_file(path / "some" / "deep" / "directory" / "empty1.dat") == ""
            assert read_file(path / "some" / "deep" / "directory" / "empty2.dat") == ""
            assert read_file(path / "some" / "deep" / "directory" / "empty3.dat") == ""


@pytest.mark.parametrize("runner", [api_run(post=[]), cli_run(commands=["run"])])
@pytest.mark.parametrize("suffix", [".yaml", ".gold"])
def test_template_files(runner, suffix):
    path = DATADIR / "run" / "template-files" / f"grevling{suffix}"
    runner(path)

    with Case(path) as case:
        storagepath = case.storagepath

    for b in "abc":
        path = storagepath / b
        assert read_file(path / "file.txt") == f"{b}\n${{bravo}}\n"
        assert read_file(path / f"templated-{b}.txt") == f"{b}\n{b}\n"


@pytest.mark.parametrize("runner", [api_run(), cli_run()])
@pytest.mark.parametrize("suffix", [".yaml", ".gold"])
def test_capture(runner, suffix):
    path = DATADIR / "run" / "capture" / f"grevling{suffix}"
    runner(path)

    with Case(path) as case:
        data = case.load_dataframe()

    check_df(
        data,
        pd.DataFrame(
            index=pd.Index(range(9), dtype=int),
            data={
                "alpha": [
                    1.234,
                    1.234,
                    1.234,
                    2.345,
                    2.345,
                    2.345,
                    3.456,
                    3.456,
                    3.456,
                ],
                "bravo": pd.array([1, 2, 3, 1, 2, 3, 1, 2, 3], dtype=pd.Int64Dtype()),
                "firstalpha": [
                    1.234,
                    1.234,
                    1.234,
                    2.345,
                    2.345,
                    2.345,
                    3.456,
                    3.456,
                    3.456,
                ],
                "lastalpha": [
                    4.936,
                    4.936,
                    4.936,
                    9.38,
                    9.38,
                    9.38,
                    13.824,
                    13.824,
                    13.824,
                ],
                "allalpha": [
                    [1.234, 2.468, 3.702, 4.936],
                    [1.234, 2.468, 3.702, 4.936],
                    [1.234, 2.468, 3.702, 4.936],
                    [2.345, 4.690, 7.035, 9.380],
                    [2.345, 4.690, 7.035, 9.380],
                    [2.345, 4.690, 7.035, 9.380],
                    [3.456, 6.912, 10.368, 13.824],
                    [3.456, 6.912, 10.368, 13.824],
                    [3.456, 6.912, 10.368, 13.824],
                ],
                "firstbravo": pd.array([1, 2, 3, 1, 2, 3, 1, 2, 3], dtype=pd.Int64Dtype()),
                "lastbravo": pd.array([4, 8, 12, 4, 8, 12, 4, 8, 12], dtype=pd.Int64Dtype()),
                "allbravo": [
                    [1, 2, 3, 4],
                    [2, 4, 6, 8],
                    [3, 6, 9, 12],
                    [1, 2, 3, 4],
                    [2, 4, 6, 8],
                    [3, 6, 9, 12],
                    [1, 2, 3, 4],
                    [2, 4, 6, 8],
                    [3, 6, 9, 12],
                ],
                "g_success": pd.array([True] * 9, dtype=pd.BooleanDtype()),
            },
        ),
    )


@pytest.mark.parametrize(
    "runner", [api_run(post=["collect", "capture"]), cli_run(commands=["run", "collect", "capture"])]
)
@pytest.mark.parametrize("suffix", [".yaml", ".gold"])
def test_double_capture(runner, suffix):
    path = DATADIR / "run" / "capture" / f"grevling{suffix}"
    runner(path)

    with Case(path) as case:
        data = case.load_dataframe()

    check_df(
        data,
        pd.DataFrame(
            index=pd.Index(range(9), dtype=int),
            data={
                "alpha": [
                    1.234,
                    1.234,
                    1.234,
                    2.345,
                    2.345,
                    2.345,
                    3.456,
                    3.456,
                    3.456,
                ],
                "bravo": pd.array([1, 2, 3, 1, 2, 3, 1, 2, 3], dtype=pd.Int64Dtype()),
                "firstalpha": [
                    1.234,
                    1.234,
                    1.234,
                    2.345,
                    2.345,
                    2.345,
                    3.456,
                    3.456,
                    3.456,
                ],
                "lastalpha": [
                    4.936,
                    4.936,
                    4.936,
                    9.38,
                    9.38,
                    9.38,
                    13.824,
                    13.824,
                    13.824,
                ],
                "allalpha": [
                    [1.234, 2.468, 3.702, 4.936],
                    [1.234, 2.468, 3.702, 4.936],
                    [1.234, 2.468, 3.702, 4.936],
                    [2.345, 4.690, 7.035, 9.380],
                    [2.345, 4.690, 7.035, 9.380],
                    [2.345, 4.690, 7.035, 9.380],
                    [3.456, 6.912, 10.368, 13.824],
                    [3.456, 6.912, 10.368, 13.824],
                    [3.456, 6.912, 10.368, 13.824],
                ],
                "firstbravo": pd.array([1, 2, 3, 1, 2, 3, 1, 2, 3], dtype=pd.Int64Dtype()),
                "lastbravo": pd.array([4, 8, 12, 4, 8, 12, 4, 8, 12], dtype=pd.Int64Dtype()),
                "allbravo": [
                    [1, 2, 3, 4],
                    [2, 4, 6, 8],
                    [3, 6, 9, 12],
                    [1, 2, 3, 4],
                    [2, 4, 6, 8],
                    [3, 6, 9, 12],
                    [1, 2, 3, 4],
                    [2, 4, 6, 8],
                    [3, 6, 9, 12],
                ],
                "g_success": pd.array([True] * 9, dtype=pd.BooleanDtype()),
            },
        ),
    )


@pytest.mark.parametrize("runner", [api_run(), cli_run()])
@pytest.mark.parametrize("suffix", [".yaml", ".gold"])
def test_failing(runner, suffix):
    path = DATADIR / "run" / "failing" / f"grevling{suffix}"
    runner(path)

    with Case(path) as case:
        data = case.load_dataframe()

    check_df(
        data,
        pd.DataFrame(
            index=pd.Index([0, 1], dtype=int),
            data={
                "retcode": pd.array([0, 1], dtype=pd.Int64Dtype()),
                "before": pd.array([12, 12], dtype=pd.Int64Dtype()),
                "return": pd.array([0, 1], dtype=pd.Int64Dtype()),
                "next": pd.array([0, pd.NA], dtype=pd.Int64Dtype()),
                "after": pd.array([13, pd.NA], dtype=pd.Int64Dtype()),
                "g_success": pd.array([True, False], dtype=pd.BooleanDtype()),
            },
        ),
    )


@pytest.mark.parametrize("runner", [api_run(post=[]), cli_run(commands=["run"])])
@pytest.mark.parametrize("suffix", [".yaml", ".gold"])
def test_stdout(runner, suffix):
    path = DATADIR / "run" / "stdout" / f"grevling{suffix}"
    runner(path)

    with Case(path) as case:
        path = case.storagepath

    assert read_file(path / "out-0" / ".grevling" / "good.stdout") == "stdout 0\n"
    assert read_file(path / "out-0" / ".grevling" / "good.stderr") == "stderr 0\n"
    assert read_file(path / "out-0" / ".grevling" / "bad.stdout") == "stdout 0\n"
    assert read_file(path / "out-0" / ".grevling" / "bad.stderr") == "stderr 0\n"
    assert read_file(path / "out-1" / ".grevling" / "good.stdout") == "stdout 1\n"
    assert read_file(path / "out-1" / ".grevling" / "good.stderr") == "stderr 1\n"
    assert read_file(path / "out-1" / ".grevling" / "bad.stdout") == "stdout 1\n"
    assert read_file(path / "out-1" / ".grevling" / "bad.stderr") == "stderr 1\n"


@pytest.mark.skipif(os.name == "nt" or shutil.which("docker") is None, reason="requires docker and *nix")
@pytest.mark.parametrize("runner", [api_run(post=[]), cli_run(commands=["run"])])
@pytest.mark.parametrize("suffix", [".yaml", ".gold"])
def test_docker(runner, suffix):
    path = DATADIR / "run" / "docker" / f"grevling{suffix}"
    runner(path)

    with Case(path) as case:
        path = case.storagepath

    assert "Hello from Docker!" in read_file(path / "0" / ".grevling" / "empty.stdout")


@pytest.mark.skipif(os.name == "nt" or shutil.which("docker") is None, reason="requires docker and *nix")
@pytest.mark.parametrize("runner", [api_run(post=[]), cli_run(commands=["run"])])
@pytest.mark.parametrize("suffix", [".yaml"])
def test_docker_args(runner, suffix):
    path = DATADIR / "run" / "docker-args" / f"grevling{suffix}"
    runner(path)

    with Case(path) as case:
        path = case.storagepath

    assert 'NAME="Alpine Linux"' in read_file(path / "0" / ".grevling" / "alpine.stdout")


@pytest.mark.skipif(shutil.which("sleep") is None, reason="requires sleep")
@pytest.mark.parametrize("suffix", [".yaml", ".gold"])
def test_sleep(suffix):
    with Case(DATADIR / "run" / "sleep" / f"grevling{suffix}") as case:
        case.clear_cache()
        with LocalWorkflow(nprocs=20) as w:
            start = time()
            assert w.pipeline(case).run(case.create_instances())
            duration = time() - start

    # The case is configured to launch 20 processes, each sleeping 1/2 second
    # with 20 concurrent processes, this should take < 1 sec under normal cirumstances.
    # Use generous margin for test stability.
    assert duration < 18.0


@pytest.mark.skipif(os.name == "nt" or shutil.which("sh") is None, reason="requires sh and *nix")
@pytest.mark.parametrize("suffix", [".yaml", ".gold"])
def test_workdir(suffix):
    with Case(DATADIR / "run" / "workdir" / f"grevling{suffix}") as case:
        case.clear_cache()

        # Inject three temporary paths as a new parameter
        # We do this to avoid hardcoding directories in the test file
        with TemporaryDirectory() as temp:
            paths = [Path(temp) / name for name in ["a", "b", "c"]]
            for path in paths:
                path.mkdir()
            case.context_mgr.parameters["workdir"] = Parameter(
                "workdir",
                list(map(str, paths)),
            )
            assert case.run()

            path = case.storagepath

        assert str(paths[0]) in read_file(path / "0" / ".grevling" / "pwd.stdout")
        assert str(paths[1]) in read_file(path / "1" / ".grevling" / "pwd.stdout")
        assert str(paths[2]) in read_file(path / "2" / ".grevling" / "pwd.stdout")
