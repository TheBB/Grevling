from __future__ import annotations

import json
import sys
import traceback
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, cast

import click
from asteval import Interpreter  # type: ignore

import grevling
import grevling.workflow.local

from . import Case, api, util

if TYPE_CHECKING:
    import io


def workflows(func):
    func = click.option("--local", "workflow", is_flag=True, flag_value="local", default=True)(func)
    return click.option("--azure", "workflow", is_flag=True, flag_value="azure")(func)


class CustomClickException(click.ClickException):
    def show(self):
        util.log.critical(str(self))


class CaseType(click.Path):
    def convert(self, value, param, ctx):
        if isinstance(value, Case):
            return value
        path = Path(cast(str, super().convert(value, param, ctx)))
        casefile = path
        if path.is_dir():
            for candidate in ["grevling.gold", "grevling.yaml", "badger.yaml"]:
                if (path / candidate).exists():
                    casefile = path / candidate
                    break
        if not casefile.exists():
            raise click.FileError(str(casefile), hint="does not exist")
        if not casefile.is_file():
            raise click.FileError(str(casefile), hint="is not a file")

        return Case(casefile)


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(grevling.__version__)
    ctx.exit()


@click.group()
@click.option("--case", "-c", default=".", type=CaseType(file_okay=True, dir_okay=True))
@click.option("--version", is_flag=True, callback=print_version, expose_value=False, is_eager=True)
@click.option("--debug", "verbosity", flag_value="DEBUG")
@click.option("--info", "verbosity", flag_value="INFO", default=True)
@click.option("--warning", "verbosity", flag_value="WARNING")
@click.option("--error", "verbosity", flag_value="ERROR")
@click.option("--critical", "verbosity", flag_value="CRITICAL")
@click.pass_context
def main(ctx: click.Context, case: Case, verbosity: str) -> None:
    util.initialize_logging(level=verbosity, show_time=False)

    case = case.__enter__()
    ctx.call_on_close(partial(case.__exit__, None, None, None))
    ctx.ensure_object(dict)
    ctx.obj["case"] = case


class PluginCli(click.MultiCommand):
    def list_commands(self, ctx: click.Context) -> list[str]:
        cs: Case = ctx.obj["case"]
        return [cast(str, command.name) for plugin in cs.plugins for command in plugin.commands(ctx)]

    def get_command(self, ctx: click.Context, name: str) -> click.Command:
        cs: Case = ctx.obj["case"]
        for plugin in cs.plugins:
            for command in plugin.commands(ctx):
                if command.name == name:
                    return command
        assert False


@main.command(cls=PluginCli)
def plugin():
    pass


@main.command("run-all")
@click.option("-j", "nprocs", default=1, type=int)
@workflows
@click.pass_context
def run_all(ctx: click.Context, workflow: str, nprocs: int):
    cs: Case = ctx.obj["case"]
    try:
        cs.clear_cache()
        with api.Workflow.get_workflow(workflow)(nprocs) as w:
            success = w.pipeline(cs).run(cs.create_instances())
        if not success:
            util.log.error("An error happened, aborting")
            sys.exit(1)
        cs.collect()
        cs.plot()
    except Exception as ex:
        util.log.critical(str(ex))
        util.log.debug("Backtrace:")
        util.log.debug("".join(traceback.format_tb(ex.__traceback__)))
        sys.exit(1)


@main.command("run")
@click.option("-j", "nprocs", default=1, type=int)
@workflows
@click.pass_context
def run(ctx: click.Context, workflow: str, nprocs: int):
    cs: Case = ctx.obj["case"]
    try:
        cs.clear_cache()
        with api.Workflow.get_workflow(workflow)(nprocs) as w:
            if not w.pipeline(cs).run(cs.create_instances()):
                sys.exit(1)
    except Exception as ex:
        util.log.critical(str(ex))
        util.log.debug("Backtrace:")
        util.log.debug("".join(traceback.format_tb(ex.__traceback__)))
        sys.exit(1)


@main.command("run-with")
@click.option("--target", "-t", default=".", type=click.Path(path_type=Path))
@click.argument("context", nargs=-1, type=str)
@workflows
@click.pass_context
def run_with(ctx: click.Context, target: Path, workflow: str, context: list[str]):
    cs: Case = ctx.obj["case"]
    evaluator = Interpreter()
    parsed_context = {}
    for s in context:
        k, v = s.split("=", 1)
        parsed_context[k] = evaluator.eval(v)
    instance = cs.create_instance(api.Context(parsed_context), logdir=target)
    with api.Workflow.get_workflow(workflow)() as w:
        if not w.pipeline(cs).run([instance]):
            sys.exit(1)


@main.command("capture")
@click.pass_context
def capture(ctx: click.Context):
    cs: Case = ctx.obj["case"]
    cs.capture()


@main.command("collect")
@click.pass_context
def collect(ctx: click.Context):
    cs: Case = ctx.obj["case"]
    cs.clear_dataframe()
    cs.collect()


@main.command("plot")
@click.pass_context
def plot(ctx: click.Context):
    cs: Case = ctx.obj["case"]
    cs.plot()


@main.command()
@click.option("--fmt", "-f", default="json", type=click.Choice(["json"]))
@click.argument("output", type=click.File("w"))
@click.pass_context
def dump(ctx: click.Context, fmt: str, output: io.StringIO):
    cs: Case = ctx.obj["case"]
    data = cs.load_dataframe()
    if fmt == "json":
        json.dump(
            data.to_dict("records"),
            output,
            sort_keys=True,
            indent=4,
            cls=util.JSONEncoder,
        )


@main.group()
def advanced():
    pass


@advanced.command()
@click.pass_context
def touch(ctx: click.Context):
    pass
