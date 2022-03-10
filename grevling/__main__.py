from __future__ import annotations

from functools import partial
import io
import json
from pathlib import Path
import sys

from typing import List

from asteval import Interpreter
import click
from ruamel.yaml.parser import ParserError as YAMLParserError
from strictyaml import YAMLValidationError

import grevling
from . import Case, util, api

import grevling.workflow.local


def workflows(func):
    func = click.option(
        '--local', 'workflow', is_flag=True, flag_value='local', default=True
    )(func)
    func = click.option('--azure', 'workflow', is_flag=True, flag_value='azure')(func)
    return func


class CustomClickException(click.ClickException):
    def show(self):
        util.log.critical(str(self))


class CaseType(click.Path):
    def convert(self, value, param, ctx):
        if isinstance(value, Case):
            return value
        path = Path(super().convert(value, param, ctx))
        casefile = path
        if path.is_dir():
            for candidate in ['grevling.gold', 'grevling.yaml', 'badger.yaml']:
                if (path / candidate).exists():
                    casefile = path / candidate
                    break
        if not casefile.exists():
            raise click.FileError(str(casefile), hint='does not exist')
        if not casefile.is_file():
            raise click.FileError(str(casefile), hint='is not a file')
        try:
            case = Case(path)
        except (YAMLValidationError, YAMLParserError) as error:
            raise CustomClickException(str(error))

        case = case.__enter__()
        if ctx:
            ctx.call_on_close(partial(case.__exit__, None, None, None))
        return case


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(grevling.__version__)
    ctx.exit()


@click.group()
@click.option(
    '--version', is_flag=True, callback=print_version, expose_value=False, is_eager=True
)
@click.option('--debug', 'verbosity', flag_value='DEBUG')
@click.option('--info', 'verbosity', flag_value='INFO', default=True)
@click.option('--warning', 'verbosity', flag_value='WARNING')
@click.option('--error', 'verbosity', flag_value='ERROR')
@click.option('--critical', 'verbosity', flag_value='CRITICAL')
def main(verbosity: str):
    util.initialize_logging(level=verbosity, show_time=False)


@main.command('run-all')
@click.option('--case', '-c', default='.', type=CaseType(file_okay=True, dir_okay=True))
@click.option('-j', 'nprocs', default=1, type=int)
@workflows
def run_all(case: Case, workflow: str, nprocs: int):
    case.clear_cache()
    with api.Workflow.get_workflow(workflow)(nprocs) as w:
        success = w.pipeline(case).run(case.create_instances())
    if not success:
        util.log.error("An error happened, aborting")
        sys.exit(1)
    case.collect()
    case.plot()


@main.command('run')
@click.option('--case', '-c', default='.', type=CaseType(file_okay=True, dir_okay=True))
@click.option('-j', 'nprocs', default=1, type=int)
@workflows
def run(case: Case, workflow: str, nprocs: int):
    case.clear_cache()
    with api.Workflow.get_workflow(workflow)(nprocs) as w:
        if not w.pipeline(case).run(case.create_instances()):
            sys.exit(1)


@main.command('run-with')
@click.option('--case', '-c', default='.', type=CaseType(file_okay=True, dir_okay=True))
@click.option('--target', '-t', default='.', type=click.Path())
@workflows
@click.argument('context', nargs=-1, type=str)
def run_with(case: Case, target: str, workflow: str, context: List[str]):
    evaluator = Interpreter()
    parsed_context = {}
    for s in context:
        k, v = s.split('=', 1)
        parsed_context[k] = evaluator.eval(v)
    instance = case.create_instance(parsed_context, logdir=target)
    with api.Workflow.get_workflow(workflow)() as w:
        if not w.pipeline(case).run([instance]):
            sys.exit(1)


@main.command('capture')
@click.option('--case', '-c', default='.', type=CaseType(file_okay=True, dir_okay=True))
def capture(case: Case):
    case.capture()


@main.command('collect')
@click.option('--case', '-c', default='.', type=CaseType(file_okay=True, dir_okay=True))
def collect(case: Case):
    case.clear_dataframe()
    case.collect()


@main.command('plot')
@click.option('--case', '-c', default='.', type=CaseType(file_okay=True, dir_okay=True))
def plot(case: Case):
    case.plot()


@main.command()
@click.option('--fmt', '-f', default='json', type=click.Choice(['json']))
@click.option('--case', '-c', default='.', type=CaseType(file_okay=True, dir_okay=True))
@click.argument('output', type=click.File('w'))
def dump(case: Case, fmt: str, output: io.IOBase):
    data = case.load_dataframe()
    if fmt == 'json':
        json.dump(
            data.to_dict('records'),
            output,
            sort_keys=True,
            indent=4,
            cls=util.JSONEncoder,
        )
