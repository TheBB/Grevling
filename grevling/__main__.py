from functools import wraps
import json
import multiprocessing
from pathlib import Path
import sys

import click
import numpy as np
from ruamel.yaml.parser import ParserError as YAMLParserError
from simpleeval import SimpleEval
from strictyaml import YAMLValidationError

import grevling
from . import util


class CustomClickException(click.ClickException):

    def show(self):
        util.log.critical(str(self))


class Case(click.Path):

    def convert(self, value, param, ctx):
        if isinstance(value, grevling.Case):
            return value
        path = Path(super().convert(value, param, ctx))
        if path.is_dir():
            for candidate in ['grevling.yaml', 'badger.yaml']:
                if (path / candidate).exists():
                    casefile = path / candidate
                    break
        else:
            casefile = path
        if not casefile.exists():
            raise click.FileError(str(casefile), hint='does not exist')
        if not casefile.is_file():
            raise click.FileError(str(casefile), hint='is not a file')
        try:
            return grevling.Case(path)
        except (YAMLValidationError, YAMLParserError) as error:
            raise CustomClickException(str(error))


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(grevling.__version__)
    ctx.exit()


@click.group()
@click.option('--version', is_flag=True, callback=print_version, expose_value=False, is_eager=True)
@click.option('--debug', 'verbosity', flag_value='DEBUG')
@click.option('--info', 'verbosity', flag_value='INFO', default=True)
@click.option('--warning', 'verbosity', flag_value='WARNING')
@click.option('--error', 'verbosity', flag_value='ERROR')
@click.option('--critical', 'verbosity', flag_value='CRITICAL')
@click.option('--rich/--no-rich', default=True)
@click.pass_context
def main(ctx, verbosity, rich):
    util.initialize_logging(level=verbosity, show_time=False)
    multiprocessing.current_process().name = 'M'


@main.command()
@click.option('--case', '-c', default='.', type=Case(file_okay=True, dir_okay=True))
def check(case):
    case.check(interactive=True)


@main.command('run-all')
@click.option('--case', '-c', default='.', type=Case(file_okay=True, dir_okay=True))
def run_all(case):
    if not case.check(interactive=False):
        sys.exit(1)
    case.clear_cache()
    case.run()
    case.collect()
    case.plot()


@main.command('run')
@click.option('--case', '-c', default='.', type=Case(file_okay=True, dir_okay=True))
@click.option('-j', 'nprocs', default=None, type=int)
@click.option('--azure', flag_value=True, default=False)
def run(case, nprocs, azure):
    if not case.check(interactive=False):
        sys.exit(1)
    case.clear_cache()
    case.run(nprocs=nprocs)


@main.command('run-with')
@click.option('--case', '-c', default='.', type=Case(file_okay=True, dir_okay=True))
@click.option('--target', '-t', default='.', type=click.Path())
@click.argument('context', nargs=-1, type=str)
def run_with(case, target, context):
    evaluator = SimpleEval()
    parsed_context = {}
    for s in context:
        k, v = s.split('=', 1)
        parsed_context[k] = evaluator.eval(v)
    parsed_context = case.context_mgr.evaluate_context(parsed_context)
    case.run_single(index=0, namespace=parsed_context, logdir=Path(target))


@main.command('capture')
@click.option('--case', '-c', default='.', type=Case(file_okay=True, dir_okay=True))
def capture(case):
    if not case.check(interactive=False):
        sys.exit(1)
    case.capture()


@main.command('collect')
@click.option('--case', '-c', default='.', type=Case(file_okay=True, dir_okay=True))
def collect(case):
    if not case.check(interactive=False):
        sys.exit(1)
    case.clear_dataframe()
    case.collect()


@main.command('plot')
@click.option('--case', '-c', default='.', type=Case(file_okay=True, dir_okay=True))
def plot(case):
    if not case.check(interactive=False):
        sys.exit(1)
    case.plot()


@main.command()
@click.option('--fmt', '-f', default='json', type=click.Choice(['json']))
@click.option('--case', '-c', default='.', type=Case(file_okay=True, dir_okay=True))
@click.argument('output', type=click.File('w'))
def dump(case, fmt, output):
    with case.lock():
        data = case.load_dataframe()
    if fmt == 'json':
        json.dump(data.to_dict('records'), output, sort_keys=True, indent=4, cls=util.JSONEncoder)
