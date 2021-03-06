from functools import wraps
import json
import multiprocessing
from pathlib import Path
import sys

import click
import numpy as np
from ruamel.yaml.parser import ParserError as YAMLParserError
from strictyaml import YAMLValidationError

import badger
from . import util


class CustomClickException(click.ClickException):

    def show(self):
        util.log.critical(str(self))


class Case(click.Path):

    def convert(self, value, param, ctx):
        path = Path(super().convert(value, param, ctx))
        if path.is_dir():
            casefile = path / 'badger.yaml'
        else:
            casefile = path
        if not casefile.exists():
            raise click.FileError(str(casefile), hint='does not exist')
        if not casefile.is_file():
            raise click.FileError(str(casefile), hint='is not a file')
        try:
            return badger.Case(path)
        except (YAMLValidationError, YAMLParserError) as error:
            raise CustomClickException(str(error))


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(badger.__version__)
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
def run(case, nprocs):
    if not case.check(interactive=False):
        sys.exit(1)
    case.clear_cache()
    case.run(nprocs=nprocs)


@main.command('capture')
@click.option('--case', '-c', default='.', type=Case(file_okay=True, dir_okay=True))
def collect(case):
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
