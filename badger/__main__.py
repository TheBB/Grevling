from functools import wraps
from pathlib import Path
import sys

import click
import treelog as log
from strictyaml import YAMLValidationError
from ruamel.yaml.parser import ParserError as YAMLParserError

import badger


class CustomClickException(click.ClickException):

    def show(self):
        log.error(str(self))


class Case(click.Path):

    def convert(self, value, param, ctx):
        path = Path(super().convert(value, param, ctx))
        casefile = path / 'badger.yaml'
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


def with_logger(logger):
    def decorator(func):
        @wraps(func)
        def inner(*args, **kwargs):
            with log.set(logger):
                return func(*args, **kwargs)
        return inner
    return decorator


class RichOutputLog(log.RichOutputLog):

    def __init__(self, stream):
        super().__init__()
        self.stream = stream

    def write(self, text, level):
        message = ''.join([self._cmap[level.value], text, '\033[0m\n', self._current])
        click.echo(message, file=self.stream, nl=False)


@click.group()
@click.option('--version', is_flag=True, callback=print_version, expose_value=False, is_eager=True)
@click.option('--debug', 'verbosity', flag_value='debug')
@click.option('--info', 'verbosity', flag_value='info', default=True)
@click.option('--user', 'verbosity', flag_value='user')
@click.option('--warning', 'verbosity', flag_value='warning')
@click.option('--error', 'verbosity', flag_value='error')
@click.option('--rich/--no-rich', default=True)
@click.pass_context
def main(ctx, verbosity, rich):
    if rich:
        logger = RichOutputLog(sys.stdout)
    else:
        logger = log.TeeLog(
            log.FilterLog(log.StdoutLog(), maxlevel=log.proto.Level.user),
            log.FilterLog(log.StderrLog(), minlevel=log.proto.Level.warning),
        )
    log.current = log.FilterLog(logger, minlevel=getattr(log.proto.Level, verbosity))


@main.command()
@click.argument('case', default='.', type=Case(file_okay=False))
def check(case):
    case.check()


@main.command()
@click.argument('case', default='.', type=Case(file_okay=False))
def run(case):
    case.run()
