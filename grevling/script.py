from __future__ import annotations

import asyncio
from collections import namedtuple
from contextlib import contextmanager
from dataclasses import field, InitVar, dataclass
import datetime
from functools import partial
import os
from pathlib import Path
import shlex
from time import time as osclock

from typing import Any, Dict, List, Optional, Union, Callable

from . import api, util, schema
from .capture import Capture, CaptureCollection
# from .render import render, renderable


@contextmanager
def time():
    start = osclock()
    yield lambda: end - start
    end = osclock()


Result = namedtuple('Result', ['stdout', 'stderr', 'returncode'])


async def run(
    command: List[str], shell: bool, env: Dict[str, str], cwd: Path
) -> Result:
    kwargs = {
        'env': {**os.environ, **env},
        'cwd': cwd,
        'stdout': asyncio.subprocess.PIPE,
        'stderr': asyncio.subprocess.PIPE,
    }

    if shell:
        scommand = ' '.join(shlex.quote(c) if c != '&&' else c for c in command)
        proc = await asyncio.create_subprocess_shell(scommand, **kwargs)  # type: ignore
    else:
        proc = await asyncio.create_subprocess_exec(*command, **kwargs)   # type: ignore

    assert proc.stdout is not None

    stdout = b''
    with util.log.with_context('stdout'):
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            stdout += line
            util.log.debug(line.decode().rstrip())

    remaining_stdout, stderr = await proc.communicate()
    stdout += remaining_stdout
    return Result(stdout, stderr, proc.returncode)


@dataclass()
class Command:

    name: str
    args: Union[str, List[str]]
    env: Dict[str, str] = field(default_factory=dict)
    workdir: Optional[str] = None

    container: Optional[str] = None
    container_args: List[str] = field(default_factory=list)

    shell: bool = False
    retry_on_fail: bool = False
    allow_failure: bool = False

    captures: List[Capture] = field(default_factory=list)

    @staticmethod
    def from_schema(schema: schema.CommandSchema) -> Command:
        kwargs: Dict = {
            'shell': False
        }

        command = schema.command
        if isinstance(command, str):
            command = shlex.split(command)
            kwargs['shell'] = True
        kwargs['name'] = schema.name or (Path(command[0]).name if command else 'TODO')

        captures = [Capture.from_schema(entry) for entry in schema.capture]
        kwargs['captures'] = captures

        kwargs['allow_failure'] = schema.allow_failure
        kwargs['retry_on_fail'] = schema.retry_on_fail
        kwargs['env'] = schema.env
        kwargs['container'] = schema.container

        cargs = schema.container_args
        if isinstance(cargs, str):
            cargs = shlex.split(cargs)

        kwargs['container_args'] = cargs
        kwargs['workdir'] = schema.workdir

        return Command(args=command, **kwargs)

    async def execute(self, cwd: Path, log_ws: api.Workspace) -> bool:
        kwargs = {
            'cwd': cwd,
            'shell': self.shell,
            'env': self.env,
        }

        if self.workdir:
            kwargs['cwd'] = Path(self.workdir)

        command = self.args
        if self.container:
            docker_command = [
                'docker',
                'run',
                *self.container_args,
                f'-v{cwd}:/workdir',
                '--workdir',
                '/workdir',
                self.container,
            ]
            if command:
                docker_command.extend(
                    ['sh', '-c', ' '.join(shlex.quote(c) for c in command)]
                )
            kwargs['shell'] = False
            command = docker_command

        util.log.debug(' '.join(shlex.quote(c) for c in command))

        # TODO: How to get good timings when we run async?
        with time() as duration:
            while True:
                result = await run(command, **kwargs)  # type: ignore
                if self.retry_on_fail and result.returncode:
                    util.log.info('Failed, retrying...')
                    continue
                break
        duration = duration()

        log_ws.write_file(f'{self.name}.stdout', result.stdout)
        log_ws.write_file(f'{self.name}.stderr', result.stderr)
        log_ws.write_file(
            'grevling.txt', f'g_walltime_{self.name}={duration}\n', append=True
        )

        if result.returncode:
            level = util.log.warn if self.allow_failure else util.log.error
            level(f"command returned exit status {result.returncode}")
            level(f"stdout stored")
            level(f"stderr stored")
            return self.allow_failure
        else:
            util.log.info(f"{self.name} success ({util.format_seconds(duration)})")

        return True

    def capture(self, collector: CaptureCollection, workspace: api.Workspace):
        try:
            with workspace.open_str(f'{self.name}.stdout', 'r') as f:
                stdout = f.read()
        except FileNotFoundError:
            return
        for capture in self.captures:
            capture.find_in(collector, stdout)


@dataclass
class Script:

    commands: List[Command]

    @staticmethod
    def from_schema(schema: List[schema.CommandSchema]) -> Script:
        return Script([
            Command.from_schema(entry)
            for entry in schema
        ])

    async def run(self, cwd: Path, log_ws: api.Workspace) -> bool:
        log_ws.write_file(
            'grevling.txt', f'g_started={datetime.datetime.now()}\n', append=True
        )
        try:
            for cmd in self.commands:
                if not await cmd.execute(cwd, log_ws):
                    log_ws.write_file('grevling.txt', 'g_success=0\n', append=True)
                    return False
            log_ws.write_file('grevling.txt', 'g_success=1\n', append=True)
            return True
        finally:
            log_ws.write_file(
                'grevling.txt', f'g_finished={datetime.datetime.now()}\n', append=True
            )

    def capture(self, collector: CaptureCollection, workspace: api.Workspace):
        for cmd in self.commands:
            cmd.capture(collector, workspace)



class ScriptTemplate:

    func: Callable[[api.Context], List[schema.CommandSchema]]

    def __init__(self, func: Callable[[api.Context], List[schema.CommandSchema]]):
        self.func = func

    def render(self, ctx: api.Context) -> Script:
        return Script.from_schema(self.func(ctx))
