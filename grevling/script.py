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

from . import api, util
from .capture import Capture, CaptureCollection
from .schema import CommandSchema


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
        command_str = ' '.join(shlex.quote(c) if c != '&&' else c for c in command)
        proc = await asyncio.create_subprocess_shell(command_str, **kwargs)  # type: ignore
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


@dataclass(frozen=True)
class Command:

    name: str
    command: Optional[List[str]]
    env: Dict[str, str]
    workdir: Optional[str]

    container: Optional[str]
    container_args: List[str]

    shell: bool
    retry_on_fail: bool
    allow_failure: bool

    captures: List[Capture]

    @staticmethod
    def from_schema(schema: CommandSchema) -> Command:
        args = schema.dict(exclude={
            'command', 'name', 'capture', 'container_args'
        })

        if isinstance(schema.command, str):
            args['command'] = shlex.split(schema.command)
            args['shell'] = True
        else:
            args['command'] = schema.command
            args['shell'] = False

        if not schema.name:
            args['name'] = Path(args['command'][0]).name if args['command'] else 'TODO'
        else:
            args['name'] = schema.name

        args['captures'] = [Capture.from_schema(entry) for entry in schema.capture]

        if isinstance(schema.container_args, str):
            args['container_args'] = shlex.split(schema.container_args)
        else:
            args['container_args'] = schema.container_args

        return Command(**args)

    async def execute(self, cwd: Path, log_ws: api.Workspace) -> bool:
        kwargs = {
            'cwd': cwd,
            'shell': self.shell,
            'env': self.env,
        }

        if self.workdir:
            kwargs['cwd'] = Path(self.workdir)

        command = self.command
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

        if not command:
            util.log.error('No command available')
            return False

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


@dataclass(frozen=True)
class Script:

    commands: List[Command]

    @staticmethod
    def from_schema(schema: List[CommandSchema]) -> Script:
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



@dataclass(frozen=True)
class ScriptTemplate:

    func: Callable[[api.Context], List[CommandSchema]]

    def render(self, ctx: api.Context) -> Script:
        return Script.from_schema(self.func(ctx))
