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

from typing import Any, Dict, List, Optional, Union

from . import api, util, schema
from .capture import Capture, CaptureCollection
from .render import render, renderable


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
        command = ' '.join(shlex.quote(c) for c in command)
        proc = await asyncio.create_subprocess_shell(command, **kwargs)
    else:
        proc = await asyncio.create_subprocess_exec(*command, **kwargs)

    assert proc.stdout is not None

    stdout = b''
    with util.log.with_context('stdout'):
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            stdout += line
            line = line.decode().rstrip()
            util.log.debug(line)

    remaining_stdout, stderr = await proc.communicate()
    stdout += remaining_stdout
    return Result(stdout, stderr, proc.returncode)


@dataclass()
class Command:

    name: str
    args: Union[str, List[str]]
    env: Dict[str, str] = field(default_factory=dict)

    container: Optional[str] = None
    container_args: List[str] = field(default_factory=list)

    shell: bool = False
    retry_on_fail: bool = False
    allow_failure: bool = False

    captures: List[Capture] = field(default_factory=list)

    @classmethod
    def load(cls, data: Any) -> Command:
        kwargs = {
            'shell': False
        }

        command = data.get('command', '')
        if isinstance(command, str):
            command = shlex.split(command)
            kwargs['shell'] = True
        kwargs['name'] = data.get('name') or Path(command[0]).name

        captures = data.get('capture', [])
        if isinstance(captures, (str, dict)):
            kwargs['captures'] = [Capture.load(captures)]
        else:
            kwargs['captures'] = [Capture.load(c) for c in captures]
        kwargs['allow_failure'] = data.get('allow-failure', False)
        kwargs['retry_on_fail'] = data.get('retry-on-fail', False)
        kwargs['env'] = data.get('env', {})
        kwargs['container'] = data.get('container', None)
        container_args = data.get('container-args', [])
        if isinstance(container_args, str):
            container_args = shlex.split(container_args)
        kwargs['container_args'] = container_args

        return cls(args=command, **kwargs)

    async def execute(self, cwd: Path, log_ws: api.Workspace) -> bool:
        kwargs = {
            'cwd': cwd,
            'shell': self.shell,
            'env': self.env,
        }

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
                result = await run(command, **kwargs)
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
            with workspace.open_file(f'{self.name}.stdout', 'r') as f:
                stdout = f.read()
        except FileNotFoundError:
            return
        for capture in self.captures:
            capture.find_in(collector, stdout)


@dataclass
class Script:

    commands: List[Command]

    @classmethod
    def load(cls, data: List) -> Script:
        return cls([Command.load(spec) for spec in data])

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


def ScriptTemplate(data: Any) -> api.Renderable[Script]:
    return renderable(
        data, Script.load, schema.Script.validate,
        '[*][command,container-args]', '[*][command,container-args][*]', '[*][env][*]',
    )
