from __future__ import annotations

import asyncio
from collections import namedtuple
from contextlib import contextmanager
from dataclasses import field, InitVar, dataclass
import datetime
import os
from pathlib import Path
import shlex
from time import time as osclock

from typing import Dict, List, Optional, Union

from . import api, util
from .capture import Capture, CaptureCollection
from .render import render
from .typing import TypeManager, Stage


@contextmanager
def time():
    start = osclock()
    yield lambda: end - start
    end = osclock()


def shell_list_render(arg: Union[str, List[str]], context: api.Context) -> List[str]:
    if isinstance(arg, str):
        return shlex.split(render(arg, context, mode='shell'))
    return [render(c, context) for c in arg]


Result = namedtuple('Result', ['stdout', 'stderr', 'returncode'])


async def run(
    command: List[str], shell: bool, env: Dict[str, str], cwd: Path
) -> Result:
    kwargs = {
        'env': env,
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
                    ['bash', '-c', ' '.join(shlex.quote(c) for c in command)]
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


@dataclass()
class CommandTemplate:

    name: str = ''

    command: Union[str, List[str]] = ''
    env: Dict[str, str] = field(default_factory=dict)
    captures: List[Capture] = field(init=False)

    container: Optional[str] = None
    container_args: Union[str, List[str]] = field(init=False)

    retry_on_fail: bool = False
    allow_failure: bool = False

    # Init-only variables, used for initializing captures and container_args
    container_args_spec: InitVar[Dict] = {}
    capture: InitVar[Union[str, Dict, List]] = []

    @classmethod
    def load(cls, spec, containers={}) -> CommandTemplate:
        if isinstance(spec, (str, list)):
            return cls(command=spec)
        spec.pop('capture-output', None)
        spec.pop('capture-walltime', None)
        return util.call_yaml(cls, spec, container_args_spec=containers)

    def __post_init__(self, container_args_spec, capture):
        if not self.name:
            assert self.command
            exe = (
                shlex.split(self.command)[0]
                if isinstance(self.command, str)
                else self.command[0]
            )
            self.name = Path(exe).name

        self.container_args = container_args_spec.get(self.container, [])

        self.captures = []
        if isinstance(capture, (str, dict)):
            self.captures.append(Capture.load(capture))
        elif isinstance(capture, list):
            self.captures.extend(Capture.load(c) for c in capture)

    def add_types(self, types: TypeManager):
        types.add(f'g_walltime_{self.name}', float, Stage.post)
        for cap in self.captures:
            cap.add_types(types)

    @util.with_context('{self.name}')
    def render(self, context: Dict) -> Command:
        kwargs = {
            'shell': isinstance(self.command, str),
        }

        if self.env:
            kwargs['env'] = os.environ.copy()
            for k, v in self.env.items():
                kwargs['env'][k] = render(v, context)

        command = shell_list_render(self.command, context)

        return Command(
            name=self.name,
            args=command,
            env=kwargs.get('env'),
            shell=kwargs['shell'],
            retry_on_fail=self.retry_on_fail,
            allow_failure=self.allow_failure,
            container=self.container,
            container_args=shell_list_render(self.container_args, context),
        )

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


class ScriptTemplate:

    commands: List[CommandTemplate]

    @classmethod
    def load(cls, commands: List, containers: Dict) -> ScriptTemplate:
        script = cls()
        script.commands.extend(
            CommandTemplate.load(spec, containers) for spec in commands
        )
        return script

    def __init__(self):
        self.commands = []

    def capture(self, collector: CaptureCollection, workspace: api.Workspace):
        for command in self.commands:
            command.capture(collector, workspace=workspace)

    def render(self, context: api.Context) -> Script:
        return Script([cmd.render(context) for cmd in self.commands])

    def add_types(self, types: api.Types):
        for command in self.commands:
            command.add_types(types)

    def capture(self, collector: CaptureCollection, workspace: api.Workspace):
        for cmd in self.commands:
            cmd.capture(collector, workspace)
