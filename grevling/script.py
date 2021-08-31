from contextlib import contextmanager
from logging import log
import os
from pathlib import Path
import shlex
import subprocess
from time import time as osclock

from typing import Dict, Any, List, Optional, Union

from . import api, util
from .capture import Capture, ResultCollector
from .render import render


@contextmanager
def time():
    start = osclock()
    yield lambda: end - start
    end = osclock()


def shell_list_render(arg: Union[str, List[str]], context: api.Context) -> List[str]:
    if isinstance(arg, str):
        return shlex.split(render(arg, context, mode='shell'))
    return [render(c, context) for c in arg]


class Command:

    name: str
    args: Union[str, List[str]]
    env: Optional[Dict[str, str]]
    captures: List[Capture]
    shell: bool

    container: Optional[str]
    container_args: List[str]

    retry_on_fail: bool
    capture_walltime: bool
    allow_failure: bool

    def __init__(self, name: str, args: Union[str, List[str]], env: Optional[Dict[str, str]],
                 captures: List[Capture] = [], shell: bool = False, retry_on_fail: bool = False,
                 capture_walltime: bool = False, allow_failure: bool = False,
                 container: Optional[str] = None, container_args: List[str] = []):
        self.name = name
        self.args = args
        self.env = env
        self.captures = captures
        self.shell = shell
        self.container = container
        self.container_args = container_args
        self.retry_on_fail = retry_on_fail
        self.capture_walltime = capture_walltime
        self.allow_failure = allow_failure

    def execute(self, collector: ResultCollector, cwd: Path, log_ws: api.Workspace):
        kwargs = {
            'cwd': cwd,
            'shell': self.shell,
            'stdout': subprocess.PIPE,
            'stderr': subprocess.PIPE,
            'env': self.env,
        }

        command = self.args
        if self.container:
            docker_command = [
                'docker', 'run', *self.container_args,
                f'-v{cwd}:/workdir', '--workdir', '/workdir',
                self.container,
            ]
            if command:
                docker_command.extend(['bash', '-c', ' '.join(shlex.quote(c) for c in command)])
            kwargs['shell'] = False
            command = docker_command

        util.log.debug(' '.join(shlex.quote(c) for c in command))

        if kwargs['shell']:
            command = ' '.join(shlex.quote(c) for c in command)

        with time() as duration:
            while True:
                result = subprocess.run(command, **kwargs)
                if self.retry_on_fail and result.returncode:
                    util.log.info('Failed, retrying...')
                    continue
                break
        duration = duration()

        log_ws.write_file(f'{self.name}.stdout', result.stdout)
        log_ws.write_file(f'{self.name}.stderr', result.stderr)

        self.capture(collector, stdout=result.stdout, duration=duration)

        if result.returncode:
            level = util.log.warn if self.allow_failure else util.log.error
            level(f"command returned exit status {result.returncode}")
            level(f"stdout stored")
            level(f"stderr stored")
            return self.allow_failure
        else:
            util.log.info(f"success ({util.format_seconds(duration)})")

        return True

    def capture(self, collector: ResultCollector,
                stdout: Optional[bytes] = None,
                workspace: Optional[api.Workspace] = None,
                duration: Optional[float] = None):
        if stdout is None:
            assert workspace is not None
            with workspace.open_file(f'{self.name}.stdout', 'rb') as f:
                stdout = f.read()
        assert isinstance(stdout, bytes)
        stdout = stdout.decode()
        for capture in self.captures:
            capture.find_in(collector, stdout)
        if self.capture_walltime and duration is not None:
            collector.collect(f'walltime/{self.name}', duration)


class CommandTemplate:

    name: str
    command: Union[str, List[str]]
    env: Optional[Dict[str, str]]
    captures: List[Capture]

    container: Optional[str]
    container_args: Union[str, List[str]]

    capture_walltime: bool
    retry_on_fail: bool
    allow_failure: bool

    @classmethod
    def load(cls, spec, containers={}):
        if isinstance(spec, (str, list)):
            return cls(spec)
        if 'capture-output' in spec:
            del spec['capture-output']
        return util.call_yaml(cls, spec, container_args=containers)

    def __init__(self, command='', name=None, capture=None, capture_walltime=False,
                 retry_on_fail=False, env=None, container=None, container_args={},
                 allow_failure=False):
        self.command = command
        self.env = env

        self.capture_walltime = capture_walltime
        self.retry_on_fail = retry_on_fail
        self.allow_failure = allow_failure

        self.container = container
        self.container_args = container_args.get(container, [])

        if name is None:
            exe = shlex.split(command)[0] if isinstance(command, str) else command[0]
            self.name = Path(exe).name
        else:
            self.name = name

        self.captures = []
        if isinstance(capture, (str, dict)):
            self.captures.append(Capture.load(capture))
        elif isinstance(capture, list):
            self.captures.extend(Capture.load(c) for c in capture)

    def add_types(self, types: Dict[str, Any]):
        if self.capture_walltime:
            types[f'walltime/{self.name}'] = float
        for cap in self.captures:
            cap.add_types(types)

    @util.with_context('{self.name}')
    def render(self, context: Dict) -> Command:
        kwargs = {
            'shell': isinstance(self.command, str),
        }

        if self.env:
            kwargs['env'] = os.environ.copy()
            for k, v in self._env.items():
                kwargs['env'][k] = render(v, context)

        command = shell_list_render(self.command, context)

        cmd = Command(self.name, command, kwargs.get('env'), self.captures, kwargs['shell'],
                      retry_on_fail=self.retry_on_fail, capture_walltime=self.capture_walltime,
                      allow_failure=self.allow_failure,
                      container=self.container, container_args=shell_list_render(self.container_args, context))
        return cmd

    def capture(self, collector: ResultCollector,
                stdout: Optional[bytes] = None,
                workspace: Optional[api.Workspace] = None,
                duration: Optional[float] = None):
        if stdout is None:
            assert workspace is not None
            with workspace.open_file(f'{self.name}.stdout', 'rb') as f:
                stdout = f.read()
        assert isinstance(stdout, bytes)
        stdout = stdout.decode()
        for capture in self.captures:
            capture.find_in(collector, stdout)
        if self.capture_walltime and duration is not None:
            collector.collect(f'walltime/{self.name}', duration)


class ScriptTemplate:

    commands: List[CommandTemplate]

    @classmethod
    def load(cls, commands: List, containers: Dict) -> 'ScriptTemplate':
        script = cls()
        script.commands.extend(CommandTemplate.load(spec, containers) for spec in commands)
        return script

    def __init__(self):
        self.commands = []

    def capture(self, collector: ResultCollector, workspace: api.Workspace):
        for command in self.commands:
            command.capture(collector, workspace=workspace)

    def run(self, collector: ResultCollector, context: api.Context, cwd: Path, log_ws: api.Workspace) -> bool:
        for command in self.commands:
            cmd = command.render(context)
            if not cmd.execute(collector, cwd, log_ws):
                return False
        return True

    def add_types(self, types):
        for command in self.commands:
            command.add_types(types)
