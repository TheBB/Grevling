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


class Command:

    args: Union[str, List[str]]
    shell: bool

    def __init__(self, args: Union[str, List[str]], shell: bool = False):
        self.args = args
        self.shell = shell

    def execute(self, cwd: Path, log_ws: api.Workspace):
        ...


class CommandTemplate:

    @classmethod
    def load(cls, spec, containers={}):
        if isinstance(spec, (str, list)):
            return cls(spec)
        if 'capture-output' in spec:
            del spec['capture-output']
        return util.call_yaml(cls, spec, container_args=containers)

    def __init__(self, command, name=None, capture=None, capture_walltime=False,
                retry_on_fail=False, env=None, container=None, container_args={},
                allow_failure=False):
        self._command = command
        self._capture_walltime = capture_walltime
        self._retry_on_fail = retry_on_fail
        self._env = env
        self._allow_failure = allow_failure

        self._container = container
        self._container_args = container_args.get(container)

        if name is None:
            exe = shlex.split(command)[0] if isinstance(command, str) else command[0]
            self.name = Path(exe).name
        else:
            self.name = name

        self._capture = []
        if isinstance(capture, (str, dict)):
            self._capture.append(Capture.load(capture))
        elif isinstance(capture, list):
            self._capture.extend(Capture.load(c) for c in capture)

    def add_types(self, types: Dict[str, Any]):
        if self._capture_walltime:
            types[f'walltime/{self.name}'] = float
        for cap in self._capture:
            cap.add_types(types)

    @util.with_context('{self.name}')
    def run(self, collector: ResultCollector, context: Dict, workpath: Path, log_ws) -> bool:
        kwargs = {
            'cwd': workpath,
            'shell': False,
            'stdout': subprocess.PIPE,
            'stderr': subprocess.PIPE,
        }

        if self._env:
            kwargs['env'] = os.environ.copy()
            for k, v in self._env.items():
                kwargs['env'][k] = render(v, context)

        if isinstance(self._command, str):
            kwargs['shell'] = True
            command = render(self._command, context, mode='shell')
        else:
            command = [render(arg, context) for arg in self._command]

        if self._container:
            if isinstance(command, list):
                command = ' '.join(shlex.quote(c) for c in command)
            if isinstance(self._container_args, str):
                args = shlex.split(render(self._container_args, context, mode='shell'))
            elif isinstance(self._container_args, list):
                args = [render(arg, context) for arg in self._container_args]
            else:
                args = []
            command = [
                'docker', 'run', *args, f'-v{workpath}:/workdir', '--workdir', '/workdir',
                self._container, 'bash', '-c', command,
            ]
            kwargs['shell'] = False

        util.log.debug(command if isinstance(command, str) else ' '.join(shlex.quote(c) for c in command))
        with time() as duration:
            while True:
                result = subprocess.run(command, **kwargs)
                if self._retry_on_fail and result.returncode:
                    util.log.info('Failed, retrying...')
                    continue
                break
        duration = duration()

        log_ws.write_file(f'{self.name}.stdout', result.stdout)
        log_ws.write_file(f'{self.name}.stderr', result.stderr)

        self.capture(collector, stdout=result.stdout, duration=duration)

        if result.returncode:
            level = util.log.warn if self._allow_failure else util.log.error
            level(f"command returned exit status {result.returncode}")
            level(f"stdout stored")
            level(f"stderr stored")
            return self._allow_failure
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
        for capture in self._capture:
            capture.find_in(collector, stdout)
        if self._capture_walltime and duration is not None:
            collector.collect(f'walltime/{self.name}', duration)


class ScriptTemplate:

    commands: List[CommandTemplate]

    @classmethod
    def load(cls, commands: List, containers: Dict) -> 'Script':
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
            if not command.run(collector, context, cwd, log_ws):
                return False
        return True

    def add_types(self, types):
        for command in self.commands:
            command.add_types(types)
