from __future__ import annotations

import asyncio
import traceback
from abc import ABC, abstractmethod
from io import StringIO
from itertools import chain
from typing import TYPE_CHECKING, Any, Optional, cast

if TYPE_CHECKING:
    from collections.abc import Iterable

    from grevling import Case, Instance

from grevling import api, util


class Pipe(ABC):
    ncopies: int = 1

    def run(self, inputs: Iterable[Any]) -> bool:
        return asyncio.run(self._run(inputs))

    @abstractmethod
    async def _run(self, inputs: Iterable[Any]) -> bool:
        ...

    @abstractmethod
    async def work(self, in_queue: asyncio.Queue, out_queue: Optional[asyncio.Queue] = None) -> None:
        ...


class PipeSegment(Pipe):
    name: str
    npiped: int
    ncopies: int
    tasks: list[asyncio.Task]

    def __init__(self, ncopies: int = 1):
        self.ncopies = ncopies
        self.npiped = 0
        self.tasks = []

    def create_tasks(self, in_queue: asyncio.Queue, out_queue: Optional[asyncio.Queue] = None) -> None:
        self.tasks = [asyncio.create_task(self.work(in_queue, out_queue)) for _ in range(self.ncopies)]

    async def close_tasks(self) -> None:
        for task in self.tasks:
            task.cancel()

    async def _run(self, inputs: Iterable[Any]) -> bool:
        queue = util.to_queue(inputs)
        ninputs = cast(int, queue.qsize())
        asyncio.create_task(self.work(queue))
        await queue.join()
        return self.npiped == ninputs

    async def work(self, in_queue: asyncio.Queue, out_queue: Optional[asyncio.Queue] = None) -> None:
        try:
            while True:
                arg = await in_queue.get()
                try:
                    ret = await self.apply(arg)
                    if out_queue:
                        await out_queue.put(ret)
                    self.npiped += 1
                    in_queue.task_done()
                except Exception as e:
                    util.log.error(str(e))
                    with StringIO() as buf:
                        traceback.print_exc(file=buf)
                        util.log.error(buf.getvalue())
                    in_queue.task_done()
                    continue
        except asyncio.CancelledError:
            pass

    def finalize(self, succcess: bool) -> None:
        pass

    @abstractmethod
    async def apply(self, arg: Any) -> Any:
        ...


class Pipeline(Pipe):
    pipes: list[PipeSegment]

    def __init__(self, *pipes: PipeSegment) -> None:
        self.pipes = list(pipes)

    async def _run(self, inputs: Iterable[Any]) -> bool:
        queue = util.to_queue(inputs)
        ninputs = cast(int, queue.qsize())
        await self.work(queue)
        success = self.pipes[-1].npiped == ninputs
        for pipe in self.pipes:
            pipe.finalize(success)
        return success

    async def work(self, in_queue: asyncio.Queue, out_queue: Optional[asyncio.Queue] = None) -> None:
        ntasks = len(self.pipes)
        queues: list[asyncio.Queue] = [asyncio.Queue(maxsize=1) for _ in range(ntasks - 1)]
        in_queues = chain([in_queue], queues)
        out_queues = chain(queues, [out_queue])

        for pipe, inq, outq in zip(self.pipes, in_queues, out_queues):
            pipe.create_tasks(inq, outq)

        for pipe, queue in zip(self.pipes, chain([in_queue], queues)):
            await queue.join()
            await pipe.close_tasks()
            util.log.info(f"{pipe.name} finished: {pipe.npiped} instances handled")

        await asyncio.gather(*chain.from_iterable(pipe.tasks for pipe in self.pipes))


class PrepareInstance(PipeSegment):
    name = "Prepare"

    def __init__(self, workspaces: api.WorkspaceCollection):
        super().__init__()
        self.workspaces = workspaces

    @util.with_context("I {instance.index}")
    @util.with_context("Pre")
    async def apply(self, instance: Instance) -> Instance:
        with instance.bind_remote(self.workspaces):
            instance.prepare()
        return instance


class DownloadResults(PipeSegment):
    name = "Download"

    workspaces: api.WorkspaceCollection
    case: Case

    def __init__(self, workspaces: api.WorkspaceCollection, case: Case):
        super().__init__()
        self.workspaces = workspaces
        self.case = case

    @util.with_context("I {instance.index}")
    @util.with_context("Down")
    async def apply(self, instance: Instance) -> Instance:
        with instance.bind_remote(self.workspaces):
            instance.download()
        self.workspaces.destroy_workspace(instance.logdir)
        return instance
