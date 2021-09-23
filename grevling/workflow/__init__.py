from abc import ABC, abstractmethod
import asyncio
from itertools import chain

from .. import util


class Pipe(ABC):

    def run(self, inputs):
        asyncio.run(self._run(inputs))

    @abstractmethod
    def _run(self, inputs):
        ...

    @abstractmethod
    async def work(self, in_queue, out_queue=None):
        ...


class PipeSegment(Pipe):

    async def _run(self, inputs):
        queue = util.to_queue(inputs)
        asyncio.create_task(self.work(queue))
        await queue.join()

    async def work(self, in_queue, out_queue=None):
        while True:
            arg = await in_queue.get()
            ret = self.apply(arg)
            if out_queue:
                await out_queue.put(ret)
            in_queue.task_done()

    @abstractmethod
    def apply(self, arg):
        ...


class Pipeline(Pipe):

    def __init__(self, *pipes):
        self.pipes = pipes

    async def _run(self, inputs):
        await self.work(util.to_queue(inputs))

    async def work(self, in_queue, out_queue=None):
        ntasks = len(self.pipes)
        queues = [asyncio.Queue(maxsize=1) for _ in range(ntasks-1)]
        in_queues = chain([in_queue], queues)
        out_queues = chain(queues, [out_queue])

        for pipe, inq, outq in zip(self.pipes, in_queues, out_queues):
            asyncio.create_task(pipe.work(inq, outq))

        await in_queue.join()
        for queue in queues:
            await queue.join()


class PrepareInstance(PipeSegment):

    def __init__(self, case, workspaces):
        self.case = case
        self.workspaces = workspaces

    @util.with_context('I {instance.index}')
    @util.with_context('Pre')
    def apply(self, instance):
        workspace = instance.open_workspace(self.workspaces)
        self.case.prepare_instance(instance, workspace)
        return instance


class DownloadResults(PipeSegment):

    def __init__(self, case, workspaces):
        self.case = case
        self.workspaces = workspaces

    @util.with_context('I {instance.index}')
    @util.with_context('Down')
    def apply(self, instance):
        source_ws = instance.open_workspace(self.workspaces)
        source_log = source_ws.subspace('.grevling')

        with source_log.read_file('grevling.txt') as f:
            ignore_missing = self.case._ignore_missing or b'success=0' in f.read()

        self.case.postmap.copy(instance.context, source_ws, instance.logspace, ignore_missing=ignore_missing)

        source_log.copy_all_to(instance.bookspace)
        return instance
