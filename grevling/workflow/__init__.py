from abc import ABC, abstractmethod
import asyncio
from io import StringIO
from itertools import chain
import os
import traceback

from .. import util


class Pipe(ABC):

    ncopies: int = 1

    def run(self, inputs):
        # TODO: As far as I can tell, this is only needed on Python 3.7
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        asyncio.run(self._run(inputs))

    @abstractmethod
    def _run(self, inputs):
        ...

    @abstractmethod
    async def work(self, in_queue, out_queue=None):
        ...


class PipeSegment(Pipe):

    def __init__(self, ncopies: int = 1):
        self.ncopies = ncopies

    async def _run(self, inputs):
        queue = util.to_queue(inputs)
        asyncio.create_task(self.work(queue))
        await queue.join()

    async def work(self, in_queue, out_queue=None):
        while True:
            arg = await in_queue.get()
            try:
                ret = await self.apply(arg)
            except Exception as e:
                util.log.error(str(e))
                with StringIO() as buf:
                    traceback.print_exc(file=buf)
                    util.log.error(buf.getvalue())
                in_queue.task_done()
                continue
            if out_queue:
                await out_queue.put(ret)
            in_queue.task_done()

    @abstractmethod
    async def apply(self, arg):
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
            for _ in range(pipe.ncopies):
                asyncio.create_task(pipe.work(inq, outq))

        await in_queue.join()
        for queue in queues:
            await queue.join()


class PrepareInstance(PipeSegment):

    def __init__(self, workspaces):
        self.workspaces = workspaces

    @util.with_context('I {instance.index}')
    @util.with_context('Pre')
    async def apply(self, instance):
        with instance.bind_remote(self.workspaces):
            instance.prepare()
        return instance


class DownloadResults(PipeSegment):

    def __init__(self, workspaces):
        self.workspaces = workspaces

    @util.with_context('I {instance.index}')
    @util.with_context('Down')
    async def apply(self, instance):
        with instance.bind_remote(self.workspaces):
            instance.download()
        return instance
