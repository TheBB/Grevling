from abc import ABC, abstractmethod
import asyncio
from io import StringIO
from itertools import chain
import os
import traceback

from typing import List

from .. import util


class Pipe(ABC):

    ncopies: int = 1

    def run(self, inputs) -> bool:
        # TODO: As far as I can tell, this is only needed on Python 3.7
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        return asyncio.run(self._run(inputs))

    @abstractmethod
    def _run(self, inputs) -> bool:
        ...

    @abstractmethod
    async def work(self, in_queue, out_queue=None):
        ...


class PipeSegment(Pipe):

    name: str
    npiped: int

    def __init__(self, ncopies: int = 1):
        self.ncopies = ncopies
        self.npiped = 0

    async def _run(self, inputs) -> bool:
        queue = util.to_queue(inputs)
        ninputs = queue.qsize()
        asyncio.create_task(self.work(queue))
        await queue.join()
        return self.npiped == ninputs

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
            self.npiped += 1
            in_queue.task_done()

    @abstractmethod
    async def apply(self, arg):
        ...


class Pipeline(Pipe):

    pipes: List[PipeSegment]

    def __init__(self, *pipes: PipeSegment):
        self.pipes = list(pipes)

    async def _run(self, inputs) -> bool:
        queue = util.to_queue(inputs)
        ninputs = queue.qsize()
        await self.work(queue)
        return self.pipes[-1].npiped == ninputs

    async def work(self, in_queue, out_queue=None):
        ntasks = len(self.pipes)
        queues = [asyncio.Queue(maxsize=1) for _ in range(ntasks-1)]
        in_queues = chain([in_queue], queues)
        out_queues = chain(queues, [out_queue])

        for pipe, inq, outq in zip(self.pipes, in_queues, out_queues):
            for _ in range(pipe.ncopies):
                asyncio.create_task(pipe.work(inq, outq))

        for pipe, queue in zip(self.pipes, chain([in_queue], queues)):
            await queue.join()
            util.log.info(f"{pipe.name} finished: {pipe.npiped} instances handled")


class PrepareInstance(PipeSegment):

    name = 'Prepare'

    def __init__(self, workspaces):
        super().__init__()
        self.workspaces = workspaces

    @util.with_context('I {instance.index}')
    @util.with_context('Pre')
    async def apply(self, instance):
        with instance.bind_remote(self.workspaces):
            instance.prepare()
        return instance


class DownloadResults(PipeSegment):

    name = 'Download'

    def __init__(self, workspaces):
        super().__init__()
        self.workspaces = workspaces

    @util.with_context('I {instance.index}')
    @util.with_context('Down')
    async def apply(self, instance):
        with instance.bind_remote(self.workspaces):
            instance.download()
        return instance
