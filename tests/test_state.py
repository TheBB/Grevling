from __future__ import annotations

from pathlib import Path

from grevling import Case, Instance
from grevling.api import Status
from grevling.workflow import PipeSegment
from grevling.workflow.local import LocalWorkflow

DATADIR = Path(__file__).parent / "data"


class VerifyRunningPipe(PipeSegment):
    name = "Test"

    async def apply(self, instance: Instance) -> Instance:
        assert instance._case.state.running
        return instance


class VerifyInstanceStatus(PipeSegment):
    name = "Test"

    status: Status

    def __init__(self, status: Status):
        super().__init__()
        self.status = status

    async def apply(self, instance: Instance) -> Instance:
        assert instance.status == self.status
        return instance


def test_casestate():
    with Case(DATADIR / "run" / "echo") as case:
        case.clear_cache()

        assert not case.state.running
        assert not case.state.has_data
        assert not case.state.has_captured
        assert not case.state.has_collected
        assert not case.state.has_plotted

        with LocalWorkflow() as w:
            pipeline = w.pipeline(case)
            pipeline.pipes.insert(1, VerifyRunningPipe())
            assert pipeline.run(case.create_instances())

        assert not case.state.running
        assert case.state.has_data
        assert not case.state.has_captured
        assert not case.state.has_collected
        assert not case.state.has_plotted

        case.capture()

        assert not case.state.running
        assert case.state.has_data
        assert case.state.has_captured
        assert not case.state.has_collected
        assert not case.state.has_plotted

        case.collect()

        assert not case.state.running
        assert case.state.has_data
        assert case.state.has_captured
        assert case.state.has_collected
        assert not case.state.has_plotted

    with Case(DATADIR / "run" / "echo") as case:
        assert not case.state.running
        assert case.state.has_data
        assert case.state.has_captured
        assert case.state.has_collected
        assert not case.state.has_plotted

        case.plot()

        assert not case.state.running
        assert case.state.has_data
        assert case.state.has_captured
        assert case.state.has_collected
        assert case.state.has_plotted

        case.run()

        assert not case.state.running
        assert case.state.has_data
        assert not case.state.has_captured
        assert not case.state.has_collected
        assert not case.state.has_plotted

        case.clear_cache()

        assert not case.state.running
        assert not case.state.has_data
        assert not case.state.has_captured
        assert not case.state.has_collected
        assert not case.state.has_plotted


def test_instance_status():
    with Case(DATADIR / "run" / "echo") as case:
        case.clear_cache()

        with LocalWorkflow() as w:
            pipeline = w.pipeline(case)

            pipeline.pipes = [
                VerifyInstanceStatus(Status.Created),
                pipeline.pipes[0],
                VerifyInstanceStatus(Status.Prepared),
                pipeline.pipes[1],
                VerifyInstanceStatus(Status.Finished),
                pipeline.pipes[2],
                VerifyInstanceStatus(Status.Downloaded),
            ]

            assert pipeline.run(case.create_instances())
