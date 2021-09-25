from . import Pipeline, PipeSegment, PrepareInstance, DownloadResults
from ..runner.local import TempWorkspaceCollection
from .. import util
from ..instance import Status


class RunInstance(PipeSegment):

    def __init__(self, workspaces):
        self.workspaces = workspaces

    @util.with_context('I {instance.index}')
    @util.with_context('Run')
    def apply(self, instance):
        instance.status = Status.Started
        workspace = instance.open_workspace(self.workspaces)
        instance.script.run(workspace.root, workspace.subspace('.grevling'))
        instance.status = Status.Finished
        return instance


class LocalWorkflow:

    def __init__(self, case):
        self.case = case

    def __enter__(self):
        self.workspaces = TempWorkspaceCollection('WRK').__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self.workspaces.__exit__(*args, **kwargs)

    def pipeline(self):
        return Pipeline(
            PrepareInstance(self.workspaces),
            RunInstance(self.workspaces),
            DownloadResults(self.workspaces),
        )
