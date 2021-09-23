from . import Pipeline, PipeSegment, PrepareInstance, DownloadResults
from ..runner.local import TempWorkspaceCollection
from .. import util


class RunInstance(PipeSegment):

    def __init__(self, case, workspaces):
        self.case = case
        self.workspaces = workspaces

    @util.with_context('I {instance.index}')
    @util.with_context('Run')
    def apply(self, instance):
        workspace = instance.open_workspace(self.workspaces)
        instance.script.run(workspace.root, workspace.subspace('.grevling'))
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
            PrepareInstance(self.case, self.workspaces),
            RunInstance(self.case, self.workspaces),
            DownloadResults(self.case, self.workspaces),
        )
