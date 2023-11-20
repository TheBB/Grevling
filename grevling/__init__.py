from __future__ import annotations

import json
import os
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Callable, Iterable, Iterator, List, Optional

import pandas as pd  # type: ignore
import sqlalchemy as sql
from alembic import command as alembic_command
from alembic.config import Config as AlembicCfg
from fasteners import InterProcessLock  # type: ignore
from sqlalchemy.orm import Session

from grevling.typing import TypeManager

from . import api, db, util
from .api import Status
from .capture import CaptureCollection
from .context import ContextProvider
from .filemap import FileMap, FileMapTemplate
from .parameters import ParameterSpace
from .plotting import Plot
from .schema import CaseSchema, PluginSchema, load
from .script import Script, ScriptTemplate
from .workflow.local import LocalWorkflow, LocalWorkspace, LocalWorkspaceCollection

__version__ = "3.0.0a2"

Migrator = Callable[["Case"], None]


def migrator_v1(case: Case) -> None:
    def instances() -> Iterator[db.Instance]:
        statepath = case.storagepath / "state.json"
        if statepath.exists():
            with open(statepath, "r") as f:
                state = json.load(f)
                has_captured = state["has_captured"]
        else:
            has_captured = False
        for name in case.storage_spaces.workspace_names():
            local = case.storage_spaces.open_workspace(name)
            if not local.exists(".grevling/status.txt"):
                continue
            book = local.subspace(".grevling")
            with book.open_str("context.json", "r") as f:
                context = api.Context(json.load(f))
            with book.open_str("status.txt", "r") as f:
                status = Status(f.read())
            if has_captured:
                captured = CaptureCollection(case.types)
                captured.collect_from_cache(book)
            else:
                captured = None
            db_instance = db.Instance(
                index=context["g_index"],
                logdir=context["g_logdir"],
                context=context,
                captured=captured,
                status=status,
            )
            yield db_instance

    case.session.add_all(instances())
    case.session.commit()


def migrator_v2(case: Case) -> None:
    statepath = case.storagepath / "state.json"
    if not statepath.exists():
        db_case = db.Case(index=0)
    else:
        with open(statepath, "r") as f:
            state = json.load(f)
        db_case = db.Case(
            index=0,
            has_collected=state["has_collected"],
            has_plotted=state["has_plotted"],
        )

    case.session.add(db_case)
    case.session.commit()


DB_MAJOR_VERSION = 2
MIGRATORS: dict[int, Migrator] = {1: migrator_v1, 2: migrator_v2}


def load_plugin(case: Case, spec: PluginSchema) -> api.Plugin:
    module = import_module(spec.name)
    return module.Plugin(case, spec.settings)


class Case:
    lock: InterProcessLock
    engine: sql.Engine
    session: Session
    dbc: db.Case

    # Configs may be provided in pure data, in which case they don't correspond to a file
    configpath: Optional[Path]

    # Raw structured data used to initialize this case
    schema: CaseSchema

    sourcepath: Path
    storagepath: Path
    dataframepath: Path
    dbpath: Path

    context_mgr: ContextProvider

    premap: FileMapTemplate
    postmap: FileMapTemplate
    script: ScriptTemplate
    plots: List[Plot]

    _ignore_missing: bool

    types: TypeManager

    plugins: list[api.Plugin]

    def __init__(
        self,
        localpath: api.PathStr = ".",
        storagepath: Optional[Path] = None,
        casedata: Optional[CaseSchema] = None,
    ):
        configpath: Optional[Path] = None

        if isinstance(localpath, str):
            localpath = Path(localpath)
        if localpath.is_file():
            configpath = localpath
            localpath = configpath.parent
        elif localpath.is_dir() and casedata is None:
            for candidate in ["grevling.gold", "grevling.yaml", "badger.yaml"]:
                if (localpath / candidate).exists():
                    configpath = localpath / candidate
                    break
        self.configpath = configpath

        self.sourcepath = localpath
        self.local_space = LocalWorkspace(self.sourcepath, "SRC")

        if casedata is None:
            if configpath is None:
                raise ValueError("Could not find a valid grevling configuration")
            if configpath is not None and not configpath.is_file():
                raise FileNotFoundError("Found a grevling configuration, but it's not a file")
            casedata = load(configpath)

        # Load plugins first
        self.plugins = [load_plugin(self, plugin) for plugin in casedata.plugins]

        if storagepath is None:
            storagepath = self.sourcepath / casedata.settings.storagedir
        assert storagepath is not None
        storagepath.mkdir(parents=True, exist_ok=True)
        self.storagepath = storagepath
        self.storage_spaces = LocalWorkspaceCollection(self.storagepath)

        self.dataframepath = storagepath / "dataframe.parquet"
        self.dbpath = storagepath / "grevling.db"

        assert isinstance(casedata, CaseSchema)
        self.schema = casedata
        self.context_mgr = ContextProvider.from_schema(casedata)

        # Read file mappings
        self.premap = FileMapTemplate(casedata.prefiles)
        self.postmap = FileMapTemplate(casedata.postfiles)

        # Read commands
        self.script = ScriptTemplate(casedata.script)

        # Read types
        self.types = TypeManager()
        self.types.fill_obj(self.context_mgr.parameters)
        self.types.fill_string(casedata.types)

        # Read settings
        settings = casedata.settings
        self._logdir = settings.logdir
        self._ignore_missing = settings.ignore_missing_files

        # Construct plot objects
        self.plots = [Plot.from_schema(schema, self.parameters) for schema in casedata.plots]

    @property
    def is_running(self) -> bool:
        return self.session.query(
            sql.select(db.Instance)
            .where(db.Instance.status.in_((api.Status.Started, api.Status.Prepared)))
            .exists()
        ).scalar()

    @property
    def has_data(self) -> bool:
        return self.session.query(
            sql.select(db.Instance).where(db.Instance.status == api.Status.Downloaded).exists()
        ).scalar()

    @property
    def has_captured(self) -> bool:
        return not self.session.query(
            sql.select(db.Instance).where(db.Instance.captured.is_(None)).exists()
        ).scalar()

    @property
    def has_collected(self) -> bool:
        return self.dbc.has_collected

    @has_collected.setter
    def has_collected(self, value: bool) -> None:
        self.dbc.has_collected = value

    @property
    def has_plotted(self) -> bool:
        return self.dbc.has_plotted

    @has_plotted.setter
    def has_plotted(self, value: bool) -> None:
        self.dbc.has_plotted = value

    def acquire_lock(self):
        self.lock = InterProcessLock(self.storagepath / "lockfile").__enter__()

    def release_lock(self, *args, **kwargs):
        assert self.lock
        self.lock.__exit__(*args, **kwargs)
        del self.lock

    def __enter__(self) -> Case:
        self.acquire_lock()
        self.engine = sql.create_engine(f"sqlite:///{self.dbpath}")
        self.session = Session(self.engine).__enter__()
        self.auto_migrate()

        dbc = self.session.scalar(sql.select(db.Case))
        assert dbc
        self.dbc = dbc

        return self

    def __exit__(self, *args, **kwargs):
        with open(self.storagepath / "state.json", "w") as f:
            json.dump(
                {
                    "running": self.is_running,
                    "has_data": self.has_data,
                    "has_captured": self.has_captured,
                    "has_collected": self.has_collected,
                    "has_plotted": self.has_plotted,
                },
                f,
            )
        self.session.commit()
        del self.session
        self.engine.dispose()
        del self.engine
        self.release_lock(*args, **kwargs)

    def auto_migrate(self) -> None:
        inspector = sql.inspect(self.engine)
        assert inspector is not None
        if not inspector.has_table("dbinfo"):
            current_version = -1
        else:
            db_info = self.session.scalar(sql.select(db.DbInfo))
            assert db_info is not None
            current_version = db_info.version

        migrations_path = Path(__file__).parent / "migrations"
        config = AlembicCfg()
        config.set_main_option("script_location", str(migrations_path))
        config.set_main_option("sqlalchemy.url", f"sqlite:///{self.dbpath}")
        # config.set_section_option("")

        if current_version >= DB_MAJOR_VERSION:
            self.migrate_to_head(config)
            return

        for version in range(current_version + 1, DB_MAJOR_VERSION + 1):
            self.migrate_to_major(config, version)
        self.migrate_to_head(config)

    def migrate_to_major(self, config: AlembicCfg, version: int) -> None:
        alembic_command.upgrade(config, f"v{version}")
        migrator = MIGRATORS.get(version)
        if migrator:
            migrator(self)
        db_info = self.session.scalar(sql.select(db.DbInfo))
        assert db_info is not None
        db_info.version = version
        self.session.commit()

    def migrate_to_head(self, config: AlembicCfg) -> None:
        alembic_command.upgrade(config, "head")

    # @contextmanager
    # def session(self) -> Iterator[Session]:
    #     assert self.engine is not None
    #     with Session(self.engine) as session:
    #         yield session

    def instance_by_index(self, index: int) -> Instance:
        db_instance = self.session.scalar(sql.select(db.Instance).where(db.Instance.index == index))
        assert db_instance is not None
        return Instance(self, db_instance)

    @property
    def parameters(self) -> ParameterSpace:
        return self.context_mgr.parameters

    def clear_cache(self):
        for instance in self.instances():
            instance.destroy()
        self.has_collected = False
        self.has_plotted = False
        self.dataframepath.unlink(missing_ok=True)

    def clear_dataframe(self):
        self.dataframepath.unlink(missing_ok=True)
        self.has_collected = False

    def load_dataframe(self) -> pd.DataFram:
        if self.has_collected:
            return pd.read_parquet(self.dataframepath, engine="pyarrow")
        types = self.type_guess()
        data = {k: pd.Series([], dtype=v) for k, v in types.pandas().items() if k != "g_index"}
        return pd.DataFrame(index=pd.Index([], dtype=int), data=data)

    def save_dataframe(self, df: pd.DataFrame):
        df.to_parquet(self.dataframepath, engine="pyarrow", index=True)

    def type_guess(self) -> TypeManager:
        manager = TypeManager()
        for instance in self.instances(Status.Downloaded):
            manager.merge(instance.cached_capture(raw=True))
        return manager

    def create_instances(self) -> Iterable[Instance]:
        base_ctx = {"g_sourcedir": os.getcwd()}
        for i, ctx in enumerate(self.context_mgr.fullspace(context=base_ctx)):
            ctx["g_logdir"] = self._logdir(ctx)

            # TODO: Think more about this
            db_instance = self.session.scalar(
                sql.select(db.Instance).where(db.Instance.index == ctx["g_index"])
            )
            if db_instance is not None:
                Instance(self, db_instance).destroy()

            db_instance = db.Instance(
                index=ctx["g_index"],
                logdir=ctx["g_logdir"],
                context=ctx,
                captured=None,
                status=Status.Created,
            )
            self.session.add(db_instance)
            yield Instance(self, db_instance)

    def create_instance(
        self,
        ctx: api.Context,
        logdir: Optional[Path] = None,
        index: Optional[int] = None,
    ) -> Instance:
        if index is None:
            index = 0
        sourcedir = os.getcwd()
        ctx = self.context_mgr.evaluate_context(
            {
                **ctx,
                "g_index": index,
                "g_sourcedir": sourcedir,
            }
        )
        if logdir is None:
            logdir = Path(self._logdir(ctx))
        ctx["g_logdir"] = str(logdir)
        workspace = LocalWorkspace(Path(ctx["g_logdir"]), name="LOG")
        db_instance = db.Instance(
            index=ctx["g_index"],
            logdir=ctx["g_logdix"],
            context=ctx,
            captured=None,
            status=Status.Created,
        )
        self.session.add(db_instance)
        return Instance(self, db_instance, local=workspace)

    def instances(self, *statuses: api.Status) -> Iterator[Instance]:
        select = sql.select(db.Instance)
        if statuses:
            select = select.where(db.Instance.status.in_(statuses))
        for db_instance in self.session.scalars(select):
            yield Instance(self, db_instance)

    def capture(self):
        for instance in self.instances(Status.Downloaded):
            instance.capture()

    def collect(self):
        data = self.load_dataframe()
        for instance in self.instances(Status.Downloaded):
            collector = instance.cached_capture()
            data = collector.commit_to_dataframe(data)
        data = data.sort_index()
        self.save_dataframe(data)
        self.has_collected = True

    def plot(self):
        for plot in self.plots:
            plot.generate_all(self)
        self.has_plotted = True

    def run(self, nprocs=1) -> bool:
        nprocs = nprocs or 1
        with LocalWorkflow(nprocs=nprocs) as workflow:
            return workflow.pipeline(self).run(self.create_instances())

    def run_single(self, namespace: api.Context, logdir: Path, index: int = 0):
        instance = self.create_instance(namespace, logdir=logdir, index=index)
        with LocalWorkflow() as workflow:
            workflow.pipeline(self).run([instance])


class Instance:
    local: api.Workspace
    local_book: api.Workspace

    dbo: db.Instance

    remote: Optional[api.Workspace]
    remote_book: Optional[api.Workspace]

    _case: Case

    def __init__(
        self,
        case: Case,
        dbo: db.Instance,
        /,
        local: Optional[api.Workspace] = None,
    ):
        self._case = case
        self.dbo = dbo

        if local is None:
            self.local = self.open_workspace(case.storage_spaces)
        else:
            self.local = local

        self.local_book = self.local.subspace(".grevling")
        self.remote = self.remote_book = None

    @property
    def status(self) -> api.Status:
        return self.dbo.status

    @status.setter
    def status(self, value: api.Status):
        self.dbo.status = value
        with self.local_book.open_str("status.txt", "w") as f:
            f.write(value.value)

    @property
    def context(self) -> api.Context:
        return api.Context(self.dbo.context)

    @property
    def logdir(self) -> str:
        return self.dbo.logdir

    @property
    def types(self) -> TypeManager:
        return self._case.types

    @contextmanager
    def bind_remote(self, spaces: api.WorkspaceCollection):
        self.remote = self.open_workspace(spaces, "WRK")
        self.remote_book = self.remote.subspace(".grevling")
        try:
            yield
        finally:
            self.remote = self.remote_book = None

    def clean(self) -> None:
        self.local.destroy()
        self.status = Status.Created
        self.dbo.captured = None
        self.commit()

    def destroy(self) -> None:
        self.local.destroy()
        self._case.session.delete(self.dbo)
        self._case.session.commit()

    def commit(self) -> None:
        self._case.session.commit()

    @property
    def index(self) -> int:
        return self.dbo.index

    @property
    def script(self) -> Script:
        return self._case.script.render(self.context)

    def write_context(self):
        with self.local_book.open_str("context.json", "w") as f:
            f.write(self.context.json(sort_keys=True, indent=4))

    def open_workspace(self, workspaces, name="") -> api.Workspace:
        return workspaces.open_workspace(self.logdir, name)

    def prepare(self):
        assert self.remote
        assert self.status == Status.Created

        src = self._case.local_space
        util.log.debug(f"Using SRC='{src}', WRK='{self.remote}'")

        premap = self._case.premap.render(self.context)
        premap.copy(self.context, src, self.remote, ignore_missing=self._case._ignore_missing)

        self.status = Status.Prepared
        self.commit()

    def download(self):
        assert self.remote
        assert self.remote_book
        assert self.status == Status.Finished

        collector = CaptureCollection(self.types)
        collector.update(self.context)

        bookmap = FileMap.everything()
        bookmap.copy(self.context, self.remote_book, self.local_book)
        collector.collect_from_info(self.local_book)

        ignore_missing = self._case._ignore_missing or not collector["g_success"]
        postmap = self._case.postmap.render(self.context)
        postmap.copy(self.context, self.remote, self.local, ignore_missing=ignore_missing)

        self._case.script.render(self.context).capture(collector, self.local_book)
        collector.commit_to_file(self.local_book)

        self.status = Status.Downloaded
        self._case.has_collected = False
        self._case.has_plotted = False

        self.dbo.captured = collector
        self.commit()

    def capture(self):
        assert self.status == Status.Downloaded
        collector = CaptureCollection(self.types)
        collector.update(self.context)
        collector.collect_from_info(self.local_book)
        self._case.script.render(self.context).capture(collector, self.local_book)
        collector.commit_to_file(self.local_book)

        self.dbo.captured = collector
        self.commit()

    def cached_capture(self, raw: bool = False) -> CaptureCollection:
        assert self.dbo.captured is not None
        collector = CaptureCollection(self.types)
        collector.update(self.dbo.captured)
        return collector
