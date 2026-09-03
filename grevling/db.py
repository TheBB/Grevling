from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON

from . import api, util
from .capture import CaptureCollection

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from sqlalchemy.orm import Session


def engine(path: Path) -> Engine:
    return create_engine(f"sqlite://{path}/grevling.db")


class Base(DeclarativeBase):
    type_annotation_map = {
        api.Context: JSON,
        CaptureCollection: JSON,
    }


class DbInfo(Base):
    __tablename__ = "dbinfo"

    id: Mapped[int] = mapped_column(primary_key=True)
    version: Mapped[int] = mapped_column(default=0)


class Case(Base):
    __tablename__ = "case"

    index: Mapped[int] = mapped_column("id", primary_key=True)
    has_collected: Mapped[bool] = mapped_column(default=False)
    has_plotted: Mapped[bool] = mapped_column(default=False)


class Instance(Base):
    __tablename__ = "instance"

    index: Mapped[int] = mapped_column("id", primary_key=True)
    logdir: Mapped[str]
    context: Mapped[api.Context]
    captured: Mapped[CaptureCollection | None] = mapped_column(default=None)
    status: Mapped[api.Status]


# Real columns on the instance table that must not be shadowed by a generated
# column with the same name.
_RESERVED_INSTANCE_COLUMNS = frozenset({"id", "logdir", "context", "captured", "status"})

# JSON blob columns that generated columns are projected out of, in priority
# order: a downloaded instance has the full picture in `captured`, one that has
# only been created still has its inputs in `context`.
_JSON_SOURCES = ("captured", "context")

# Name of the view that unnests list-valued captures into one row per element.
_LIST_VALUE_VIEW = "instance_list_value"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _json_path(name: str) -> str:
    escaped = name.replace("\\", "\\\\").replace('"', '\\"').replace("'", "''")
    return f"'$.\"{escaped}\"'"


def _generated_expr(name: str) -> str:
    path = _json_path(name)
    parts = [f"json_extract({_quote_ident(src)}, {path})" for src in _JSON_SOURCES]
    return f"coalesce({', '.join(parts)})"


def _existing_columns(session: Session, table: str) -> set[str]:
    # table_xinfo (not table_info) also reports generated / hidden columns, so
    # this stays correct across repeated calls.
    rows = session.execute(text(f"PRAGMA table_xinfo({_quote_ident(table)})")).all()
    return {row[1] for row in rows}


def _discover_json_keys(session: Session, table: str) -> set[str]:
    keys: set[str] = set()
    for src in _JSON_SOURCES:
        # Identifiers are quoted and never user-supplied.
        stmt = text(f"""
            SELECT DISTINCT je.key
            FROM {_quote_ident(table)} AS t, json_each(t.{_quote_ident(src)}) AS je
            WHERE t.{_quote_ident(src)} IS NOT NULL
        """)  # noqa: S608
        try:
            keys.update(k for (k,) in session.execute(stmt) if isinstance(k, str))
        except Exception as err:  # noqa: BLE001
            util.log.warning(f"Could not inspect JSON column {src!r} on {table!r}: {err}")
    return keys


def sync_generated_columns(
    session: Session,
    *,
    table: str = "instance",
    extra_names: Iterable[str] = (),
) -> list[str]:
    """Ensure the instance table has a VIRTUAL generated column for every
    top-level key present in its JSON ``context`` / ``captured`` blobs (plus any
    ``extra_names``), so aggregated data can be queried as plain columns from
    external tools.

    The columns are deliberately untyped: SQLite computes each one on read via
    ``json_extract`` and stores whatever value that yields, per row. Returns the
    names of the columns that were added.
    """
    session.commit()
    have = _existing_columns(session, table)
    want = _discover_json_keys(session, table)
    want.update(name for name in extra_names if isinstance(name, str))
    want -= _RESERVED_INSTANCE_COLUMNS
    want -= have

    added: list[str] = []
    for name in sorted(want):
        ddl = f"""
            ALTER TABLE {_quote_ident(table)} ADD COLUMN {_quote_ident(name)}
            GENERATED ALWAYS AS ({_generated_expr(name)}) VIRTUAL
        """
        try:
            session.execute(text(ddl))
            added.append(name)
        except Exception as err:  # noqa: BLE001
            util.log.warning(f"Could not add generated column {name!r}: {err}")
    session.commit()
    if added:
        util.log.debug(f"Added generated columns to {table!r}: {', '.join(added)}")

    ensure_list_value_view(session, table=table)
    return added


def ensure_list_value_view(
    session: Session,
    *,
    table: str = "instance",
    view: str = _LIST_VALUE_VIEW,
) -> None:
    """(Re)create a view that unnests every top-level JSON array in the
    ``context`` / ``captured`` blobs into one row per element, so list-valued
    captures can be aggregated with ordinary SQL:

        SELECT instance_id, max(value), avg(value), count(*)
        FROM instance_list_value
        WHERE name = 'residual'
        GROUP BY instance_id

    ``idx`` preserves element order; ``value`` keeps each element's JSON type.
    The view is recomputed on read, so it never drifts from the blobs.
    """
    src = ", ".join(f"t.{_quote_ident(s)}" for s in _JSON_SOURCES)
    # Identifiers only, never user-supplied.
    body = f"""
        SELECT
            t.id AS instance_id,
            blob.key AS name,
            elem.key AS idx,
            elem.value AS value
        FROM {_quote_ident(table)} AS t
        JOIN json_each(coalesce({src})) AS blob
        JOIN json_each(CASE WHEN blob.type = 'array' THEN blob.value ELSE '[]' END) AS elem
        WHERE blob.type = 'array'
    """  # noqa: S608
    try:
        session.execute(text(f"DROP VIEW IF EXISTS {_quote_ident(view)}"))
        session.execute(text(f"CREATE VIEW {_quote_ident(view)} AS {body}"))
        session.commit()
    except Exception as err:  # noqa: BLE001
        util.log.warning(f"Could not (re)create view {view!r}: {err}")
