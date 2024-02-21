from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON

from . import api
from .capture import CaptureCollection

if TYPE_CHECKING:
    from pathlib import Path


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
    captured: Mapped[Optional[CaptureCollection]] = mapped_column(default=None)
    status: Mapped[api.Status]
