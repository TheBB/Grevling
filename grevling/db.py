from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def engine(path: Path) -> Engine:
    return create_engine(f"sqlite://{path}/grevling.db")


class Base(DeclarativeBase):
    pass


class DbInfo(Base):
    __tablename__ = "dbinfo"

    id: Mapped[int] = mapped_column(primary_key=True)
    version: Mapped[int] = mapped_column(default=0)


# class Instance(Base):
#     __tablename__ = 'instance'

#     id: Mapped[int] = mapped_column(primary_key=True)
#     logdir: Mapped[str]
