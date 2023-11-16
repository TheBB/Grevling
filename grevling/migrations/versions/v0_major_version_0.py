"""Major version 0

Revision ID: v0
Revises:
Create Date: 2023-11-16 11:30:23.185518

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "v0"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    dbinfo = op.create_table(
        "dbinfo",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.bulk_insert(dbinfo, [{"id": 0, "version": 0}])


def downgrade() -> None:
    op.drop_table("dbinfo")
