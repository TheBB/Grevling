"""Major version 1

Revision ID: v1
Revises: v0
Create Date: 2023-11-16 22:48:14.270179

"""
from __future__ import annotations

from typing import TYPE_CHECKING, Union

import sqlalchemy as sa
from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "v1"
down_revision: Union[str, None] = "v0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "instance",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("logdir", sa.String(), nullable=False),
        sa.Column("context", sa.JSON(), nullable=False),
        sa.Column("captured", sa.JSON(), nullable=True),
        sa.Column(
            "status",
            sa.Enum("Created", "Prepared", "Started", "Finished", "Downloaded", name="status"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("instance")
    # ### end Alembic commands ###
