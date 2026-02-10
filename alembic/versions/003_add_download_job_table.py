"""add_download_job_table

Revision ID: 003_add_download_job_table
Revises: 002_add_username
Create Date: 2026-02-09 20:22:40.857506

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = '003_add_download_job_table'
down_revision: Union[str, None] = '002_add_username'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create downloadjob table only
    op.create_table(
        'downloadjob',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('url', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('user_id', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('status', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('progress_percent', sa.Float(), nullable=False),
        sa.Column('filename', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('error_message', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('started_at', sa.DateTime(), nullable=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('video_title', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('video_duration', sa.Integer(), nullable=True),
        sa.Column('file_size_bytes', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_downloadjob_created_at'), 'downloadjob', ['created_at'], unique=False)
    op.create_index(op.f('ix_downloadjob_status'), 'downloadjob', ['status'], unique=False)
    op.create_index(op.f('ix_downloadjob_url'), 'downloadjob', ['url'], unique=False)
    op.create_index(op.f('ix_downloadjob_user_id'), 'downloadjob', ['user_id'], unique=False)


def downgrade() -> None:
    # Drop downloadjob table only
    op.drop_index(op.f('ix_downloadjob_user_id'), table_name='downloadjob')
    op.drop_index(op.f('ix_downloadjob_url'), table_name='downloadjob')
    op.drop_index(op.f('ix_downloadjob_status'), table_name='downloadjob')
    op.drop_index(op.f('ix_downloadjob_created_at'), table_name='downloadjob')
    op.drop_table('downloadjob')
