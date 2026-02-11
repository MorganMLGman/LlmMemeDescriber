"""add_token_lookup_hash

Revision ID: 004_add_token_lookup_hash
Revises: 003_add_download_job_table
Create Date: 2026-02-10 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = '004_add_token_lookup_hash'
down_revision: Union[str, None] = '003_add_download_job_table'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add token_lookup_hash column to usertoken table (nullable first, will be populated then made non-nullable)
    op.add_column('usertoken', sa.Column('token_lookup_hash', sqlmodel.sql.sqltypes.AutoString(), nullable=True))
    
    # Create index on token_lookup_hash for fast lookups
    op.create_index(op.f('ix_usertoken_token_lookup_hash'), 'usertoken', ['token_lookup_hash'], unique=False)
    
    # Note: Existing tokens will have NULL token_lookup_hash. They will be populated on next use.
    # New tokens will have token_lookup_hash set at creation time.


def downgrade() -> None:
    # Drop index and column
    op.drop_index(op.f('ix_usertoken_token_lookup_hash'), table_name='usertoken')
    op.drop_column('usertoken', 'token_lookup_hash')
