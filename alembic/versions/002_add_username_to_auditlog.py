"""Add username column to auditlog

Revision ID: 002_add_username
Revises: 001_nullable_resource
Create Date: 2026-02-08 01:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '002_add_username'
down_revision: Union[str, None] = '001_nullable_resource'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add username column to auditlog table."""
    # Add username column
    op.execute(sa.text("ALTER TABLE auditlog ADD COLUMN username VARCHAR"))
    
    # Create index on username
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_auditlog_username ON auditlog (username)"))


def downgrade() -> None:
    """Remove username column from auditlog table."""
    # SQLite requires table recreation to drop a column
    op.execute(sa.text("""
        CREATE TABLE auditlog_new (
            id INTEGER PRIMARY KEY,
            user_id VARCHAR NOT NULL,
            action VARCHAR NOT NULL,
            resource VARCHAR,
            resource_type VARCHAR DEFAULT 'meme',
            details VARCHAR,
            timestamp DATETIME NOT NULL,
            ip_address VARCHAR,
            status VARCHAR DEFAULT 'success'
        )
    """))
    
    # Create indexes (without username)
    op.execute(sa.text("CREATE INDEX ix_auditlog_user_id ON auditlog_new (user_id)"))
    op.execute(sa.text("CREATE INDEX ix_auditlog_action ON auditlog_new (action)"))
    op.execute(sa.text("CREATE INDEX ix_auditlog_resource ON auditlog_new (resource)"))
    op.execute(sa.text("CREATE INDEX ix_auditlog_timestamp ON auditlog_new (timestamp)"))
    
    # Copy data (excluding username column)
    op.execute(sa.text("""
        INSERT INTO auditlog_new (id, user_id, action, resource, resource_type, details, timestamp, ip_address, status)
        SELECT id, user_id, action, resource, resource_type, details, timestamp, ip_address, status
        FROM auditlog
    """))
    
    # Drop old table
    op.execute(sa.text("DROP TABLE auditlog"))
    
    # Rename new table
    op.execute(sa.text("ALTER TABLE auditlog_new RENAME TO auditlog"))
