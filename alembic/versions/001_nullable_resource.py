"""Make auditlog.resource nullable

Revision ID: 001_nullable_resource
Revises: 
Create Date: 2026-02-08 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '001_nullable_resource'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Make auditlog.resource column nullable.
    
    SQLite doesn't support ALTER COLUMN, so we need to:
    1. Create new table with correct schema
    2. Copy data
    3. Drop old table
    4. Rename new table
    """
    # Check if auditlog table exists first
    conn = op.get_bind()
    result = conn.execute(sa.text(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='auditlog'"
    ))
    
    if not result.fetchone():
        # Table doesn't exist yet, nothing to migrate
        return
    
    # Check if resource column is already nullable
    result = conn.execute(sa.text("PRAGMA table_info(auditlog)"))
    columns = result.fetchall()
    resource_is_not_null = False
    for col in columns:
        if col[1] == 'resource' and col[3] == 1:  # col[3] is notnull flag
            resource_is_not_null = True
            break
    
    if not resource_is_not_null:
        # Already nullable, nothing to do
        return
    
    # Clean up any failed previous migration attempts
    op.execute(sa.text("DROP INDEX IF EXISTS ix_auditlog_user_id"))
    op.execute(sa.text("DROP INDEX IF EXISTS ix_auditlog_action"))
    op.execute(sa.text("DROP INDEX IF EXISTS ix_auditlog_resource"))
    op.execute(sa.text("DROP INDEX IF EXISTS ix_auditlog_timestamp"))
    op.execute(sa.text("DROP TABLE IF EXISTS auditlog_new"))
    
    # Create new table with resource as nullable
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
    
    # Create indexes
    op.execute(sa.text("CREATE INDEX ix_auditlog_user_id ON auditlog_new (user_id)"))
    op.execute(sa.text("CREATE INDEX ix_auditlog_action ON auditlog_new (action)"))
    op.execute(sa.text("CREATE INDEX ix_auditlog_resource ON auditlog_new (resource)"))
    op.execute(sa.text("CREATE INDEX ix_auditlog_timestamp ON auditlog_new (timestamp)"))
    
    # Copy data
    op.execute(sa.text("INSERT INTO auditlog_new SELECT * FROM auditlog"))
    
    # Drop old table
    op.execute(sa.text("DROP TABLE auditlog"))
    
    # Rename new table
    op.execute(sa.text("ALTER TABLE auditlog_new RENAME TO auditlog"))


def downgrade() -> None:
    """Make auditlog.resource NOT NULL again.
    
    Note: This will fail if there are NULL values in the resource column.
    """
    # Create new table with resource as NOT NULL
    op.execute(sa.text("""
        CREATE TABLE auditlog_new (
            id INTEGER PRIMARY KEY,
            user_id VARCHAR NOT NULL,
            action VARCHAR NOT NULL,
            resource VARCHAR NOT NULL,
            resource_type VARCHAR DEFAULT 'meme',
            details VARCHAR,
            timestamp DATETIME NOT NULL,
            ip_address VARCHAR,
            status VARCHAR DEFAULT 'success'
        )
    """))
    
    # Create indexes
    op.execute(sa.text("CREATE INDEX ix_auditlog_user_id ON auditlog_new (user_id)"))
    op.execute(sa.text("CREATE INDEX ix_auditlog_action ON auditlog_new (action)"))
    op.execute(sa.text("CREATE INDEX ix_auditlog_resource ON auditlog_new (resource)"))
    op.execute(sa.text("CREATE INDEX ix_auditlog_timestamp ON auditlog_new (timestamp)"))
    
    # Copy data (will fail if there are NULL values)
    op.execute(sa.text("INSERT INTO auditlog_new SELECT * FROM auditlog"))
    
    # Drop old table
    op.execute(sa.text("DROP TABLE auditlog"))
    
    # Rename new table
    op.execute(sa.text("ALTER TABLE auditlog_new RENAME TO auditlog"))
