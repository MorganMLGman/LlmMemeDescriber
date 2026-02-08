"""Initial database schema

Revision ID: 000_initial_schema
Revises: 
Create Date: 2026-02-07 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '000_initial_schema'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create initial database schema."""
    # Check if meme table already exists (skip if database already initialized)
    conn = op.get_bind()
    result = conn.execute(sa.text(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='meme'"
    ))
    
    if result.fetchone():
        # Tables already exist, skip creation
        return
    
    # Create meme table
    op.execute(sa.text("""
        CREATE TABLE meme (
            id INTEGER PRIMARY KEY,
            filename VARCHAR NOT NULL UNIQUE,
            category VARCHAR,
            description VARCHAR,
            keywords VARCHAR,
            text_in_image VARCHAR,
            source_url VARCHAR,
            status VARCHAR DEFAULT 'pending' NOT NULL,
            attempts INTEGER DEFAULT 0 NOT NULL,
            last_error VARCHAR,
            last_attempt_at DATETIME,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            phash VARCHAR,
            is_false_positive BOOLEAN DEFAULT 0 NOT NULL
        )
    """))
    
    # Create indexes for meme table
    op.execute(sa.text("CREATE INDEX ix_meme_filename ON meme (filename)"))
    op.execute(sa.text("CREATE INDEX ix_meme_status ON meme (status)"))
    op.execute(sa.text("CREATE INDEX ix_meme_created_at ON meme (created_at)"))
    op.execute(sa.text("CREATE INDEX ix_meme_phash ON meme (phash)"))
    
    # Create duplicate table
    op.execute(sa.text("""
        CREATE TABLE duplicate (
            id INTEGER PRIMARY KEY,
            filename_a VARCHAR NOT NULL,
            filename_b VARCHAR NOT NULL,
            is_false_positive BOOLEAN DEFAULT 0 NOT NULL,
            created_at DATETIME NOT NULL
        )
    """))
    
    # Create indexes for duplicate table
    op.execute(sa.text("CREATE INDEX ix_duplicate_filename_a ON duplicate (filename_a)"))
    op.execute(sa.text("CREATE INDEX ix_duplicate_filename_b ON duplicate (filename_b)"))
    op.execute(sa.text("CREATE INDEX ix_duplicate_is_false_positive ON duplicate (is_false_positive)"))
    
    # Create duplicategroup table
    op.execute(sa.text("""
        CREATE TABLE duplicategroup (
            id INTEGER PRIMARY KEY,
            created_at DATETIME NOT NULL
        )
    """))
    
    # Create memeduplicategroup table
    op.execute(sa.text("""
        CREATE TABLE memeduplicategroup (
            id INTEGER PRIMARY KEY,
            group_id INTEGER NOT NULL,
            filename VARCHAR NOT NULL,
            created_at DATETIME NOT NULL
        )
    """))
    
    # Create indexes for memeduplicategroup table
    op.execute(sa.text("CREATE INDEX ix_memeduplicategroup_group_id ON memeduplicategroup (group_id)"))
    op.execute(sa.text("CREATE INDEX ix_memeduplicategroup_filename ON memeduplicategroup (filename)"))
    
    # Create usertoken table
    op.execute(sa.text("""
        CREATE TABLE usertoken (
            id INTEGER PRIMARY KEY,
            user_id VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            token_hash VARCHAR NOT NULL,
            created_at DATETIME NOT NULL,
            last_used_at DATETIME,
            expires_at DATETIME,
            revoked BOOLEAN DEFAULT 0 NOT NULL
        )
    """))
    
    # Create indexes for usertoken table
    op.execute(sa.text("CREATE INDEX ix_usertoken_user_id ON usertoken (user_id)"))
    op.execute(sa.text("CREATE INDEX ix_usertoken_created_at ON usertoken (created_at)"))
    op.execute(sa.text("CREATE INDEX ix_usertoken_revoked ON usertoken (revoked)"))
    
    # Create basicauthuser table
    op.execute(sa.text("""
        CREATE TABLE basicauthuser (
            id INTEGER PRIMARY KEY,
            username VARCHAR NOT NULL UNIQUE,
            password_hash VARCHAR NOT NULL,
            enabled BOOLEAN DEFAULT 1 NOT NULL,
            created_at DATETIME NOT NULL,
            last_used_at DATETIME,
            failed_attempts INTEGER DEFAULT 0 NOT NULL,
            locked_until DATETIME
        )
    """))
    
    # Create indexes for basicauthuser table
    op.execute(sa.text("CREATE INDEX ix_basicauthuser_username ON basicauthuser (username)"))
    op.execute(sa.text("CREATE INDEX ix_basicauthuser_enabled ON basicauthuser (enabled)"))
    
    # Create filesharetoken table
    op.execute(sa.text("""
        CREATE TABLE filesharetoken (
            id INTEGER PRIMARY KEY,
            filename VARCHAR NOT NULL,
            token_hash VARCHAR NOT NULL,
            created_by VARCHAR NOT NULL,
            created_at DATETIME NOT NULL,
            expires_at DATETIME NOT NULL,
            used_count INTEGER DEFAULT 0 NOT NULL
        )
    """))
    
    # Create indexes for filesharetoken table
    op.execute(sa.text("CREATE INDEX ix_filesharetoken_filename ON filesharetoken (filename)"))
    op.execute(sa.text("CREATE INDEX ix_filesharetoken_created_by ON filesharetoken (created_by)"))
    op.execute(sa.text("CREATE INDEX ix_filesharetoken_expires_at ON filesharetoken (expires_at)"))
    
    # Create auditlog table
    op.execute(sa.text("""
        CREATE TABLE auditlog (
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
    
    # Create indexes for auditlog table
    op.execute(sa.text("CREATE INDEX ix_auditlog_user_id ON auditlog (user_id)"))
    op.execute(sa.text("CREATE INDEX ix_auditlog_action ON auditlog (action)"))
    op.execute(sa.text("CREATE INDEX ix_auditlog_resource ON auditlog (resource)"))
    op.execute(sa.text("CREATE INDEX ix_auditlog_timestamp ON auditlog (timestamp)"))


def downgrade() -> None:
    """Drop all tables."""
    op.execute(sa.text("DROP TABLE IF EXISTS auditlog"))
    op.execute(sa.text("DROP TABLE IF EXISTS filesharetoken"))
    op.execute(sa.text("DROP TABLE IF EXISTS basicauthuser"))
    op.execute(sa.text("DROP TABLE IF EXISTS usertoken"))
    op.execute(sa.text("DROP TABLE IF EXISTS memeduplicategroup"))
    op.execute(sa.text("DROP TABLE IF EXISTS duplicategroup"))
    op.execute(sa.text("DROP TABLE IF EXISTS duplicate"))
    op.execute(sa.text("DROP TABLE IF EXISTS meme"))
