from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# Import SQLModel metadata
from sqlmodel import SQLModel

# Import all models so they're registered with SQLModel.metadata
from llm_memedescriber.models import (
    Meme,
    Duplicate,
    DuplicateGroup,
    MemeDuplicateGroup,
    BasicAuthUser,
    UserToken,
    FileShareToken,
    AuditLog
)

config = context.config
target_metadata = SQLModel.metadata


def get_url():
    """Get database URL from environment or config."""
    import os
    # Try to get from environment first (for Docker)
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        # Default to the standard location
        database_url = "sqlite:////data/memes.db"
    return database_url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    configuration = config.get_section(config.config_ini_section)
    
    # Use URL from config if set (via db.init_db), otherwise fall back to get_url()
    if config.get_main_option("sqlalchemy.url"):
        configuration["sqlalchemy.url"] = config.get_main_option("sqlalchemy.url")
    else:
        configuration["sqlalchemy.url"] = get_url()
    
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
