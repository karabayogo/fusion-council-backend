"""Alembic environment configuration for fusion-council-backend.

Reads DATABASE_URL or DATABASE_PATH from the environment to determine
the target database. Supports both PostgreSQL (production) and SQLite (tests).

Usage:
    alembic upgrade head      # apply all pending migrations
    alembic downgrade -1      # roll back one migration
    alembic current           # show current revision
    alembic history           # show migration history
"""

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool, text

# Alembic Config object
config = context.config

# Set up logging from alembic.ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Target metadata (None — we use raw SQL migrations, not ORM models)
target_metadata = None


def get_url() -> str:
    """Resolve the database URL from environment variables."""
    db_url = os.environ.get("DATABASE_URL", "")
    if db_url and (db_url.startswith("postgresql") or db_url.startswith("postgres")):
        return db_url
    db_path = os.environ.get("DATABASE_PATH", ":memory:")
    return f"sqlite:///{db_path}"


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emit SQL without connecting).

    Used for generating SQL scripts for review/CI.
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
    """Run migrations against a live database."""
    url = get_url()
    is_sqlite = url.startswith("sqlite")
    connectable = create_engine(
        url,
        poolclass=pool.NullPool,
        **( 
            {"connect_args": {"check_same_thread": False, "timeout": 30}}
            if is_sqlite
            else {}
        ),
    )

    with connectable.connect() as connection:
        # Acquire advisory lock for PostgreSQL to prevent concurrent migrations
        if not is_sqlite:
            connection.execute(
                text("SELECT pg_advisory_lock(hashtext('fusion_council_schema_migrations'))")
            )
            connection.commit()

        try:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                transaction_per_migration=True,
            )
            with context.begin_transaction():
                context.run_migrations()
        finally:
            if not is_sqlite:
                connection.execute(
                    text("SELECT pg_advisory_unlock(hashtext('fusion_council_schema_migrations'))")
                )
                connection.commit()


# ── Dispatch ──
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
