"""Alembic migration environment.

Reads the DB URL from the DATABASE_URL env var (same source of truth as the
app, via app.config.settings) rather than duplicating it in alembic.ini.
Alembic runs synchronously, so the async `+asyncpg` driver is swapped for
plain `postgresql://` (psycopg2) here.
"""
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.config import settings
from common.models import Base

config = context.config

sync_db_url = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
config.set_main_option("sqlalchemy.url", sync_db_url)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Emit SQL without a live DB connection (`alembic upgrade --sql`)."""
    context.configure(
        url=sync_db_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live DB connection (the normal path)."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
