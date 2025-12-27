"""
Alembic migrations environment configuration.

Reads database connection from environment variables (same as worker/db.py).
Supports both sync and async migrations.
"""

import os
import sys
from logging.config import fileConfig

from sqlalchemy import create_engine, pool
from alembic import context

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from worker.models import Base

# Alembic Config object
config = context.config

# Setup logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Model metadata for autogenerate
target_metadata = Base.metadata


def get_database_url() -> str:
    """
    Build database URL from environment variables.
    Same pattern as worker/db.py for consistency.
    """
    host = os.getenv("PG_HOST")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASS")
    database = os.getenv("PG_DB")
    port = os.getenv("PG_PORT", "5432")
    sslmode = os.getenv("PG_SSLMODE", "require")
    
    # Validate required vars
    missing = []
    if not host: missing.append("PG_HOST")
    if not user: missing.append("PG_USER")
    if not password: missing.append("PG_PASS")
    if not database: missing.append("PG_DB")
    
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
    
    # Build PostgreSQL URL
    return f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode={sslmode}"


def run_migrations_offline() -> None:
    """
    Run migrations in 'offline' mode.
    
    Generates SQL script without connecting to database.
    Useful for reviewing migrations before applying.
    """
    url = get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """
    Run migrations in 'online' mode.
    
    Connects to database and applies migrations directly.
    """
    connectable = create_engine(
        get_database_url(),
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


# Run appropriate migration mode
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
