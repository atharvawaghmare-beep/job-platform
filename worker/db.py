"""Sync SQLAlchemy session factory for the worker.

One session per message, opened and closed within `process_message` — a
worker process handles one job at a time (see main.py's `prefetch_count=1`),
so there's no need for the pooling/async machinery the API's db.py uses.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import settings

engine = create_engine(settings.sync_database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
