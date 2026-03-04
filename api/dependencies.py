"""
api/dependencies.py

FastAPI dependency providers.

The registry is a single cached instance per process — one SQLAlchemy engine,
one connection pool. All routes share it via Depends(get_registry).
"""

import os
from functools import lru_cache

from core.registry import MetadataRegistry


@lru_cache(maxsize=1)
def get_registry() -> MetadataRegistry:
    """
    Return the process-wide MetadataRegistry instance.
    Configured via DATABASE_URL env var (default: SQLite in working directory).
    """
    db_url = os.environ.get("DATABASE_URL", "sqlite:///./metadata.db")
    return MetadataRegistry(db_url)
