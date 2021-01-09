"""
`sweeper` 🧹  move data (files) around

.. include:: ../documentation.md
"""

import os
import dataset

context = {}


def get_db():
    """Singleton for db connection"""
    if "db" not in context:
        context["db"] = dataset.connect(os.getenv("DATABASE_URL", "sqlite:///jobs.db"))
    return context["db"]


def close_db():
    """Close db after usage"""
    if "db" in context:
        context["db"].close()


def clean_db():
    """Delete all tables from DB (used for tests)"""
    db = get_db()
    tables = db.tables
    for table in tables:
        db[table].drop()
