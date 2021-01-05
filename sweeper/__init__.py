import os
import dataset

context = {}


def get_db():
    if "db" not in context:
        context["db"] = dataset.connect(os.getenv("DATABASE_URL", "sqlite:///jobs.db"))
    return context["db"]


def close_db():
    context["db"].close()
