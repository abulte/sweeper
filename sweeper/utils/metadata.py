from datetime import datetime

from sweeper import get_db


class Metadata():
    id = None
    table = None

    def __init__(self):
        db = get_db()
        self.table = db["metadata"]

    def start(self, job):
        self.id = self.table.insert({
            "started_at": datetime.utcnow(),
            "job": job,
        })

    def end(self, main_error, run_errors):
        error = main_error
        self.table.update({
            "id": self.id,
            "ended_at": datetime.utcnow(),
            "error": error,
            "has_run_errors": False,
        }, ["id"])
