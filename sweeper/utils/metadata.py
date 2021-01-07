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
        error = str(main_error) if main_error else None
        self.table.update({
            "id": self.id,
            "ended_at": datetime.utcnow(),
            "error": error,
            "has_run_errors": len(run_errors) > 0,
        }, ["id"])
