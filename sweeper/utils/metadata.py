from datetime import datetime

from sweeper import get_db


class Metadata():

    def __init__(self):
        db = get_db()
        self.table = db["metadata"]

    def start(self, job):
        self.id = self.table.insert({
            "started_at": datetime.utcnow(),
            "job": job,
        })

    def end(self, main_error, run_errors):
        error = None
        if main_error:
            error = str(main_error)
        if run_errors:
            error = '' if not error else error
            error += ' | ' + ' | '.join([' - '.join(str(e)) for e in run_errors])
        self.table.update({
            "id": self.id,
            "ended_at": datetime.utcnow(),
            "error": error,
        }, ["id"])
