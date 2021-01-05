from datetime import datetime

from .base import BaseBackend


class MetadataBackend(BaseBackend):
    name = "metadata"

    def start(self, job):
        self.id = self.table.insert({
            "started_at": datetime.utcnow(),
            "job": job,
        })

    def end(self, meta_error, run_errors):
        error = None
        if meta_error:
            error = str(meta_error)
        if run_errors:
            error = '' if not error else error
            error += ' | ' + ' | '.join([' - '.join(str(e)) for e in run_errors])
        self.table.update({
            "id": self.id,
            "ended_at": datetime.utcnow(),
            "error": error,
        }, ["id"])
