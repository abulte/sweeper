from datetime import datetime

from .base import BaseBackend


class MetadataBackend(BaseBackend):
    name = "metadata"

    def start(self, job):
        self.id = self.table.insert({
            "started_at": datetime.utcnow(),
            "job": job,
        })

    def end(self, error):
        if error:
            error = str(error)
        self.table.update({
            "id": self.id,
            "ended_at": datetime.utcnow(),
            "error": error,
        }, ["id"])
        self.db.close()
