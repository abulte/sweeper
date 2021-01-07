import os
import shutil

from datetime import datetime
from pathlib import Path

from sweeper import get_db
from sweeper.models import Resource


class BaseBackend():
    name = None

    def __init__(self, metadata_id, config):
        if not self.name:
            raise Exception("No name defined for backend")
        self.metadata_id = metadata_id
        self.config = config[self.name]
        self.errors = []
        secrets = config.get("secrets", {})
        self.secrets = {k: os.getenv(v) for k, v in secrets.items()}
        self.table = get_db()[self.name]
        self.tmp_dir = Path(config["main"]["tmp_dir"]) / self.name
        self.tmp_dir.mkdir(exist_ok=True, parents=True)

    def pre_run(self):
        """Executed before run()"""
        pass

    def run(self):
        """Main logic"""
        raise NotImplementedError()

    def post_run(self):
        """Executed after run() in finally clause (will always run)"""
        pass

    def _teardown(self):
        """Do not override w/o calling super()"""
        shutil.rmtree(self.tmp_dir)

    def file_has_changed(self, filename, sha1sum=None, size=None):
        """Has file changed vs latest info from DB?"""
        res = self.table.find_one(name=filename, error=None, order_by='-created_at', _limit=1)
        if not res:
            return True
        elif sha1sum and sha1sum != res["sha1sum"]:
            return True
        elif size and size != res["size"]:
            return True
        return False

    def register_file(self, resource: Resource):
        """Store info from file in DB"""
        resource.metadata_id = self.metadata_id
        resource.created_at = datetime.utcnow()
        data = resource.__dict__
        data.pop("file")
        self.table.insert(data)

    def register_error(self, resource: Resource):
        assert resource.error is not None
        print(f"[error] {resource}")
        self.errors.append(resource)
        self.register_file(resource)
