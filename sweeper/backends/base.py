import os
import shutil
from datetime import datetime
from pathlib import Path

from sweeper import get_db


class BaseBackend():
    name = None

    def __init__(self, main_config, job_config, secrets):
        if not self.name:
            raise Exception("No name defined for backend")
        self.main_config = main_config
        self.config = job_config
        self.secrets = {k: os.getenv(v) for k, v in secrets.items()}
        self.table = get_db()[self.name]
        self.tmp_dir = Path(self.main_config["tmp_dir"]) / self.name
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
        res = self.table.find_one(name=filename, order_by='-created_at', _limit=1)
        if not res:
            return True
        elif sha1sum and sha1sum != res["sha1sum"]:
            return True
        elif size and size != res["size"]:
            return True
        return False

    def store_file_info(self, **kwargs):
        """Store info from file in DB"""
        kwargs["created_at"] = datetime.utcnow()
        self.table.insert(kwargs)
