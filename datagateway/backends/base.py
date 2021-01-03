import os
import shutil
from pathlib import Path

import dataset

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///jobs.db")


class BaseBackend():
    name = None

    def __init__(self, main_config, job_config, secrets):
        self.main_config = main_config
        self.config = job_config
        self.secrets = {k: os.getenv(v) for k, v in secrets.items()}
        self.db = dataset.connect(DATABASE_URL)
        if not self.name:
            raise Exception("No name defined for backend")
        self.table = self.db[self.name]
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
        self.db.close()
        shutil.rmtree(self.tmp_dir)
