import os
import shutil
import logging

from datetime import datetime
from pathlib import Path

from sweeper import get_db
from sweeper.models import Resource

log = logging.getLogger(__name__)


class BasePipeline():
    """
    ## Base class for all pipelines.

    ### Init

    The pipeline class will be instantiated for you by `sweeper run` so you don't need
    to bother with that.

    ### Inherit

    `name` should be defined, with a value compatible with a dabase table name.

    When writing your own pipeline, those methods can or must be redefined:

    - `BasePipeline.run` _must_ be defined
    - `BasePipeline.pre_run` _can_ be defined
    - `BasePipeline.post_run` _can_ be defined

    ### Use

    When writing your own pipeline, those methods should be useful:

    - `BasePipeline.register_file`
    - `BasePipeline.register_error`
    - `BasePipeline.file_has_changed`

    """
    name = None

    def __init__(self, metadata_id: int, config: dict):
        if not self.name:
            raise Exception("No name defined for backend")
        self.metadata_id = metadata_id
        self.config = config[self.name].get("config", {})
        self.errors = []
        secrets = config[self.name].get("secrets", {})
        self.secrets = {k: os.getenv(v) for k, v in secrets.items()}
        self.table = get_db()[self.name]
        self.tmp_dir = Path(config["main"]["tmp_dir"]) / self.name
        self.tmp_dir.mkdir(exist_ok=True, parents=True)

    def pre_run(self):
        """
        Executed before `BasePipeline.run`.

        Useful to prepare some stuff before running.
        """
        pass

    def run(self):
        """
        Main logic goes here.

        This *must* be redefined.
        """
        raise NotImplementedError()

    def post_run(self):
        """
        It is called after `BasePipeline.run`.

        Runs even when there's an error, so it can be usefull to clean up stuff.
        """
        pass

    def _teardown(self):
        """Do not override w/o calling super()"""
        shutil.rmtree(self.tmp_dir)

    def file_has_changed(self, filename, sha1sum=None, size=None) -> bool:
        """
        Has file changed vs latest info from DB?

        This should be used by the pipeline to check a file has changed and
        act accordingly (eg keep going or abort).

        It can be checked againt `sha1sum` or `size` (in bytes) values.
        """
        res = self.table.find_one(name=filename, error=None, order_by='-created_at', _limit=1)
        if not res:
            return True
        elif sha1sum and sha1sum != res["sha1sum"]:
            return True
        elif size and size != res["size"]:
            return True
        return False

    def register_file(self, resource: Resource):
        """
        Store info from file (Resource) in DB

        This should be used when a file has been successfully moved around,
        with as much `sweeper.models.Resource` attributes filled up as possible.

        Those info will be used by `BasePipeline.file_has_changed` to check if
        a file has or not since last run.
        """
        resource.metadata_id = self.metadata_id
        resource.created_at = datetime.utcnow()
        data = resource.__dict__
        data.pop("file")
        self.table.insert(data)

    def register_error(self, resource: Resource):
        """
        Register an error for a `sweeper.models.Resource`.

        Useful for logging/debugging purposes.
        """
        assert resource.error is not None
        log.error(f"[error] {resource}")
        self.errors.append(resource)
        self.register_file(resource)
