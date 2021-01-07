from sweeper.backends.base import BaseBackend
from sweeper.models import Resource


class TestBackend(BaseBackend):
    __test__ = False
    name = "test"

    def run(self):
        resource = Resource(
            name="dumdum",
            sha1sum="sha1sum",
            size=1,
        )
        self.register_file(resource)


class TestBackendError(TestBackend):
    name = "test_error"

    def run(self):
        raise Exception("ERROR")


class TestBackendErrorRunError(TestBackend):
    name = "test_run_error"

    def run(self):
        self.register_error(Resource(name="dumdum", error="ERROR"))
