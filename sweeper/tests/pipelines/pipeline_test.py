from sweeper.pipelines.base import BasePipeline
from sweeper.models import Resource


class TestPipeline(BasePipeline):
    __test__ = False
    name = "test"

    def run(self):
        resource = Resource(
            name="dumdum",
            sha1sum="sha1sum",
            size=1,
        )
        self.register_file(resource)


class TestPipelineError(TestPipeline):
    name = "test_error"

    def run(self):
        raise Exception("ERROR")


class TestPipelineErrorRunError(TestPipeline):
    name = "test_run_error"

    def run(self):
        self.register_error(Resource(name="dumdum", error="ERROR"))
