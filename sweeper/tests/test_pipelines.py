import io

import pytest

from sweeper.pipelines.sirene import SirenePipeline
from sweeper.models import Resource
from sweeper.tests.pipelines.pipeline_test import TestPipeline


class TestGenericBackend():

    def test_backend(self, config, db):
        backend = TestPipeline(0, config)
        resource = Resource(name="dumdum", sha1sum="sha1", size=1)
        backend.register_file(resource)
        # file_has_changed
        assert backend.file_has_changed("dumdum", sha1sum="sha2")
        assert not backend.file_has_changed("dumdum", sha1sum="sha1")
        assert backend.file_has_changed("dumdum", size=2)
        assert not backend.file_has_changed("dumdum", size=1)
        # result in DB
        assert db["test"].count(name="dumdum") == 1
        _run = db["test"].find_one(name="dumdum")
        assert _run["metadata_id"] == 0
        assert _run["name"] == "dumdum"
        assert _run["sha1sum"] == "sha1"
        assert _run["size"] == 1
        # config and secrets
        assert backend.config["foo"] == "bar"
        assert backend.secrets["test_secret"] == "sqlite:///:memory:"


@pytest.fixture
def mock_ssh(mocker):
    m1 = mocker.patch("sweeper.gateways.ssh.SSHGateway.__init__", return_value=None)
    m2 = mocker.patch("sweeper.gateways.ssh.SSHGateway.upload", return_value=None)
    m3 = mocker.patch("sweeper.gateways.ssh.SSHGateway.teardown", return_value=None)
    return [m1, m2, m3]


class TestSireneBackend():

    def test_backend(self, config, requests_mock, mock_ssh, db):
        backend = SirenePipeline(0, config)
        requests_mock.get(
            "https://example.com/list.xml",
            text="""<?xml version="1.0" encoding="UTF-8"?>
                    <ns2:ServiceDepotRetrait xmlns:ns2="http://xml.insee.fr/schema/outils">
                        <Fichiers>
                            <id>monfichier.zip</id>
                            <URI>https://example.com/monfichier.zip</URI>
                        </Fichiers>
                        <Fichiers>
                            <id>ignore-moi_pas-dans-le-mapping.zip</id>
                            <URI>https://example.com/ignoremoi.zip</URI>
                        </Fichiers>
                    </ns2:ServiceDepotRetrait>
                """
        )
        body = io.BytesIO(b"some initial binary data: \x00\x01")
        fmock = requests_mock.get("https://example.com/monfichier.zip", body=body)
        pmock = requests_mock.put(
            "https://www.data.gouv.fr/api/1/datasets/dataset_id/resources/resource_id/",
            json={},
        )
        backend.run()
        for m in mock_ssh:
            assert m.called
        assert fmock.called
        assert pmock.called

        assert db["sirene"].count() == 1
        run = db["sirene"].find_one()
        assert run["name"] == "monfichier.zip"
