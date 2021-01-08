import hashlib
import io

import pytest

from sweeper.gateways.datagouvfr import DataGouvFrGateway
from sweeper.gateways.http import HTTPDownloadGateway


class TestHTTP():
    test_bytes = b"some initial binary data: \x00\x01"
    sha1sum = hashlib.sha1(test_bytes).hexdigest()

    @pytest.fixture
    def body(self):
        return io.BytesIO(self.test_bytes)

    def has_not_changed(self, filename, sha1sum=None, size=None):
        return False

    def has_changed(self, filename, sha1sum=None, size=None):
        return True

    def test_download_not_changed_no_length(self, tmp_path, requests_mock, mocker, body):
        gw = HTTPDownloadGateway(self.has_not_changed, tmp_path)
        fmock = requests_mock.get("https://example.com/monfichier.zip", body=body)
        spy_changed = mocker.spy(gw, "has_changed")
        changed, resource = gw.download("https://example.com/monfichier.zip", "dumdum")
        assert fmock.called
        assert not changed
        assert resource.file.read_bytes() == self.test_bytes
        assert resource.name == "dumdum"
        assert resource.sha1sum == self.sha1sum
        assert resource.size == resource.file.stat().st_size
        spy_changed.assert_called_once_with("dumdum", sha1sum=self.sha1sum)

    def test_download_changed_no_length(self, tmp_path, requests_mock, mocker, body):
        gw = HTTPDownloadGateway(self.has_changed, tmp_path)
        fmock = requests_mock.get("https://example.com/monfichier.zip", body=body)
        spy_changed = mocker.spy(gw, "has_changed")
        changed, resource = gw.download("https://example.com/monfichier.zip", "dumdum")
        assert fmock.called
        assert changed
        assert resource.file.read_bytes() == self.test_bytes
        assert resource.name == "dumdum"
        assert resource.sha1sum == self.sha1sum
        assert resource.size == resource.file.stat().st_size
        spy_changed.assert_called_once_with("dumdum", sha1sum=self.sha1sum)

    def test_download_changed_w_length(self, tmp_path, requests_mock, mocker, body):
        gw = HTTPDownloadGateway(self.has_changed, tmp_path)
        fmock = requests_mock.get("https://example.com/monfichier.zip", body=body,
                                  headers={"Content-Length": "1"})
        spy_changed = mocker.spy(gw, "has_changed")
        changed, resource = gw.download("https://example.com/monfichier.zip", "dumdum")
        assert fmock.called
        assert changed
        assert resource.file.read_bytes() == self.test_bytes
        assert resource.name == "dumdum"
        assert resource.sha1sum == self.sha1sum
        assert resource.size == resource.file.stat().st_size
        assert spy_changed.call_args_list == [
            (("dumdum", ), {"size": 1}),
            (("dumdum", ), {"sha1sum": self.sha1sum}),
        ]

    def test_download_not_changed_w_length(self, tmp_path, requests_mock, mocker, body):
        gw = HTTPDownloadGateway(self.has_not_changed, tmp_path)
        fmock = requests_mock.get("https://example.com/monfichier.zip", body=body,
                                  headers={"Content-Length": "1"})
        spy_changed = mocker.spy(gw, "has_changed")
        changed, resource = gw.download("https://example.com/monfichier.zip", "dumdum")
        assert fmock.called
        assert not changed
        assert resource.name == "dumdum"
        # file has not been downloaded
        assert resource.file is None
        assert resource.size == 1
        assert resource.sha1sum is None
        spy_changed.assert_called_once_with("dumdum", size=1)


class TestDataGouvFr():

    def test_upload_replace_resource_no_update(self, requests_mock, tmp_path):
        gw = DataGouvFrGateway("TOKEN")
        pmock = requests_mock.post(
            "https://www.data.gouv.fr/api/1/datasets/dataset_id/resources/resource_id/upload/",
            json={"title": "old"},
        )
        tmp_file = tmp_path / "test.csv"
        tmp_file.write_text("file content")
        res = gw.upload_replace_resource("dataset_id", "resource_id", tmp_file)
        assert pmock.called
        r = pmock.last_request
        assert r.headers["x-api-key"] == "TOKEN"
        # no way to access .files in requests-mock
        assert "file content" in r.text
        assert res == {"title": "old"}

    def test_upload_replace_resource_w_update(self, requests_mock, tmp_path):
        gw = DataGouvFrGateway("TOKEN")
        requests_mock.post(
            "https://www.data.gouv.fr/api/1/datasets/dataset_id/resources/resource_id/upload/",
            json={"title": "old"},
        )
        umock = requests_mock.put(
            "https://www.data.gouv.fr/api/1/datasets/dataset_id/resources/resource_id/",
            json={"title": "new"}
        )
        tmp_file = tmp_path / "test.csv"
        tmp_file.write_text("file content")
        res = gw.upload_replace_resource("dataset_id", "resource_id", tmp_file,
                                         title="new", ignoreme="nothing")
        assert umock.called
        r = umock.last_request
        assert r.headers["x-api-key"] == "TOKEN"
        assert r.json() == {"title": "new"}
        assert res == {"title": "new"}

    def test_remote_replace_resource(self, requests_mock):
        gw = DataGouvFrGateway("TOKEN")
        pmock = requests_mock.put(
            "https://www.data.gouv.fr/api/1/datasets/dataset_id/resources/resource_id/",
            json={},
        )
        gw.remote_replace_resource("dataset_id", "resource_id", "https://example.com/dataset",
                                   "title", dummy="dumdum")
        assert pmock.called
        assert pmock.last_request.headers["x-api-key"] == "TOKEN"
        assert pmock.last_request.json() == {
            "title": "title",
            "url": "https://example.com/dataset",
            "dummy": "dumdum",
        }
