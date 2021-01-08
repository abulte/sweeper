import hashlib
import io

import pytest

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
