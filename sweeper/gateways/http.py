import hashlib
from pathlib import Path
from typing import Tuple, Callable

import requests
from sweeper.utils.progress import ProgressBar
from sweeper.models import Resource


class HTTPDownloadGateway():

    def __init__(self, has_changed: Callable[..., bool], tmp_dir: Path, auth=None, chunk_size=8192):
        self.chunk_size = chunk_size
        self.auth = auth
        self.tmp_dir = tmp_dir
        self.has_changed = has_changed

    def download(self, url: str, file_id: str) -> Tuple[bool, Resource]:
        sha1sum = hashlib.sha1()
        size = 0
        with requests.get(url, stream=True, auth=self.auth) as r:
            if 'content-length' in r.headers:
                size = int(r.headers['content-length'])
                if not self.has_changed(file_id, size=size):
                    return False, Resource(name=file_id, size=size)
            print(f"Downloading {file_id}...")
            r.raise_for_status()
            bar = ProgressBar(
                animation="{stream}" if not size else "{progress}",
                steps=["~", "=", "•"],
                total=size,
                template="|{animation}| {done:B}/{total:B} ({speed:B}/s)",
            )
            ofile_path = self.tmp_dir / file_id
            with open(ofile_path, "wb") as ofile:
                count = 0
                for chunk in r.iter_content(chunk_size=self.chunk_size):
                    sha1sum.update(chunk)
                    ofile.write(chunk)
                    count += 1
                    bar.update(done=count * self.chunk_size)
        has_changed = self.has_changed(file_id, sha1sum=sha1sum.hexdigest())
        return has_changed, Resource(**{
            "file": ofile_path,
            "name": file_id,
            "sha1sum": sha1sum.hexdigest(),
            "size": ofile_path.stat().st_size,
        })
