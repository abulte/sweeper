import hashlib

import requests
from progressist import ProgressBar


class HTTPDownloadGateway():

    def __init__(self, job_table, tmp_dir, auth=None, chunk_size=8192):
        self.chunk_size = chunk_size
        self.auth = auth
        self.tmp_dir = tmp_dir
        self.table = job_table

    def has_changed(self, filename, sha1sum):
        res = self.table.find_one(name=filename, order_by='-created_at', _limit=1)
        if not res or sha1sum != res["sha1sum"]:
            return True
        return False

    def download(self, url, file_id):
        sha1sum = hashlib.sha1()
        with requests.get(url, stream=True, auth=self.auth) as r:
            # TODO: handle r.headers["content-length"] if any (has_changed)
            print(f"Downloading {file_id}...")
            r.raise_for_status()
            bar = ProgressBar(
                animation="{stream}",
                steps=["‡", "="],
                template=file_id + ": {animation} {done:B} ({speed:B}/s)",
            )
            ofile_path = self.tmp_dir / file_id
            with open(ofile_path, "wb") as ofile:
                count = 0
                for chunk in r.iter_content(chunk_size=self.chunk_size):
                    sha1sum.update(chunk)
                    ofile.write(chunk)
                    count += 1
                    bar.update(done=count * self.chunk_size)
        has_changed = self.has_changed(file_id, sha1sum.hexdigest())
        return has_changed, {
            "file": ofile_path,
            "name": file_id,
            "sha1sum": sha1sum.hexdigest(),
            "size": ofile_path.stat().st_size,
        }
