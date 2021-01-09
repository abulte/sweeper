import logging
import math
import typing

from uuid import uuid4
from pathlib import Path

import requests

from sweeper.utils.progress import ProgressBar

log = logging.getLogger(__name__)


class DataGouvFrGateway():
    def __init__(self, token: typing.Optional[str], demo=False):
        self.token = token
        self.domain = f"https://{'demo' if demo else 'www'}.data.gouv.fr"
        self.base_url = f"{self.domain}/api/1"

    def update_resource(self, dataset_id, resource_id, **kwargs) -> dict:
        r = requests.put(
            f"{self.base_url}/datasets/{dataset_id}/resources/{resource_id}/",
            json=kwargs,
            headers={"X-Api-Key": self.token}
        )
        r.raise_for_status()
        return r.json()

    def chunk_upload(self, url: str, filepath: Path, chunk_size: int):
        index = 0
        size = filepath.stat().st_size
        total_parts = math.ceil(size / chunk_size)
        uuid = str(uuid4())
        transfered = 0
        bar = ProgressBar(
            template="|{animation}| {done:B}/{total:B} ({speed:B}/s)",
            total=size,
        )
        # tuples for multipart form data compliance
        data = {
            "totalparts": ("", total_parts),
            "size": ("", size),
            "filename": ("", filepath.name),
            "uuid": ("", uuid),
        }
        with filepath.open("rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not len(chunk):
                    r = requests.post(url, files=data, headers={"X-Api-Key": self.token})
                    r.raise_for_status()
                    return r.json()
                chunk_data = dict(data, **{
                    "partindex": ("", index),
                    "chunksize": ("", len(chunk)),
                    "partbyteoffset": ("", transfered),
                    "file": ("blob", chunk)
                })
                r = requests.post(url, files=chunk_data, headers={"X-Api-Key": self.token})
                r.raise_for_status()
                assert r.json()["success"]
                index += 1
                transfered += len(chunk)
                bar.update(done=transfered)

    def upload_replace_resource(
        self, dataset_id: str, resource_id: str, filepath: Path, **kwargs
    ) -> dict:
        chunk_size = 2000000
        url = f"{self.base_url}/datasets/{dataset_id}/resources/{resource_id}/upload/"
        log.info(f"Replacing file resource {url}...")
        if filepath.stat().st_size <= chunk_size:
            r = requests.post(
                url,
                files={"file": (filepath.name, filepath.read_bytes())},
                headers={"X-Api-Key": self.token},
            )
            r.raise_for_status()
            data = r.json()
        else:
            data = self.chunk_upload(url, filepath, chunk_size)
        # make a second request with updated kwargs <-> resource attributes if any
        if any([kw in data for kw in kwargs]):
            for kw, v in kwargs.items():
                if kw in data:
                    data[kw] = v
            data = self.update_resource(dataset_id, resource_id, **data)
        return data

    def upload_add_resource(self, dataset_id, filepath, type='main', **kwargs):
        raise NotImplementedError

    def remote_replace_resource(self, dataset_id, resource_id, url, title, **kwargs) -> dict:
        resource_url = f"{self.domain}/datasets/{dataset_id}/#resource-{resource_id}"
        log.info(f"Replacing remote resource {resource_url}: {title} | {url}...")
        return self.update_resource(dataset_id, resource_id, **{
            "title": title,
            "url": url,
            **kwargs,
        })

    def remote_add_resource(self, dataset_id, url, title, **kwargs):
        raise NotImplementedError
