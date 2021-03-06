import logging
import math
import typing

from uuid import uuid4
from pathlib import Path

import requests

from sweeper.utils.progress import ProgressBar

log = logging.getLogger(__name__)


class DataGouvFrGateway():
    """
    Upload to [data.gouv.fr](https://www.data.gouv.fr).

    Example usage (replace a resource by uploading a local file):
    ```python
    gw = DataGouvFrGateway("secret-api-token")
    gw.upload_replace_resource(
        "mon-dataset",
        "afc7986c-ef6c-42e1-989c-0a9a498dd6f0",
        Path("./fichier.zip"),
        title="Mon fichier"
    )
    ```
    """
    CHUNK_SIZE = 2000000
    """Acts as lower limit for chunk upload and chunk size"""

    def __init__(self, token: typing.Optional[str], demo=False):
        self.token = token
        self.domain = f"https://{'demo' if demo else 'www'}.data.gouv.fr"
        self.base_url = f"{self.domain}/api/1"

    def update_resource(self, dataset_id, resource_id, **kwargs) -> dict:
        """Helper function to update a resource (PUT)"""
        r = requests.put(
            f"{self.base_url}/datasets/{dataset_id}/resources/{resource_id}/",
            json=kwargs,
            headers={"X-Api-Key": self.token}
        )
        r.raise_for_status()
        return r.json()

    def post_file_chunked(self, url: str, filepath: Path):
        """Helper function to post a local file to given url, with chunked upload"""
        index = 0
        size = filepath.stat().st_size
        total_parts = math.ceil(size / self.CHUNK_SIZE)
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
                chunk = f.read(self.CHUNK_SIZE)
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

    def _is_chunked(self, filepath: Path) -> bool:
        return filepath.stat().st_size <= self.CHUNK_SIZE

    def post_file(self, url, filepath):
        """Helper function to post a local file to given url"""
        r = requests.post(
            url,
            files={"file": (filepath.name, filepath.read_bytes())},
            headers={"X-Api-Key": self.token},
        )
        r.raise_for_status()
        return r.json()

    def _update_from_kwargs(self, data: dict, kwargs: dict):
        """Update data dict with kwargs if any key matches"""
        if any([kw in data for kw in kwargs]):
            for kw, v in kwargs.items():
                if kw in data:
                    data[kw] = v
            return data

    def upload_replace_resource(
        self, dataset_id: str, resource_id: str, filepath: Path, **kwargs
    ) -> dict:
        """
        Replace a resource by uploading a local file.

        Supports chunked upload for files > 2MB.
        """
        url = f"{self.base_url}/datasets/{dataset_id}/resources/{resource_id}/upload/"
        log.info(f"Replacing file resource {url}...")
        if self._is_chunked(filepath):
            data = self.post_file(url, filepath)
        else:
            data = self.post_file_chunked(url, filepath)
        # make a second request with updated kwargs <-> resource attributes if any
        update = self._update_from_kwargs(data, kwargs)
        if update:
            data = self.update_resource(dataset_id, resource_id, **update)
        return data

    def upload_add_resource(self, dataset_id, filepath, type='main', **kwargs):
        """
        Add a new resource to a dataset by uploading a local file.

        Supports chunked upload for files > 2MB.
        """
        url = f"{self.base_url}/datasets/{dataset_id}/upload/"
        log.info(f"Adding file to {dataset_id} w/ resource {filepath}...")
        if self._is_chunked(filepath):
            data = self.post_file(url, filepath)
        else:
            data = self.post_file_chunked(url, filepath)
        # make a second request with updated kwargs <-> resource attributes if any
        update = self._update_from_kwargs(data, kwargs)
        if update:
            resource_id = update["id"]
            data = self.update_resource(dataset_id, resource_id, **update)
        return data

    def remote_replace_resource(self, dataset_id, resource_id, url, title, **kwargs) -> dict:
        """Replace a remote resource by updating (at least) url and title"""
        resource_url = f"{self.domain}/datasets/{dataset_id}/#resource-{resource_id}"
        log.info(f"Replacing remote resource {resource_url}: {title} | {url}...")
        return self.update_resource(dataset_id, resource_id, **{
            "title": title,
            "url": url,
            **kwargs,
        })

    def remote_add_resource(self, dataset_id, url, title, **kwargs):
        raise NotImplementedError
