from pathlib import Path

import requests


class DataGouvFrGateway():
    def __init__(self, token: str, demo=False):
        self.token = token
        self.domain = f"https://{'demo' if demo else 'www'}.data.gouv.fr"
        self.base_url = f"{self.domain}/api/1"

    # def _read_in_chunks(file_object, chunk_size=1024):
    #     """Lazy function (generator) to read a file piece by piece.
    #     Default chunk size: 1k."""
    #     while True:
    #         data = file_object.read(chunk_size)
    #         if not data:
    #             break
    #         yield data

    def update_resource(self, dataset_id, resource_id, **kwargs) -> dict:
        r = requests.put(
            f"{self.base_url}/datasets/{dataset_id}/resources/{resource_id}/",
            json=kwargs,
            headers={'X-Api-Key': self.token}
        )
        r.raise_for_status()
        return r.json()

    # TODO: try chunked upload (see above)
    def upload_replace_resource(
        self, dataset_id: str, resource_id: str, filepath: Path, **kwargs
    ) -> dict:
        files = {"file": filepath.read_bytes()}
        r = requests.post(
            f"{self.base_url}/datasets/{dataset_id}/resources/{resource_id}/upload/",
            files=files,
            headers={'X-Api-Key': self.token},
        )
        r.raise_for_status()
        data = r.json()
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
        print(f"Replacing remote resource {resource_url}: {title} | {url}...")
        return self.update_resource(dataset_id, resource_id, **{
            "title": title,
            "url": url,
            **kwargs,
        })

    def remote_add_resource(self, dataset_id, url, title, **kwargs):
        raise NotImplementedError
