from pathlib import Path
from typing import Union

import boto3

from botocore.client import Config


class S3Gateway():
    """
    Upload to S3

    For access configuration, you can use:
    - AWS_ACCESS_KEY_ID The access key for your AWS account.
    - AWS_SECRET_ACCESS_KEY The secret key for your AWS account.
    or other standard methods
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
    """

    def __init__(self, bucket: str, s3_endpoint_url: str = None):
        s3_endpoint_url = s3_endpoint_url or "https://s3.amazonaws.com"
        self.bucket = bucket
        self.s3 = boto3.resource(
            "s3",
            endpoint_url=s3_endpoint_url,
            config=Config(signature_version='s3v4'),
        )

    def upload(self, local: Union[str, Path], remote: str):
        if not isinstance(local, str):
            local = str(local)
        # TODO: test that remote=(/)mydir/test.csv works correctly on real s3
        self.s3.Bucket(self.bucket).upload_file(local, remote)
