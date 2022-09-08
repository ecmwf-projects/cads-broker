"""utility module to interface to the object storage."""
import json
import minio  # type: ignore
from typing import Any
import urllib.parse


DOWNLOAD_POLICY_TEMPLATE: dict[str, Any] = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Resource": ["arn:aws:s3:::%(bucket_name)s"],
                },
                {
                    "Action": ["s3:GetObject"],
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Resource": ["arn:aws:s3:::%(bucket_name)s/*"],
                },
            ],
        }


def create_download_bucket(bucket_name: str, object_storage_url: str, **storage_kws: Any) -> None:
    """Create a bucket with download policy in the object storage, if not already existing

    Parameters
    ----------
    bucket_name: name of the bucket
    object_storage_url: endpoint URL of the object storage
    storage_kws: dictionary of parameters used to pass to the storage client
    """
    client = minio.Minio(urllib.parse.urlparse(object_storage_url).netloc, **storage_kws)
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        policy_json = json.dumps(DOWNLOAD_POLICY_TEMPLATE) % {"bucket_name": bucket_name}
        client.set_bucket_policy(bucket_name, policy_json)
