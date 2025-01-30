"""utility module to interface to the object storage."""

import os.path
import urllib.parse
from typing import Any

import boto3  # type: ignore
import botocore  # type: ignore
import structlog

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


def parse_data_volumes_config(path: str | None = None) -> list[str]:
    if path is None:
        path = os.environ["DATA_VOLUMES_CONFIG"]

    data_volumes = []
    with open(path) as fp:
        for line in fp:
            if data_volume := os.path.expandvars(line.rstrip("\n")):
                data_volumes.append(data_volume)
    return data_volumes


def is_bucket_existing(client: Any, bucket_name: str) -> bool | None:
    """Return True if the bucket exists."""
    try:
        client.head_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
    return None


def is_bucket_read_only(client: Any, bucket_name: str) -> bool:
    """Return True if the bucket is read-only for all users."""
    response = client.get_bucket_acl(Bucket=bucket_name)
    try:
        for grant in response["Grants"]:
            grantee = grant["Grantee"]
            if (
                grantee["Type"] == "Group"
                and grantee["URI"] == "http://acs.amazonaws.com/groups/global/AllUsers"
            ):
                return grant["Permission"] == "READ"  # type: ignore
    except KeyError:
        logger.error(f"ACL of bucket {bucket_name} not parsable")
    return False


def create_download_bucket(
    bucket_name: str, object_storage_url: str, client: Any = None, **storage_kws: Any
) -> None:
    """Create a public-read bucket (if not existing).

    Parameters
    ----------
    bucket_name: name of the bucket (something as 's3://mybucketname' or just 'mybucketname')
    object_storage_url: endpoint URL of the object storage
    client: client to use, default is boto3 (used for testing)
    storage_kws: dictionary of parameters used to pass to the storage client.
    """
    bucket_url_obj = urllib.parse.urlparse(bucket_name)
    scheme = "s3"
    if bucket_url_obj.scheme:
        scheme = bucket_url_obj.scheme
        bucket_name = bucket_url_obj.netloc
    if not client:
        client = boto3.client(scheme, endpoint_url=object_storage_url, **storage_kws)
    if not is_bucket_existing(client, bucket_name):
        logger.info(f"creation of bucket {bucket_name}")
        client.create_bucket(Bucket=bucket_name)
        logger.info(f"setup ACL public-read on bucket {bucket_name}")
        client.put_bucket_acl(ACL="public-read", Bucket=bucket_name)
    elif not is_bucket_read_only(client, bucket_name):
        logger.warning(f"setting ACL public-read on bucket {bucket_name}")
        client.put_bucket_acl(ACL="public-read", Bucket=bucket_name)
