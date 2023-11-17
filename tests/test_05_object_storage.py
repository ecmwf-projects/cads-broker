from typing import Any

import botocore  # type: ignore
import pytest
import pytest_mock

from cads_broker import object_storage


class DummyStorageObject:
    """Simulate an object stored."""

    def __init__(self, name: str, extra_args: dict[str, Any] | None = None) -> None:
        self.name = name
        if extra_args is None:
            extra_args = dict()
        new_extra_args = extra_args.copy()
        acl = new_extra_args.pop("ACL", None)
        if acl and acl != "public-read":
            raise NotImplementedError
        self.acl = {
            "ResponseMetadata": {
                "RequestId": "a-request-id",
                "HTTPStatusCode": 200,
            },
            "Grants": [
                {
                    "Grantee": {
                        "Type": "Group",
                        "URI": "http://acs.amazonaws.com/groups/global/AllUsers",
                    },
                    "Permission": "READ",
                },
            ],
        }
        for key, value in new_extra_args.items():
            setattr(self, key, value)


class DummyBucket:
    """Simulate a bucket."""

    def __init__(
        self,
        name: str,
        cors: dict[Any, Any] | None = None,
        acl: dict[Any, Any] | None = None,
    ) -> None:
        self.name = name
        self.cors = cors
        self.acl = acl
        self.objects: list[DummyStorageObject] = []

    def __contains__(self, item: str) -> bool:
        names_contained = [r.name for r in self.objects]
        return item in names_contained

    def __getitem__(self, item: str) -> DummyStorageObject:
        objs_contained = {r.name: r for r in self.objects}
        return objs_contained[item]

    def __setitem__(self, key: str, value: DummyStorageObject) -> None:
        new_object = DummyStorageObject(name=key)
        if key in self:
            names_contained = [r.name for r in self.objects]
            index = names_contained.index(key)
            self.objects[index] = new_object
        else:
            self.objects.append(new_object)


class DummyBotoClient:
    """Simulate a boto client."""

    def __init__(
        self,
        resource: str,
        endpoint_url: str | None = None,
        **storage_kws: dict[str, Any],
    ) -> None:
        self.resource = resource
        self.endpoint_url = endpoint_url
        self.storage_kws = storage_kws
        self.buckets: dict[str, DummyBucket] = dict()

    def create_bucket(self, Bucket: str) -> None:
        if Bucket in self.buckets:
            return
        bucket_obj = DummyBucket(name=Bucket)
        self.buckets[Bucket] = bucket_obj

    def head_bucket(self, Bucket: str) -> None:
        if Bucket not in self.buckets:
            error_response = {"Error": {"Code": "404"}}
            error = botocore.exceptions.ClientError(error_response, "head bucket")
            raise error

    def get_bucket_acl(self, Bucket: str) -> dict[Any, Any]:
        if Bucket not in self.buckets:
            raise ValueError("No such bucket")
        bucket_obj = self.buckets[Bucket]
        if not bucket_obj.acl:
            raise ValueError("bucket doesn't have ACL")
        return bucket_obj.acl

    def put_bucket_acl(self, Bucket: str, ACL: dict[Any, Any]) -> None:
        if Bucket not in self.buckets:
            raise ValueError("bucket doesn't exists")
        bucket_obj = self.buckets[Bucket]
        if ACL != "public-read":
            raise NotImplementedError
        bucket_obj.acl = {
            "ResponseMetadata": {
                "RequestId": "a-request-id",
                "HTTPStatusCode": 200,
            },
            "Grants": [
                {
                    "Grantee": {
                        "Type": "Group",
                        "URI": "http://acs.amazonaws.com/groups/global/AllUsers",
                    },
                    "Permission": "READ",
                },
            ],
        }

    def head_object(self, Bucket: str, Key: str) -> None:
        if Bucket not in self.buckets:
            raise ValueError("bucket doesn't exists")
        bucket_obj = self.buckets[Bucket]
        if Key not in bucket_obj:
            error_response = {"Error": {"Code": "404"}}
            error = botocore.exceptions.ClientError(error_response, "head object")
            raise error


@pytest.mark.filterwarnings("ignore:Exception ignored")
def test_create_download_bucket(mocker: pytest_mock.MockerFixture) -> None:
    object_storage_url = "http://myobject-storage:myport/"
    bucket_name = "download-bucket"
    storage_kws: dict[str, Any] = {
        "aws_access_key_id": "storage_user",
        "aws_secret_access_key": "storage_password",
    }
    test_client = DummyBotoClient("s3", object_storage_url, **storage_kws)
    # define spies
    head_bucket = mocker.spy(DummyBotoClient, "head_bucket")
    create_bucket = mocker.spy(DummyBotoClient, "create_bucket")
    put_bucket_acl = mocker.spy(DummyBotoClient, "put_bucket_acl")
    get_bucket_acl = mocker.spy(DummyBotoClient, "get_bucket_acl")

    # first test: bucket doesn't exist
    object_storage.create_download_bucket(
        bucket_name, object_storage_url, client=test_client, **storage_kws
    )

    head_bucket.assert_called_once_with(test_client, bucket_name)
    create_bucket.assert_called_once_with(test_client, Bucket=bucket_name)
    put_bucket_acl.assert_called_once_with(
        test_client, ACL="public-read", Bucket=bucket_name
    )
    get_bucket_acl.assert_not_called()
    assert object_storage.is_bucket_existing(test_client, bucket_name)
    assert object_storage.is_bucket_read_only(test_client, bucket_name)

    for spy in [
        head_bucket,
        create_bucket,
        get_bucket_acl,
        put_bucket_acl,
    ]:
        spy.reset_mock()

    # second test: bucket already exists
    object_storage.create_download_bucket(
        bucket_name, object_storage_url, client=test_client, **storage_kws
    )

    head_bucket.assert_called_once_with(test_client, bucket_name)
    create_bucket.assert_not_called()
    put_bucket_acl.assert_not_called()
    get_bucket_acl.assert_called_once_with(test_client, bucket_name)
    assert object_storage.is_bucket_existing(test_client, bucket_name)
    assert object_storage.is_bucket_read_only(test_client, bucket_name)

    for spy in [
        head_bucket,
        create_bucket,
        get_bucket_acl,
        put_bucket_acl,
    ]:
        spy.reset_mock()
