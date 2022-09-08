
import json
from typing import Any

import minio  # type: ignore
import pytest

from cads_broker import object_storage


@pytest.mark.filterwarnings("ignore:Exception ignored")
def test_store_file(mocker) -> None:
    object_storage_url = "http://myobject-storage:myport/"
    bucket_name = "download-bucket"
    ro_policy = json.dumps(object_storage.DOWNLOAD_POLICY_TEMPLATE) % {
        "bucket_name": bucket_name
    }
    storage_kws: dict[str, Any] = {
        "access_key": "storage_user",
        "secret_key": "storage_password",
        "secure": False,
    }
    # patching the used Minio client APIs
    patch1 = mocker.patch.object(minio.Minio, "__init__", return_value=None)
    patch2 = mocker.patch.object(minio.Minio, "bucket_exists", return_value=False)
    patch3 = mocker.patch.object(minio.Minio, "make_bucket")
    patch4 = mocker.patch.object(minio.Minio, "set_bucket_policy")

    object_storage.create_download_bucket(bucket_name, object_storage_url, **storage_kws)

    patch1.assert_called_once_with("myobject-storage:myport", **storage_kws)
    patch2.assert_called_once()
    patch3.assert_called_once_with(bucket_name)
    patch4.assert_called_once_with(bucket_name, ro_policy)
