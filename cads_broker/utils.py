import pathlib
import shutil
from typing import Any

import distributed
from dask.typing import Key


def get_task_path(
    worker_or_nanny: distributed.Worker | distributed.Nanny, key: Key | None
) -> pathlib.Path:
    if isinstance(worker_or_nanny, distributed.Worker):
        root = worker_or_nanny.local_directory
    elif isinstance(worker_or_nanny, distributed.Nanny):
        root = worker_or_nanny.worker_dir
    else:
        raise TypeError(
            f"`worker_or_nanny` is of the wrong type: {type(worker_or_nanny)}"
        )
    path = pathlib.Path(root) / "tasks_working_dir"
    if key is not None:
        path /= str(key)
    return path


def rm_task_path(
    worker_or_nanny: distributed.Worker | distributed.Nanny,
    key: Key | None,
    **kwargs: Any,
) -> pathlib.Path:
    path = get_task_path(worker_or_nanny, key)
    if path.exists():
        shutil.rmtree(path, **kwargs)
    return path
