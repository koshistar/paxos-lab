"""pytest 公共夹具。

**不用 pytest 自带的 ``tmp_path``**，原因是它在某些受限环境（Windows 沙箱、
只读临时目录、CI 挂载特殊）会直接 PermissionError。我们自己造一个工程内的
临时目录，随用随删，行为在任何平台上都一致。

可用的夹具：

* ``work_dir`` —— 一个干净的临时目录（``run/test-tmp/<随机名>``），测试结束自动删
"""

from __future__ import annotations

import shutil
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

#: 工程内的临时根目录（.gitignore 已排除 run/）
TMP_ROOT = Path("run/test-tmp")


@pytest.fixture()
def work_dir() -> Iterator[Path]:
    """提供一个干净的、测试结束后自动删除的目录。"""
    path = TMP_ROOT / uuid.uuid4().hex[:10]
    path.mkdir(parents=True, exist_ok=True)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
