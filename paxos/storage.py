"""Acceptor / Learner 的持久化存储。

Paxos 要求 Acceptor 在**回应之前**把 ``promised_id``、``accepted_id``、
``accepted_value`` 落到稳定存储上，否则崩溃重启后可能"忘记承诺"，导致
同一次选举里两个不同的值都被选定（破坏安全性）。

实现上用**追加写 JSON Lines 日志 + 启动时重放**，而不是"改写整个状态文件"：

* 追加写只需要一次顺序写，成本远低于重写全文件；
* 崩溃时最多丢掉末尾一条不完整的记录，重放时跳过即可；
* 天然可以做成"批量一次写多条"，这也是本工程批处理优化的一部分。

``fsync`` 开关是本作业的实验变量之一：

* ``fsync=on``  ：每条记录 ``flush + os.fsync``，掉电也不丢，但每次写都要等磁盘；
* ``fsync=off`` ：只写进用户态缓冲区（累计 4 KiB 才 flush），**进程被 kill -9
  时缓冲区内的记录会丢失**，于是重启后 promised 编号会回退——第 7 章用它
  直观演示"没有 fsync 的持久化是假的"。

三档写入策略（``Journal.mode``）

=================  =============================================  ==========================
mode               动作                                            进程被 kill 后 / 掉电后
=================  =============================================  ==========================
``fsync``          write + flush + os.fsync                       都还在 / 都还在
``flush``          write + flush（进内核页缓存）                    还在 / 可能丢
``buffer``         write（用户态缓冲，4 KiB 才 flush）             可能丢 / 可能丢
=================  =============================================  ==========================

**Acceptor 用 fsync（或 buffer 做对照实验），Learner 用 flush。**
理由：Acceptor 的 promised_id 是安全性的根基，丢了会破坏正确性；而 Learner 的
决定日志是"派生数据"，丢了可以从同伴那里重新学回来（缺口回填），没必要为它
付出每次 fsync 的代价。这个区分本身就是分布式存储里很重要的一课。
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

#: fsync=off 时的用户态缓冲阈值（字节）
BUFFER_FLUSH_BYTES = 4096

#: 合法的写入策略
MODES = ("fsync", "flush", "buffer")


class Journal:
    """追加写的 JSON Lines 日志。

    记录类型（``k`` 字段）::

        {"k": "pa", "b": [r, n]}            promised_all（Multi-Paxos 全局承诺）
        {"k": "p",  "i": 3, "b": [r, n]}    promised[3]（Basic Paxos 按实例承诺）
        {"k": "a",  "i": 3, "b": [r, n], "v": "命令"}
                                            accepted[3] = (b, v)
        {"k": "m",  "items": [...]}         批量 accept（一次 fsync 写多条）
        {"k": "c",  "i": 3, "v": "命令"}    Learner 已学到的选定值
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        mode: str = "flush",
    ) -> None:
        if mode not in MODES:
            raise ValueError(f"未知的写入策略 {mode!r}，可选: {MODES}")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.mode = mode
        self._fh = self.path.open("a", encoding="utf-8", newline="\n")
        self._buffer_bytes = 0
        self.records = 0
        self.fsyncs = 0

    @property
    def fsync(self) -> bool:
        return self.mode == "fsync"

    # ------------------------------------------------------------------ 写
    def append(self, record: dict[str, Any]) -> None:
        """追加一条记录。"""
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        self._fh.write(line)
        self.records += 1
        if self.mode == "fsync":
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self.fsyncs += 1
        elif self.mode == "flush":
            self._fh.flush()
        else:  # buffer
            self._buffer_bytes += len(line)
            if self._buffer_bytes >= BUFFER_FLUSH_BYTES:
                self._fh.flush()
                self._buffer_bytes = 0

    def flush(self) -> None:
        self._fh.flush()
        self._buffer_bytes = 0
        if self.mode == "fsync":
            os.fsync(self._fh.fileno())

    # ------------------------------------------------------------------ 读
    def replay(self) -> Iterator[dict[str, Any]]:
        """重放历史记录；末尾被截断的半行会被安全跳过。"""
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    log.warning("%s 第 %d 行损坏（多半是崩溃时写了一半），已跳过", self.path, lineno)

    # ---------------------------------------------------------------- 生命周期
    def close(self) -> None:
        try:
            self._fh.flush()
            if self.mode == "fsync":
                os.fsync(self._fh.fileno())
            self._fh.close()
        except OSError:  # pragma: no cover - 关闭时的异常不影响结果
            log.debug("关闭日志失败: %s", self.path, exc_info=True)

    def truncate(self) -> None:
        """清空日志（开新一轮实验前调用）。"""
        self._fh.close()
        self.path.write_text("", encoding="utf-8")
        self._fh = self.path.open("a", encoding="utf-8", newline="\n")
        self._buffer_bytes = 0
        self.records = 0

    def __enter__(self) -> Journal:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
