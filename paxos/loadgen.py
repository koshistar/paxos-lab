"""闭环压测客户端：模拟"就近接入某个副本"的客户端。

设计要点：

* **每个并发就是一个 TCP 连接**（闭环模型）：一条连接上串行请求，N 条连接就是
  N 个并发。这样测出来的延迟是真实的客户端往返时延，吞吐 = 并发 / 延迟；
* 客户端只认"命令被多数派确认"这一件事，收到 ``ok`` 才算成功；
* 遇到 ``not_leader`` 会自动发现 Leader 并改连（等价的客户端行为），
  发现失败次数单独计数，用来观察"Leader 挂了以后客户端要难受多久"；
* 每个请求带唯一 ``req``，节点侧状态机按它去重，于是重试不会造成重复写入。

既可以当库被实验脚本调用，也可以单独当命令行工具用。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import statistics
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from .transport import decode, enable_nodelay, encode

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10.0


# --------------------------------------------------------------------- 记录
@dataclass
class OpRecord:
    """一次客户端请求的观测结果。"""

    seq: int
    ts_ms: float
    latency_ms: float
    ok: bool
    err: str
    target: str
    node: str
    attempts: int

    def as_row(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "ts_ms": round(self.ts_ms, 3),
            "latency_ms": round(self.latency_ms, 3),
            "ok": int(self.ok),
            "err": self.err,
            "target": self.target,
            "node": self.node,
            "attempts": self.attempts,
        }


@dataclass
class LoadSummary:
    """一次压测的汇总指标（报告里的核心数据）。"""

    ops: int = 0
    ok: int = 0
    errors: int = 0
    duration_ms: float = 0.0
    throughput: float = 0.0
    p50: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    mean: float = 0.0
    max: float = 0.0
    error_rate: float = 0.0
    error_kinds: dict[str, int] = field(default_factory=dict)
    redirects: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "ops": self.ops,
            "ok": self.ok,
            "errors": self.errors,
            "duration_ms": round(self.duration_ms, 1),
            "throughput": round(self.throughput, 2),
            "p50": round(self.p50, 3),
            "p95": round(self.p95, 3),
            "p99": round(self.p99, 3),
            "mean": round(self.mean, 3),
            "max": round(self.max, 3),
            "error_rate": round(self.error_rate, 4),
            "redirects": self.redirects,
            "error_kinds": dict(sorted(self.error_kinds.items())),
        }


def percentile(values: Sequence[float], q: float) -> float:
    """线性插值分位数（与 numpy 默认口径一致，P50/P95/P99 直接可用）。"""
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q
    low = int(pos)
    high = min(low + 1, len(ordered) - 1)
    frac = pos - low
    return ordered[low] * (1 - frac) + ordered[high] * frac


def summarize(records: Sequence[OpRecord], *, warmup_ms: float = 0.0) -> LoadSummary:
    """把逐条记录汇总成 P50/P95/P99 + 吞吐。"""
    usable = [r for r in records if r.ts_ms >= warmup_ms]
    if not usable:
        usable = list(records)
    latencies = [r.latency_ms for r in usable if r.ok]
    errors = [r for r in usable if not r.ok]
    duration_ms = 0.0
    if usable:
        duration_ms = max(r.ts_ms + r.latency_ms for r in usable) - min(r.ts_ms for r in usable)
    summary = LoadSummary(
        ops=len(usable),
        ok=len(latencies),
        errors=len(errors),
        duration_ms=max(duration_ms, 1e-9),
        throughput=(len(latencies) / (duration_ms / 1000.0)) if duration_ms > 0 else 0.0,
        p50=percentile(latencies, 0.50),
        p95=percentile(latencies, 0.95),
        p99=percentile(latencies, 0.99),
        mean=statistics.fmean(latencies) if latencies else 0.0,
        max=max(latencies) if latencies else 0.0,
        error_rate=(len(errors) / len(usable)) if usable else 0.0,
    )
    for record in errors:
        summary.error_kinds[record.err] = summary.error_kinds.get(record.err, 0) + 1
    return summary


# --------------------------------------------------------------------- 压测器
class LoadGen:
    """闭环并发压测器。"""

    def __init__(
        self,
        targets: Sequence[tuple[str, str, int]],
        *,
        concurrency: int = 8,
        duration: float = 5.0,
        total_ops: int = 0,
        timeout: float = DEFAULT_TIMEOUT,
        command_fn: Callable[[int], str] | None = None,
        rate: float = 0.0,
        seed: int = 2025,
    ) -> None:
        #: ``(节点名, host, port)`` 列表，压测器按轮转选择接入点
        self.targets = list(targets)
        self.concurrency = max(1, concurrency)
        self.duration = duration
        self.total_ops = total_ops
        self.timeout = timeout
        self.rate = rate
        self._rng = random.Random(seed)
        self._command_fn = command_fn or self._default_command
        self._records: list[OpRecord] = []
        self._seq = 0
        self._stop_at = 0.0
        self._redirects = 0
        self._op_budget = total_ops

    # ---------------------------------------------------------------- 命令生成
    @staticmethod
    def _default_command(seq: int) -> str:
        key = f"k{seq % 97}"
        return json.dumps({"req": f"r{seq}", "op": "incr", "k": key, "v": 1}, separators=(",", ":"))

    def _make_command(self, seq: int) -> str:
        """给命令注入唯一的幂等键，保证"重试不会重复写"。"""
        template = self._command_fn(seq)
        try:
            body = json.loads(template)
            body["req"] = uuid.uuid4().hex
            return json.dumps(body, separators=(",", ":"))
        except json.JSONDecodeError:
            return template

    # -------------------------------------------------------------------- 运行
    async def run(self, on_start: Callable[[], Any] | None = None) -> LoadSummary:
        self._records.clear()
        self._seq = 0
        start = time.perf_counter()
        self._stop_at = start + self.duration
        workers = [asyncio.create_task(self._worker(i, start), name=f"lg-{i}") for i in range(self.concurrency)]
        if on_start is not None:
            await asyncio.sleep(0)
            result = on_start()
            if asyncio.iscoroutine(result):
                await result
        await asyncio.gather(*workers, return_exceptions=True)
        summary = summarize(self._records)
        summary.redirects = self._redirects
        log.info(
            "压测完成: ops=%d ok=%d err=%d p50=%.2fms p95=%.2fms p99=%.2fms 吞吐=%.1f/s",
            summary.ops,
            summary.ok,
            summary.errors,
            summary.p50,
            summary.p95,
            summary.p99,
            summary.throughput,
        )
        return summary

    def _should_stop(self) -> bool:
        if self.total_ops:
            return self._seq >= self.total_ops
        return time.perf_counter() >= self._stop_at

    async def _worker(self, index: int, start: float) -> None:
        target = self.targets[index % len(self.targets)]
        reader: asyncio.StreamReader | None = None
        writer: asyncio.StreamWriter | None = None
        try:
            while not self._should_stop():
                if reader is None or writer is None:
                    try:
                        reader, writer = await asyncio.open_connection(target[1], target[2])
                        enable_nodelay(writer)
                        # 先握手，声明自己是"客户端"而不是同伴节点
                        writer.write(encode({"t": "hello", "id": "client"}))
                        await writer.drain()
                    except OSError:
                        await asyncio.sleep(0.05)
                        continue
                self._seq += 1
                seq = self._seq
                command = self._make_command(seq)
                sent_at = time.perf_counter()
                ts_ms = (sent_at - start) * 1000.0
                payload: dict[str, Any]
                try:
                    writer.write(encode({"t": "client_req", "rid": str(seq), "cmd": command, "req": str(seq)}))
                    await writer.drain()
                    line = await asyncio.wait_for(reader.readline(), timeout=self.timeout)
                    if not line:
                        raise ConnectionError("对端关闭了连接")
                    payload = decode(line).get("p") or {}
                except asyncio.TimeoutError:
                    payload = {"ok": False, "err": "timeout"}
                except (ConnectionError, OSError):
                    payload = {"ok": False, "err": "conn"}
                    with contextlib.suppress(Exception):
                        writer.close()
                    reader, writer = None, None

                latency_ms = (time.perf_counter() - sent_at) * 1000.0
                ok = bool(payload.get("ok"))
                err = "" if ok else str(payload.get("err", "unknown"))

                # 不是 Leader → 跟着提示换接入点（真实客户端的做法）
                if not ok and (payload.get("redirect") or err in {"not_leader", "timeout"}):
                    self._redirects += 1
                    leader = payload.get("leader")
                    if leader:
                        target = next((t for t in self.targets if t[0] == leader), target)
                    elif not payload.get("leader"):
                        target = self.targets[(index + 1) % len(self.targets)]

                self._records.append(
                    OpRecord(
                        seq=seq,
                        ts_ms=ts_ms,
                        latency_ms=latency_ms,
                        ok=ok,
                        err=err,
                        target=target[0],
                        node=str(payload.get("node", "")),
                        attempts=int(payload.get("attempts", 0) or 0),
                    )
                )
                if self.rate > 0:
                    await asyncio.sleep(max(0.0, 1.0 / self.rate))
        finally:
            if writer is not None:
                with contextlib.suppress(Exception):
                    writer.close()

    @property
    def records(self) -> list[OpRecord]:
        return self._records


# --------------------------------------------------------------------- 便捷入口
def run_load(
    targets: Sequence[tuple[str, str, int]],
    *,
    concurrency: int = 8,
    duration: float = 5.0,
    total_ops: int = 0,
    timeout: float = DEFAULT_TIMEOUT,
) -> LoadSummary:
    """同步封装，方便脚本里一行调用。"""
    gen = LoadGen(
        targets,
        concurrency=concurrency,
        duration=duration,
        total_ops=total_ops,
        timeout=timeout,
    )
    return asyncio.run(gen.run())
