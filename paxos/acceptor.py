"""Acceptor：Paxos 的"记忆"所在，也是安全性唯一的守门人。

它必须持久化的三样东西：

* ``promised_id``      —— 已经承诺过的最新提案编号（收到 Prepare 时更新）
* ``accepted_id``      —— 最近一次 Accept 的提案编号
* ``accepted_value``   —— 最近一次 Accept 的值

规则只有两条，却撑起了整个 Paxos 的安全性：

1. **Prepare(b)**：若 ``b`` 比已承诺的编号新，则承诺不再接受更小的编号，
   并把自己已接受过的 (accepted_id, accepted_value) 一并回报；
2. **Accept(b, v)**：若 ``b`` 不小于已承诺的编号，则接受该值。

注意规则 2 用的是 ``>=`` 而不是 ``>``：同一个提案者可以在同一编号上重复
提交（重传），这是幂等的。

本实现还多维护了一个 ``promised_all``：Multi-Paxos 的 Leader 用
"一次 Prepare 覆盖全部实例"来省掉每个实例的 Phase 1，这是它比 Basic Paxos
少一轮 RTT 的关键。
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable
from typing import Any

from .ballot import Ballot, later
from .storage import Journal

log = logging.getLogger(__name__)

#: Prepare 中 ins 取该值表示"作用于所有实例"（Multi-Paxos 的 Phase 1 优化）
ALL_INSTANCES = -1

#: 全局 Phase 1 应答里最多回报多少条"未决"实例。
#: 这是防止单条消息无界增长的硬上限（正常情况只有 0~个位数条）。
MAX_REPORTED_INSTANCES = 1024


class Acceptor:
    """单个节点的 Acceptor 角色。"""

    def __init__(
        self,
        node_id: str,
        journal: Journal,
        *,
        fsync: bool,
        is_decided: Callable[[int], bool] | None = None,
    ) -> None:
        self.node_id = node_id
        self.journal = journal
        self.fsync = fsync
        #: 判断某个 slot 是否已经被本地 Learner 选定 —— 已选定的不用再回报
        self._is_decided: Callable[[int], bool] = is_decided or (lambda _slot: False)

        self.promised_all: Ballot | None = None
        self.promised: dict[int, Ballot] = {}
        self.accepted: dict[int, tuple[Ballot, str]] = {}

        self.prepares = 0
        self.accepts = 0
        self.rejections = 0
        self.fast_conflicts = 0

        self._replay()

    # ---------------------------------------------------------------- 持久化
    def _replay(self) -> None:
        """启动时重放日志，把内存状态恢复到崩溃前。"""
        for rec in self.journal.replay():
            kind = rec.get("k")
            if kind == "pa":
                self.promised_all = later(self.promised_all, Ballot.parse(rec["b"]))
            elif kind == "p":
                ins = int(rec["i"])
                self.promised[ins] = later(self.promised.get(ins), Ballot.parse(rec["b"])) or Ballot(0, 0)
            elif kind == "a":
                ins = int(rec["i"])
                self.accepted[ins] = (Ballot.parse(rec["b"]), str(rec["v"]))
            elif kind == "m":  # 批处理写入的多条 accept
                for item in rec["items"]:
                    self.accepted[int(item[0])] = (Ballot.parse(item[1]), str(item[2]))
            # "c" 记录属于 Learner，由 Learner 自己重放
        if self.promised or self.accepted:
            log.info(
                "[%s] 恢复 Acceptor 状态: promised_all=%s, %d 个实例有承诺, %d 个实例已接受",
                self.node_id,
                self.promised_all or "-",
                len(self.promised),
                len(self.accepted),
            )

    async def _persist(self, record: dict[str, Any]) -> None:
        """落盘。fsync=on 时用线程池做同步 fsync，避免阻塞事件循环。"""
        if self.fsync:
            await asyncio.to_thread(self.journal.append, record)
        else:
            self.journal.append(record)

    # ------------------------------------------------------------------ 规则
    def _effective_promised(self, ins: int) -> Ballot | None:
        """实例 ins 上实际生效的承诺 = max(promised_all, promised[ins])。"""
        return later(self.promised_all, self.promised.get(ins))

    async def prepare(self, ins: int, ballot: Ballot) -> dict[str, Any]:
        """Phase 1b。``ins == ALL_INSTANCES`` 时是 Multi-Paxos 的全局 Phase 1。"""
        self.prepares += 1
        if ins == ALL_INSTANCES:
            # 注意用 "<" 而不是 "<="：同一个编号重复 Prepare 是**幂等**的，
            # Leader 做"空槽修复"时要能用自己的 epoch 再问一遍。
            if self.promised_all is not None and ballot < self.promised_all:
                self.rejections += 1
                return {
                    "ok": False,
                    "promised": self.promised_all.to_list(),
                    "accepted": [],
                }
            self.promised_all = ballot
            await self._persist({"k": "pa", "b": ballot.to_list()})
            # 只回报"编号比新提案旧、且我还没学到它已被选定"的条目。
            #
            # 为什么要加这个过滤：一开始这里回报**全部**已接受条目，跑久了就会
            # 撑爆单条消息 —— 实测 2889 个 slot 时这条应答约 170 KB，超过 asyncio
            # StreamReader 默认 64 KiB 的行上限，对端 `readline()` 直接抛
            # `ValueError: Separator is not found, and chunk exceed the limit`，
            # 读循环死掉、链路断开、集群再也选不出 Leader。
            # 已经被 Learner 学到的 slot 不需要回报（Leader 通过缺口回填就能拿到），
            # 过滤掉它们之后，应答长度只和"真正未决的 slot 数"成正比（通常个位数）。
            reported = [
                [i, b.to_list(), v]
                for i, (b, v) in sorted(self.accepted.items())
                if b < ballot and not self._is_decided(i)
            ]
            if len(reported) > MAX_REPORTED_INSTANCES:
                log.warning(
                    "[%s] 未决实例过多（%d 个），本次只回报最近 %d 个",
                    self.node_id,
                    len(reported),
                    MAX_REPORTED_INSTANCES,
                )
                reported = reported[-MAX_REPORTED_INSTANCES:]
            return {"ok": True, "promised": ballot.to_list(), "accepted": reported}

        current = self.promised.get(ins)
        current = later(current, self.promised_all)
        if current is not None and ballot < current:
            self.rejections += 1
            return {"ok": False, "promised": current.to_list(), "accepted": []}
        self.promised[ins] = ballot
        await self._persist({"k": "p", "i": ins, "b": ballot.to_list()})
        accepted = self.accepted.get(ins)
        reported = [] if accepted is None else [[ins, accepted[0].to_list(), accepted[1]]]
        return {"ok": True, "promised": ballot.to_list(), "accepted": reported}

    async def accept(self, ballot: Ballot, entries: Iterable[tuple[int, str]]) -> dict[str, Any]:
        """Phase 2b 的批量版本：一次处理若干 ``(实例, 值)``。

        批量写一条日志记录，把 N 次 fsync 压成 1 次——这是"批处理"优化能显著
        提升吞吐的直接原因。
        """
        self.accepts += 1
        items = list(entries)
        accepted_now: list[int] = []
        rejected: list[int] = []
        conflict = False
        highest: Ballot | None = None

        records: list[dict[str, Any]] = []
        for ins, value in items:
            current = self._effective_promised(ins)
            if current is not None and ballot < current:
                rejected.append(ins)
                highest = later(highest, current)
                continue
            previous = self.accepted.get(ins)
            if previous is not None and previous[0] == ballot and previous[1] != value:
                # 同一快速轮里对同一个实例接受了不同的值 —— Fast Paxos 的冲突
                conflict = True
                self.fast_conflicts += 1
                rejected.append(ins)
                highest = later(highest, current)
                continue
            self.accepted[ins] = (ballot, value)
            if ins == ALL_INSTANCES:  # pragma: no cover - 防御性分支
                continue
            records.append({"k": "a", "i": ins, "b": ballot.to_list(), "v": value})
            accepted_now.append(ins)

        if records:
            # 一次写入多条（批处理）：把批量日志当成一条记录提交
            await self._persist_batch(records)

        if rejected:
            self.rejections += 1
        fallback = self._effective_promised(items[0][0]) if items else None
        return {
            "ok": not rejected,
            "conflict": conflict,
            "accepted": accepted_now,
            "rejected": rejected,
            "promised": (later(highest, fallback or ballot) or ballot).to_list(),
        }

    async def _persist_batch(self, records: list[dict[str, Any]]) -> None:
        """把多条 accept 记录合成一条日志记录写下去。"""
        if len(records) == 1:
            await self._persist(records[0])
            return
        merged = {"k": "m", "items": [[r["i"], r["b"], r["v"]] for r in records]}
        if self.fsync:
            await asyncio.to_thread(self.journal.append, merged)
        else:
            self.journal.append(merged)

    # ------------------------------------------------------------------ 观测
    def state(self) -> dict[str, Any]:
        """给 admin ``status`` 用。"""
        return {
            "promised_all": self.promised_all.to_list() if self.promised_all else None,
            "instances_promised": len(self.promised),
            "instances_accepted": len(self.accepted),
            "prepares": self.prepares,
            "accepts": self.accepts,
            "rejections": self.rejections,
            "fast_conflicts": self.fast_conflicts,
        }
