"""Learner：把"已被选定"的实例按 slot 顺序应用，并回填缺口。

为什么必须**按顺序**应用：Paxos 只保证单个实例（slot）的共识，不保证 slot
之间的先后。业务顺序来自"slot 编号"，所以 Learner 必须等前面的 slot 都确定
了才能应用后面的，否则不同节点可能以不同顺序应用同一批命令。

缺口回填（gap fill）是 Learner 的第二个职责：广播的 commit 报文可能丢包，
于是一个节点可能先学到 slot 10 而缺 slot 7。这时它主动向同伴发 ``learn_ask``
把缺的补回来——不然日志永远卡在缺口处。
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from .ballot import Ballot
from .storage import Journal

log = logging.getLogger(__name__)


class Learner:
    """Learner + Listener 共用同一套学习逻辑。"""

    def __init__(
        self,
        node_id: str,
        journal: Journal,
        *,
        on_apply: Callable[[int, str], None],
        fsync: bool,
    ) -> None:
        self.node_id = node_id
        self.journal = journal
        self._on_apply = on_apply
        self.fsync = fsync

        #: slot -> 已选定的值（= 本节点的"决定日志"）
        self.chosen: dict[int, str] = {}
        #: 已按顺序应用到的下一个 slot
        self.next_apply = 0
        self.learned = 0
        self.gaps = 0
        self.safety_violations = 0
        self.commit_index = -1

        self._replay()

    # ---------------------------------------------------------------- 持久化
    def _replay(self) -> None:
        for rec in self.journal.replay():
            if rec.get("k") == "c":
                slot = int(rec["i"])
                value = str(rec["v"])
                self.chosen[slot] = value
        # 重放后把已经可以应用的部分一次性应用掉
        self._drain()
        if self.chosen:
            log.info("[%s] 恢复 Learner 状态: 已学 %d 个 slot", self.node_id, len(self.chosen))

    # ------------------------------------------------------------------ 学习
    def choose(self, slot: int, value: str, ballot: Ballot | None = None) -> bool:
        """记录"slot 上的值已被选定"。返回是否是新决定。

        若同一个 slot 出现了两个不同的值，说明 Paxos 的安全性被破坏了——
        这种事件必须计数并在报告里明确写"从未发生"，所以这里单独记一个计数器。
        """
        previous = self.chosen.get(slot)
        if previous is not None:
            if previous != value:
                self.safety_violations += 1
                log.error(
                    "[%s] 安全性违例！slot=%d 已有值 %r，又收到 %r (ballot=%s)",
                    self.node_id,
                    slot,
                    previous,
                    value,
                    ballot,
                )
            return False
        self.chosen[slot] = value
        self.commit_index = max(self.commit_index, slot)
        self._persist({"k": "c", "i": slot, "v": value})
        self.learned += 1
        self._drain()
        return True

    def _persist(self, rec: dict[str, Any]) -> None:
        # Learner 的写入不阻塞主路径（值既然已经被多数派选定，重放丢掉最多
        # 造成一次缺口回填），所以这里直接同步写，不做线程池调度。
        self.journal.append(rec)

    def _drain(self) -> None:
        """把从 next_apply 开始连续可用的 slot 依次应用到状态机。"""
        while self.next_apply in self.chosen:
            slot = self.next_apply
            value = self.chosen[slot]
            self._on_apply(slot, value)
            self.next_apply += 1

    # ------------------------------------------------------------------ 缺口
    def missing(self, limit: int = 64) -> list[int]:
        """列出 [0, commit_index) 之间还不知道的 slot。"""
        if self.commit_index < 0:
            return []
        return [slot for slot in range(self.commit_index + 1) if slot not in self.chosen][:limit]

    def note_gap(self) -> None:
        self.gaps += 1

    def snapshot(self) -> dict[str, Any]:
        return {
            "learned": self.learned,
            "chosen_slots": len(self.chosen),
            "next_apply": self.next_apply,
            "commit_index": self.commit_index,
            "gaps": self.gaps,
            "safety_violations": self.safety_violations,
            "missing": self.missing(16),
        }
