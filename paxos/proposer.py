"""三种提案机制：Basic Paxos / Multi-Paxos / Fast Paxos。

三者的差别只在一句话上：**为了决定一个值，需要几轮网络往返、要不要 Leader。**

+---------------+--------+----------+------------------------------+
| 机制           | RTT/提案 | 提案者    | 关键机制                       |
+===============+========+==========+==============================+
| Basic Paxos   | 2      | 任意节点  | Prepare + Accept 两轮           |
| Multi-Paxos   | 1      | 仅 Leader | 稳定 Leader 后跳过 Phase 1 + 批量 |
| Fast Paxos    | 1      | 任意节点  | 任意提案者直投 Accept，快速仲裁    |
+---------------+--------+----------+------------------------------+

三种机制共用同一条安全性铁律：**Acceptor 只在提案编号不小于已承诺编号时才接受**，
于是任意两个多数派相交 —— 这就是 Paxos 在异步网络 + 节点崩溃下仍然正确的原因。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .ballot import Ballot
from .transport import FaultProfile  # noqa: F401  (仅用于类型提示)

if TYPE_CHECKING:  # pragma: no cover
    from .node import PaxosNode

log = logging.getLogger(__name__)

#: 单次提案最多重试多少次
MAX_ATTEMPTS = 12


# --------------------------------------------------------------------- 结果
@dataclass
class ProposalResult:
    """一次提案的结局，直接回给客户端。"""

    ok: bool
    slot: int = -1
    value: str = ""
    err: str = ""
    attempts: int = 0
    ballot: Ballot | None = None
    chosen_value: str = ""
    #: 为 True 时客户端应该换一个节点重试（如"我不是 Leader"）
    redirect: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"ok": self.ok}
        if self.ok:
            payload.update(
                {
                    "slot": self.slot,
                    "value": self.chosen_value or self.value,
                    "attempts": self.attempts,
                    "ballot": self.ballot.to_list() if self.ballot else None,
                }
            )
        else:
            payload["err"] = self.err
            payload["attempts"] = self.attempts
        return payload


# --------------------------------------------------------------------- 工具
async def fanout(
    node: PaxosNode,
    targets: Sequence[str],
    message: dict[str, Any],
    quorum: int,
    timeout: float,
    *,
    ok_field: str = "ok",
) -> list[dict[str, Any]]:
    """向若干节点并发发同一条请求，**凑够 quorum 个成功就提前返回**。

    提前返回是 Paxos 的关键工程优化：多数派一旦到齐，剩下的回包可以不等。
    这也是"P99 延迟"比"最慢节点"低得多的原因。

    这里刻意接收"一条消息"而不是"一个按目标生成消息的函数"：Paxos 的广播报文
    对所有接收者都一样，传 dict 更省内存，也避免在循环里写闭包（容易踩到
    "闭包捕获了循环变量"的经典 bug）。
    """
    tasks = {
        asyncio.create_task(node.rpc(peer, message, timeout=timeout), name=f"rpc-{peer}"): peer for peer in targets
    }
    payloads: list[dict[str, Any]] = []
    good = 0
    pending = set(tasks)
    try:
        while pending and good < quorum:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task.cancelled() or task.exception() is not None:
                    continue
                payload = task.result()
                payloads.append(payload)
                if payload.get(ok_field):
                    good += 1
            if good + len(pending) < quorum:
                break  # 剩下的全成功也凑不够，提前失败
    finally:
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
    return payloads


def pick_highest_accepted(payloads: Iterable[dict[str, Any]]) -> tuple[Ballot, str] | None:
    """从 Phase-1b 的回包里挑出"编号最高的已接受值"（Basic Paxos 的核心规则）。

    Paxos 不允许提案者随意用自己的值：如果多数派里已经有人接受过值，就必须
    沿用其中**编号最大**的那个，否则可能覆盖一个已经被选定的值。
    """
    best: tuple[Ballot, str] | None = None
    for payload in payloads:
        for _, raw_ballot, value in payload.get("accepted", []) or []:
            candidate = (Ballot.parse(raw_ballot), str(value))
            if best is None or candidate[0] > best[0]:
                best = candidate
    return best


def resolve_value(payloads: Iterable[dict[str, Any]], slot: int, default: str) -> tuple[str, Ballot | None, int]:
    """给定一批 Phase-1b 回报，判断实例 ``slot`` 上"可能已被选定"的值。

    规则：**取编号最高的轮次；同一轮次里取出现次数最多的值；出现次数并列
    则说明谁也没被选定，可以自由用 ``default``。**

    为什么这条规则是安全的（报告里的安全性论证就靠这段）：

    记经典多数派为 ``Q``、快速仲裁为 ``F``。若值 v 真的被选定，则至少 ``F`` 个
    Acceptor 接受了 v，于是 ``Q`` 中至少有 ``Q + F - n`` 个接受过 v；而与 v 冲突的
    另一个值 w 只可能落在剩下的 ``n - F`` 个 Acceptor 上。可以验证

        ``Q + F - n > n - F``   （n=3/4/5 时分别为 2>0、2>1、2>1）

    所以"真正被选定的值在 Q 的回报里出现次数严格最多"，取最大计数永远不会
    覆盖一个已经选定的值；而计数并列时，两边都达不到 ``Q + F - n`` 的下界，
    说明确实谁都还没被选定。这个规则同时解决了 Fast Paxos 的"看不见的冲突"。
    """
    buckets: dict[Ballot, dict[str, int]] = {}
    for payload in payloads:
        for ins, raw_ballot, value in payload.get("accepted", []) or []:
            if int(ins) != slot:
                continue
            ballot = Ballot.parse(raw_ballot)
            bucket = buckets.setdefault(ballot, {})
            bucket[str(value)] = bucket.get(str(value), 0) + 1
    if not buckets:
        return default, None, 0
    highest = max(buckets)
    counts = buckets[highest]
    top = max(counts.values())
    winners = [value for value, count in counts.items() if count == top]
    if len(winners) == 1:
        return winners[0], highest, top
    return default, highest, 0


# --------------------------------------------------------------------- 基类
class Proposer:
    """提案者基类，负责重试与计时这类公共逻辑。"""

    name = "base"

    def __init__(self, node: PaxosNode) -> None:
        self.node = node
        self.retries = 0

    async def submit(self, command: str, req_id: str = "") -> ProposalResult:  # pragma: no cover
        raise NotImplementedError

    # -------------------------------------------------------------- 公共片段
    def next_ballot(self) -> Ballot:
        """生成一个严格递增的新提案编号。"""
        return Ballot(self.node.bump_round(), self.node.ballot_id)

    def max_attempts(self) -> int:
        return MAX_ATTEMPTS

    def backoff(self, attempt: int) -> float:
        """指数退避 + 抖动：避免所有提案者在同一时刻再次撞车（Basic Paxos 活锁的克星）。"""
        base = min(0.002 * (2 ** (attempt - 1)), 0.05)
        return base * (0.5 + random.random())

    def _rng_jump(self) -> int:
        """撞车后随机往前跳几个 slot，尽快跳出"两个提案者步长相同"的共振。"""
        return random.randint(1, 3)

    async def stop(self) -> None:  # pragma: no cover - 子类按需覆盖
        return None


# --------------------------------------------------------------------- Basic
class BasicProposer(Proposer):
    """Basic Paxos：每个提案独立跑 Prepare + Accept，共 2 轮 RTT。

    没有 Leader 意味着**任何节点都能提案**，于是并发提案一定会撞车：
    两个提案者互相用更大的编号把对方顶掉，就可能活锁（livelock）。
    本实现用"随机退避 + 换新 slot 重试"来打破活锁，并把每次重试都计入统计，
    这样实验里可以直接看到"冲突率 → 延迟长尾"的因果关系。
    """

    name = "basic"

    async def submit(self, command: str, req_id: str = "") -> ProposalResult:
        attempts = 0
        for attempt in range(1, self.max_attempts() + 1):
            attempts = attempt
            slot = self.node.alloc_slot()
            ballot = self.next_ballot()

            # ---- Phase 1: Prepare / Promise（第 1 轮 RTT）----
            promises = await fanout(
                self.node,
                self.node.all_ids,
                {"t": "prepare", "ins": slot, "b": ballot.to_list()},
                self.node.quorum,
                self.node.attempt_timeout,
            )
            granted = [p for p in promises if p.get("ok")]
            if len(granted) < self.node.quorum:
                for payload in promises:
                    if payload.get("promised"):
                        self.node.observe_ballot(Ballot.parse(payload["promised"]))
                self.retries += 1
                self.node.counters["retries"] = self.node.counters.get("retries", 0) + 1
                await asyncio.sleep(self.backoff(attempt))
                continue

            # 如果多数派里已经有人接受过值，必须沿用编号最大的那个
            highest = pick_highest_accepted(granted)
            value = highest[1] if highest else command
            adopted_other = highest is not None and highest[1] != command
            if adopted_other:
                self.node.counters["adopted"] = self.node.counters.get("adopted", 0) + 1

            # ---- Phase 2: Accept / Accepted（第 2 轮 RTT）----
            accepted = await fanout(
                self.node,
                self.node.all_ids,
                {"t": "accept", "b": ballot.to_list(), "entries": [[slot, value]]},
                self.node.quorum,
                self.node.attempt_timeout,
            )
            good = [p for p in accepted if slot in (p.get("accepted") or [])]
            if len(good) >= self.node.quorum:
                self.node.on_chosen(slot, value, ballot)
                if adopted_other:
                    # 这个 slot 已经被别人的值占了：先帮它落地（避免日志空洞），
                    # 自己的命令换个新 slot 重来 —— 这就是 Basic Paxos 撞车的代价。
                    self.node.counters["conflicts"] = self.node.counters.get("conflicts", 0) + 1
                    self.retries += 1
                    self.node.skip_slots(self._rng_jump())
                    await asyncio.sleep(self.backoff(attempt))
                    continue
                return ProposalResult(
                    ok=True, slot=slot, value=value, attempts=attempts, ballot=ballot, chosen_value=value
                )

            for payload in accepted:
                if payload.get("promised"):
                    self.node.observe_ballot(Ballot.parse(payload["promised"]))
            self.retries += 1
            self.node.skip_slots(self._rng_jump())
            await asyncio.sleep(self.backoff(attempt))

        return ProposalResult(ok=False, err="basic_paxos_exhausted", attempts=attempts)


# --------------------------------------------------------------------- Multi
@dataclass
class _BatchItem:
    """Leader 批处理队列里的一条待提案命令。"""

    slot: int
    command: str
    req_id: str
    future: asyncio.Future[ProposalResult] = field(repr=False)


class MultiPaxosProposer(Proposer):
    """Multi-Paxos：先选出一个稳定 Leader，之后所有提案跳过 Phase 1。

    Leader 只需一次 Phase 1（"覆盖全部实例"的 Prepare）就能接管，此后每条
    命令只花 1 轮 RTT。再加上两个工程优化：

    * **批处理（batching）**：把同一小段时间窗口内的多条命令塞进一条 Accept，
      分摊一次 RTT 和一次 fsync；
    * **流水线（pipelining）**：多批 Accept 同时在途，不必等上一批回来。

    代价是 Leader 变成单点：Leader 一挂就要重新选主（一次 Phase 1）。
    """

    name = "multipaxos"

    def __init__(self, node: PaxosNode) -> None:
        super().__init__(node)
        self.epoch: Ballot | None = None
        self.is_leader = False
        self._batch: list[_BatchItem] = []
        self._batch_tasks: set[asyncio.Task[None]] = set()
        self._wake = asyncio.Event()
        self._batch_loop_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._recovery_task: asyncio.Task[None] | None = None
        self._repair_task: asyncio.Task[None] | None = None
        self._inflight = 0
        self._stop = False
        self.leader_changes = 0
        self._last_reopen = 0.0

    # ---------------------------------------------------------------- 生命周期
    def start(self) -> None:
        self._batch_loop_task = asyncio.create_task(self._batch_loop(), name="mp-batch")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="mp-heartbeat")
        self._repair_task = asyncio.create_task(self._repair_loop(), name="mp-repair")

    async def _repair_loop(self) -> None:
        """Leader 的空槽修复协程。

        任何一个"被少数 Acceptor 接受、但没能拿到多数派"的 slot 都会变成空洞，
        Learner 会卡在空洞处不再前进。Leader 用自己的 epoch 重新走一次
        Phase 1（幂等）拿到别人接受过的值，再 Accept 一次把它定下来；
        如果谁都没接受过，就填一个 noop。
        """
        while not self._stop:
            await asyncio.sleep(0.2)
            try:
                if not self.is_leader or self.epoch is None:
                    continue
                holes = self.node.undecided_slots(limit=8, min_age=0.8)
                for slot in holes:
                    await self._repair_slot(slot)
            except Exception:  # pragma: no cover - 修复失败不该打死 Leader
                log.debug("[%s] 空槽修复异常", self.node.id, exc_info=True)

    async def _repair_slot(self, slot: int, preferred_value: str = "") -> None:
        """用一个已经拿到的 epoch 编号收尾某个 slot（不抬轮次，所以不会打断别人）。

        ``preferred_value`` 是请求方希望落地的命令。沿用"最高轮次 + 最大计数"的
        规则：如果这个 slot 上已经有人接受过值，就必须沿用那个值；只有在谁都没
        接受过的时候，才用 ``preferred_value``（否则填 noop）。
        """
        assert self.epoch is not None
        ballot = self.epoch
        promises = await fanout(
            self.node,
            self.node.all_ids,
            {"t": "prepare", "ins": slot, "b": ballot.to_list()},
            self.node.quorum,
            self.node.attempt_timeout,
        )
        granted = [p for p in promises if p.get("ok")]
        if len(granted) < self.node.quorum:
            return
        default = preferred_value or self._noop(slot)
        value, _b, _count = resolve_value(granted, slot, default)
        replies = await fanout(
            self.node,
            self.node.all_ids,
            {"t": "accept", "b": ballot.to_list(), "entries": [[slot, value]]},
            self.node.quorum,
            self.node.attempt_timeout,
        )
        if len([p for p in replies if slot in (p.get("accepted") or [])]) >= self.node.quorum:
            self.node.on_chosen(slot, value, ballot)
            self.node.counters["repaired"] = self.node.counters.get("repaired", 0) + 1

    async def stop(self) -> None:
        self._stop = True
        for task in (self._batch_loop_task, self._heartbeat_task, self._recovery_task, self._repair_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        for item in self._batch:
            if not item.future.done():
                item.future.set_result(ProposalResult(ok=False, err="shutdown"))
        self._batch.clear()

    # -------------------------------------------------------------------- 选主
    async def ensure_leader(self, *, force: bool = False) -> bool:
        """通过一次"覆盖全部实例"的 Phase 1 抢到 Leader 身份。

        Phase 1 之后 Leader 会知道"哪些 slot 已经被某些 Acceptor 接受过但还没
        确定" —— 这些 slot 必须用**原来的值**重新走一遍 Accept，否则就会出现
        日志空洞（后续 slot 永远等不到前面的确定值）。这就是"崩溃恢复"要做的
        事，也是 Leader 切换时那段延迟的主要来源。
        """
        if self.is_leader and not force and self.epoch is not None:
            return True
        for attempt in range(3):
            ballot = self.next_ballot()
            promises = await fanout(
                self.node,
                self.node.all_ids,
                {"t": "prepare", "ins": -1, "b": ballot.to_list()},
                self.node.quorum,
                self.node.attempt_timeout,
            )
            granted = [p for p in promises if p.get("ok")]
            if len(granted) < self.node.quorum:
                for payload in promises:
                    if payload.get("promised"):
                        self.node.observe_ballot(Ballot.parse(payload["promised"]))
                await asyncio.sleep(self.backoff(attempt + 1))
                continue

            # 找出"可能已选定但本地还不知道"的 slot，交给恢复协程重新走 Accept
            pending_slots: set[int] = set()
            for payload in granted:
                for ins, _raw_ballot, _value in payload.get("accepted", []) or []:
                    slot = int(ins)
                    if not self.node.has_local_choice(slot):
                        pending_slots.add(slot)

            self.epoch = ballot
            self.is_leader = True
            self.leader_changes += 1
            self.node.set_leader(self.node.id, ballot)
            self.node.rebuild_slot_allocator()
            # 立刻宣告一次身份，缩短其他副本的"无主"窗口
            self.node.broadcast(
                {
                    "t": "heartbeat",
                    "b": ballot.to_list(),
                    "leader": self.node.id,
                    "commit_index": self.node.learner.commit_index,
                }
            )
            log.info(
                "[%s] 成为 Leader, ballot=%s, 待恢复 slot=%d",
                self.node.id,
                ballot,
                len(pending_slots),
            )
            if pending_slots:
                self._start_recovery(granted, pending_slots)
            return True
        return False

    def _start_recovery(self, granted: list[dict[str, Any]], slots: set[int]) -> None:
        """后台补齐"已接受未确定"的 slot（不阻塞客户端请求）。"""
        if self._recovery_task is not None and not self._recovery_task.done():
            return
        self._recovery_task = asyncio.create_task(self._recover(granted, slots), name="mp-recover")

    async def _recover(self, granted: list[dict[str, Any]], slots: set[int]) -> None:
        assert self.epoch is not None
        budget = max(slots) + 1 if slots else 0
        # 空洞用 noop 补（Paxos 里"占位"是合法且必要的，ZooKeeper 的 no-op 同源）
        fill = [slot for slot in range(budget) if slot not in slots and not self.node.has_local_choice(slot)]
        entries: list[list[Any]] = []
        for slot in sorted(slots):
            value, _ballot, _count = resolve_value(granted, slot, self._noop(slot))
            entries.append([slot, value])
        for slot in fill:
            entries.append([slot, self._noop(slot)])
        if not entries:
            return
        assert self.epoch is not None
        epoch_ballot = self.epoch
        for start in range(0, len(entries), self.node.batch_size):
            chunk = entries[start : start + self.node.batch_size]
            replies = await fanout(
                self.node,
                self.node.all_ids,
                {"t": "accept", "b": epoch_ballot.to_list(), "entries": chunk},
                self.node.quorum,
                self.node.attempt_timeout,
            )
            if len([p for p in replies if p.get("ok")]) < self.node.quorum:
                self.step_down("恢复阶段 accept 失败")
                return
            for slot, value in chunk:
                self.node.on_chosen(int(slot), str(value), epoch_ballot)
        log.info("[%s] 恢复完成，补齐 %d 个 slot", self.node.id, len(entries))

    @staticmethod
    def _noop(slot: int) -> str:
        return json.dumps({"req": f"noop-{slot}", "op": "noop"}, separators=(",", ":"))

    def step_down(self, reason: str) -> None:
        if self.is_leader:
            log.info("[%s] 让位: %s", self.node.id, reason)
        self.is_leader = False
        self.node.counters["step_downs"] = self.node.counters.get("step_downs", 0) + 1

    # -------------------------------------------------------------------- 提案
    async def submit(self, command: str, req_id: str = "") -> ProposalResult:
        if not self.is_leader:
            leader = self.node.leader_id
            if leader and leader != self.node.id and self.node.forward_enabled:
                # 转发超时用 attempt_timeout（而不是客户端超时）：Leader 挂了要
                # **立刻**告诉客户端失败，让它可以重试/换节点，而不是干等 5 秒。
                payload = await self.node.rpc(
                    leader,
                    {"t": "client_req", "cmd": command, "req": req_id, "fwd": True},
                    timeout=self.node.attempt_timeout,
                )
                return ProposalResult(
                    ok=bool(payload.get("ok")),
                    slot=int(payload.get("slot", -1)),
                    value=command,
                    chosen_value=str(payload.get("value", "")),
                    err=str(payload.get("err", "")),
                    redirect=bool(payload.get("redirect", False)) or not payload.get("ok", False),
                )
            return ProposalResult(ok=False, err="not_leader", redirect=True, value=command)

        for attempt in range(1, 4):
            if not await self.ensure_leader():
                return ProposalResult(ok=False, err="election_failed", redirect=True, attempts=attempt)
            assert self.epoch is not None
            slot = self.node.alloc_slot()
            future: asyncio.Future[ProposalResult] = asyncio.get_running_loop().create_future()
            self._batch.append(_BatchItem(slot=slot, command=command, req_id=req_id, future=future))
            self._wake.set()
            try:
                result = await asyncio.wait_for(future, timeout=self.node.client_timeout)
            except asyncio.TimeoutError:
                return ProposalResult(ok=False, err="timeout", attempts=attempt)
            if result.ok:
                return result
            self.retries += 1
            await asyncio.sleep(self.backoff(attempt))
        return ProposalResult(ok=False, err="leader_retry_exhausted")

    # ---------------------------------------------------------------- 批处理协程
    async def _batch_loop(self) -> None:
        """攒批 + 起飞。

        这里**不能用 sleep 轮询**：Windows 上 ``asyncio.sleep(0.001)`` 实际要睡
        15.6ms，一个 1ms 的 RTT 会被凭空放大一个数量级。所以用 ``asyncio.Event``
        精确唤醒，只有"当前批次已满/有在途批次"时才短暂等待。
        """
        while not self._stop:
            if not self._batch or self._inflight >= self.node.max_inflight:
                self._wake.clear()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), timeout=0.05)
                continue
            window = self.node.batch_ms / 1000.0
            if window > 0:
                # 攒批窗口：让同一时间片的请求挤进同一次 Accept
                await asyncio.sleep(window)
            items = self._batch[: self.node.batch_size]
            del self._batch[: self.node.batch_size]
            if not items:
                continue
            self._inflight += 1
            # 每批一个任务（流水线）：必须持有引用，否则可能被 GC 掉导致一批请求悬空
            task = asyncio.create_task(self._drive_batch(items))
            self._batch_tasks.add(task)
            task.add_done_callback(self._batch_tasks.discard)
            await asyncio.sleep(0)  # 让出一次，尽快收敛同一批请求

    async def _drive_batch(self, items: list[_BatchItem]) -> None:
        try:
            await self._run_batch(items)
        except Exception:  # pragma: no cover - 兜底，避免协程静默死掉
            log.exception("[%s] 批处理异常", self.node.id)
            for item in items:
                if not item.future.done():
                    item.future.set_result(ProposalResult(ok=False, err="internal"))
        finally:
            self._inflight -= 1
            self._wake.set()

    async def _run_batch(self, items: list[_BatchItem]) -> None:
        assert self.epoch is not None
        ballot = self.epoch
        entries = [[item.slot, item.command] for item in items]
        replies = await fanout(
            self.node,
            self.node.all_ids,
            {"t": "accept", "b": ballot.to_list(), "entries": entries},
            self.node.quorum,
            self.node.attempt_timeout,
        )
        # 统计"整批都被接受"的 Acceptor 个数；只要够多数派，这批命令就全部选定
        accepted_count = 0
        for payload in replies:
            if not payload.get("ok"):
                if payload.get("promised"):
                    self.node.observe_ballot(Ballot.parse(payload["promised"]))
                continue
            accepted_count += 1
        if accepted_count >= self.node.quorum:
            for item in items:
                self.node.on_chosen(item.slot, item.command, ballot)
                if not item.future.done():
                    item.future.set_result(
                        ProposalResult(
                            ok=True,
                            slot=item.slot,
                            value=item.command,
                            chosen_value=item.command,
                            ballot=ballot,
                        )
                    )
        else:
            # 关键区分：**超时 ≠ 丢权**。只有当某个 Acceptor 明确报告了更高的
            # 提案编号时才说明自己被顶掉了；单纯超时（网络慢/磁盘慢）应该重试同一
            # 批，而不是立刻让位引发选主风暴。
            preempted = any(
                payload.get("promised") and Ballot.parse(payload["promised"]) > ballot for payload in replies
            )
            if preempted:
                self.step_down("accept 收到更高编号")
                self.node.set_leader(None, None)
            for item in items:
                if not item.future.done():
                    item.future.set_result(
                        ProposalResult(ok=False, err="preempted" if preempted else "accept_incomplete")
                    )

    # ------------------------------------------------------------------ 心跳
    async def _heartbeat_loop(self) -> None:
        """Leader 周期性发心跳：宣告身份、广播已提交位置、并给 Fast Paxos 提供开放轮次。"""
        while not self._stop:
            await asyncio.sleep(self.node.heartbeat_ms / 1000.0)
            if not self.is_leader or self.epoch is None:
                continue
            self.node.broadcast(
                {
                    "t": "heartbeat",
                    "b": self.epoch.to_list(),
                    "leader": self.node.id,
                    "commit_index": self.node.learner.commit_index,
                }
            )

    # ------------------------------------------------------------------ 观测
    def state(self) -> dict[str, Any]:
        return {
            "is_leader": self.is_leader,
            "epoch": self.epoch.to_list() if self.epoch else None,
            "batch_pending": len(self._batch),
            "inflight_batches": self._inflight,
            "leader_changes": self.leader_changes,
            "retries": self.retries,
        }


# --------------------------------------------------------------------- Fast
class FastPaxosProposer(MultiPaxosProposer):
    """Fast Paxos：任意节点都可以把值直接投给 Acceptor，只需 1 轮 RTT。

    与 Multi-Paxos 的差别只有一处，但很关键：

    * Multi-Paxos 的提案必须**流经 Leader**（客户 → 就近副本 → Leader → Acceptor）；
    * Fast Paxos 里 Leader 只是"把轮次打开"（做一次 Phase 1 拿到承诺），之后
      **任何一个副本都可以用自己的客户端值直接 Accept**，值不必流经 Leader。

    代价是冲突：同一个轮次里不同副本投了不同的值，Acceptor 只能接受先到的那个，
    另一部分请求就得回退到经典轮再谈一次。所以"冲突率"是本机制的核心指标。
    """

    name = "fast"

    def __init__(self, node: PaxosNode) -> None:
        super().__init__(node)
        self.conflicts = 0
        self.fast_accepts = 0
        self.fallbacks = 0
        self._last_reopen = 0.0
        #: 我已经知道"至少要这么大的轮次才有人听"（被更高编号顶过之后的记忆）
        self._need_round = 0

    def max_attempts(self) -> int:
        return 24

    # --------------------------------------------------------------- 打开轮次
    async def submit(self, command: str, req_id: str = "") -> ProposalResult:
        """Fast Paxos 的客户端路径。

        关键点：**客户端请求不必流经 Leader**。Follower 拿着 Leader 已经打开的
        轮次编号，直接把值投给全体 Acceptor —— 比 Multi-Paxos 少了一跳。
        """
        attempts = 0
        deadline = self.node.loop_time() + self.node.client_timeout
        while attempts < self.max_attempts() and self.node.loop_time() < deadline:
            ballot = await self._wait_open_round(deadline)
            if ballot is None:
                return ProposalResult(ok=False, err="no_open_round", redirect=True)
            attempts += 1
            slot = self.node.alloc_slot()

            # ---- 快速轮：直接 Accept，1 轮 RTT ----
            replies = await fanout(
                self.node,
                self.node.all_ids,
                {"t": "accept", "b": ballot.to_list(), "entries": [[slot, command]]},
                self.node.fast_quorum,
                self.node.attempt_timeout,
            )
            self.fast_accepts += 1
            winners = [p for p in replies if slot in (p.get("accepted") or [])]
            if len(winners) >= self.node.fast_quorum:
                self.node.on_chosen(slot, command, ballot)
                return ProposalResult(
                    ok=True, slot=slot, value=command, chosen_value=command, ballot=ballot, attempts=attempts
                )

            conflict = any(p.get("conflict") for p in replies)
            # 只有**严格更高**的编号才叫"被顶掉"；同一个 ballot 上的拒绝只是
            # "同一个槽被别人先占了"，属于正常的值冲突，不能让整轮作废。
            preempted = False
            for payload in replies:
                raw = payload.get("promised")
                if not raw:
                    continue
                seen = Ballot.parse(raw)
                self.node.observe_ballot(seen)
                if seen > ballot:
                    preempted = True
                    self._need_round = max(self._need_round, seen.round + 1)

            # 冲突 / 抢输了的处理策略：**换一个新 slot 重投**，抢输的那个 slot 交给
            # Leader 的空槽修复统一收尾（`repair` 通知 → Leader 用 epoch 编号跑经典轮）。
            #
            # 为什么不让每个提案者自己跑经典轮：实验里试过，3 个提案者会各自用一个
            # 新编号 (r, me) 抢同一个 slot 的收尾权，互相顶掉，并把正在使用的快速轮
            # 作废（(r, me) > (r, -1)），于是所有人一起失败 → 重开 → 再失败。
            # 实测把 3 节点 8 并发的吞吐从 ~170 ops/s 打到 **0**。
            # 让"收尾"这件事只有一个执行者（Leader），是这题最值钱的一条工程结论。
            self.node.counters["conflicts"] = self.node.counters.get("conflicts", 0) + 1
            self.node.skip_slots(self._rng_jump())
            if preempted:
                await self._request_reopen()
            if self.node.leader_id:
                # 请 Leader 收尾自己这个 slot（把我的值一起带过去，避免命令白丢）
                self.node.send(
                    self.node.leader_id,
                    {"t": "repair", "ins": slot, "value": command},
                )
                if await self._await_chosen(slot, command):
                    return ProposalResult(
                        ok=True,
                        slot=slot,
                        value=command,
                        chosen_value=command,
                        ballot=ballot,
                        attempts=attempts,
                    )
            await asyncio.sleep(self.backoff(attempts))
            if conflict:
                self.conflicts += 1
        return ProposalResult(ok=False, err="fast_paxos_exhausted", attempts=attempts)

    async def _await_chosen(self, slot: int, command: str) -> bool:
        """等 Leader 把自己这个 slot 收尾（最多等两轮 RTT 多一点）。"""
        deadline = self.node.loop_time() + min(0.5, 2.5 * self.node.attempt_timeout)
        while self.node.loop_time() < deadline:
            if self.node.learner.chosen.get(slot) == command:
                return True
            await asyncio.sleep(0.005)
        return False

    async def _wait_open_round(self, deadline: float) -> Ballot | None:
        """等一个可用的快速轮次；没有就去催 Leader 重开一轮。

        等待窗口**故意不看齐客户端超时**：Leader 挂掉时应该尽快返回
        ``no_open_round`` 让客户端重试，而不是把客户端连接挂满 5 秒。
        """
        wait_until = min(deadline, self.node.loop_time() + max(1.0, 4.0 * self.node.cfg.rtt_ms / 1000.0))
        while self.node.loop_time() < wait_until:
            ballot = await self._open_round()
            if ballot is not None:
                return ballot
            await self._request_reopen()
            await asyncio.sleep(0.02)
        return None

    async def _open_round(self) -> Ballot | None:
        """Leader 用自己的轮次；Follower 复用 Leader 广播出来的轮次。"""
        ballot = self.epoch if self.is_leader else self.node.fast_ballot
        if ballot is None or ballot.round < self._need_round:
            return None
        return ballot

    async def _classic_round(self, slot: int, fast_ballot: Ballot, command: str) -> ProposalResult | None:
        """冲突后的经典轮：同一轮次 + 自己的编号 > 快速轮编号，因此可以覆盖它。"""
        ballot = Ballot(max(fast_ballot.round, self._need_round), self.node.ballot_id)
        promises = await fanout(
            self.node,
            self.node.all_ids,
            {"t": "prepare", "ins": slot, "b": ballot.to_list()},
            self.node.quorum,
            self.node.attempt_timeout,
        )
        granted = [p for p in promises if p.get("ok")]
        if len(granted) < self.node.quorum:
            for payload in promises:
                if payload.get("promised"):
                    self.node.observe_ballot(Ballot.parse(payload["promised"]))
            return None

        value = self._resolve_value(granted, slot, command)
        accepted = await fanout(
            self.node,
            self.node.all_ids,
            {"t": "accept", "b": ballot.to_list(), "entries": [[slot, value]]},
            self.node.quorum,
            self.node.attempt_timeout,
        )
        if len([p for p in accepted if slot in (p.get("accepted") or [])]) >= self.node.quorum:
            self.node.on_chosen(slot, value, ballot)
            return ProposalResult(ok=True, slot=slot, value=value, chosen_value=value, ballot=ballot)
        return None

    def _resolve_value(self, granted: list[dict[str, Any]], slot: int, command: str) -> str:
        """冲突恢复时到底该提案哪个值（安全性论证见 ``resolve_value``）。"""
        value, ballot, count = resolve_value(granted, slot, command)
        if value != command and count:
            self.node.counters["adopted"] = self.node.counters.get("adopted", 0) + 1
            log.info(
                "[%s] slot=%d 在轮次 %s 上已有 %d 个 Acceptor 接受过别的值，改为沿用该值",
                self.node.id,
                slot,
                ballot,
                count,
            )
        return value

    async def _request_reopen(self) -> None:
        """轮次被顶掉时，请 Leader 用一个更大的轮次重新打开。

        必须限流：每次重开都意味着一次全量 Phase 1，会把所有在途的快速轮 Accept
        作废。如果每个失败请求都去催一次，集群会陷入"重开 → 失败 → 再重开"的
        死亡螺旋（这是调试本工程时踩过的最深的一个坑）。
        """
        leader = self.node.leader_id
        now = self.node.loop_time()
        if not leader or now - self._last_reopen < 0.5:
            return
        self._last_reopen = now
        if leader == self.node.id:
            # 我自己就是 Leader：直接自己换个更大的轮次
            await self.ensure_leader(force=True)
            return
        with contextlib.suppress(asyncio.TimeoutError, ConnectionError, OSError):
            await self.node.rpc(
                leader,
                {"t": "reopen", "min_round": self._need_round},
                timeout=self.node.attempt_timeout,
            )

    def state(self) -> dict[str, Any]:
        data = super().state()
        data.update(
            {
                "fast_accepts": self.fast_accepts,
                "conflicts": self.conflicts,
                "fallbacks": self.fallbacks,
                "conflict_rate": round(self.conflicts / self.fast_accepts, 4) if self.fast_accepts else 0.0,
            }
        )
        return data


def make_proposer(node: PaxosNode) -> Proposer:
    """按配置创建提案者。"""
    kind = node.protocol
    if kind == "basic":
        return BasicProposer(node)
    if kind == "multipaxos":
        return MultiPaxosProposer(node)
    if kind == "fast":
        return FastPaxosProposer(node)
    raise ValueError(f"未知的提案机制: {kind}")
