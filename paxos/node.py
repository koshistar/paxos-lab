"""节点主程序：网络收发 + 三种角色黏合 + admin 控制口。

一个节点同时承担：

* **Proposer** —— 客户端请求进来后发起提案（具体策略见 ``proposer.py``）；
* **Acceptor** —— 对别人的提案投票，并把承诺/接受落盘；
* **Learner**  —— 学习已被选定的值，按 slot 顺序应用状态机；
* **Listener** —— 用 ``--listener`` 启动的只读节点：只连不发、不参与投票，
  用来观察"旁观者视角下的日志是否与参与者一致"。

管理口（``--admin-port``）暴露的是"实验室遥控器"：查看状态、注入时延/丢包/分区、
模拟崩溃、导出决定日志。实验脚本全部通过它来编排，而不是靠 ps/kill 这种
平台相关的手段。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .acceptor import Acceptor
from .ballot import (
    Ballot,
    fast_quorum_size,
    max_failures,
    quorum_size,
)
from .kv import KVStateMachine
from .learner import Learner
from .proposer import FastPaxosProposer, MultiPaxosProposer, ProposalResult, make_proposer
from .storage import Journal
from .timer import enable_fine_timers
from .transport import Counter, FaultProfile, PeerLink, decode, enable_nodelay, encode

log = logging.getLogger(__name__)


# --------------------------------------------------------------------- 配置
@dataclass
class NodeConfig:
    """一个节点的全部可调参数（都能从命令行传入，便于做实验）。"""

    node_id: str
    peers: dict[str, tuple[str, int]]
    port: int
    admin_port: int
    data_dir: Path
    protocol: str = "multipaxos"
    fsync: bool = True
    rtt_ms: float = 1.0
    loss: float = 0.0
    jitter_ms: float = 0.0
    batch_ms: float = 0.0
    batch_size: int = 64
    max_inflight: int = 16
    heartbeat_ms: float = 60.0
    election_ms: float = 400.0
    listen_host: str = "127.0.0.1"
    client_timeout: float = 10.0
    listener: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "peers": {k: list(v) for k, v in self.peers.items()},
            "port": self.port,
            "admin_port": self.admin_port,
            "data_dir": str(self.data_dir),
            "protocol": self.protocol,
            "fsync": self.fsync,
            "rtt_ms": self.rtt_ms,
            "loss": self.loss,
            "jitter_ms": self.jitter_ms,
            "batch_ms": self.batch_ms,
            "batch_size": self.batch_size,
            "max_inflight": self.max_inflight,
            "heartbeat_ms": self.heartbeat_ms,
            "election_ms": self.election_ms,
            "listener": self.listener,
        }


# --------------------------------------------------------------------- 节点
class PaxosNode:
    """一个 Paxos 副本进程。"""

    def __init__(self, cfg: NodeConfig) -> None:
        self.cfg = cfg
        self.id = cfg.node_id
        self.protocol = cfg.protocol
        self.fsync = cfg.fsync

        self.all_ids: list[str] = sorted(cfg.peers)
        self.peer_ids: list[str] = [peer for peer in self.all_ids if peer != self.id]
        self.ballot_id = self.all_ids.index(self.id)
        self.quorum = quorum_size(len(self.all_ids))
        self.fast_quorum = fast_quorum_size(len(self.all_ids))
        self.majority_failures = max_failures(len(self.all_ids))

        self.counter = Counter()
        self.counters: dict[str, int] = {
            "retries": 0,
            "conflicts": 0,
            "adopted": 0,
            "step_downs": 0,
            "elections": 0,
            "commits": 0,
            "client_ok": 0,
            "client_err": 0,
        }

        self._started_at = time.time()
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._handler_tasks: set[asyncio.Task[Any]] = set()
        self._repairing: set[int] = set()
        self._rid = 0
        self._stop = asyncio.Event()
        self._background: list[asyncio.Task[None]] = []

        # ---- 存储与角色 ----
        data_dir = Path(cfg.data_dir)
        # Acceptor 的承诺必须扛住掉电（fsync）；fsync=off 时退化成用户态缓冲，
        # 用来做"关掉 fsync 会怎样"的对照实验。
        self.acceptor_journal = Journal(data_dir / "acceptor.log", mode="fsync" if cfg.fsync else "buffer")
        # Learner 的决定日志是派生数据：flush 就够了（进程被 kill 不丢，掉电能重学）。
        self.learner_journal = Journal(data_dir / "learner.log", mode="flush")
        self.state_machine = KVStateMachine()
        self.applied_index = -1
        self.acceptor = Acceptor(self.id, self.acceptor_journal, fsync=cfg.fsync) if not cfg.listener else None
        self.learner = Learner(
            self.id,
            self.learner_journal,
            on_apply=self._apply_slot,
            fsync=cfg.fsync,
        )

        # ---- Leader / 轮次 ----
        self.leader_id: str | None = None
        self.fast_ballot: Ballot | None = None
        self._max_round = 0
        self._next_slot = 0
        self._hole_since: dict[int, float] = {}
        self._last_heartbeat = time.monotonic()
        self.leader_changes = 0
        self.forward_enabled = True

        # ---- 链路 ----
        self.profile = FaultProfile(rtt_ms=cfg.rtt_ms, loss=cfg.loss, jitter_ms=cfg.jitter_ms)
        self.links: dict[str, PeerLink] = {
            peer: PeerLink(
                local_id=self.id,
                peer_id=peer,
                addr=cfg.peers[peer],
                profile=self.profile,
                counter=self.counter,
                on_message=self._on_message,
            )
            for peer in self.peer_ids
        }

        self.proposer = make_proposer(self)
        self._rebuild_slot_allocator()

        # ---- 报文处理表 ----
        self._requests: dict[str, Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]] = {
            "prepare": self._on_prepare,
            "accept": self._on_accept,
            "learn_ask": self._on_learn_ask,
            "client_req": self._on_client_req,
            "status": self._on_status_req,
            "ping": self._on_ping,
        }
        self._notifications: dict[str, Callable[[dict[str, Any]], Awaitable[None]]] = {
            "commit": self._on_commit,
            "heartbeat": self._on_heartbeat,
            "reopen": self._on_reopen,
            "repair": self._on_repair,
        }

    # ================================================================ 基础工具
    def loop_time(self) -> float:
        return time.monotonic()

    @property
    def attempt_timeout(self) -> float:
        """单轮 RPC 的超时：随注入的 RTT 放大，并给 fsync 留足时间。

        太小会让"磁盘慢"被误判成"网络丢包"，进而触发无谓的重试与选主风暴
        （实验里踩过这个坑：100ms 的超时让 32 并发的 Basic Paxos 直接活锁）。
        """
        return max(0.3, 10.0 * self.cfg.rtt_ms / 1000.0 + 0.1)

    @property
    def client_timeout(self) -> float:
        return max(2.0, 12.0 * self.cfg.rtt_ms / 1000.0 + 0.5)

    @property
    def election_timeout(self) -> float:
        return self.cfg.election_ms / 1000.0

    # 便捷参数代理：proposer 直接用得到
    @property
    def heartbeat_ms(self) -> float:
        return self.cfg.heartbeat_ms

    @property
    def batch_ms(self) -> float:
        return self.cfg.batch_ms

    @property
    def batch_size(self) -> int:
        return self.cfg.batch_size

    @property
    def max_inflight(self) -> int:
        return self.cfg.max_inflight

    def bump_round(self) -> int:
        """分配一个比"已知最大轮次"更大的轮次，保证提案编号严格递增。"""
        self._max_round += 1
        return self._max_round

    def observe_ballot(self, ballot: Ballot) -> None:
        if ballot.round > self._max_round:
            self._max_round = ballot.round

    # ------------------------------------------------------------------ slot
    def _rebuild_slot_allocator(self) -> None:
        highest = -1
        if self.learner.chosen:
            highest = max(highest, max(self.learner.chosen))
        if self.acceptor is not None and self.acceptor.accepted:
            highest = max(highest, max(self.acceptor.accepted))
        self._next_slot = max(self._next_slot, highest + 1)

    def rebuild_slot_allocator(self) -> None:
        """公开版本：选主/恢复后重新对齐 slot 分配器。"""
        self._rebuild_slot_allocator()

    def alloc_slot(self) -> int:
        """分配一个 slot（实例编号）。Basic/Fast 下不同节点会分到同一个 slot，
        这正是"并发提案冲突"的来源；Multi-Paxos 下只有 Leader 分配，天然不冲突。"""
        self._rebuild_slot_allocator()
        slot = self._next_slot
        self._next_slot += 1
        return slot

    def skip_slots(self, count: int = 1) -> None:
        """撞车后主动往前跳几个 slot，避免所有提案者以同样的步长反复撞在同一点。"""
        self._next_slot += max(1, count)

    def has_local_choice(self, slot: int) -> bool:
        return slot in self.learner.chosen

    def undecided_slots(self, limit: int = 8, min_age: float = 1.0) -> list[int]:
        """列出"已经被某些 Acceptor 接受过、但还没被选定"的 slot（空槽）。

        Leader 会把它们补成一个值（原值或 noop）。不补的话 Learner 会永远停在
        空洞处——这是 Paxos 里非常容易被忽略、但一定会踩到的工程问题。
        """
        top = -1
        if self.learner.chosen:
            top = max(top, max(self.learner.chosen))
        if self.acceptor is not None and self.acceptor.accepted:
            top = max(top, max(self.acceptor.accepted))
        if top < 0:
            return []
        now = self.loop_time()
        holes: list[int] = []
        for slot in range(self.learner.next_apply, top + 1):
            if slot in self.learner.chosen:
                continue
            first_seen = self._hole_since.setdefault(slot, now)
            if now - first_seen >= min_age:
                holes.append(slot)
            if len(holes) >= limit:
                break
        # 清掉已经解决的记录，避免字典无限增长
        for slot in list(self._hole_since):
            if slot in self.learner.chosen:
                self._hole_since.pop(slot, None)
        return holes

    # ------------------------------------------------------------------ 状态机
    def _apply_slot(self, slot: int, value: str) -> None:
        self.applied_index = max(self.applied_index, slot)
        self.state_machine.apply(value)

    def on_chosen(self, slot: int, value: str, ballot: Ballot | None) -> None:
        """某个值被多数派接受了 —— 本地学会它，并广播给其他 Learner。"""
        self.counters["commits"] += 1
        self.learner.choose(slot, value, ballot)
        self.broadcast({"t": "commit", "ins": slot, "v": value, "b": ballot.to_list() if ballot else None})

    def set_leader(self, leader: str | None, ballot: Ballot | None) -> None:
        if leader != self.leader_id:
            self.leader_changes += 1
            log.info("[%s] Leader -> %s", self.id, leader or "-")
        self.leader_id = leader
        if ballot is not None:
            self.fast_ballot = ballot if self.protocol == "fast" else None
        elif self.protocol == "fast":
            self.fast_ballot = None
        # 无论上位还是让位都重置计时器：让位后至少等一个选举超时才重新参选，
        # 避免两个副本反复用更大的编号互相顶掉（选举风暴）。
        self._last_heartbeat = self.loop_time()

    # ================================================================ 网络发送
    def send(self, peer: str, msg: dict[str, Any]) -> None:
        if peer == self.id:
            return
        link = self.links.get(peer)
        if link is not None:
            link.send(msg)

    def broadcast(self, msg: dict[str, Any]) -> None:
        for peer in self.peer_ids:
            self.send(peer, msg)

    async def rpc(self, peer: str, msg: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        """带关联 id 的请求/应答。发给自己的请求直接本地处理，不经过网络。"""
        if peer == self.id:
            return await self._serve(str(msg.get("t")), msg)
        self._rid += 1
        rid = str(self._rid)
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[rid] = future
        self.send(peer, {**msg, "rid": rid})
        try:
            return await asyncio.wait_for(future, timeout or self.attempt_timeout)
        except asyncio.TimeoutError:
            if not future.done():
                future.cancel()
            return {"ok": False, "err": "timeout"}
        finally:
            self._pending.pop(rid, None)

    # ================================================================ 报文分发
    async def _on_message(self, peer_id: str, msg: dict[str, Any]) -> None:
        kind = str(msg.get("t", ""))
        if kind == "hello":
            return
        if kind == "rep":
            fut = self._pending.get(str(msg.get("rid")))
            if fut is not None and not fut.done():
                fut.set_result(msg.get("p") or {})
            return
        handler = self._notifications.get(kind)
        if handler is not None:
            await handler(msg)
            return
        if kind in self._requests:
            # 每个请求一个任务，这样慢请求（比如要 fsync）不会堵住同一条链路
            task = asyncio.create_task(self._respond(peer_id, msg))
            self._handler_tasks.add(task)
            task.add_done_callback(self._handler_tasks.discard)
            return
        log.warning("[%s] 收到未知报文类型 %r（来自 %s）", self.id, kind, peer_id)

    async def _respond(self, peer_id: str, msg: dict[str, Any]) -> None:
        payload = await self._serve(str(msg.get("t")), msg)
        rid = msg.get("rid")
        if rid is not None:
            self.send(peer_id, {"t": "rep", "rid": rid, "p": payload})

    async def _serve(self, kind: str, msg: dict[str, Any]) -> dict[str, Any]:
        handler = self._requests.get(kind)
        if handler is None:
            return {"ok": False, "err": f"unsupported:{kind}"}
        try:
            return await handler(msg)
        except Exception:  # pragma: no cover - 防御性
            log.exception("[%s] 处理 %s 出错", self.id, kind)
            return {"ok": False, "err": "internal"}

    # ------------------------------------------------------------ Acceptor 侧
    async def _on_prepare(self, msg: dict[str, Any]) -> dict[str, Any]:
        if self.acceptor is None:
            return {"ok": False, "err": "listener", "promised": None, "accepted": []}
        ballot = Ballot.parse(msg["b"])
        self.observe_ballot(ballot)
        return await self.acceptor.prepare(int(msg["ins"]), ballot)

    async def _on_accept(self, msg: dict[str, Any]) -> dict[str, Any]:
        ballot = Ballot.parse(msg["b"])
        self.observe_ballot(ballot)
        if self.acceptor is None:
            return {"ok": False, "err": "listener", "promised": None, "accepted": [], "conflict": False}
        entries = [(int(i), str(v)) for i, v in msg["entries"]]
        return await self.acceptor.accept(ballot, entries)

    # ------------------------------------------------------------- Learner 侧
    async def _on_commit(self, msg: dict[str, Any]) -> None:
        slot = int(msg["ins"])
        value = str(msg["v"])
        ballot = Ballot.parse(msg["b"]) if msg.get("b") else None
        self.learner.choose(slot, value, ballot)

    async def _on_learn_ask(self, msg: dict[str, Any]) -> dict[str, Any]:
        slot = int(msg["ins"])
        if slot in self.learner.chosen:
            return {"ok": True, "chosen": True, "value": self.learner.chosen[slot]}
        return {"ok": True, "chosen": False, "value": ""}

    async def _on_heartbeat(self, msg: dict[str, Any]) -> None:
        leader = str(msg.get("leader", ""))
        ballot = Ballot.parse(msg["b"])
        self.observe_ballot(ballot)
        self._last_heartbeat = self.loop_time()
        if leader != self.leader_id:
            self.leader_id = leader
            self.leader_changes += 1
        if self.protocol == "fast" and leader and leader != self.id:
            self.fast_ballot = ballot
        elif self.protocol == "multipaxos":
            self.fast_ballot = None
        # Leader 告知的 commit_index 可以用来触发缺口回填
        commit_index = int(msg.get("commit_index", -1))
        if commit_index > self.learner.commit_index and self.learner.missing(1):
            self.learner.note_gap()

    async def _on_repair(self, msg: dict[str, Any]) -> None:
        """Follower 请求 Leader 收尾一个"没被选定"的 slot（Fast Paxos 冲突后留下的空洞）。

        为什么要"请 Leader 收尾"而不是自己跑经典轮：经典轮需要一个新的提案编号，
        谁都能跑，但**多个提案者同时跑就会互相顶掉**，还会把别人正在用的快速轮作废
        （``(r, me) > (r, -1)``），实测能把 3 节点 8 并发的吞吐打到 0。
        统一交给 Leader 做，是"有 Leader 之后 Paxos 才好用"的又一个例证。

        Leader 用的是**自己已经拿到的 epoch 编号**（不抬轮次），所以不会干扰别人；
        ``value`` 是请求方希望落地的命令：如果这个 slot 上确实没人接受过值，
        Leader 就替它提出来 —— 这样客户端的命令不会白丢。
        """
        proposer = self.proposer
        if not isinstance(proposer, MultiPaxosProposer | FastPaxosProposer):
            return
        if not proposer.is_leader or proposer.epoch is None:
            return
        slot = int(msg.get("ins", -1))
        if slot < 0 or self.has_local_choice(slot) or slot in self._repairing:
            return
        value = str(msg.get("value", "") or "")
        self._repairing.add(slot)
        # 不阻塞链路上的其他消息（一个 repair 要 2 轮 RTT）
        task = asyncio.create_task(self._repair_one(slot, value))
        self._handler_tasks.add(task)
        task.add_done_callback(self._handler_tasks.discard)

    async def _repair_one(self, slot: int, value: str) -> None:
        """跑一次空槽收尾，结束后释放槽位上的"正在修复"标记。"""
        try:
            proposer = self.proposer
            if isinstance(proposer, MultiPaxosProposer | FastPaxosProposer):
                await proposer._repair_slot(slot, value)
        finally:
            self._repairing.discard(slot)

    async def _on_reopen(self, msg: dict[str, Any]) -> None:
        """Follower 发现自己的快速轮不够新时，请 Leader 处理。

        **关键设计**：如果 Leader 手上的轮次已经够用了，就只补发一次心跳（零成本）；
        只有当请求方需要更大的轮次时才重新 Phase 1。早期版本"收到 reopen 就
        Phase 1"，会让每次重开都作废全集群在途的 Accept，形成重开风暴。
        """
        if not isinstance(self.proposer, FastPaxosProposer):
            return
        proposer = self.proposer
        if not proposer.is_leader or proposer.epoch is None:
            return
        need = int(msg.get("min_round", 0) or 0)
        if proposer.epoch.round >= need:
            self.broadcast(
                {
                    "t": "heartbeat",
                    "b": proposer.epoch.to_list(),
                    "leader": self.id,
                    "commit_index": self.learner.commit_index,
                    "reopened": True,
                }
            )
            return
        now = self.loop_time()
        if now - proposer._last_reopen < self.election_timeout:
            return
        proposer._last_reopen = now
        if await proposer.ensure_leader(force=True) and proposer.epoch is not None:
            self.broadcast(
                {
                    "t": "heartbeat",
                    "b": proposer.epoch.to_list(),
                    "leader": self.id,
                    "commit_index": self.learner.commit_index,
                    "reopened": True,
                }
            )

    # -------------------------------------------------------------- 客户端侧
    async def _on_client_req(self, msg: dict[str, Any]) -> dict[str, Any]:
        command = str(msg.get("cmd", ""))
        req_id = str(msg.get("req", ""))
        _t0 = self.loop_time()
        result: ProposalResult = await self.proposer.submit(command, req_id)
        if os.environ.get("PAXOS_DEBUG_TIMING"):
            log.warning(
                "[%s] client_req 处理耗时 %.2f ms ok=%s err=%s",
                self.id,
                (self.loop_time() - _t0) * 1000,
                result.ok,
                result.err,
            )
        payload = result.to_dict()
        if not result.ok:
            self.counters["client_err"] += 1
            payload["leader"] = self.leader_id
            payload["node"] = self.id
            return payload
        self.counters["client_ok"] += 1
        payload["node"] = self.id
        return payload

    async def _on_status_req(self, msg: dict[str, Any]) -> dict[str, Any]:
        return self.status()

    async def _on_ping(self, msg: dict[str, Any]) -> dict[str, Any]:
        """空载往返探测：用来验证"注入的 RTT 到底有没有生效"。"""
        return {"ok": True, "id": self.id, "t": msg.get("t0")}

    # ================================================================ 后台任务
    def start_background(self) -> None:
        for link in self.links.values():
            link.start()
        if self.cfg.listener:
            # Listener（Observer）只学不投票，也**绝不参选**：它没有 Acceptor，
            # 当上 Leader 也提交不了任何东西，只会制造无谓的选举。
            self._background.append(asyncio.create_task(self._gap_fill_loop(), name="gapfill"))
            return
        if isinstance(self.proposer, MultiPaxosProposer | FastPaxosProposer):
            self.proposer.start()  # type: ignore[union-attr]
            self._background.append(asyncio.create_task(self._election_loop(), name="election"))
        self._background.append(asyncio.create_task(self._gap_fill_loop(), name="gapfill"))

    async def _election_loop(self) -> None:
        """Multi-Paxos / Fast Paxos 的选主协程。

        规则：超过 ``election_ms`` 没收到心跳就可以参选，参选时刻按节点编号抖动，
        避免所有副本同时抢主（同时抢主会出现大量无效 Phase 1）。
        """
        delay = self.election_timeout * (1.0 + 0.8 * self.ballot_id) * (0.9 + 0.2 * random.random()) + 0.15
        await asyncio.sleep(delay)
        while not self._stop.is_set():
            try:
                proposer = self.proposer
                is_leader = bool(getattr(proposer, "is_leader", False))
                idle = self.loop_time() - self._last_heartbeat
                if not is_leader and idle >= self.election_timeout:
                    if self.leader_id and self.leader_id != self.id and idle < 2 * self.election_timeout:
                        await asyncio.sleep(0.05)
                        continue
                    # **随机退避**：两个候选者若同时参选，会各自拿到"自己 + 对方"两张
                    # 票（在多副本只剩多数派时足够），然后用更大的编号互相顶掉对方，
                    # 形成选举活锁（实测能连续换主 50+ 次）。随机退避是 Paxos/Raft
                    # 家族解决这个问题的标准手段。
                    await asyncio.sleep(random.uniform(0.0, self.election_timeout))
                    if self.loop_time() - self._last_heartbeat < self.election_timeout:
                        continue  # 退避期间有人当上 Leader 了
                    self.counters["elections"] += 1
                    ok = await proposer.ensure_leader()  # type: ignore[union-attr]
                    if ok and self.protocol == "fast":
                        self.broadcast(
                            {
                                "t": "heartbeat",
                                "b": proposer.epoch.to_list(),  # type: ignore[union-attr]
                                "leader": self.id,
                                "commit_index": self.learner.commit_index,
                            }
                        )
                    elif not ok:
                        # 选不上（可能是分区/多数派不存在）就退避更久，别把 CPU 和
                        # 网络全花在无望的 Phase 1 上。
                        await asyncio.sleep(random.uniform(0.2, 0.6))
            except Exception:  # pragma: no cover - 选主失败不应打死进程
                log.exception("[%s] 选主异常", self.id)
            await asyncio.sleep(0.03)

    async def _gap_fill_loop(self) -> None:
        """Learner 缺口回填：发现 [0, commit_index) 里有洞就向同伴要。"""
        while not self._stop.is_set():
            try:
                missing = self.learner.missing(4)
                for slot in missing:
                    for peer in self.peer_ids:
                        payload = await self.rpc(peer, {"t": "learn_ask", "ins": slot}, timeout=0.15)
                        if payload.get("chosen"):
                            self.learner.choose(slot, str(payload["value"]), None)
                            break
            except Exception:  # pragma: no cover
                log.debug("[%s] 缺口回填异常", self.id, exc_info=True)
            await asyncio.sleep(0.05)

    # ================================================================ 服务端
    async def _handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """接入一条 TCP 连接：先握手分辨"同伴"还是"客户端"。

        约定：连上后第一行是 ``{"t":"hello","id":...}``。为了容错，如果第一行
        直接就是一条请求（比如用 ``nc`` 手工敲报文），也照样按客户端处理。
        """
        peer = writer.get_extra_info("peername")
        enable_nodelay(writer)
        try:
            hello_line = await asyncio.wait_for(reader.readline(), timeout=10.0)
            if not hello_line:
                return
            try:
                first = decode(hello_line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                return
            who = str(first.get("id", "")) if first.get("t") == "hello" else ""
            if who in self.links:
                link = self.links[who]
                link.attach_writer(writer)
                self.counter.count_recv("hello")
                await self._read_peer(link, reader)
            else:
                if who == "" and first.get("t") not in {"hello", ""}:
                    # 第一行就是业务请求：直接处理，别把它当握手吃掉
                    payload = await self._serve(str(first.get("t")), first)
                    writer.write(encode({"t": "rep", "rid": first.get("rid"), "p": payload}))
                    with contextlib.suppress(ConnectionError, OSError):
                        await writer.drain()
                await self._read_client(reader, writer)
        except (asyncio.TimeoutError, ConnectionError, OSError, json.JSONDecodeError):
            log.debug("[%s] 连接 %s 握手失败", self.id, peer)
        finally:
            with contextlib.suppress(Exception):
                writer.close()

    async def _read_peer(self, link: PeerLink, reader: asyncio.StreamReader) -> None:
        while not self._stop.is_set():
            line = await reader.readline()
            if not line:
                return
            self.counter.bytes_recv += len(line)
            try:
                msg = decode(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                log.warning("[%s] 来自 %s 的报文无法解析", self.id, link.peer_id)
                continue
            await self._on_message(link.peer_id, msg)

    async def _read_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """一个客户端连接就是一个会话：请求串行处理，靠流水线量化并发。"""
        while not self._stop.is_set():
            line = await reader.readline()
            if not line:
                return
            self.counter.bytes_recv += len(line)
            try:
                msg = decode(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            payload = await self._serve(str(msg.get("t")), msg)
            rid = msg.get("rid")
            writer.write(encode({"t": "rep", "rid": rid, "p": payload}))
            with contextlib.suppress(ConnectionError, OSError):
                await writer.drain()

    # ================================================================ 管理口
    async def _handle_admin(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while not self._stop.is_set():
                line = await reader.readline()
                if not line:
                    return
                try:
                    msg = decode(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                payload = await self._admin_command(msg)
                writer.write(encode(payload))
                with contextlib.suppress(ConnectionError, OSError):
                    await writer.drain()
                if payload.get("shutdown"):
                    asyncio.get_running_loop().call_later(0.05, self._stop.set)
                    return
        except (ConnectionError, OSError):
            return
        finally:
            with contextlib.suppress(Exception):
                writer.close()

    async def _admin_command(self, msg: dict[str, Any]) -> dict[str, Any]:
        cmd = str(msg.get("cmd", "status"))
        if cmd == "status":
            return self.status()
        if cmd == "inject":
            self.inject_faults(
                rtt_ms=msg.get("rtt_ms"),
                loss=msg.get("loss"),
                jitter_ms=msg.get("jitter_ms"),
                partition=msg.get("partition"),
            )
            return {"ok": True, "profile": self.profile.as_dict()}
        if cmd == "partition":
            return self.set_partition(str(msg.get("peer", "")), bool(msg.get("blocked", True)))
        if cmd == "dump_log":
            return {
                "ok": True,
                "chosen": [[slot, value] for slot, value in sorted(self.learner.chosen.items())],
                "applied_index": self.applied_index,
                "state": self.state_machine.snapshot(),
            }
        if cmd == "elect":
            proposer = self.proposer
            if isinstance(proposer, MultiPaxosProposer | FastPaxosProposer):
                ok = await proposer.ensure_leader(force=True)
                return {"ok": ok, "epoch": proposer.epoch.to_list() if proposer.epoch else None}
            return {"ok": False, "err": "basic paxos 没有 Leader"}
        if cmd == "crash":
            # 模拟进程被 kill -9：直接退出，不 flush、不响应任何清理逻辑
            log.warning("[%s] 收到 crash 指令，立即退出（模拟掉电/被 kill）", self.id)
            asyncio.get_running_loop().call_later(0.02, os._exit, 9)
            return {"ok": True, "crash": True}
        if cmd == "shutdown":
            return {"ok": True, "shutdown": True}
        if cmd == "flush":
            self.acceptor_journal.flush()
            self.learner_journal.flush()
            return {"ok": True}
        if cmd == "ping_peer":
            return await self._ping_peer(str(msg.get("peer", "")), int(msg.get("count", 10)))
        if cmd == "reset_counters":
            self.counter = Counter()
            for link in self.links.values():
                link.counter = self.counter
            return {"ok": True}
        return {"ok": False, "err": f"unknown admin cmd: {cmd}"}

    async def _ping_peer(self, peer: str, count: int) -> dict[str, Any]:
        """在本节点上向某个同伴发 ``count`` 次空载 RPC，返回实测 RTT 统计。"""
        if peer not in self.links:
            return {"ok": False, "err": f"没有到 {peer} 的链路"}
        samples: list[float] = []
        for _ in range(max(1, count)):
            start = self.loop_time()
            payload = await self.rpc(peer, {"t": "ping"}, timeout=max(0.5, self.attempt_timeout))
            if payload.get("ok"):
                samples.append((self.loop_time() - start) * 1000.0)
        if not samples:
            return {"ok": False, "err": "全部超时", "profile": self.profile.as_dict()}
        samples.sort()
        return {
            "ok": True,
            "peer": peer,
            "count": len(samples),
            "min_ms": round(samples[0], 3),
            "median_ms": round(samples[len(samples) // 2], 3),
            "max_ms": round(samples[-1], 3),
            "profile": self.profile.as_dict(),
        }

    def inject_faults(
        self,
        *,
        rtt_ms: float | None = None,
        loss: float | None = None,
        jitter_ms: float | None = None,
        partition: bool | None = None,
    ) -> None:
        if rtt_ms is not None:
            self.profile.rtt_ms = float(rtt_ms)
        if loss is not None:
            self.profile.loss = max(0.0, min(float(loss), 1.0))
        if jitter_ms is not None:
            self.profile.jitter_ms = max(0.0, float(jitter_ms))
        if partition is not None:
            self.profile.partition = bool(partition)

    def set_partition(self, peer: str, blocked: bool) -> dict[str, Any]:
        """定向分区：只挡住到某个同伴的链路（比整体 partition 更细）。"""
        link = self.links.get(peer)
        if link is None:
            return {"ok": False, "err": f"没有到 {peer} 的链路"}
        link.profile.partition = blocked
        return {"ok": True, "peer": peer, "blocked": blocked}

    # ================================================================ 观测
    def status(self) -> dict[str, Any]:
        proposer_state: dict[str, Any] = {}
        if isinstance(self.proposer, MultiPaxosProposer | FastPaxosProposer):
            proposer_state = self.proposer.state()
        return {
            "ok": True,
            "id": self.id,
            "protocol": self.protocol,
            "role": self.role,
            "uptime_s": round(time.time() - self._started_at, 3),
            "leader": self.leader_id,
            "leader_changes": self.leader_changes,
            "quorum": self.quorum,
            "fast_quorum": self.fast_quorum,
            "tolerated_failures": self.majority_failures,
            "fsync": self.fsync,
            "profile": self.profile.as_dict(),
            "max_round": self._max_round,
            "applied_index": self.applied_index,
            "next_slot": self._next_slot,
            "counters": dict(self.counters),
            "messages": self.counter.as_dict(),
            "acceptor": self.acceptor.state() if self.acceptor else None,
            "learner": self.learner.snapshot(),
            "state_machine": self.state_machine.snapshot(),
            "proposer": proposer_state,
        }

    @property
    def role(self) -> str:
        if self.cfg.listener:
            return "listener"
        if isinstance(self.proposer, MultiPaxosProposer | FastPaxosProposer) and self.proposer.is_leader:
            return "leader"
        if self.protocol == "basic":
            return "proposer"
        return "follower"

    # ================================================================ 主循环
    async def run(self) -> None:
        enable_fine_timers()
        server = await asyncio.start_server(self._handle_conn, self.cfg.listen_host, self.cfg.port, backlog=512)
        admin = await asyncio.start_server(self._handle_admin, self.cfg.listen_host, self.cfg.admin_port, backlog=64)
        self.start_background()
        log.info(
            "[%s] 启动完成 protocol=%s port=%d admin=%d quorum=%d fast_quorum=%d fsync=%s listener=%s",
            self.id,
            self.protocol,
            self.cfg.port,
            self.cfg.admin_port,
            self.quorum,
            self.fast_quorum,
            self.fsync,
            self.cfg.listener,
        )
        try:
            await self._stop.wait()
        finally:
            await self.shutdown(server, admin)

    async def shutdown(self, server: Any = None, admin: Any = None) -> None:
        log.info("[%s] 正在关闭...", self.id)
        self._stop.set()
        for task in self._background:
            task.cancel()
        for task in self._background:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        await self.proposer.stop()
        for link in self.links.values():
            await link.close()
        for srv in (server, admin):
            if srv is not None:
                srv.close()
                with contextlib.suppress(Exception):
                    await srv.wait_closed()
        self.acceptor_journal.close()
        self.learner_journal.close()
        log.info("[%s] 已关闭", self.id)
