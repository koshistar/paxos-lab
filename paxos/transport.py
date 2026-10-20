"""TCP 传输层：自定义报文、链路时延/丢包注入、消息计数。

为什么用 **JSON Lines**（一行一条 JSON，''\\n'' 结尾）而不是二进制协议：

1. 可以用 ``nc``/``telnet`` 直接手工收发报文，实验演示与排错极其方便；
2. Paxos 报文很小（几十到几百字节），JSON 的体积和解析开销可以接受；
3. 抓下来的报文本身就能当实验数据，写报告时可以直接引用。

节点之间是**全连接网状**：编号小的节点主动连接编号大的节点，同一条 TCP
连接双向复用，因此每对节点只有一条链路。链路上挂一个写队列，写协程负责
注入时延与丢包——这样既不会阻塞业务协程，又能保证同一条链路内消息有序。

只对**服务器之间**的链路注入时延/丢包；客户端（压测器）到节点的连接不注入，
因为"RTT"在本实验里的定义就是 Paxos 副本之间的往返时延。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import random
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from .timer import sleep_precise

log = logging.getLogger(__name__)

#: 单行报文上限。Paxos 报文通常只有几百字节，4 MiB 是"正常永远不会碰到"的上限；
#: 真的碰到说明有消息在无界增长（本工程踩过：全局 Phase 1 应答把整本日志都带上，
#: 崩溃前大约是 170 KB，正好越过 asyncio 默认的 64 KiB 行上限）。
MAX_LINE = 4 * 1024 * 1024

#: admin 口的上限给得更大：``dump_log`` 会把整个决定日志一次性吐出来（安全性比对用），
#: 长跑之后可能有几万个 slot。admin 口只监听本机/容器网段，风险可控。
MAX_ADMIN_LINE = 64 * 1024 * 1024
#: 链路断开后的重连间隔（秒）
RECONNECT_INTERVAL = 0.2


# --------------------------------------------------------------------- 编解码
def encode(msg: dict[str, Any]) -> bytes:
    """把消息编码成一行 bytes（JSON Lines）。"""
    return (json.dumps(msg, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


def decode(raw: bytes | str) -> dict[str, Any]:
    """把一行报文解码成 dict。"""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)


# --------------------------------------------------------------------- 故障注入
@dataclass
class FaultProfile:
    """一条链路的故障画像。

    ``rtt_ms`` 是**往返**时延，注入时按单程 ``rtt/2`` 施加（发送方向延迟一次，
    接收方向不延迟，等价于往返 rtt）。实验里 RTT 一栏取 1/10/50/100 ms。
    """

    rtt_ms: float = 1.0
    loss: float = 0.0
    jitter_ms: float = 0.0
    partition: bool = False

    @property
    def one_way_delay(self) -> float:
        """单程时延（秒）。"""
        return max(self.rtt_ms, 0.0) / 2.0 / 1000.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "rtt_ms": round(self.rtt_ms, 3),
            "loss": round(self.loss, 4),
            "jitter_ms": round(self.jitter_ms, 3),
            "partition": self.partition,
        }


@dataclass
class Counter:
    """节点级报文计数（写报告的"消息数"指标就靠它）。"""

    sent: int = 0
    recv: int = 0
    dropped: int = 0
    bytes_sent: int = 0
    bytes_recv: int = 0
    by_type: dict[str, int] = field(default_factory=dict)

    def count_sent(self, msg_type: str) -> None:
        self.sent += 1
        self.by_type[msg_type] = self.by_type.get(msg_type, 0) + 1

    def count_recv(self, msg_type: str) -> None:
        self.recv += 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "sent": self.sent,
            "recv": self.recv,
            "dropped": self.dropped,
            "bytes_sent": self.bytes_sent,
            "bytes_recv": self.bytes_recv,
            "by_type": dict(sorted(self.by_type.items())),
        }


MessageHandler = Callable[[str, dict[str, Any]], Awaitable[None]]


def enable_nodelay(writer: asyncio.StreamWriter) -> None:
    """打开 TCP_NODELAY。

    Paxos 报文都很小（几十~几百字节），如果让 Nagle 算法攒包，会撞上对端的
    "延迟 ACK"（Windows/Linux 都是 ~40ms），一次提案凭空多出几十毫秒。
    实测：不开这个开关，1ms RTT 的集群 P50 会从 2ms 涨到 40ms —— 这也是
    "分布式系统里最贵的往往是协议栈，而不是算法"的经典例子。
    """
    sock = writer.get_extra_info("socket")
    if sock is None:
        return
    with contextlib.suppress(OSError):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


# --------------------------------------------------------------------- 链路
class PeerLink:
    """一条到对端节点的长连接，带写队列、时延/丢包注入与自动重连。"""

    def __init__(
        self,
        local_id: str,
        peer_id: str,
        addr: tuple[str, int],
        profile: FaultProfile,
        counter: Counter,
        on_message: MessageHandler,
    ) -> None:
        self.local_id = local_id
        self.peer_id = peer_id
        self.addr = addr
        self.profile = profile
        self.counter = counter
        self._on_message = on_message

        self._queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=20000)
        self._writer: asyncio.StreamWriter | None = None
        self._pending_tasks: set[asyncio.Task[Any]] = set()
        self._closed = False
        self._tasks: list[asyncio.Task[None]] = []
        #: 编号小的节点负责拨号，保证一对节点只有一条连接
        self.is_dialer = local_id < peer_id

    # ---------------------------------------------------------------- 生命周期
    def start(self) -> None:
        self._tasks.append(asyncio.create_task(self._writer_loop(), name=f"writer-{self.peer_id}"))
        if self.is_dialer:
            self._tasks.append(asyncio.create_task(self._dial_loop(), name=f"dial-{self.peer_id}"))

    async def close(self) -> None:
        self._closed = True
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if self._writer is not None:
            with contextlib.suppress(Exception):
                self._writer.close()

    # ------------------------------------------------------------------- 发送
    def send(self, msg: dict[str, Any]) -> None:
        """投递一条消息到写队列（不阻塞调用方）。

        队列满时直接丢弃并计数——这本身就是"节点过载时丢包"的真实行为，
        还能反过来检验 Paxos 在丢包下的表现。
        """
        data = encode(msg)
        self.counter.bytes_sent += len(data)
        try:
            self._queue.put_nowait(data)
            self.counter.count_sent(str(msg.get("t", "?")))
        except asyncio.QueueFull:
            self.counter.dropped += 1
            log.warning("到 %s 的写队列已满，丢弃 %s", self.peer_id, msg.get("t"))

    async def _writer_loop(self) -> None:
        """从队列取消息 → 检查丢包/分区 → **按消息各自定时**写出。

        这里千万不要"取一条、睡 delay、写一条"：那会让同一条链路彻底串行化，
        32 条并发请求会被 32 × delay 排队拖死（实测能把吞吐压到 1/10）。
        真实网络的时延是"每条消息各自在路上跑"，所以这里给每条消息起一个
        定时任务，互不阻塞；定时用的是 ``sleep_precise``（见 ``timer.py``），
        否则 Windows 上 0.5ms 会被系统放大成 15.6ms。
        """
        debug = bool(os.environ.get("PAXOS_DEBUG_TIMING"))
        while not self._closed:
            data = await self._queue.get()
            if self._should_drop():
                continue
            delay = self.profile.one_way_delay
            if self.profile.jitter_ms > 0:
                delay += random.uniform(0, self.profile.jitter_ms / 1000.0)
            if delay > 0:
                # 必须持有引用：asyncio 只保留弱引用，任务被 GC 掉就等于丢消息
                task = asyncio.create_task(self._delayed_emit(data, delay, debug))
                self._pending_tasks.add(task)
                task.add_done_callback(self._pending_tasks.discard)
            else:
                self._emit_now(data, debug)

    async def _delayed_emit(self, data: bytes, delay: float, debug: bool = False) -> None:
        await sleep_precise(delay)
        self._emit_now(data, debug)

    def _emit_now(self, data: bytes, debug: bool = False) -> None:
        """真正写 socket（由事件循环在时延到点后调用）。"""
        writer = self._writer
        if writer is None or writer.is_closing():
            self.counter.dropped += 1
            return
        try:
            writer.write(data)
            # 只在缓冲明显堆积时才等 drain，避免把写路径变成同步等待
            transport = writer.transport
            if transport is not None and transport.get_write_buffer_size() > 512 * 1024:
                task = asyncio.ensure_future(writer.drain())
                self._pending_tasks.add(task)
                task.add_done_callback(self._pending_tasks.discard)
        except (ConnectionError, OSError, RuntimeError):
            self.counter.dropped += 1
            self._detach(writer)
            return
        if debug:
            log.warning("LINK %s->%s 已写出 %d 字节", self.local_id, self.peer_id, len(data))

    def _should_drop(self) -> bool:
        if self.profile.partition:
            self.counter.dropped += 1
            return True
        if self.profile.loss > 0 and random.random() < self.profile.loss:
            self.counter.dropped += 1
            return True
        return False

    # --------------------------------------------------------------- 连接管理
    def attach_writer(self, writer: asyncio.StreamWriter) -> None:
        """由被动方（服务端 accept 回来的连接）挂上写端。"""
        self._writer = writer

    def _detach(self, writer: asyncio.StreamWriter) -> None:
        if self._writer is writer:
            self._writer = None
            with contextlib.suppress(Exception):
                writer.close()

    async def _dial_loop(self) -> None:
        while not self._closed:
            writer = self._writer
            if writer is None or writer.is_closing():
                try:
                    reader, new_writer = await asyncio.wait_for(
                        asyncio.open_connection(self.addr[0], self.addr[1], limit=MAX_LINE), timeout=2.0
                    )
                except (OSError, asyncio.TimeoutError):
                    await asyncio.sleep(RECONNECT_INTERVAL)
                    continue
                enable_nodelay(new_writer)
                new_writer.write(encode({"t": "hello", "id": self.local_id}))
                with contextlib.suppress(ConnectionError, OSError):
                    await new_writer.drain()
                self.attach_writer(new_writer)
                self._tasks.append(
                    asyncio.create_task(self._read_loop(reader, new_writer), name=f"reader-{self.peer_id}")
                )
            await asyncio.sleep(RECONNECT_INTERVAL)

    async def _read_loop(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while not self._closed:
                line = await reader.readline()
                if not line:
                    break
                self.counter.bytes_recv += len(line)
                await self._dispatch_line(line)
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            pass
        finally:
            self._detach(writer)

    async def _dispatch_line(self, line: bytes) -> None:
        try:
            msg = decode(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            log.warning("来自 %s 的报文无法解析: %r", self.peer_id, line[:120])
            return
        await self._on_message(self.peer_id, msg)
