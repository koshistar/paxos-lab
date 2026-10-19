"""本地集群编排：用多个操作系统进程模拟多台机器。

为什么用"多进程"而不是"多线程"：

* 每个节点有自己的内存、自己的日志文件、自己的进程号 —— 才能用 ``kill -9``
  真正演示"崩溃恢复"（线程里做不到）；
* 进程崩溃不会把整台"机器"带走，其他副本还能继续工作；
* 与 Docker/VirtualBox 部署共用同一份代码，只是启动方式不同。

Docker 部署时用 :meth:`Cluster.external`：不拉起进程，直接给一组
``host:port`` 端点，实验脚本照常编排。
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .ballot import quorum_size

log = logging.getLogger(__name__)

#: 集群元信息（供 up/down/status 等子命令在多个终端之间共享）
STATE_FILE = "run/cluster.json"


@dataclass
class NodeEndpoint:
    """一个节点的对外地址。"""

    node_id: str
    host: str
    port: int
    admin_port: int

    def as_tuple(self) -> tuple[str, str, int]:
        return (self.node_id, self.host, self.port)


@dataclass
class ClusterSpec:
    """怎么起一个集群。"""

    nodes: int = 3
    protocol: str = "multipaxos"
    base_port: int = 17100
    host: str = "127.0.0.1"
    data_root: Path = Path("data")
    log_root: Path = Path("run/logs")
    fsync: bool = True
    rtt_ms: float = 1.0
    loss: float = 0.0
    batch_ms: float = 0.0
    heartbeat_ms: float = 60.0
    election_ms: float = 400.0
    listeners: int = 0
    extra_args: list[str] = field(default_factory=list)
    keep_data: bool = False
    #: 是否把集群状态写进 ``run/cluster.json``。
    #: 只有 ``up`` 命令需要（让另一个终端里的 status / bench / down 能找到集群）；
    #: smoke、实验矩阵这些"自己起自己停"的临时集群必须关掉，否则会留下幽灵状态，
    #: 让用户之后执行 status 时看到一堆 DOWN 节点。
    publish_state: bool = True

    def ids(self) -> list[str]:
        return [f"n{i + 1}" for i in range(self.nodes + self.listeners)]

    def endpoints(self) -> list[NodeEndpoint]:
        result = []
        for index, node_id in enumerate(self.ids()):
            result.append(
                NodeEndpoint(
                    node_id=node_id,
                    host=self.host,
                    port=self.base_port + index,
                    admin_port=self.base_port + 1000 + index,
                )
            )
        return result

    def peers_arg(self) -> str:
        return ",".join(f"{ep.node_id}={ep.host}:{ep.port}" for ep in self.endpoints())


# --------------------------------------------------------------------- 工具
def admin_call(host: str, port: int, payload: dict[str, Any], timeout: float = 3.0) -> dict[str, Any]:
    """向节点的管理口发一条命令（同步、短连接，够用且简单）。"""
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            sock.sendall((json.dumps(payload) + "\n").encode("utf-8"))
            buffer = b""
            while not buffer.endswith(b"\n"):
                chunk = sock.recv(65536)
                if not chunk:
                    break
                buffer += chunk
        if not buffer:
            return {"ok": False, "err": "empty"}
        return json.loads(buffer.decode("utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"ok": False, "err": f"{type(exc).__name__}: {exc}"}


def port_open(host: str, port: int, timeout: float = 0.2) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# --------------------------------------------------------------------- 集群
class Cluster:
    """一组 Paxos 节点进程的句柄。"""

    def __init__(self, spec: ClusterSpec, endpoints: list[NodeEndpoint] | None = None) -> None:
        self.spec = spec
        self.endpoints = endpoints if endpoints is not None else spec.endpoints()
        self.procs: dict[str, subprocess.Popen[bytes]] = {}
        self.started_at = 0.0
        self.project_root = Path(__file__).resolve().parent.parent

    # ------------------------------------------------------------------ 启动
    def start(self, *, wait: bool = True, timeout: float = 25.0) -> None:
        """拉起所有节点进程并等待它们真正可用。"""
        if not self.spec.keep_data:
            shutil.rmtree(self.spec.data_root, ignore_errors=True)
        self.spec.log_root.mkdir(parents=True, exist_ok=True)
        for stale in self.spec.log_root.glob("*.log"):
            # Windows 上上一个进程可能还没放开文件句柄，删不掉就算了
            with contextlib.suppress(OSError):
                stale.unlink(missing_ok=True)
        self.spec.data_root.mkdir(parents=True, exist_ok=True)
        self.started_at = time.time()
        for index, ep in enumerate(self.endpoints):
            is_listener = index >= self.spec.nodes
            self._spawn(ep, is_listener=is_listener)
        if wait:
            self.wait_ready(timeout=timeout)
        if self.spec.publish_state:
            self.write_state()

    def _spawn(self, ep: NodeEndpoint, *, is_listener: bool = False) -> None:
        log_path = self.spec.log_root / f"{ep.node_id}.log"
        err_path = self.spec.log_root / f"{ep.node_id}.err.log"
        cmd = [
            sys.executable or "python",
            "-m",
            "paxos.cli",
            "node",
            "--id",
            ep.node_id,
            "--peers",
            self.spec.peers_arg(),
            "--port",
            str(ep.port),
            "--admin-port",
            str(ep.admin_port),
            "--data-dir",
            str(Path(self.spec.data_root) / ep.node_id),
            "--protocol",
            self.spec.protocol,
            "--fsync",
            "on" if self.spec.fsync else "off",
            "--rtt-ms",
            str(self.spec.rtt_ms),
            "--loss",
            str(self.spec.loss),
            "--batch-ms",
            str(self.spec.batch_ms),
            "--heartbeat-ms",
            str(self.spec.heartbeat_ms),
            "--election-ms",
            str(self.spec.election_ms),
            "--host",
            self.spec.host,
        ]
        if is_listener:
            cmd.append("--listener")
        cmd.extend(self.spec.extra_args)

        creationflags = 0
        if os.name == "nt":
            creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        env = dict(os.environ)
        env["PYTHONPATH"] = str(self.project_root) + os.pathsep + env.get("PYTHONPATH", "")
        env["PYTHONUNBUFFERED"] = "1"
        with log_path.open("a", encoding="utf-8") as out, err_path.open("a", encoding="utf-8") as err:
            proc = subprocess.Popen(
                cmd,
                cwd=str(self.project_root),
                stdout=out,
                stderr=err,
                env=env,
                creationflags=creationflags,
            )
        self.procs[ep.node_id] = proc

    def wait_ready(self, timeout: float = 25.0) -> None:
        """等所有节点管理口可应答且角色就绪。"""
        deadline = time.time() + timeout
        pending = {ep.node_id for ep in self.endpoints}
        while pending and time.time() < deadline:
            for ep in self.endpoints:
                if ep.node_id not in pending:
                    continue
                status = admin_call(ep.host, ep.admin_port, {"cmd": "status"}, timeout=0.5)
                if status.get("ok"):
                    pending.discard(ep.node_id)
            if pending:
                time.sleep(0.1)
        if pending:
            raise TimeoutError(f"节点启动超时: {sorted(pending)}")
        # 再多等一轮，让选主完成（Multi-Paxos / Fast 需要一个 Leader）
        if self.spec.protocol != "basic":
            self.wait_leader(timeout=min(timeout, 10.0))

    def wait_leader(self, timeout: float = 10.0, stable_ms: float = 400.0) -> str | None:
        """等到"多数节点认可同一个 Leader 并持续一小段时间"，避免在选举风暴中开测。"""
        q = quorum_size(self.spec.nodes)
        deadline = time.time() + timeout
        candidate: str | None = None
        since = 0.0
        while time.time() < deadline:
            statuses = self.status_all()
            votes: dict[str, int] = {}
            for status in statuses.values():
                leader = status.get("leader")
                if leader:
                    votes[leader] = votes.get(leader, 0) + 1
            agreed = max(votes, key=lambda k: votes[k]) if votes and max(votes.values()) >= q else None
            if agreed is not None and agreed == candidate:
                if (time.time() - since) * 1000.0 >= stable_ms:
                    return agreed
            else:
                candidate, since = agreed, time.time()
            time.sleep(0.05)
        return candidate

    # ------------------------------------------------------------------ 停止
    def stop(self, *, graceful: bool = True) -> None:
        for ep in self.endpoints:
            proc = self.procs.get(ep.node_id)
            if proc is None or proc.poll() is not None:
                continue
            if graceful:
                admin_call(ep.host, ep.admin_port, {"cmd": "shutdown"}, timeout=1.0)
        deadline = time.time() + 4.0
        for ep in self.endpoints:
            proc = self.procs.get(ep.node_id)
            if proc is None:
                continue
            try:
                proc.wait(timeout=max(0.1, deadline - time.time()))
            except subprocess.TimeoutExpired:
                self._kill(proc)
        self.procs.clear()
        if self.spec.publish_state:
            self.clear_state()

    def clear_state(self) -> None:
        """删掉 ``run/cluster.json``（只删"自己写的"那份，避免踩到别的集群）。"""
        path = Path(STATE_FILE)
        state = Cluster.load_state()
        if state is None:
            return
        mine = {ep.node_id for ep in self.endpoints}
        theirs = {item.get("id") for item in state.get("endpoints", [])}
        if mine and mine != theirs:
            return
        with contextlib.suppress(OSError):
            path.unlink(missing_ok=True)

    def _kill(self, proc: subprocess.Popen[bytes]) -> None:
        try:
            if os.name == "nt":
                proc.kill()
            else:
                proc.send_signal(signal.SIGKILL)
        except (OSError, ProcessLookupError):  # pragma: no cover
            pass

    def kill_node(self, node_id: str, *, hard: bool = True) -> None:
        """杀掉一个节点。``hard=True`` 走 ``crash`` 管理命令（等价 kill -9，且不刷盘）。"""
        ep = self.endpoint(node_id)
        if hard:
            admin_call(ep.host, ep.admin_port, {"cmd": "crash"}, timeout=1.0)
            time.sleep(0.05)
            proc = self.procs.get(node_id)
            if proc is not None and proc.poll() is None:
                self._kill(proc)
        else:
            proc = self.procs.get(node_id)
            if proc is not None:
                self._kill(proc)
        proc = self.procs.get(node_id)
        if proc is not None:
            with contextlib.suppress(subprocess.TimeoutExpired):  # pragma: no cover
                proc.wait(timeout=2.0)
            self.procs.pop(node_id, None)

    def restart_node(self, node_id: str, *, wait: bool = True) -> None:
        """重启一个节点（数据目录保留 —— 正好检验持久化）。"""
        index = [ep.node_id for ep in self.endpoints].index(node_id)
        ep = self.endpoints[index]
        self._spawn(ep, is_listener=index >= self.spec.nodes)
        if wait:
            deadline = time.time() + 15.0
            while time.time() < deadline:
                if admin_call(ep.host, ep.admin_port, {"cmd": "status"}, timeout=0.5).get("ok"):
                    break
                time.sleep(0.05)

    # ------------------------------------------------------------------ 观测
    def endpoint(self, node_id: str) -> NodeEndpoint:
        for ep in self.endpoints:
            if ep.node_id == node_id:
                return ep
        raise KeyError(f"没有节点 {node_id}")

    def admin(self, node_id: str, payload: dict[str, Any], timeout: float = 3.0) -> dict[str, Any]:
        ep = self.endpoint(node_id)
        return admin_call(ep.host, ep.admin_port, payload, timeout=timeout)

    def status_all(self) -> dict[str, dict[str, Any]]:
        return {ep.node_id: self.admin(ep.node_id, {"cmd": "status"}) for ep in self.endpoints}

    def dump_logs(self) -> dict[str, dict[int, str]]:
        """导出每个节点的"决定日志"，用于安全性比对。"""
        result: dict[str, dict[int, str]] = {}
        for ep in self.endpoints:
            payload = self.admin(ep.node_id, {"cmd": "dump_log"})
            if payload.get("ok"):
                result[ep.node_id] = {int(slot): str(value) for slot, value in payload["chosen"]}
        return result

    def inject(self, node_id: str, **kwargs: Any) -> dict[str, Any]:
        return self.admin(node_id, {"cmd": "inject", **kwargs})

    def inject_all(self, **kwargs: Any) -> None:
        for ep in self.endpoints:
            self.admin(ep.node_id, {"cmd": "inject", **kwargs})

    def set_partition(self, groups: list[list[str]], partition_off: bool = False) -> None:
        """把节点切成若干个互不可达的组。

        每个节点只挡住"对侧组"的链路，因此这是真正的双向分区（A 到 B 和 B 到 A
        都断），而组内通信不受影响。``partition_off=True`` 时恢复全部链路。
        """
        all_ids = [ep.node_id for ep in self.endpoints]
        if partition_off or not groups:
            for ep in self.endpoints:
                for other in all_ids:
                    if other != ep.node_id:
                        self.admin(ep.node_id, {"cmd": "partition", "peer": other, "blocked": False})
            return
        group_of: dict[str, int] = {}
        for index, group in enumerate(groups):
            for node_id in group:
                group_of[node_id] = index
        for ep in self.endpoints:
            mine = group_of.get(ep.node_id, -1)
            for other in all_ids:
                if other == ep.node_id:
                    continue
                blocked = group_of.get(other, -1) != mine
                self.admin(ep.node_id, {"cmd": "partition", "peer": other, "blocked": blocked})

    # ------------------------------------------------------------------ 状态文件
    def write_state(self) -> None:
        state_path = Path(STATE_FILE)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state = {
            "spec": {
                "nodes": self.spec.nodes,
                "protocol": self.spec.protocol,
                "fsync": self.spec.fsync,
                "rtt_ms": self.spec.rtt_ms,
                "loss": self.spec.loss,
                "batch_ms": self.spec.batch_ms,
            },
            "endpoints": [
                {"id": ep.node_id, "host": ep.host, "port": ep.port, "admin_port": ep.admin_port}
                for ep in self.endpoints
            ],
            "pids": {node_id: proc.pid for node_id, proc in self.procs.items()},
            "started_at": self.started_at,
        }
        state_path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")

    @staticmethod
    def load_state() -> dict[str, Any] | None:
        path = Path(STATE_FILE)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None

    @staticmethod
    def from_state() -> Cluster | None:
        """根据 ``run/cluster.json`` 重建句柄（供第二个终端里的命令使用）。"""
        state = Cluster.load_state()
        if not state:
            return None
        spec = ClusterSpec(
            nodes=int(state["spec"]["nodes"]),
            protocol=str(state["spec"]["protocol"]),
            fsync=bool(state["spec"]["fsync"]),
            rtt_ms=float(state["spec"]["rtt_ms"]),
            loss=float(state["spec"]["loss"]),
            batch_ms=float(state["spec"]["batch_ms"]),
        )
        endpoints = [
            NodeEndpoint(
                node_id=str(item["id"]),
                host=str(item["host"]),
                port=int(item["port"]),
                admin_port=int(item["admin_port"]),
            )
            for item in state["endpoints"]
        ]
        return Cluster(spec, endpoints)

    @staticmethod
    def external(endpoints: Iterable[tuple[str, str, int, int]], protocol: str = "multipaxos") -> Cluster:
        """不拉起进程，直接接管一组已经跑着的节点（Docker 部署用）。"""
        eps = [
            NodeEndpoint(node_id=nid, host=host, port=port, admin_port=admin) for nid, host, port, admin in endpoints
        ]
        spec = ClusterSpec(nodes=len(eps), protocol=protocol, keep_data=True, publish_state=False)
        return Cluster(spec, eps)
