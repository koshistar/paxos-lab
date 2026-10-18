"""集成测试：真的起一个 3 节点集群跑通三种协议。

默认**跳过**（跑一次要十几秒，而且会占用端口）。需要时：

    $env:PAXOS_INTEGRATION="1"; python -m pytest tests/test_integration.py -v

它会验证：
1. 三种协议都能提交成功；
2. 所有节点的决定日志完全一致（安全性）；
3. kill -9 一个节点后仍能提交，重启后日志还在。
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from paxos.cluster import Cluster, ClusterSpec
from paxos.loadgen import LoadGen

pytestmark = pytest.mark.skipif(
    os.environ.get("PAXOS_INTEGRATION") != "1",
    reason="集成测试需要开端口，设置环境变量 PAXOS_INTEGRATION=1 后运行",
)

BASE_PORT = 17950


def run_cluster(protocol: str, ops: int = 20) -> None:
    spec = ClusterSpec(
        nodes=3,
        protocol=protocol,
        base_port=BASE_PORT,
        data_root=Path("data/itest"),
        log_root=Path("run/itest-logs"),
    )
    cluster = Cluster(spec)
    try:
        cluster.start()
        targets = [(ep.node_id, ep.host, ep.port) for ep in cluster.endpoints]
        summary = asyncio.run(LoadGen(targets, concurrency=4, duration=10.0, total_ops=ops).run())
        assert summary.ok >= int(ops * 0.8), f"{protocol} 成功率过低: {summary.as_dict()}"

        logs = cluster.dump_logs()
        slots: set[int] = set()
        for chosen in logs.values():
            slots.update(chosen)
        for slot in slots:
            values = {chosen[slot] for chosen in logs.values() if slot in chosen}
            assert len(values) == 1, f"slot {slot} 上出现不同值: {values}"
    finally:
        cluster.stop()


@pytest.mark.parametrize("protocol", ["basic", "multipaxos", "fast"])
def test_protocol_reaches_consensus(protocol: str) -> None:
    run_cluster(protocol)


def test_crash_recovery_keeps_log() -> None:
    spec = ClusterSpec(
        nodes=3,
        protocol="multipaxos",
        base_port=BASE_PORT + 10,
        data_root=Path("data/itest2"),
        log_root=Path("run/itest2-logs"),
    )
    cluster = Cluster(spec)
    try:
        cluster.start()
        targets = [(cluster.endpoints[1].node_id, cluster.endpoints[1].host, cluster.endpoints[1].port)]
        first = asyncio.run(LoadGen(targets, concurrency=2, duration=10.0, total_ops=10).run())
        assert first.ok > 0

        victim = "n3"
        cluster.kill_node(victim)
        after = asyncio.run(LoadGen(targets, concurrency=2, duration=10.0, total_ops=10).run())
        assert after.ok > 0, "杀掉一个非 Leader 节点后应该仍然可以提交"
        cluster.restart_node(victim)
        status = cluster.admin(victim, {"cmd": "status"})
        assert (status.get("learner") or {}).get("chosen_slots", 0) > 0, "重启后应该从日志恢复"
    finally:
        cluster.stop()
