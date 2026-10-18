"""实验用例集：把作业第四节的"对比项"翻译成可执行的用例。

作业要求对比的东西很多，如果一条条手工跑会非常痛苦。这里把它们整理成
**成组的用例**（同一组用例共用一次集群启动），一条命令跑完并落成 CSV。

用例分四类（``group`` 字段）：

============  ===========================================================
group         回答作业里的哪个问题
============  ===========================================================
``protocol``  Basic / Multi-Paxos / Fast Paxos 谁快、快多少、消息数差多少
``nodes``     3/4/5 节点的多数派与容错能力
``rtt``       网络 RTT 到多少时吞吐开始崩
``loss``      丢包到多少时吞吐开始崩
``fsync``     关掉 fsync 能快多少、代价是什么
``batch``     批处理窗口对吞吐/延迟的影响
``concurrency`` 并发提案数把系统推到什么拐点
============  ===========================================================
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace


@dataclass(frozen=True)
class Case:
    """一个实验用例。"""

    group: str
    protocol: str
    nodes: int
    concurrency: int
    fsync: bool = True
    rtt_ms: float = 1.0
    loss: float = 0.0
    batch_ms: float = 0.0
    target: str = "near"
    duration: float = 8.0
    label: str = ""
    notes: str = ""

    @property
    def run_id(self) -> str:
        parts = [
            self.group,
            self.protocol,
            f"n{self.nodes}",
            f"c{self.concurrency}",
            f"rtt{int(self.rtt_ms)}",
            f"loss{int(self.loss * 100)}",
            f"fsync{'on' if self.fsync else 'off'}",
            f"b{int(self.batch_ms)}",
        ]
        if self.target != "near":
            parts.append(self.target)
        return "-".join(parts)

    @property
    def cluster_key(self) -> tuple:
        """同 key 的用例可以复用同一个集群（只有协议/节点数/fsync/批处理需要重启）。"""
        return (self.protocol, self.nodes, self.fsync, self.batch_ms)

    def spec_kwargs(self) -> dict:
        return {
            "protocol": self.protocol,
            "nodes": self.nodes,
            "fsync": self.fsync,
            "batch_ms": self.batch_ms,
        }


def quick_cases(duration: float = 6.0) -> list[Case]:
    """快速验证用（约 2~3 分钟）：够看出三种协议的差别。"""
    cases: list[Case] = []
    for protocol in ("basic", "multipaxos", "fast"):
        for concurrency in (1, 8, 32):
            cases.append(
                Case(
                    group="protocol",
                    protocol=protocol,
                    nodes=3,
                    concurrency=concurrency,
                    duration=duration,
                    target="all" if protocol != "multipaxos" else "near",
                )
            )
    cases.append(Case(group="fsync", protocol="multipaxos", nodes=3, concurrency=8, fsync=False, duration=duration))
    cases.append(Case(group="rtt", protocol="multipaxos", nodes=3, concurrency=8, rtt_ms=50.0, duration=duration))
    return cases


def protocol_cases(duration: float = 8.0) -> list[Case]:
    cases = []
    for protocol in ("basic", "multipaxos", "fast"):
        for concurrency in (1, 4, 8, 16, 32):
            cases.append(
                Case(
                    group="protocol",
                    protocol=protocol,
                    nodes=3,
                    concurrency=concurrency,
                    duration=duration,
                    # Basic/Fast 没有 Leader 仲裁 slot，必须让客户端分散到多个副本，
                    # 才能测到真实的"并发提案冲突"；Multi-Paxos 只走 Leader。
                    target="all" if protocol != "multipaxos" else "near",
                )
            )
    return cases


def node_cases(duration: float = 8.0) -> list[Case]:
    cases = []
    for nodes in (3, 4, 5):
        for protocol in ("multipaxos", "basic"):
            cases.append(
                Case(
                    group="nodes",
                    protocol=protocol,
                    nodes=nodes,
                    concurrency=8,
                    duration=duration,
                    target="near",
                )
            )
    # Fast Paxos 在 3 节点上快速仲裁等于全体，4/5 节点才有意义
    for nodes in (4, 5):
        cases.append(Case(group="nodes", protocol="fast", nodes=nodes, concurrency=8, duration=duration, target="all"))
    return cases


def rtt_cases(duration: float = 8.0) -> list[Case]:
    cases = []
    for protocol in ("basic", "multipaxos", "fast"):
        for rtt in (1.0, 10.0, 50.0, 100.0):
            cases.append(
                Case(
                    group="rtt",
                    protocol=protocol,
                    nodes=3,
                    concurrency=8,
                    rtt_ms=rtt,
                    duration=duration,
                    target="all" if protocol != "multipaxos" else "near",
                )
            )
    return cases


def loss_cases(duration: float = 8.0) -> list[Case]:
    cases = []
    for protocol in ("multipaxos", "basic"):
        for loss in (0.0, 0.01, 0.05, 0.10):
            cases.append(
                Case(
                    group="loss",
                    protocol=protocol,
                    nodes=3,
                    concurrency=8,
                    loss=loss,
                    duration=duration,
                    target="near",
                )
            )
    return cases


def fsync_cases(duration: float = 8.0) -> list[Case]:
    cases = []
    for fsync in (True, False):
        for concurrency in (1, 8, 32):
            cases.append(
                Case(
                    group="fsync",
                    protocol="multipaxos",
                    nodes=3,
                    concurrency=concurrency,
                    fsync=fsync,
                    duration=duration,
                )
            )
    return cases


def batch_cases(duration: float = 8.0) -> list[Case]:
    cases = []
    for batch_ms in (0.0, 2.0, 5.0):
        cases.append(
            Case(
                group="batch",
                protocol="multipaxos",
                nodes=3,
                concurrency=32,
                batch_ms=batch_ms,
                duration=duration,
            )
        )
    return cases


def full_cases(duration: float = 8.0) -> list[Case]:
    """完整矩阵（约 10~15 分钟）。"""
    return (
        protocol_cases(duration)
        + node_cases(duration)
        + rtt_cases(duration)
        + loss_cases(duration)
        + fsync_cases(duration)
        + batch_cases(duration)
    )


PROFILES = {
    "quick": quick_cases,
    "protocol": protocol_cases,
    "nodes": node_cases,
    "rtt": rtt_cases,
    "loss": loss_cases,
    "fsync": fsync_cases,
    "batch": batch_cases,
    "full": full_cases,
}


# --------------------------------------------------------------------- 故障场景
@dataclass(frozen=True)
class FaultStep:
    """故障时间线上的一个动作。"""

    at_s: float
    action: str  # kill | restart | partition | heal | inject
    node: str = ""
    groups: tuple[tuple[str, ...], ...] = ()
    kwargs: dict = field(default_factory=dict)


@dataclass(frozen=True)
class FaultCase:
    """一个故障/恢复实验。"""

    name: str
    protocol: str
    nodes: int
    concurrency: int
    steps: tuple[FaultStep, ...]
    duration: float = 12.0
    rtt_ms: float = 1.0
    fsync: bool = True
    description: str = ""
    expect: str = ""
    #: 客户端接入点：near / all / leader / "n3,n4,n5"
    target: str = "near"


def fault_cases(duration: float = 12.0) -> list[FaultCase]:
    """作业要求的故障类实验：kill Acceptor / kill Leader / 网络分区。"""
    return [
        FaultCase(
            name="kill_acceptor_3n",
            protocol="multipaxos",
            nodes=3,
            concurrency=8,
            steps=(FaultStep(2.5, "kill", node="__follower__"), FaultStep(7.0, "restart", node="__follower__")),
            duration=duration,
            description="3 节点集群杀掉一个**非 Leader** 的 Acceptor（此时只剩 2/3，仍是多数派）",
            expect="服务不中断：多数派 2/3 还在，P99 可能有毛刺",
        ),
        FaultCase(
            name="kill_acceptor_4n",
            protocol="multipaxos",
            nodes=4,
            concurrency=8,
            steps=(FaultStep(2.5, "kill", node="__follower__"), FaultStep(7.0, "restart", node="__follower__")),
            duration=duration,
            description="4 节点集群杀掉一个非 Leader 的 Acceptor（剩 3/4，仍是多数派）",
            expect="服务不中断，但注意容错能力并没有比 3 节点更强",
        ),
        FaultCase(
            name="kill_two_acceptors_4n",
            protocol="multipaxos",
            nodes=4,
            concurrency=8,
            steps=(
                FaultStep(2.5, "kill", node="__follower__"),
                FaultStep(3.5, "kill", node="__follower2__"),
                FaultStep(8.0, "restart", node="__follower__"),
                FaultStep(8.0, "restart", node="__follower2__"),
            ),
            duration=duration,
            description="4 节点杀掉两个 Acceptor（剩 2/4，凑不够多数派 3）",
            expect="不可用：客户端持续失败，直到节点恢复；但已决定的值不会丢",
        ),
        FaultCase(
            name="kill_leader_3n",
            protocol="multipaxos",
            nodes=3,
            concurrency=8,
            steps=(FaultStep(2.5, "kill", node="__leader__"), FaultStep(8.0, "restart", node="__leader__")),
            duration=duration,
            description="杀掉 Multi-Paxos 的 Leader，观察重新选主与恢复",
            expect="短暂不可用（选举 + 恢复），随后恢复；恢复时间就是「Leader 故障恢复多久」的答案",
        ),
        FaultCase(
            name="kill_leader_fast_3n",
            protocol="fast",
            nodes=3,
            concurrency=8,
            steps=(FaultStep(2.5, "kill", node="__leader__"), FaultStep(8.0, "restart", node="__leader__")),
            duration=duration,
            description="Fast Paxos 下杀掉 Leader（快速轮次失效，需要重开）",
            expect="比 Multi-Paxos 恢复更慢：要等新 Leader 打开新的快速轮",
        ),
        FaultCase(
            name="partition_2v2_4n",
            protocol="multipaxos",
            nodes=4,
            concurrency=8,
            steps=(
                FaultStep(2.5, "partition", groups=(("n1", "n2"), ("n3", "n4"))),
                FaultStep(7.5, "heal"),
            ),
            duration=duration,
            description="4 节点切成 2|2 两个分区（两边都凑不够多数派 3）",
            expect="整体不可用，但**不会**出现两个 Leader 各自提交不同的值（安全性优先于可用性）",
        ),
        FaultCase(
            name="partition_2v3_5n",
            protocol="multipaxos",
            nodes=5,
            concurrency=8,
            steps=(
                FaultStep(2.5, "partition", groups=(("n1", "n2"), ("n3", "n4", "n5"))),
                FaultStep(7.5, "heal"),
            ),
            duration=duration,
            description="5 节点切成 2|3，客户端**只连多数派那一侧**（n3/n4/n5）",
            expect="多数派一侧完全不受影响：这是 5 节点比 3 节点值钱的地方",
            target="n3,n4,n5",
        ),
        FaultCase(
            name="partition_minority_5n",
            protocol="multipaxos",
            nodes=5,
            concurrency=8,
            steps=(
                FaultStep(2.5, "partition", groups=(("n1", "n2"), ("n3", "n4", "n5"))),
                FaultStep(7.5, "heal"),
            ),
            duration=duration,
            description="同样的 2|3 分区，但客户端**只连少数派那一侧**（n1/n2）",
            expect="少数派一侧完全不可用（凑不到 3 台），恢复后才能继续——安全性优先于可用性",
            target="n1,n2",
        ),
    ]


def with_duration(cases: list[Case], duration: float) -> list[Case]:
    return [replace(case, duration=duration) for case in cases]
