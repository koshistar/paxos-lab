"""提案编号（Ballot）与多数派计算。

Paxos 的安全性依赖一条铁律：**任意两个多数派必然相交**。因此

* 一个 Acceptor 一旦承诺了编号 ``b``，就不再接受编号小于 ``b`` 的提案；
* 提案编号必须全局唯一且单调递增。

编号用二元组 ``(round, node)`` 表示并按字典序比较：

* ``round`` 是"轮次"，每次重试 +1，保证比之前所有编号都大；
* ``node``  是提案者编号，用来打破轮次相同时的平局（同一轮里不同提案者
  会生成不同的编号，从而不会互相覆盖）。

Fast Paxos 的"快速轮"没有指定提案者，用 ``node = FAST_NODE``（-1）表示，
于是 ``(r, -1) < (r, c)``：快速轮之后，任何协调者都可以用同一轮次加自己的
编号发起经典轮来"覆盖"快速轮，这正是 Fast Paxos 冲突恢复所需要的性质。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

#: 快速轮（无提案者）在编号里使用的占位节点号
FAST_NODE = -1


@dataclass(frozen=True, order=True)
class Ballot:
    """不可变的提案编号，按 ``(round, node)`` 字典序比较。"""

    round: int
    node: int

    # ---------------------------------------------------------------- 构造
    @classmethod
    def parse(cls, raw: Any) -> Ballot:
        """把线上的 ``[round, node]`` 还原成 Ballot（也接受 Ballot 本身）。"""
        if isinstance(raw, Ballot):
            return raw
        if isinstance(raw, list | tuple) and len(raw) == 2:
            return cls(int(raw[0]), int(raw[1]))
        raise ValueError(f"无法解析提案编号: {raw!r}")

    def to_list(self) -> list[int]:
        """转成可 JSON 序列化的形式。"""
        return [self.round, self.node]

    # ---------------------------------------------------------------- 性质
    @property
    def is_fast(self) -> bool:
        """是否属于 Fast Paxos 的快速轮。"""
        return self.node == FAST_NODE

    def bump(self, node: int) -> Ballot:
        """轮次 +1 并指定提案者：用于"我比已知的所有编号都大"的重试。"""
        return Ballot(self.round + 1, node)

    def __str__(self) -> str:  # 日志里更好读
        return f"({self.round},{self.node})"


def later(a: Ballot | None, b: Ballot | None) -> Ballot | None:
    """返回两者中较新的编号，``None`` 视为"没有编号"。"""
    if a is None:
        return b
    if b is None:
        return a
    return a if a >= b else b


# -------------------------------------------------------------------- 多数派
def quorum_size(n: int) -> int:
    """经典多数派大小 ⌊n/2⌋+1。

    ========== ====== ================ ==================
    节点数 n   多数派  可容忍故障数      说明
    ========== ====== ================ ==================
    3          2      1                最小可用集合
    4          3      1                多一个节点却不多一份容错
    5          3      2                容错才真正提升
    ========== ====== ================ ==================
    """
    if n <= 0:
        raise ValueError("节点数必须为正")
    return n // 2 + 1


def fast_quorum_size(n: int) -> int:
    """Fast Paxos 的快速仲裁大小 ⌈3n/4⌉。

    快速轮里 Acceptor 可能对同一个轮次接受不同的值，因此必须用更大的仲裁
    集合才能保证"同一个值不可能被两个不同的快速仲裁同时选定"（要求
    ``2 * F > n``）。n=3 时 F=3（等于全体），这也是"3 节点上 Fast Paxos
    收益最高的同时最脆弱"的数学根源。
    """
    if n <= 0:
        raise ValueError("节点数必须为正")
    return -(-3 * n // 4)


def max_failures(n: int) -> int:
    """仍然可用的最大故障节点数 n - quorum。"""
    return n - quorum_size(n)
