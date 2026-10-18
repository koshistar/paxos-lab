"""提案编号与多数派计算的单元测试。

重点覆盖两件事：
1. 编号必须严格可比、可序列化、可还原（安全性依赖它）；
2. 多数派/快速仲裁的数学性质（任意两个多数派相交）。
"""

from __future__ import annotations

import pytest

from paxos.ballot import (
    FAST_NODE,
    Ballot,
    fast_quorum_size,
    later,
    max_failures,
    quorum_size,
)


def test_ballot_ordering_is_round_then_node() -> None:
    assert Ballot(1, 5) < Ballot(2, 0)  # 轮次优先
    assert Ballot(2, 0) < Ballot(2, 1)  # 同轮比节点号
    assert Ballot(2, 1) == Ballot(2, 1)


def test_fast_ballot_is_lower_than_classic_ballot_of_same_round() -> None:
    """Fast Paxos 靠这个性质让经典轮覆盖快速轮。"""
    assert Ballot(3, FAST_NODE) < Ballot(3, 0)
    assert Ballot(3, FAST_NODE).is_fast
    assert not Ballot(3, 0).is_fast


def test_ballot_round_trip() -> None:
    ballot = Ballot(7, 2)
    assert Ballot.parse(ballot.to_list()) == ballot
    assert Ballot.parse(ballot) == ballot
    assert ballot.to_list() == [7, 2]


def test_ballot_bump_is_strictly_greater() -> None:
    ballot = Ballot(7, 2)
    assert ballot.bump(9) > ballot
    assert ballot.bump(0).round == 8


def test_ballot_parse_rejects_garbage() -> None:
    with pytest.raises(ValueError):
        Ballot.parse("not-a-ballot")
    with pytest.raises(ValueError):
        Ballot.parse([1, 2, 3])


def test_later_handles_none() -> None:
    assert later(None, None) is None
    assert later(Ballot(1, 0), None) == Ballot(1, 0)
    assert later(None, Ballot(1, 0)) == Ballot(1, 0)
    assert later(Ballot(1, 0), Ballot(2, 0)) == Ballot(2, 0)


@pytest.mark.parametrize(
    ("nodes", "quorum", "fast", "failures"),
    [
        (3, 2, 3, 1),
        (4, 3, 3, 1),
        (5, 3, 4, 2),
        (6, 4, 5, 2),
        (7, 4, 6, 3),
    ],
)
def test_quorum_math(nodes: int, quorum: int, fast: int, failures: int) -> None:
    assert quorum_size(nodes) == quorum
    assert fast_quorum_size(nodes) == fast
    assert max_failures(nodes) == failures


def test_three_and_four_nodes_tolerate_same_failures() -> None:
    """这就是"4 节点不一定比 3 节点好"的数学依据。"""
    assert max_failures(3) == max_failures(4) == 1
    assert max_failures(5) == 2


@pytest.mark.parametrize("nodes", [3, 4, 5, 6, 7])
def test_any_two_quorums_intersect(nodes: int) -> None:
    """Paxos 安全性的地基：任意两个多数派都相交。"""
    q = quorum_size(nodes)
    assert 2 * q > nodes


@pytest.mark.parametrize("nodes", [3, 4, 5, 6, 7])
def test_fast_quorum_cannot_be_two_disjoint(nodes: int) -> None:
    """Fast Paxos 的前提：两个不同的值不可能各自凑到一个快速仲裁。"""
    fast = fast_quorum_size(nodes)
    assert 2 * fast > nodes


@pytest.mark.parametrize("nodes", [3, 4, 5, 6, 7])
def test_classic_quorum_intersects_every_fast_quorum(nodes: int) -> None:
    """冲突恢复的正确性前提：经典多数派与任何快速仲裁都相交。"""
    assert quorum_size(nodes) + fast_quorum_size(nodes) > nodes


@pytest.mark.parametrize("nodes", [3, 4, 5])
def test_resolve_value_prefers_chosen_value(nodes: int) -> None:
    """恢复时"取最大计数"必须能挑出真正被选定的那个值。

    构造：值 A 被 fast 个 Acceptor 接受（即已选定），值 B 只被剩下的接受。
    经典多数派里看到 A 的次数严格多于 B。
    """
    fast = fast_quorum_size(nodes)
    q = quorum_size(nodes)
    seen_a = q + fast - nodes  # 交集下界
    seen_b = nodes - fast  # 另一值最多只能落在这些节点上
    assert seen_a > seen_b


def test_quorum_size_rejects_empty_cluster() -> None:
    with pytest.raises(ValueError):
        quorum_size(0)
    with pytest.raises(ValueError):
        fast_quorum_size(0)
