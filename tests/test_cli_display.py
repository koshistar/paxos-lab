"""CLI 里"纯展示"逻辑的测试（不需要网络、不开端口）。

为什么要专门测这个：``up`` 命令在**拉完集群之后**才打印状态表。如果打印代码有 bug，
集群已经起来了、父进程却崩了，就会留下占端口的孤儿进程 —— 本工程真踩过这个坑
（``f"{title:<width}"`` 这种写法在宽度是变量时是非法的）。

所以这里用假集群（stub）把这些展示函数全部跑一遍，保证它们在各种脏数据下都不抛异常。
"""

from __future__ import annotations

import pytest

from paxos.cli import (
    STATUS_COLUMNS,
    display_width,
    pad_display,
    print_quorum_table,
    print_status_table,
)


class StubCluster:
    """只实现展示函数用到的那几个方法。"""

    def __init__(self, statuses: dict) -> None:
        self._statuses = statuses

    def status_all(self) -> dict:
        return self._statuses


def healthy_status(**overrides) -> dict:
    base = {
        "ok": True,
        "id": "n1",
        "role": "leader",
        "leader": "n1",
        "max_round": 3,
        "applied_index": 42,
        "counters": {"elections": 1},
        "messages": {"sent": 128, "dropped": 2},
        "learner": {"chosen_slots": 43},
    }
    base.update(overrides)
    return base


def test_display_width_counts_cjk_as_two() -> None:
    assert display_width("abc") == 3
    assert display_width("节点") == 4
    assert display_width("n1") == 2
    assert display_width("消息(发/丢)") == 8 + 3


def test_pad_display_aligns_by_display_width() -> None:
    assert pad_display("abc", 6) == "abc   "
    assert pad_display("节点", 6) == "节点  "  # 4 列 + 2 空格
    assert display_width(pad_display("节点", 6)) == 6
    # 超长内容不能把列挤爆（宁可溢出也不抛异常）
    assert pad_display("a-very-long-value", 4).startswith("a-very-long-value")


def test_status_columns_widths_are_sane() -> None:
    assert len(STATUS_COLUMNS) == 8
    for title, width in STATUS_COLUMNS:
        assert width > display_width(title), f"{title} 的列宽 {width} 装不下自己"


@pytest.mark.parametrize(
    "statuses",
    [
        {"n1": healthy_status()},
        # 节点 DOWN（进程不在、admin 口不通）
        {"n1": {"ok": False, "err": "timeout"}},
        # 各种字段缺失：老版本节点、刚启动还没选出 Leader
        {"n1": {"ok": True}},
        {"n1": healthy_status(leader=None, role="follower", learner={}, messages={}, counters={})},
        # 多个节点 + 混合状态
        {
            "n1": healthy_status(),
            "n2": healthy_status(role="follower", leader="n1"),
            "n3": {"ok": False},
        },
        # 没有节点（全挂）
        {},
    ],
)
def test_print_status_table_never_raises(statuses: dict, capsys: pytest.CaptureFixture[str]) -> None:
    print_status_table(StubCluster(statuses))
    out = capsys.readouterr().out
    assert "节点" in out and "Leader" in out


def test_print_status_table_shows_down_marker(capsys: pytest.CaptureFixture[str]) -> None:
    print_status_table(StubCluster({"n9": {"ok": False}}))
    assert "DOWN" in capsys.readouterr().out


@pytest.mark.parametrize("nodes", [3, 4, 5])
def test_print_quorum_table(nodes: int, capsys: pytest.CaptureFixture[str]) -> None:
    print_quorum_table(nodes)
    out = capsys.readouterr().out
    assert f"n={nodes}" in out
