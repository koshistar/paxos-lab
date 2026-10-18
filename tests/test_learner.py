"""Learner / KV 状态机 / 冲突恢复规则的单元测试。

重点：
* 必须按 slot 顺序应用（不能"谁先学到谁先应用"）
* 缺口要能回填、缺口不能丢
* 同一个 slot 出现两个不同值 = 安全性违例，必须计数
* ``resolve_value`` 的"最高轮次 + 最大计数"规则
"""

from __future__ import annotations

import json

from paxos.ballot import Ballot
from paxos.kv import KVStateMachine, checksum
from paxos.learner import Learner
from paxos.proposer import pick_highest_accepted, resolve_value
from paxos.storage import Journal


def make_learner(
    work_dir, applied: list[tuple[int, str]] | None = None
) -> tuple[Learner, list[tuple[int, str]], Journal]:
    applied = applied if applied is not None else []
    journal = Journal(work_dir / "learner.log", mode="flush")
    learner = Learner("n1", journal, on_apply=lambda slot, value: applied.append((slot, value)), fsync=False)
    return learner, applied, journal


def test_applies_in_slot_order(work_dir) -> None:
    learner, applied, journal = make_learner(work_dir)
    try:
        learner.choose(0, "a")
        learner.choose(1, "b")
        assert applied == [(0, "a"), (1, "b")]
        assert learner.next_apply == 2
    finally:
        journal.close()


def test_out_of_order_learn_waits_for_gap(work_dir) -> None:
    """先学到 slot 2，必须等 slot 0/1 到齐才能应用 —— 这是日志顺序的来源。"""
    learner, applied, journal = make_learner(work_dir)
    try:
        learner.choose(2, "c")
        assert applied == []
        assert learner.missing() == [0, 1]
        learner.choose(0, "a")
        assert applied == [(0, "a")]
        learner.choose(1, "b")
        assert applied == [(0, "a"), (1, "b"), (2, "c")]
    finally:
        journal.close()


def test_safety_violation_is_counted(work_dir) -> None:
    learner, applied, journal = make_learner(work_dir)
    try:
        learner.choose(0, "a")
        learner.choose(0, "b")  # 同一个 slot 两个值 = 灾难
        assert learner.safety_violations == 1
        assert applied == [(0, "a")]
    finally:
        journal.close()


def test_replay_restores_applied_log(work_dir) -> None:
    learner, applied, journal = make_learner(work_dir)
    try:
        learner.choose(0, "a")
        learner.choose(1, "b")
    finally:
        journal.close()

    learner2, applied2, journal2 = make_learner(work_dir)
    try:
        assert applied2 == [(0, "a"), (1, "b")]
        assert learner2.next_apply == 2
    finally:
        journal2.close()


def test_kv_is_idempotent_by_request_id(work_dir) -> None:
    state = KVStateMachine()
    command = json.dumps({"req": "r1", "op": "set", "k": "x", "v": 1})
    state.apply(command)
    state.apply(command)  # 客户端重试，必须被幂等键挡住
    assert state.data == {"x": 1}
    assert state.duplicates == 1
    assert state.applied == 1, "重复请求不应重复计入应用次数"


def test_kv_incr_and_del() -> None:
    state = KVStateMachine()
    for index in range(3):
        state.apply(json.dumps({"req": f"i{index}", "op": "incr", "k": "cnt", "v": 1}))
    assert state.data["cnt"] == 3
    state.apply(json.dumps({"req": "d", "op": "del", "k": "cnt"}))
    assert "cnt" not in state.data


def test_kv_checksum_is_stable_and_sensitive() -> None:
    left = {"a": 1, "b": 2}
    right = {"b": 2, "a": 1}
    assert checksum(left) == checksum(right)  # 与插入顺序无关
    assert checksum(left) != checksum({"a": 1, "b": 3})


def test_pick_highest_accepted() -> None:
    payloads = [
        {"ok": True, "accepted": [[0, [2, 0], "b"]]},
        {"ok": True, "accepted": [[0, [5, 1], "c"]]},
        {"ok": True, "accepted": []},
    ]
    ballot, value = pick_highest_accepted(payloads)
    assert ballot == Ballot(5, 1)
    assert value == "c"


def test_resolve_value_uses_highest_ballot() -> None:
    payloads = [
        {"accepted": [[0, [2, 0], "old"]]},
        {"accepted": [[0, [3, 0], "new"]]},
    ]
    value, ballot, count = resolve_value(payloads, 0, "mine")
    assert value == "new"
    assert ballot == Ballot(3, 0)
    assert count == 1


def test_resolve_value_prefers_majority_at_fast_ballot() -> None:
    """冲突恢复：同一个快速轮次里 A 出现 3 次、B 出现 1 次 → 必须选 A。"""
    payloads = [{"accepted": [[7, [4, -1], "A"]]} for _ in range(3)]
    payloads += [{"accepted": [[7, [4, -1], "B"]]}]
    value, _ballot, count = resolve_value(payloads, 7, "mine")
    assert value == "A"
    assert count == 3


def test_resolve_value_tie_is_free_choice() -> None:
    """并列说明谁也没被选定 → 可以自由用自己的值（不是"卡死"）。"""
    payloads = [
        {"accepted": [[7, [4, -1], "A"]]},
        {"accepted": [[7, [4, -1], "B"]]},
    ]
    value, _ballot, count = resolve_value(payloads, 7, "mine")
    assert value == "mine"
    assert count == 0


def test_resolve_value_ignores_other_slots() -> None:
    payloads = [{"accepted": [[3, [9, 0], "other"]]}]
    value, ballot, _count = resolve_value(payloads, 7, "mine")
    assert value == "mine"
    assert ballot is None
