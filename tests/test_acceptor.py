"""Acceptor 的单元测试 —— 安全性规则就靠它守住。

覆盖：
* Prepare 承诺后拒绝更小的编号
* 同一编号重复 Prepare 幂等（Leader 空槽修复依赖它）
* Accept 用 ``>=`` 判断（重传幂等）
* Fast Paxos 同一轮次接受不同值 = 冲突（不覆盖、明确拒绝）
* 重启后从日志恢复 promised/accepted（崩溃恢复）
* 批量 accept 把多条记录写成一条日志（批处理优化）
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from paxos.acceptor import ALL_INSTANCES, Acceptor
from paxos.ballot import Ballot
from paxos.storage import Journal


def make_acceptor(work_dir: Path, name: str = "n1", fsync: bool = False) -> tuple[Acceptor, Journal]:
    journal = Journal(work_dir / f"{name}-acceptor.log", mode="fsync" if fsync else "buffer")
    return Acceptor(name, journal, fsync=fsync), journal


def run(coro):
    return asyncio.run(coro)


def test_promise_blocks_lower_ballot(work_dir: Path) -> None:
    acceptor, journal = make_acceptor(work_dir)
    try:
        first = run(acceptor.prepare(0, Ballot(2, 1)))
        assert first["ok"] is True
        # 更小的编号必须被拒绝，并回报已承诺的编号
        second = run(acceptor.prepare(0, Ballot(1, 0)))
        assert second["ok"] is False
        assert Ballot.parse(second["promised"]) == Ballot(2, 1)
    finally:
        journal.close()


def test_prepare_is_idempotent_for_same_ballot(work_dir: Path) -> None:
    """同一编号重复 Prepare 必须成功 —— 否则空槽修复会永远失败。"""
    acceptor, journal = make_acceptor(work_dir)
    try:
        assert run(acceptor.prepare(3, Ballot(5, 0)))["ok"] is True
        assert run(acceptor.prepare(3, Ballot(5, 0)))["ok"] is True
    finally:
        journal.close()


def test_accept_requires_not_older_ballot(work_dir: Path) -> None:
    acceptor, journal = make_acceptor(work_dir)
    try:
        run(acceptor.prepare(0, Ballot(3, 0)))
        older = run(acceptor.accept(Ballot(2, 0), [(0, "v-old")]))
        assert older["ok"] is False
        assert 0 not in older["accepted"]
        equal = run(acceptor.accept(Ballot(3, 0), [(0, "v-ok")]))
        assert equal["ok"] is True
        assert equal["accepted"] == [0]
    finally:
        journal.close()


def test_accept_is_idempotent_for_same_ballot_and_value(work_dir: Path) -> None:
    acceptor, journal = make_acceptor(work_dir)
    try:
        assert run(acceptor.accept(Ballot(1, 0), [(0, "x")]))["ok"] is True
        again = run(acceptor.accept(Ballot(1, 0), [(0, "x")]))
        assert again["ok"] is True
        assert again["conflict"] is False
    finally:
        journal.close()


def test_fast_round_conflict_is_detected_not_overwritten(work_dir: Path) -> None:
    """同一个快速轮次里投了两个不同的值 → 明确报冲突，且不覆盖原值。"""
    acceptor, journal = make_acceptor(work_dir)
    try:
        fast = Ballot(4, -1)
        assert run(acceptor.accept(fast, [(0, "A")]))["ok"] is True
        conflicting = run(acceptor.accept(fast, [(0, "B")]))
        assert conflicting["ok"] is False
        assert conflicting["conflict"] is True
        # 原值没被覆盖
        assert acceptor.accepted[0] == (fast, "A")
    finally:
        journal.close()


def test_batch_write_is_one_record(work_dir: Path) -> None:
    acceptor, journal = make_acceptor(work_dir)
    try:
        run(acceptor.accept(Ballot(2, 0), [(0, "a"), (1, "b"), (2, "c")]))
        journal.flush()  # buffer 模式下要显式刷盘才能读到
        lines = [line for line in (work_dir / "n1-acceptor.log").read_text(encoding="utf-8").splitlines() if line]
        assert len(lines) == 1, "批量 accept 应该只写一条日志记录"
        assert '"k":"m"' in lines[0]
    finally:
        journal.close()


def test_recovery_replays_promised_and_accepted(work_dir: Path) -> None:
    acceptor, journal = make_acceptor(work_dir)
    try:
        run(acceptor.prepare(0, Ballot(6, 1)))
        run(acceptor.accept(Ballot(6, 1), [(0, "decided")]))
        run(acceptor.prepare(ALL_INSTANCES, Ballot(7, 1)))
    finally:
        journal.close()

    # 模拟进程重启：新对象重放同一个日志文件
    recovered, journal2 = make_acceptor(work_dir)
    try:
        assert recovered.promised_all == Ballot(7, 1)
        assert recovered.promised[0] == Ballot(6, 1)
        assert recovered.accepted[0] == (Ballot(6, 1), "decided")
        # 恢复后依然拒绝更小的编号
        assert run(recovered.prepare(0, Ballot(1, 0)))["ok"] is False
    finally:
        journal2.close()


def test_global_prepare_covers_all_instances(work_dir: Path) -> None:
    acceptor, journal = make_acceptor(work_dir)
    try:
        run(acceptor.accept(Ballot(1, 0), [(0, "a"), (1, "b")]))
        result = run(acceptor.prepare(ALL_INSTANCES, Ballot(2, 0)))
        assert result["ok"] is True
        reported = {int(item[0]) for item in result["accepted"]}
        assert reported == {0, 1}
    finally:
        journal.close()


def test_buffer_mode_loses_unflushed_records(work_dir: Path) -> None:
    """fsync=off（buffer 模式）时，未刷盘的记录在崩溃后确实会丢。"""
    journal = Journal(work_dir / "buffered.log", mode="buffer")
    journal.append({"k": "pa", "b": [9, 0]})
    # 不 flush，模拟 kill -9：这里用"丢弃句柄"的方式验证缓冲确实没落盘
    assert (work_dir / "buffered.log").stat().st_size == 0
    journal.flush()
    assert (work_dir / "buffered.log").stat().st_size > 0
    journal.close()


@pytest.mark.parametrize("mode", ["fsync", "flush", "buffer"])
def test_all_journal_modes_round_trip(work_dir: Path, mode: str) -> None:
    path = work_dir / f"{mode}.log"
    journal = Journal(path, mode=mode)
    journal.append({"k": "a", "i": 0, "b": [1, 0], "v": "hello"})
    journal.close()
    journal2 = Journal(path, mode=mode)
    records = list(journal2.replay())
    journal2.close()
    assert records and records[0]["v"] == "hello"


def test_journal_skips_torn_tail(work_dir: Path) -> None:
    """崩溃时可能只写了半行，重放必须跳过而不是崩掉。"""
    path = work_dir / "torn.log"
    path.write_text('{"k":"a","i":0,"b":[1,0],"v":"good"}\n{"k":"a","i":1,"b":', encoding="utf-8")
    journal = Journal(path, mode="flush")
    records = list(journal.replay())
    journal.close()
    assert len(records) == 1
    assert records[0]["v"] == "good"
