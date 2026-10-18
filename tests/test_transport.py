"""传输层与负载生成器的单元测试（不需要开端口）。

* 报文编解码必须无损（JSON Lines 的一个坑是中文和换行）
* 时延画像的换算（RTT → 单程）
* 分位数计算口径（报告里的 P50/P95/P99 就靠它）
* 汇总逻辑：错误分类、错误率、吞吐
"""

from __future__ import annotations

import pytest

from paxos.loadgen import LoadSummary, OpRecord, percentile, summarize
from paxos.transport import Counter, FaultProfile, decode, encode


def test_encode_decode_round_trip() -> None:
    message = {"t": "prepare", "ins": 3, "b": [5, 1], "note": "中文也要能过"}
    line = encode(message)
    assert line.endswith(b"\n")
    assert decode(line) == message


def test_decode_accepts_str_and_bytes() -> None:
    message = {"t": "ping"}
    assert decode(encode(message)) == message
    assert decode(encode(message).decode("utf-8")) == message


def test_fault_profile_one_way_delay() -> None:
    profile = FaultProfile(rtt_ms=100.0)
    assert profile.one_way_delay == pytest.approx(0.05)
    assert FaultProfile(rtt_ms=0.0).one_way_delay == 0.0
    assert FaultProfile(rtt_ms=-5.0).one_way_delay == 0.0  # 负数当作 0，不抛异常


def test_fault_profile_serialisation() -> None:
    data = FaultProfile(rtt_ms=10.1234, loss=0.05, jitter_ms=1.5, partition=True).as_dict()
    assert data["rtt_ms"] == 10.123
    assert data["loss"] == 0.05
    assert data["partition"] is True


def test_counter_tracks_by_type() -> None:
    counter = Counter()
    counter.count_sent("prepare")
    counter.count_sent("prepare")
    counter.count_sent("accept")
    counter.count_recv("promise")
    data = counter.as_dict()
    assert data["sent"] == 3
    assert data["by_type"]["prepare"] == 2
    assert data["by_type"]["accept"] == 1


def test_percentile_matches_numpy_convention() -> None:
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert percentile(values, 0.5) == pytest.approx(3.0)
    assert percentile(values, 0.0) == pytest.approx(1.0)
    assert percentile(values, 1.0) == pytest.approx(5.0)
    assert percentile([], 0.5) == 0.0
    assert percentile([7.0], 0.99) == pytest.approx(7.0)


def test_summarize_counts_errors_by_kind() -> None:
    records = [
        OpRecord(seq=1, ts_ms=0.0, latency_ms=10.0, ok=True, err="", target="n1", node="n1", attempts=1),
        OpRecord(seq=2, ts_ms=20.0, latency_ms=10.0, ok=True, err="", target="n1", node="n2", attempts=1),
        OpRecord(seq=3, ts_ms=40.0, latency_ms=5.0, ok=False, err="timeout", target="n1", node="", attempts=0),
        OpRecord(seq=4, ts_ms=60.0, latency_ms=5.0, ok=False, err="not_leader", target="n1", node="", attempts=0),
    ]
    summary = summarize(records)
    assert summary.ops == 4
    assert summary.ok == 2
    assert summary.errors == 2
    assert summary.error_rate == pytest.approx(0.5)
    assert summary.error_kinds == {"timeout": 1, "not_leader": 1}
    assert summary.p50 == pytest.approx(10.0)


def test_summarize_warmup_filter() -> None:
    records = [
        OpRecord(seq=1, ts_ms=0.0, latency_ms=100.0, ok=True, err="", target="n1", node="n1", attempts=1),
        OpRecord(seq=2, ts_ms=500.0, latency_ms=1.0, ok=True, err="", target="n1", node="n1", attempts=1),
    ]
    summary = summarize(records, warmup_ms=100.0)
    assert summary.ops == 1
    assert summary.p50 == pytest.approx(1.0)


def test_summary_as_dict_is_json_friendly() -> None:
    summary: LoadSummary = summarize(
        [OpRecord(seq=1, ts_ms=0.0, latency_ms=3.0, ok=True, err="", target="n1", node="n1", attempts=1)]
    )
    data = summary.as_dict()
    assert {"ops", "ok", "throughput", "p50", "p99", "error_rate"}.issubset(data)
    assert all(not isinstance(value, complex) for value in data.values())
