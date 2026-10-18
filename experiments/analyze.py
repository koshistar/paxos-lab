"""把 ``results/summary.csv`` 汇总成报告里能直接用的表格。

不依赖任何第三方库，输出：

* ``results/tables.md``      —— 六张 Markdown 表格（协议/节点数/RTT/丢包/fsync/批处理/故障）
* ``results/tables.csv``     —— 同样的数据，长表格式，方便自己再透视
* 同时在终端打印，边跑边看

用法::

    python -m experiments.analyze
    python -m experiments.analyze --csv results/summary.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

RESULTS = Path("results")


# --------------------------------------------------------------------- 读数据
def read_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# --------------------------------------------------------------------- 聚合
def aggregate(
    rows: list[dict[str, Any]],
    keys: list[str],
    metrics: list[str],
) -> list[dict[str, Any]]:
    """按 ``keys`` 分组，对 ``metrics`` 取平均（同一配置重复跑时才有意义）。"""
    buckets: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[tuple(row.get(k, "") for k in keys)].append(row)
    out: list[dict[str, Any]] = []
    for key, group in sorted(buckets.items(), key=lambda kv: [str(x) for x in kv[0]]):
        item: dict[str, Any] = dict(zip(keys, key, strict=False))
        item["runs"] = len(group)
        for metric in metrics:
            values = [num(row.get(metric)) for row in group if row.get(metric) not in ("", None)]
            if not values:
                item[metric] = None
                continue
            item[metric] = statistics.fmean(values)
            item[f"{metric}_stdev"] = statistics.pstdev(values) if len(values) > 1 else 0.0
        out.append(item)
    return out


def md_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def f(value: Any, digits: int = 1, suffix: str = "") -> str:
    if value is None or value == "":
        return "-"
    try:
        return f"{float(value):.{digits}f}{suffix}"
    except (TypeError, ValueError):
        return str(value)


def pct(value: Any, digits: int = 1) -> str:
    if value is None or value == "":
        return "-"
    return f"{float(value) * 100:.{digits}f}%"


# --------------------------------------------------------------------- 各张表
METRICS = [
    "throughput",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "ok",
    "errors",
    "error_rate",
    "msgs_per_op",
    "conflict_rate",
    "leader_changes",
]


def table_protocol(rows: list[dict[str, Any]]) -> str:
    data = aggregate([r for r in rows if r["group"] == "protocol"], ["protocol", "concurrency"], METRICS)
    headers = ["机制", "并发", "成功", "吞吐(ops/s)", "P50(ms)", "P95(ms)", "P99(ms)", "错误率", "消息/op", "冲突率"]
    body = [
        [
            item["protocol"],
            str(item["concurrency"]),
            f(item["ok"], 0),
            f(item["throughput"]),
            f(item["p50_ms"]),
            f(item["p95_ms"]),
            f(item["p99_ms"]),
            pct(item["error_rate"]),
            f(item["msgs_per_op"], 2),
            pct(item["conflict_rate"]) if item.get("conflict_rate") else "-",
        ]
        for item in data
    ]
    return "### 表 1 三种提案机制对比（3 节点，RTT≈1ms，fsync=on）\n\n" + md_table(headers, body)


def table_nodes(rows: list[dict[str, Any]]) -> str:
    data = aggregate(
        [r for r in rows if r["group"] == "nodes"], ["protocol", "nodes", "quorum", "fast_quorum"], METRICS
    )
    headers = ["机制", "节点数", "多数派Q", "快速仲裁F", "吞吐(ops/s)", "P50(ms)", "P99(ms)", "消息/op"]
    body = [
        [
            item["protocol"],
            str(item["nodes"]),
            str(item["quorum"]),
            str(item["fast_quorum"]),
            f(item["throughput"]),
            f(item["p50_ms"]),
            f(item["p99_ms"]),
            f(item["msgs_per_op"], 2),
        ]
        for item in data
    ]
    return "### 表 2 节点数对吞吐/延迟的影响（并发 8）\n\n" + md_table(headers, body)


def table_rtt(rows: list[dict[str, Any]]) -> str:
    data = aggregate(
        [r for r in rows if r["group"] == "rtt"],
        ["protocol", "rtt_set_ms"],
        [*METRICS, "rtt_measured_ms"],
    )
    headers = ["机制", "设定RTT(ms)", "实测RTT(ms)", "吞吐(ops/s)", "P50(ms)", "P99(ms)", "消息/op"]
    body = [
        [
            item["protocol"],
            f(item["rtt_set_ms"], 0),
            f(item["rtt_measured_ms"], 2),
            f(item["throughput"]),
            f(item["p50_ms"]),
            f(item["p99_ms"]),
            f(item["msgs_per_op"], 2),
        ]
        for item in data
    ]
    return "### 表 3 网络 RTT 扫描（3 节点，并发 8）\n\n" + md_table(headers, body)


def table_loss(rows: list[dict[str, Any]]) -> str:
    data = aggregate([r for r in rows if r["group"] == "loss"], ["protocol", "loss_set"], METRICS)
    headers = ["机制", "丢包率", "吞吐(ops/s)", "P50(ms)", "P99(ms)", "错误率", "消息/op"]
    body = [
        [
            item["protocol"],
            pct(item["loss_set"], 0),
            f(item["throughput"]),
            f(item["p50_ms"]),
            f(item["p99_ms"]),
            pct(item["error_rate"]),
            f(item["msgs_per_op"], 2),
        ]
        for item in data
    ]
    return "### 表 4 丢包率扫描（3 节点，并发 8）\n\n" + md_table(headers, body)


def table_fsync(rows: list[dict[str, Any]]) -> str:
    data = aggregate([r for r in rows if r["group"] == "fsync"], ["fsync", "concurrency"], METRICS)
    headers = ["fsync", "并发", "吞吐(ops/s)", "P50(ms)", "P99(ms)", "消息/op"]
    body = [
        [
            item["fsync"],
            str(item["concurrency"]),
            f(item["throughput"]),
            f(item["p50_ms"]),
            f(item["p99_ms"]),
            f(item["msgs_per_op"], 2),
        ]
        for item in data
    ]
    return "### 表 5 fsync 开/关（Multi-Paxos，3 节点）\n\n" + md_table(headers, body)


def table_batch(rows: list[dict[str, Any]]) -> str:
    data = aggregate([r for r in rows if r["group"] == "batch"], ["batch_ms"], METRICS)
    headers = ["批处理窗口(ms)", "吞吐(ops/s)", "P50(ms)", "P99(ms)", "消息/op"]
    body = [
        [f(item["batch_ms"], 1), f(item["throughput"]), f(item["p50_ms"]), f(item["p99_ms"]), f(item["msgs_per_op"], 2)]
        for item in data
    ]
    return "### 表 6 批处理窗口（Multi-Paxos，并发 32）\n\n" + md_table(headers, body)


def table_faults(rows: list[dict[str, Any]]) -> str:
    headers = [
        "场景",
        "机制",
        "节点数",
        "成功/总数",
        "事件前成功",
        "事件后成功",
        "恢复(ms)",
        "最长黑洞(ms)",
        "换主次数",
        "安全性冲突",
        "结论",
    ]
    body = [
        [
            row["scenario"],
            row["protocol"],
            row["nodes"],
            f"{row['ok']}/{row['ops']}",
            row["ok_before"],
            row["ok_after"],
            row["recovery_ms"] or "-",
            row["blackout_ms"],
            row["leader_changes"],
            row["safety_conflicts"],
            row["outcome"],
        ]
        for row in rows
    ]
    return "### 表 7 故障 / 分区实验\n\n" + md_table(headers, body)


def table_safety(rows: list[dict[str, Any]], faults: list[dict[str, Any]]) -> str:
    total = len(rows) + len(faults)
    bad = [r for r in rows if num(r.get("safety_conflicts")) > 0] + [
        r for r in faults if num(r.get("safety_conflicts")) > 0
    ]
    slots = sum(num(r.get("safety_slots")) for r in rows)
    headers = ["指标", "数值"]
    body = [
        ["参与比对的实验次数", str(total)],
        ["比对过的 slot 总数", f"{int(slots)}"],
        ["出现「两个 Learner 学到不同值」的次数", str(len(bad))],
        ["结论", "全部一致（未观察到安全性违例）" if not bad else "存在违例，需排查"],
    ]
    return "### 表 8 安全性汇总（是否出现两个 Learner 学到不同值）\n\n" + md_table(headers, body)


def table_key_numbers(rows: list[dict[str, Any]], faults: list[dict[str, Any]]) -> str:
    """把"临界点"结论需要的几个数直接摘出来。"""

    def find(group: str, **filters: Any) -> dict[str, Any] | None:
        best = None
        for row in rows:
            if row.get("group") != group:
                continue
            if not all(str(row.get(k)) == str(v) for k, v in filters.items()):
                continue
            if best is None or num(row.get("ops")) > num(best.get("ops")):
                best = row
        return best

    items: list[tuple[str, str]] = []
    mp1 = find("protocol", protocol="multipaxos", concurrency=1)
    basic1 = find("protocol", protocol="basic", concurrency=1)
    fast1 = find("protocol", protocol="fast", concurrency=1)
    if basic1 and mp1:
        items.append(("Basic vs Multi-Paxos 单并发 P50", f"{f(basic1['p50_ms'])}ms vs {f(mp1['p50_ms'])}ms"))
        items.append(
            (
                "Basic vs Multi-Paxos 单并发消息数",
                f"{f(basic1['msgs_per_op'], 2)} vs {f(mp1['msgs_per_op'], 2)} 条/op",
            )
        )
    if fast1:
        items.append(("Fast Paxos 单并发 P50", f"{f(fast1['p50_ms'])}ms（省下的是 Leader 那一跳）"))
    mp32 = find("protocol", protocol="multipaxos", concurrency=32)
    fast32 = find("protocol", protocol="fast", concurrency=32)
    if mp32 and fast32:
        items.append(("并发 32 时 Multi-Paxos vs Fast", f"{f(mp32['throughput'])} vs {f(fast32['throughput'])} ops/s"))
        if num(fast32.get("conflict_rate")) > 0:
            items.append(("Fast 在并发 32 的冲突率", pct(fast32["conflict_rate"])))
            items.append(
                (
                    "Fast 冲突率超过此值后反而变慢",
                    f"{pct(fast32['conflict_rate'])}（吞吐只有 Multi-Paxos 的 "
                    f"{num(fast32['throughput']) / max(1.0, num(mp32['throughput'])) * 100:.0f}%）",
                )
            )
    loss_rows = [r for r in rows if r["group"] == "loss" and r["protocol"] == "multipaxos"]
    if loss_rows:
        best = max(loss_rows, key=lambda r: num(r["throughput"]))
        worst = min(loss_rows, key=lambda r: num(r["throughput"]))
        items.append(
            (
                "Multi-Paxos 丢包从小到大吞吐变化",
                f"{f(num(best['throughput']))} → {f(num(worst['throughput']))} ops/s"
                f"（{pct(num(best['loss_set']), 0)} → {pct(num(worst['loss_set']), 0)} 丢包）",
            )
        )
    rtt_rows = [r for r in rows if r["group"] == "rtt" and r["protocol"] == "multipaxos"]
    if rtt_rows:
        best = max(rtt_rows, key=lambda r: num(r["throughput"]))
        worst = min(rtt_rows, key=lambda r: num(r["throughput"]))
        items.append(
            (
                "Multi-Paxos RTT 扫描吞吐变化",
                f"{f(num(best['throughput']))} → {f(num(worst['throughput']))} ops/s"
                f"（RTT {f(num(best['rtt_set_ms']), 0)} → {f(num(worst['rtt_set_ms']), 0)} ms）",
            )
        )
    if faults:
        leader_fault = next((r for r in faults if "kill_leader" in r["scenario"]), None)
        if leader_fault:
            items.append(
                (
                    "Leader 被杀后的恢复时间",
                    f"{leader_fault['recovery_ms'] or '未恢复'}ms（{leader_fault['scenario']}）",
                )
            )
        down = next((r for r in faults if "two_acceptors" in r["scenario"]), None)
        if down:
            items.append(("杀掉 2/4 个 Acceptor", f"结论：{down['outcome']}，事件后成功 {down['ok_after']} 次"))
        part = next((r for r in faults if "2v2" in r["scenario"]), None)
        if part:
            items.append(("4 节点 2|2 分区", f"结论：{part['outcome']}，安全性冲突 {part['safety_conflicts']}"))
    if not items:
        return ""
    rows_md = md_table(["问题", "实测答案"], [[k, v] for k, v in items])
    return "### 表 9 临界点速查（报告第五章可直接引用）\n\n" + rows_md


# --------------------------------------------------------------------- 主流程
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="汇总实验数据成报告表格")
    parser.add_argument("--csv", default="results/summary.csv")
    parser.add_argument("--faults", default="results/faults.csv")
    parser.add_argument("--out", default="results")
    args = parser.parse_args(argv)

    rows = read_rows(Path(args.csv))
    faults = read_rows(Path(args.faults))
    if not rows and not faults:
        print("没有找到实验结果，请先运行： python -m experiments.run_matrix --profile quick")
        return 1

    builders: list[Callable[[], str]] = []
    if any(r["group"] == "protocol" for r in rows):
        builders.append(lambda: table_protocol(rows))
    if any(r["group"] == "nodes" for r in rows):
        builders.append(lambda: table_nodes(rows))
    if any(r["group"] == "rtt" for r in rows):
        builders.append(lambda: table_rtt(rows))
    if any(r["group"] == "loss" for r in rows):
        builders.append(lambda: table_loss(rows))
    if any(r["group"] == "fsync" for r in rows):
        builders.append(lambda: table_fsync(rows))
    if any(r["group"] == "batch" for r in rows):
        builders.append(lambda: table_batch(rows))
    if faults:
        builders.append(lambda: table_faults(faults))
    builders.append(lambda: table_safety(rows, faults))
    builders.append(lambda: table_key_numbers(rows, faults))

    text = "# Paxos 实验数据汇总\n\n"
    text += f"共 {len(rows)} 个用例、{len(faults)} 个故障场景。\n\n"
    sections = [builder() for builder in builders]
    text += "\n\n".join(sections) + "\n"

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "tables.md").write_text(text, encoding="utf-8")
    print(text)
    print(f"\n表格已写入 {out_dir / 'tables.md'}")

    # 长表 CSV：把分组结果平铺，方便自己再透视
    flat_fields = (
        "group",
        "protocol",
        "nodes",
        "concurrency",
        "rtt_set_ms",
        "loss_set",
        "fsync",
        "batch_ms",
        "throughput",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "msgs_per_op",
        "conflict_rate",
        "ok",
        "errors",
        "safety_conflicts",
    )
    flat: list[dict[str, Any]] = [{k: row.get(k, "") for k in flat_fields} for row in rows]
    if flat:
        with (out_dir / "flat.csv").open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(flat[0]))
            writer.writeheader()
            writer.writerows(flat)
        print(f"长表已写入 {out_dir / 'flat.csv'}")

    meta = Path("results/meta.json")
    if meta.exists():
        data = json.loads(meta.read_text(encoding="utf-8"))
        print(f"环境: {data.get('platform')} / Python {data.get('python')} / {data.get('cases')} 用例")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
