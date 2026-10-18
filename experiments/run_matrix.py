"""一键跑完实验矩阵，把结果写成 CSV。

用法::

    python -m experiments.run_matrix --profile quick          # 2~3 分钟，先确认环境
    python -m experiments.run_matrix --profile full           # 完整矩阵，10~15 分钟
    python -m experiments.run_matrix --profile faults         # 只跑故障/分区实验
    python -m experiments.run_matrix --profile full --save-raw  # 额外保存逐条延迟

产出::

    results/summary.csv        每个用例一行（P50/P95/P99、吞吐、消息数、冲突率、安全性）
    results/faults.csv         每个故障场景一行（恢复时间、黑洞时长、安全性）
    results/raw/*.csv          逐条请求记录（可选，画延迟分布用）
    results/meta.json          环境信息（机器、RTT 实测值等，写报告"实验环境"要用）

设计要点：

* 同 ``(protocol, nodes, fsync, batch_ms)`` 的用例**复用一次集群启动**，省掉反复
  启动进程的时间；RTT 和丢包在运行中用 admin 口热注入，不需要重启；
* 每个用例开始前先量一次**实测 RTT**（节点之间空载 ping），用来校验注入值是否
  真的生效 —— 报告里"设定 RTT vs 实测 RTT"这一列就是它；
* 每个用例结束后比对所有节点的决定日志，统计"是否有两个 Learner 学到不同值"。
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import platform
import statistics
import sys
import time
import uuid
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from paxos.ballot import fast_quorum_size, quorum_size
from paxos.cluster import Cluster, ClusterSpec
from paxos.loadgen import LoadGen

from .cases import (
    PROFILES,
    Case,
    FaultCase,
    FaultStep,
    fault_cases,
)

RESULTS = Path("results")

SUMMARY_FIELDS = [
    "run_id",
    "ts",
    "group",
    "protocol",
    "nodes",
    "quorum",
    "fast_quorum",
    "tolerated_failures",
    "concurrency",
    "target",
    "rtt_set_ms",
    "rtt_measured_ms",
    "loss_set",
    "fsync",
    "batch_ms",
    "duration_s",
    "ops",
    "ok",
    "errors",
    "error_rate",
    "throughput",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "mean_ms",
    "max_ms",
    "msgs_sent",
    "msgs_recv",
    "msgs_dropped",
    "msgs_per_op",
    "msg_by_type",
    "conflicts",
    "conflict_rate",
    "retries",
    "elections",
    "leader_changes",
    "repaired_slots",
    "safety_conflicts",
    "safety_slots",
    "error_kinds",
    "notes",
]

FAULT_FIELDS = [
    "run_id",
    "ts",
    "scenario",
    "protocol",
    "nodes",
    "concurrency",
    "description",
    "expect",
    "ops",
    "ok",
    "errors",
    "ok_before",
    "ok_after",
    "recovery_ms",
    "blackout_ms",
    "leader_changes",
    "safety_conflicts",
    "safety_slots",
    "outcome",
    "notes",
]


# --------------------------------------------------------------------- 工具
def append_csv(path: Path, fields: list[str], row: dict[str, Any]) -> None:
    """追加一行 CSV（自动建表头）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with path.open("a", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        if new_file:
            writer.writeheader()
        writer.writerow(row)


def measure_rtt(cluster: Cluster, count: int = 6) -> float:
    """在节点之间做空载 ping，返回实测 RTT 中位数的平均（ms）。"""
    medians: list[float] = []
    for ep in cluster.endpoints:
        peer = next((other.node_id for other in cluster.endpoints if other.node_id != ep.node_id), "")
        if not peer:
            continue
        result = cluster.admin(ep.node_id, {"cmd": "ping_peer", "peer": peer, "count": count})
        if result.get("ok"):
            medians.append(float(result.get("median_ms", 0.0)))
    return round(statistics.fmean(medians), 3) if medians else 0.0


def verify_safety(cluster: Cluster) -> tuple[int, int]:
    """比对所有节点的决定日志，返回 (冲突 slot 数, 共同已知 slot 数)。"""
    logs = cluster.dump_logs()
    if len(logs) < 2:
        return 0, 0
    slots: set[int] = set()
    for chosen in logs.values():
        slots.update(chosen)
    conflicts = 0
    for slot in slots:
        values = {chosen[slot] for chosen in logs.values() if slot in chosen}
        if len(values) > 1:
            conflicts += 1
    return conflicts, len(slots)


def status_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """两个状态快照的差值。

    为什么必须做差：同一组用例**复用同一个集群**（省掉反复启动），而节点的消息计数
    是从进程启动起累计的。如果直接用累计值除以本用例的成功数，就会得到
    "298 条消息/op" 这种离谱数字（把前面用例的消息也算进来了）。
    """
    delta: dict[str, Any] = {}
    for key, value in after.items():
        if isinstance(value, int):
            delta[key] = value - int(before.get(key, 0) or 0)
        else:
            delta[key] = value
    # by_type 是 dict，单独做差
    delta["by_type"] = {}
    before_types = before.get("by_type") or {}
    for kind, count in (after.get("by_type") or {}).items():
        delta["by_type"][kind] = int(count) - int(before_types.get(kind, 0) or 0)
    return delta


def aggregate_status(cluster: Cluster) -> dict[str, Any]:
    """把所有节点的计数汇总成一份（消息数、冲突、选举等）。"""
    totals: dict[str, Any] = defaultdict(int)
    by_type: dict[str, list[int]] = defaultdict(list)
    leader_changes = 0
    for status in cluster.status_all().values():
        if not status.get("ok"):
            continue
        messages = status.get("messages") or {}
        counters = status.get("counters") or {}
        proposer = status.get("proposer") or {}
        totals["msgs_sent"] += messages.get("sent", 0)
        totals["msgs_recv"] += messages.get("recv", 0)
        totals["msgs_dropped"] += messages.get("dropped", 0)
        totals["conflicts"] += counters.get("conflicts", 0)
        totals["retries"] += counters.get("retries", 0)
        totals["elections"] += counters.get("elections", 0)
        totals["repaired_slots"] += counters.get("repaired", 0)
        leader_changes += status.get("leader_changes", 0)
        for kind, count in (messages.get("by_type") or {}).items():
            by_type[kind].append(count)
        totals["fast_accepts"] += proposer.get("fast_accepts", 0)
    totals["leader_changes"] = leader_changes
    totals["by_type"] = {kind: sum(values) for kind, values in by_type.items()}
    totals["msg_by_type"] = json.dumps(dict(sorted(totals["by_type"].items())), ensure_ascii=False)
    return dict(totals)


def pick_targets(cluster: Cluster, mode: str) -> list[tuple[str, str, int]]:
    status = cluster.status_all()
    leader = next((s.get("leader") for s in status.values() if s.get("leader")), None)
    endpoints = cluster.endpoints
    wanted = {part.strip() for part in mode.split(",") if part.strip()}
    if wanted and wanted <= {ep.node_id for ep in endpoints}:
        chosen = [ep for ep in endpoints if ep.node_id in wanted]
    elif mode == "leader" and leader:
        chosen = [ep for ep in endpoints if ep.node_id == leader]
    elif mode == "all" or not leader:
        chosen = list(endpoints)
    else:
        followers = [ep for ep in endpoints if ep.node_id != leader]
        chosen = followers or list(endpoints)
    return [ep.as_tuple() for ep in chosen]


def spec_for(case: Case, base_port: int) -> ClusterSpec:
    return ClusterSpec(
        nodes=case.nodes,
        protocol=case.protocol,
        base_port=base_port,
        fsync=case.fsync,
        rtt_ms=case.rtt_ms,
        loss=case.loss,
        batch_ms=case.batch_ms,
        data_root=Path("data/matrix"),
        log_root=Path("run/matrix-logs"),
    )


# --------------------------------------------------------------------- 单个用例
def run_case(cluster: Cluster, case: Case, *, save_raw: bool) -> dict[str, Any]:
    # 1) 热注入网络参数（不用重启集群）
    cluster.inject_all(rtt_ms=case.rtt_ms, loss=case.loss)
    time.sleep(0.35)
    rtt_measured = measure_rtt(cluster)
    status_before = aggregate_status(cluster)

    # 2) 压测
    targets = pick_targets(cluster, case.target)
    gen = LoadGen(
        targets,
        concurrency=case.concurrency,
        duration=case.duration,
        timeout=max(2.5, 8.0 * case.rtt_ms / 1000.0 + 1.0),
    )
    summary = asyncio.run(gen.run())

    # 3) 稳定性 + 安全性
    time.sleep(0.25)
    safety_conflicts, safety_slots = verify_safety(cluster)
    totals = status_delta(status_before, aggregate_status(cluster))
    conflict_rate = totals.get("conflicts", 0) / max(1, totals.get("fast_accepts", 0) or summary.ops)

    row = {
        "run_id": f"{case.run_id}-{uuid.uuid4().hex[:6]}",
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "group": case.group,
        "protocol": case.protocol,
        "nodes": case.nodes,
        "quorum": quorum_size(case.nodes),
        "fast_quorum": fast_quorum_size(case.nodes),
        "tolerated_failures": case.nodes - quorum_size(case.nodes),
        "concurrency": case.concurrency,
        "target": case.target,
        "rtt_set_ms": case.rtt_ms,
        "rtt_measured_ms": rtt_measured,
        "loss_set": case.loss,
        "fsync": "on" if case.fsync else "off",
        "batch_ms": case.batch_ms,
        "duration_s": case.duration,
        "ops": summary.ops,
        "ok": summary.ok,
        "errors": summary.errors,
        "error_rate": round(summary.error_rate, 4),
        "throughput": round(summary.throughput, 2),
        "p50_ms": round(summary.p50, 3),
        "p95_ms": round(summary.p95, 3),
        "p99_ms": round(summary.p99, 3),
        "mean_ms": round(summary.mean, 3),
        "max_ms": round(summary.max, 3),
        "msgs_sent": totals.get("msgs_sent", 0),
        "msgs_recv": totals.get("msgs_recv", 0),
        "msgs_dropped": totals.get("msgs_dropped", 0),
        "msgs_per_op": round(totals.get("msgs_sent", 0) / max(1, summary.ok), 2),
        "msg_by_type": totals.get("msg_by_type", "{}"),
        "conflicts": totals.get("conflicts", 0),
        "conflict_rate": round(conflict_rate, 4),
        "retries": totals.get("retries", 0),
        "elections": totals.get("elections", 0),
        "leader_changes": totals.get("leader_changes", 0),
        "repaired_slots": totals.get("repaired_slots", 0),
        "safety_conflicts": safety_conflicts,
        "safety_slots": safety_slots,
        "error_kinds": json.dumps(summary.error_kinds, ensure_ascii=False),
        "notes": case.notes,
    }

    if save_raw:
        raw_path = RESULTS / "raw" / f"{row['run_id']}.csv"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        with raw_path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(gen.records[0].as_row()) if gen.records else ["seq"])
            writer.writeheader()
            for record in gen.records:
                writer.writerow(record.as_row())
    return row


# --------------------------------------------------------------------- 故障用例
def resolve_node(node: str, leader: str | None, followers: list[str], taken: set[str]) -> str | None:
    """把占位符解析成具体节点名。

    支持三种写法：

    * ``__leader__``   —— 当前 Leader
    * ``__follower__`` —— 一个**不是 Leader** 的节点（"杀 Acceptor"必须打在它身上，
      否则这个实验就变成了"杀 Leader"，结论会完全不同）
    * ``__follower2__``—— 第二个非 Leader 节点（用来做"杀两台"）
    """
    if node == "__leader__":
        return leader
    if node in {"__follower__", "__follower2__"}:
        # 每次挑"还没被杀掉的第一个非 Leader 节点"，所以两个占位符天然拿到不同节点
        pool = [item for item in followers if item not in taken] or followers
        return pool[0] if pool else None
    return node or None


def outage_metrics(records: list[Any], event_ms: float) -> dict[str, Any]:
    """从逐条请求记录里算出"不可用窗口"和"恢复时间"。

    口径（报告里要写清楚）：

    * 每条失败请求占用 ``[发送时刻, 发送时刻+延迟]`` 一段时间，把所有**失败区间**
      取并集，最长的那个连续窗口就是"黑洞"（这段时间里客户端拿不到正确响应）；
    * ``recovery_ms`` = 事件发生 → 最后一次失败结束之后第一个成功请求的时刻；
    * 只看事件时刻之后的请求。
    """
    after = [r for r in records if r.ts_ms >= event_ms]
    if not after:
        return {"recovery_ms": None, "blackout_ms": 0.0, "ok_after": 0, "fail_after": 0}
    failures = sorted((r.ts_ms, r.ts_ms + max(r.latency_ms, 1.0)) for r in after if not r.ok)
    oks = sorted(r.ts_ms for r in after if r.ok)
    blocks: list[list[float]] = []
    for start, end in failures:
        if blocks and start <= blocks[-1][1] + 5:  # 5ms 容差，避免把相邻失败切开
            blocks[-1][1] = max(blocks[-1][1], end)
        else:
            blocks.append([start, end])
    blackout = max((end - start for start, end in blocks), default=0.0)
    recovery_ms: float | None = 0.0
    if blocks:
        # 恢复时间 = 事件时刻 → **第一段**不可用结束后的第一个成功请求。
        # 用"第一段"而不是"最长段"：一次故障可能造成两次抖动（杀节点时一次、
        # 重启后重新加入时再一次），我们关心"服务第一次恢复"，而不是最后一次。
        first_block_end = blocks[0][1]
        resumed = next((ts for ts in oks if ts >= first_block_end), None)
        recovery_ms = None if resumed is None else round(resumed - event_ms, 1)
    return {
        "recovery_ms": recovery_ms,
        "blackout_ms": round(blackout, 1),
        "ok_after": len(oks),
        "fail_after": len(failures),
    }


def run_fault(cluster: Cluster, case: FaultCase, *, save_raw: bool) -> tuple[dict[str, Any], list[Any]]:
    """跑一个故障场景：压测同时在时间线上做 kill / restart / partition。"""
    cluster.inject_all(rtt_ms=case.rtt_ms, loss=0.0)
    time.sleep(0.35)
    # 先把 __leader__/__follower__ 这类占位符解析成**具体节点名**：一是避免执行
    # 过程中 Leader 变了导致"重启了一个没被杀的节点"，二是保证"杀 Acceptor"真的
    # 打在非 Leader 节点上（否则这个实验就变成了杀 Leader）。
    status = cluster.status_all()
    current_leader = next((s.get("leader") for s in status.values() if s.get("leader")), None)
    all_ids = [ep.node_id for ep in cluster.endpoints]
    followers = [node_id for node_id in all_ids if node_id != current_leader]
    taken: set[str] = set()
    resolved_map: dict[str, str] = {}
    steps: list[FaultStep] = []
    for step in case.steps:
        placeholder = step.node in {"__leader__", "__follower__", "__follower2__"}
        if placeholder and step.node in resolved_map:
            # 重启步骤必须和它杀掉的那个节点是同一个 —— 复用第一次的解析结果
            resolved: str | None = resolved_map[step.node]
        else:
            resolved = resolve_node(step.node, current_leader, followers, taken)
            if placeholder and resolved:
                resolved_map[step.node] = resolved
        if resolved and step.action == "kill":
            taken.add(resolved)
        steps.append(FaultStep(step.at_s, step.action, resolved or step.node, step.groups, dict(step.kwargs)))
    targets = pick_targets(cluster, case.target or ("all" if case.protocol != "multipaxos" else "near"))
    gen = LoadGen(
        targets,
        concurrency=case.concurrency,
        duration=case.duration,
        timeout=max(2.5, 8.0 * case.rtt_ms / 1000.0 + 1.0),
    )

    async def scenario() -> Any:
        task = asyncio.create_task(gen.run())
        events: list[dict[str, Any]] = []
        started = time.perf_counter()
        for step in steps:
            delay = step.at_s - (time.perf_counter() - started)
            if delay > 0:
                await asyncio.sleep(delay)
            node = step.node
            record = {"at_s": step.at_s, "action": step.action, "node": node}
            if step.action == "kill" and node:
                cluster.kill_node(node)
                record["detail"] = f"kill -9 {node}"
            elif step.action == "restart" and node:
                cluster.restart_node(node)
                record["detail"] = f"restart {node}"
            elif step.action == "partition":
                cluster.set_partition([list(group) for group in step.groups])
                record["detail"] = " | ".join(",".join(group) for group in step.groups)
            elif step.action == "heal":
                cluster.set_partition([], partition_off=True)
                record["detail"] = "恢复全部链路"
            elif step.action == "inject":
                cluster.inject_all(**step.kwargs)
                record["detail"] = json.dumps(step.kwargs, ensure_ascii=False)
            events.append(record)
        summary = await task
        return summary, events

    summary, events = asyncio.run(scenario())
    time.sleep(0.4)
    safety_conflicts, safety_slots = verify_safety(cluster)
    totals = aggregate_status(cluster)
    first_event_s = steps[0].at_s if steps else 0.0
    metrics = outage_metrics(gen.records, first_event_s * 1000.0)
    ok_before = len([r for r in gen.records if r.ok and r.ts_ms < first_event_s * 1000.0])
    recovery_ms = metrics["recovery_ms"]
    blackout_ms = metrics["blackout_ms"]

    if safety_conflicts:
        outcome = "安全性违例"
    elif metrics["fail_after"] == 0:
        outcome = "完全无感（0 次失败）"
    elif recovery_ms is None:
        outcome = "事件后未恢复"
    elif blackout_ms < 300:
        outcome = f"几乎无感（{blackout_ms:.0f}ms 抖动）"
    elif blackout_ms < 2000:
        outcome = f"短暂不可用 {blackout_ms:.0f}ms，{recovery_ms:.0f}ms 后恢复"
    else:
        outcome = f"长时间不可用 {blackout_ms:.0f}ms，{recovery_ms:.0f}ms 后恢复"

    row = {
        "run_id": f"{case.name}-{uuid.uuid4().hex[:6]}",
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "scenario": case.name,
        "protocol": case.protocol,
        "nodes": case.nodes,
        "concurrency": case.concurrency,
        "description": case.description,
        "expect": case.expect,
        "ops": summary.ops,
        "ok": summary.ok,
        "errors": summary.errors,
        "ok_before": ok_before,
        "ok_after": metrics["ok_after"],
        "recovery_ms": "" if recovery_ms is None else recovery_ms,
        "blackout_ms": blackout_ms,
        "leader_changes": totals.get("leader_changes", 0),
        "safety_conflicts": safety_conflicts,
        "safety_slots": safety_slots,
        "outcome": outcome,
        "notes": json.dumps(events, ensure_ascii=False),
    }

    if save_raw:
        raw_path = RESULTS / "raw" / f"fault-{row['run_id']}.csv"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        with raw_path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["seq", "ts_ms", "latency_ms", "ok", "err", "target", "node"])
            writer.writeheader()
            for record in gen.records:
                writer.writerow({k: v for k, v in record.as_row().items() if k in writer.fieldnames})
        (RESULTS / "raw" / f"fault-{row['run_id']}.events.json").write_text(
            json.dumps({"events": events, "scenario": case.name}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return row, events


# --------------------------------------------------------------------- 主流程
def group_cases(cases: Iterable[Case]) -> list[list[Case]]:
    """按"需要重启集群的维度"分组，减少启动次数。"""
    buckets: dict[tuple, list[Case]] = defaultdict(list)
    for case in cases:
        buckets[case.cluster_key].append(case)
    return list(buckets.values())


def write_meta(rows: list[dict[str, Any]], faults: list[dict[str, Any]]) -> None:
    meta = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "python": sys.version.split()[0],
        "platform": f"{platform.system()} {platform.release()}",
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cases": len(rows),
        "faults": len(faults),
        "note": (
            "Windows 上系统定时器精度约 15.6ms，本工程用 paxos/timer.py 的混合 sleep 补偿；"
            "RTT=1ms 档实测仍会略高（约 1.5~3ms）。Linux/Docker 下更接近设定值。"
        ),
    }
    (RESULTS / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    global RESULTS
    parser = argparse.ArgumentParser(description="跑 Paxos 实验矩阵")
    parser.add_argument("--profile", default="quick", choices=[*sorted(PROFILES), "faults", "all"])
    parser.add_argument("--duration", type=float, default=0.0, help="覆盖用例默认时长（秒）")
    parser.add_argument("--base-port", type=int, default=17100)
    parser.add_argument("--save-raw", action="store_true", help="保存每个用例的逐条请求延迟")
    parser.add_argument("--out", default=str(RESULTS))
    parser.add_argument("--reset", action="store_true", help="开跑前清空 results/")
    args = parser.parse_args(argv)

    RESULTS = Path(args.out)
    if args.reset and RESULTS.exists():
        import shutil

        shutil.rmtree(RESULTS, ignore_errors=True)
    RESULTS.mkdir(parents=True, exist_ok=True)

    started = time.time()
    rows: list[dict[str, Any]] = []
    faults: list[dict[str, Any]] = []

    if args.profile in {"faults", "all"}:
        for fault in fault_cases():
            spec = ClusterSpec(
                nodes=fault.nodes,
                protocol=fault.protocol,
                base_port=args.base_port,
                fsync=fault.fsync,
                rtt_ms=fault.rtt_ms,
                data_root=Path("data/matrix"),
                log_root=Path("run/matrix-logs"),
            )
            cluster = Cluster(spec)
            print(f"\n[故障] {fault.name}  {fault.description}")
            try:
                cluster.start()
                row, _events = run_fault(cluster, fault, save_raw=True)
                append_csv(RESULTS / "faults.csv", FAULT_FIELDS, row)
                faults.append(row)
                print(
                    f"   → ok={row['ok']}/{row['ops']} 恢复={row['recovery_ms']}ms "
                    f"黑洞={row['blackout_ms']}ms 换主={row['leader_changes']} "
                    f"安全性冲突={row['safety_conflicts']} 结论={row['outcome']}"
                )
            except Exception as exc:  # pragma: no cover
                print(f"   !! 场景失败: {type(exc).__name__}: {exc}")
            finally:
                cluster.stop()

    if args.profile != "faults":
        profiles = [args.profile] if args.profile != "all" else ["full"]
        cases: list[Case] = []
        for name in profiles:
            cases.extend(PROFILES[name](args.duration) if args.duration else PROFILES[name]())
        print(f"共 {len(cases)} 个用例，分 {len(group_cases(cases))} 组启动集群")
        for index, group in enumerate(group_cases(cases), 1):
            first = group[0]
            spec = spec_for(first, args.base_port)
            cluster = Cluster(spec)
            print(
                f"\n[{index}] 启动集群 protocol={first.protocol} nodes={first.nodes} "
                f"fsync={'on' if first.fsync else 'off'} batch={first.batch_ms}ms "
                f"（本组 {len(group)} 个用例）"
            )
            try:
                cluster.start()
                for case in group:
                    row = run_case(cluster, case, save_raw=args.save_raw)
                    append_csv(RESULTS / "summary.csv", SUMMARY_FIELDS, row)
                    rows.append(row)
                    print(
                        f"   {case.run_id:<52} ok={row['ok']:<5} 吞吐={row['throughput']:>8.1f}/s "
                        f"p50={row['p50_ms']:>7.1f} p99={row['p99_ms']:>8.1f} "
                        f"msgs/op={row['msgs_per_op']:>5.1f} 冲突={row['conflicts']:<5} "
                        f"实测RTT={row['rtt_measured_ms']}ms 安全={row['safety_conflicts']}"
                    )
            except Exception as exc:  # pragma: no cover - 单个用例失败不该终止整轮
                print(f"   !! 本组用例失败: {type(exc).__name__}: {exc}")
            finally:
                cluster.stop()

    write_meta(rows, faults)
    elapsed = time.time() - started
    print(f"\n完成：{len(rows)} 个用例 + {len(faults)} 个故障场景，用时 {elapsed / 60:.1f} 分钟")
    print(f"数据已写入 {RESULTS}/summary.csv 和 {RESULTS}/faults.csv")
    print("下一步： python -m experiments.analyze        # 生成报告表格")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
