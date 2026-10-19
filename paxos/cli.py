"""命令行入口：``python -m paxos.cli <子命令>``。

一条命令就能完成"起集群 → 压测 → 注入故障 → 比对日志"的完整闭环：

.. code-block:: text

    python -m paxos.cli smoke              # 环境自检（推荐第一条命令）
    python -m paxos.cli up --nodes 4       # 前台起 4 节点集群（Ctrl+C 结束）
    python -m paxos.cli status             # 看谁在当 Leader、Paxos 到哪一步了
    python -m paxos.cli bench -c 16 -d 20  # 16 并发压 20 秒
    python -m paxos.cli inject --rtt 50 --loss 0.05
    python -m paxos.cli crash --node n2    # 模拟 kill -9
    python -m paxos.cli verify             # 比对所有节点的决定日志
    python -m paxos.cli down               # 收工
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

from . import __version__
from .ballot import fast_quorum_size, max_failures, quorum_size
from .cluster import Cluster, ClusterSpec, admin_call
from .loadgen import LoadGen
from .node import NodeConfig, PaxosNode

LOG_FORMAT = "%(asctime)s %(levelname)-5s %(name)s | %(message)s"


# --------------------------------------------------------------------- 小工具
def setup_logging(level: str = "info") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=LOG_FORMAT,
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )


def parse_peers(raw: str) -> dict[str, tuple[str, int]]:
    """解析 ``n1=127.0.0.1:17101,n2=...``。"""
    peers: dict[str, tuple[str, int]] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        node_id, addr = item.split("=", 1)
        host, port = addr.rsplit(":", 1)
        peers[node_id.strip()] = (host.strip(), int(port))
    if not peers:
        raise SystemExit("--peers 不能为空")
    return peers


def fmt_ms(value: float) -> str:
    return f"{value:8.2f}"


def cluster_from_state() -> Cluster:
    cluster = Cluster.from_state()
    if cluster is None:
        raise SystemExit("找不到运行中的集群，请先在另一个终端执行： python -m paxos.cli up --nodes 4")
    return cluster


def pick_targets(cluster: Cluster, mode: str) -> list[tuple[str, str, int]]:
    """按 "接入点策略" 挑客户端连谁。

    * ``near``   —— 连一个**非 Leader**的副本（模拟"客户端就近接入"）
    * ``leader`` —— 直接连 Leader（Multi-Paxos 的最优路径）
    * ``all``    —— 轮转全部节点
    * ``n2``     —— 指定节点
    """
    status = cluster.status_all()
    leader = next((s.get("leader") for s in status.values() if s.get("leader")), None)
    endpoints = cluster.endpoints
    if mode in {ep.node_id for ep in endpoints}:
        chosen = [ep for ep in endpoints if ep.node_id == mode]
    elif mode == "leader" and leader:
        chosen = [ep for ep in endpoints if ep.node_id == leader]
    elif mode == "all" or not leader:
        chosen = list(endpoints)
    else:  # near
        followers = [ep for ep in endpoints if ep.node_id != leader]
        chosen = followers or list(endpoints)
    return [ep.as_tuple() for ep in chosen]


# --------------------------------------------------------------------- 子命令
def cmd_node(args: argparse.Namespace) -> int:
    setup_logging(args.log_level)
    cfg = NodeConfig(
        node_id=args.id,
        peers=parse_peers(args.peers),
        port=args.port,
        admin_port=args.admin_port,
        data_dir=Path(args.data_dir),
        protocol=args.protocol,
        fsync=args.fsync == "on",
        rtt_ms=args.rtt_ms,
        loss=args.loss,
        jitter_ms=args.jitter_ms,
        batch_ms=args.batch_ms,
        batch_size=args.batch_size,
        max_inflight=args.max_inflight,
        heartbeat_ms=args.heartbeat_ms,
        election_ms=args.election_ms,
        listen_host=args.host,
        listener=args.listener,
    )
    if cfg.node_id not in cfg.peers:
        raise SystemExit(f"--peers 里必须包含自己: {cfg.node_id}")
    node = PaxosNode(cfg)
    try:
        asyncio.run(node.run())
    except KeyboardInterrupt:
        return 0
    return 0


def cmd_up(args: argparse.Namespace) -> int:
    setup_logging(args.log_level)
    spec = ClusterSpec(
        nodes=args.nodes,
        protocol=args.protocol,
        base_port=args.base_port,
        fsync=args.fsync == "on",
        rtt_ms=args.rtt_ms,
        loss=args.loss,
        batch_ms=args.batch_ms,
        heartbeat_ms=args.heartbeat_ms,
        election_ms=args.election_ms,
        listeners=args.listeners,
        keep_data=args.keep_data,
        data_root=Path(args.data_dir),
        log_root=Path(args.log_dir),
    )
    cluster = Cluster(spec)
    print(f"正在启动 {args.nodes} 节点集群（protocol={args.protocol}, fsync={args.fsync}) ...")
    cluster.start()
    try:
        print_quorum_table(args.nodes)
        print_status_table(cluster)
        print("\n集群已就绪。常用命令（在**另一个**终端执行）：")
        print("  python -m paxos.cli status")
        print("  python -m paxos.cli bench -c 8 -d 10")
        print("  python -m paxos.cli down")
        print("\n保持本终端不要关闭；按 Ctrl+C 结束集群。")
    except Exception:
        # 打印/展示出错时**必须**把刚拉起来的集群收掉，否则会留下一堆占端口的孤儿进程
        print("\n[错误] 打印集群状态失败，正在回收已启动的节点 ...")
        cluster.stop()
        raise
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n正在停止集群 ...")
        cluster.stop()
    return 0


def cmd_down(_args: argparse.Namespace) -> int:
    state = Cluster.load_state()
    if not state:
        print("没有找到集群状态文件，无需停止。")
        return 0
    for item in state["endpoints"]:
        result = admin_call(item["host"], item["admin_port"], {"cmd": "shutdown"}, timeout=1.0)
        if result.get("ok"):
            print(f"  {item['id']}: 已通知关闭")
    time.sleep(0.6)
    # 兜底：按 pid 强杀
    killed = 0
    for pid in state.get("pids", {}).values():
        try:
            os.kill(int(pid), 9)
            killed += 1
        except (OSError, ProcessLookupError, ValueError):
            continue
    if killed:
        print(f"  已强制结束 {killed} 个残留进程")
    Path("run/cluster.json").unlink(missing_ok=True)
    print("集群已停止。")
    return 0


def print_quorum_table(nodes: int) -> None:
    q = quorum_size(nodes)
    f = fast_quorum_size(nodes)
    print(f"\n节点数 n={nodes}：经典多数派 Q={q}，可容忍故障 {max_failures(nodes)} 台；Fast Paxos 快速仲裁 F={f}")
    print("  （记住这两个数：n=4 和 n=3 的容错能力一样，都是 1 台）\n")


#: 状态表的列定义（列名, 显示宽度）。中文在终端里占两列，所以按"显示宽度"而不是字符数对齐。
STATUS_COLUMNS: tuple[tuple[str, int], ...] = (
    ("节点", 8),
    ("角色", 10),
    ("Leader", 8),
    ("已学slot", 10),
    ("已应用", 8),
    ("轮次", 6),
    ("选举", 6),
    ("消息(发/丢)", 16),
)


def display_width(text: str) -> int:
    """字符串在等宽终端里占多少列（中日韩全角字符按 2 列算）。"""
    import unicodedata

    return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1 for ch in text)


def pad_display(value: object, width: int) -> str:
    """按显示宽度左对齐补空格。

    不用 f-string 的 ``:<width`` 格式说明符，是为了避免"宽度写成变量"时
    掉进 ``Invalid format specifier`` 的坑（本工程真踩过：一个手误让
    ``up`` 在集群已经拉起来之后崩掉，留下 3 个孤儿进程）。
    """
    text = str(value)
    return text + " " * max(0, width - display_width(text))


def print_status_table(cluster: Cluster) -> None:
    statuses = cluster.status_all()
    widths = [width for _title, width in STATUS_COLUMNS]
    header = "".join(pad_display(title, width) for title, width in STATUS_COLUMNS)
    print(header.rstrip())
    print("-" * sum(widths))
    for node_id, status in statuses.items():
        if not status.get("ok"):
            print(pad_display(node_id, widths[0]) + "DOWN（进程不在或端口不通）")
            continue
        learner = status.get("learner", {})
        messages = status.get("messages", {})
        counters = status.get("counters", {})
        cells = [
            node_id,
            status.get("role", ""),
            status.get("leader") or "-",
            learner.get("chosen_slots", 0),
            status.get("applied_index", -1),
            status.get("max_round", 0),
            counters.get("elections", 0),
            f"{messages.get('sent', 0)}/{messages.get('dropped', 0)}",
        ]
        print("".join(pad_display(cell, width) for cell, width in zip(cells, widths, strict=False)).rstrip())


def cmd_status(args: argparse.Namespace) -> int:
    cluster = cluster_from_state()
    if args.json:
        print(json.dumps(cluster.status_all(), indent=2, ensure_ascii=False))
        return 0
    print_status_table(cluster)
    statuses = cluster.status_all()
    if statuses and not any(s.get("ok") for s in statuses.values()):
        print("\n⚠ 状态文件里的节点全部不可达：集群可能已经退出（或端口被防火墙挡住）。")
        print("   清理残留状态：python -m paxos.cli down")
        return 1
    leaders = {status.get("leader") for status in statuses.values() if status.get("leader")}
    if len(leaders) > 1:
        print(f"\n⚠ 检测到多个 Leader 声明: {leaders} （Paxos 允许暂时多主，最终只有一个能提交）")
    return 0


def cmd_bench(args: argparse.Namespace) -> int:
    setup_logging(args.log_level)
    cluster = cluster_from_state()
    targets = pick_targets(cluster, args.target)
    print(f"压测目标: {', '.join(t[0] for t in targets)}  并发={args.concurrency} 时长={args.duration}s")
    gen = LoadGen(
        targets,
        concurrency=args.concurrency,
        duration=args.duration,
        total_ops=args.ops,
        timeout=args.timeout,
    )
    summary = asyncio.run(gen.run())
    print(f"\n结果: ops={summary.ops} ok={summary.ok} err={summary.errors} 吞吐={summary.throughput:.1f} ops/s")
    print(f"  延迟 P50={summary.p50:.2f}ms  P95={summary.p95:.2f}ms  P99={summary.p99:.2f}ms  max={summary.max:.2f}ms")
    if summary.error_kinds:
        print(f"  错误分布: {summary.error_kinds}")
    if args.csv:
        import csv

        path = Path(args.csv)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(gen.records[0].as_row()) if gen.records else ["seq"])
            writer.writeheader()
            for record in gen.records:
                writer.writerow(record.as_row())
        print(f"  逐条记录已写入 {path}")
    return 0


def cmd_inject(args: argparse.Namespace) -> int:
    cluster = cluster_from_state()
    if args.isolate:
        for node_id in args.isolate.split(","):
            cluster.inject(node_id.strip(), partition=True)
        print(f"已隔离: {args.isolate}（其余节点继续工作）")
        return 0
    if args.partition:
        groups = [group.split(",") for group in args.partition.split("|")]
        cluster.set_partition(groups)
        print(f"已分区: {groups}")
        return 0
    if args.heal:
        cluster.inject_all(partition=False)
        for ep in cluster.endpoints:
            for other in cluster.endpoints:
                if other.node_id != ep.node_id:
                    cluster.admin(ep.node_id, {"cmd": "partition", "peer": other.node_id, "blocked": False})
        print("已恢复所有链路。")
        return 0
    for ep in cluster.endpoints:
        result = cluster.inject(ep.node_id, rtt_ms=args.rtt, loss=args.loss, jitter_ms=args.jitter)
        print(f"  {ep.node_id}: {result.get('profile')}")
    return 0


def cmd_crash(args: argparse.Namespace) -> int:
    cluster = cluster_from_state()
    cluster.kill_node(args.node, hard=True)
    print(f"{args.node} 已被 kill -9（模拟崩溃，未刷盘）")
    if args.restart:
        wait = args.wait
        print(f"等待 {wait}s 后重启 ..." if wait else "立即重启 ...")
        time.sleep(wait)
        cluster.restart_node(args.node)
        status = cluster.admin(args.node, {"cmd": "status"})
        learner = status.get("learner", {})
        print(
            f"{args.node} 已重启：已学 slot={learner.get('chosen_slots')} "
            f"promised_all={((status.get('acceptor') or {}).get('promised_all'))}"
        )
    return 0


def cmd_ping(args: argparse.Namespace) -> int:
    """空载往返探测：验证"注入的 RTT 到底生效没有"。"""
    cluster = cluster_from_state()
    pairs: list[tuple[str, str]] = []
    if args.from_node and args.to_node:
        pairs = [(args.from_node, args.to_node)]
    else:
        ids = [ep.node_id for ep in cluster.endpoints]
        for index, node in enumerate(ids):
            pairs.append((node, ids[(index + 1) % len(ids)]))
    for source, target in pairs:
        result = cluster.admin(source, {"cmd": "ping_peer", "peer": target, "count": args.count})
        if not result.get("ok"):
            print(f"  {source} -> {target}: 失败 {result.get('err')}")
            continue
        profile = result.get("profile") or {}
        print(
            f"  {source} -> {target}  RTT: min={result['min_ms']}ms  median={result['median_ms']}ms  "
            f"max={result['max_ms']}ms  (注入 rtt_ms={profile.get('rtt_ms')}, loss={profile.get('loss')})"
        )
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    cluster = cluster_from_state()
    if args.settle:
        time.sleep(args.settle)
    logs = cluster.dump_logs()
    if not logs:
        print("没有任何节点可用。")
        return 1
    all_slots: set[int] = set()
    for chosen in logs.values():
        all_slots.update(chosen)
    conflicts = []
    for slot in sorted(all_slots):
        values = {node: chosen[slot] for node, chosen in logs.items() if slot in chosen}
        if len(set(values.values())) > 1:
            conflicts.append((slot, values))
    print(f"比对 {len(logs)} 个节点的决定日志，共 {len(all_slots)} 个 slot")
    for node, chosen in logs.items():
        missing = len(all_slots - set(chosen))
        print(f"  {node}: 已学 {len(chosen)} 个，缺 {missing} 个")
    if conflicts:
        print(f"\n❌ 发现 {len(conflicts)} 个 slot 上不同节点学到了不同的值（安全性被破坏）")
        for slot, values in conflicts[:5]:
            print(f"   slot {slot}: {values}")
        if args.json:
            print(json.dumps(conflicts, ensure_ascii=False, indent=2))
        return 1
    print("\n✅ 安全性检查通过：所有节点在共同已知的 slot 上取到了相同的值")
    return 0


def cmd_smoke(args: argparse.Namespace) -> int:
    """一键环境自检：三种协议各跑一遍 + 一次崩溃恢复 + 一次安全性比对。"""
    setup_logging("warn")
    results: list[tuple[str, bool, str]] = []

    def record(name: str, ok: bool, detail: str = "") -> None:
        results.append((name, ok, detail))
        print(f"[{'PASS' if ok else 'FAIL'}] {name}  {detail}")

    for protocol in ("basic", "multipaxos", "fast"):
        spec = ClusterSpec(
            nodes=args.nodes,
            protocol=protocol,
            base_port=args.base_port,
            fsync=True,
            rtt_ms=1.0,
            data_root=Path("data/smoke"),
            log_root=Path("run/smoke-logs"),
            publish_state=False,  # 临时集群，不要覆盖 run/cluster.json
        )
        cluster = Cluster(spec)
        try:
            cluster.start()
            targets = pick_targets(cluster, "near")
            gen = LoadGen(targets, concurrency=4, duration=6.0, total_ops=args.ops)
            summary = asyncio.run(gen.run())
            # 判定标准刻意宽松：自检只关心"能不能达成共识、有没有安全性违例"，
            # 不追性能（同一台机器上 4 个进程 + 压测器抢 CPU，吞吐本来就会波动）。
            ok = summary.error_rate <= 0.05 and summary.ok >= args.ops * 0.85
            record(
                f"{protocol}: {summary.ok}/{summary.ops} 成功, P50={summary.p50:.1f}ms",
                ok,
                f"吞吐={summary.throughput:.1f}/s err={summary.error_kinds}",
            )
            time.sleep(0.4)
            logs = cluster.dump_logs()
            all_slots: set[int] = set()
            for chosen in logs.values():
                all_slots.update(chosen)
            conflicts = 0
            for slot in all_slots:
                values = {chosen[slot] for chosen in logs.values() if slot in chosen}
                if len(values) > 1:
                    conflicts += 1
            record(f"{protocol}: 安全性比对（{len(all_slots)} slot）", conflicts == 0, f"冲突={conflicts}")
        finally:
            cluster.stop()
            spec.keep_data = False

    # 崩溃恢复：kill 一个 Acceptor 后仍能提交，重启后日志不丢
    spec = ClusterSpec(
        nodes=args.nodes,
        protocol="multipaxos",
        base_port=args.base_port,
        fsync=True,
        data_root=Path("data/smoke"),
        log_root=Path("run/smoke-logs"),
        publish_state=False,
    )
    cluster = Cluster(spec)
    try:
        cluster.start()
        targets = pick_targets(cluster, "near")
        asyncio.run(LoadGen(targets, concurrency=2, duration=6.0, total_ops=max(10, args.ops // 3)).run())
        # 一定要杀**非 Leader** 的节点：杀 Leader 测的是"重新选主"，
        # 那是另一个场景，混在一起会让自检结果随机抖动。
        leader = cluster.wait_leader() or ""
        candidates = [ep.node_id for ep in cluster.endpoints if ep.node_id != leader]
        victim = candidates[0] if candidates else cluster.endpoints[-1].node_id
        cluster.kill_node(victim)
        summary2 = asyncio.run(LoadGen(targets, concurrency=2, duration=10.0, total_ops=max(10, args.ops // 3)).run())
        record(
            f"kill {victim}（非 Leader）后仍可提交（{summary2.ok}/{summary2.ops}）",
            summary2.ok > 0,
            f"错误={summary2.error_kinds}",
        )
        cluster.restart_node(victim)
        status = cluster.admin(victim, {"cmd": "status"})
        learned = (status.get("learner") or {}).get("chosen_slots", 0)
        record(f"{victim} 重启后恢复日志（{learned} slot）", learned > 0)
        time.sleep(0.5)
    finally:
        cluster.stop()

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== 自检结果: {passed}/{len(results)} 通过 =====")
    if passed != len(results):
        print("请把上面的 FAIL 行以及 run/smoke-logs/*.err.log 的内容发出来定位问题。")
        return 1
    print("环境正常，可以开始做实验了。")
    return 0


def cmd_reset(args: argparse.Namespace) -> int:
    import shutil

    for path in (Path(args.data_dir), Path("run")):
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
            print(f"已删除 {path}")
    return 0


# --------------------------------------------------------------------- 参数
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="paxos.cli",
        description=f"Paxos 教学实验工程 v{__version__} —— Basic Paxos / Multi-Paxos / Fast Paxos 对比实验",
    )
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)

    node = sub.add_parser("node", help="运行单个节点（一般由 up 自动调用）")
    node.add_argument("--id", required=True)
    node.add_argument("--peers", required=True, help="n1=127.0.0.1:17101,n2=...")
    node.add_argument("--port", type=int, required=True)
    node.add_argument("--admin-port", type=int, required=True)
    node.add_argument("--data-dir", default="data/n1")
    node.add_argument("--protocol", default="multipaxos", choices=["basic", "multipaxos", "fast"])
    node.add_argument("--fsync", default="on", choices=["on", "off"])
    node.add_argument("--rtt-ms", type=float, default=1.0, dest="rtt_ms")
    node.add_argument("--loss", type=float, default=0.0)
    node.add_argument("--jitter-ms", type=float, default=0.0, dest="jitter_ms")
    node.add_argument("--batch-ms", type=float, default=0.0, dest="batch_ms")
    node.add_argument("--batch-size", type=int, default=64, dest="batch_size")
    node.add_argument("--max-inflight", type=int, default=16, dest="max_inflight")
    node.add_argument("--heartbeat-ms", type=float, default=60.0, dest="heartbeat_ms")
    node.add_argument("--election-ms", type=float, default=400.0, dest="election_ms")
    node.add_argument("--host", default="127.0.0.1")
    node.add_argument("--listener", action="store_true", help="只学不投票（Listener/Observer）")
    node.add_argument("--log-level", default="info")
    node.set_defaults(func=cmd_node)

    up = sub.add_parser("up", help="前台启动本地多进程集群")
    up.add_argument("--nodes", type=int, default=3, choices=[3, 4, 5])
    up.add_argument("--protocol", default="multipaxos", choices=["basic", "multipaxos", "fast"])
    up.add_argument("--fsync", default="on", choices=["on", "off"])
    up.add_argument("--rtt-ms", type=float, default=1.0, dest="rtt_ms")
    up.add_argument("--loss", type=float, default=0.0)
    up.add_argument("--batch-ms", type=float, default=0.0, dest="batch_ms")
    up.add_argument("--heartbeat-ms", type=float, default=60.0, dest="heartbeat_ms")
    up.add_argument("--election-ms", type=float, default=400.0, dest="election_ms")
    up.add_argument("--listeners", type=int, default=0)
    up.add_argument("--base-port", type=int, default=17100, dest="base_port")
    up.add_argument("--data-dir", default="data", dest="data_dir")
    up.add_argument("--log-dir", default="run/logs", dest="log_dir")
    up.add_argument("--keep-data", action="store_true", dest="keep_data")
    up.add_argument("--log-level", default="info")
    up.set_defaults(func=cmd_up)

    down = sub.add_parser("down", help="停止集群")
    down.set_defaults(func=cmd_down)

    status = sub.add_parser("status", help="查看集群状态")
    status.add_argument("--json", action="store_true")
    status.set_defaults(func=cmd_status)

    bench = sub.add_parser("bench", help="压测")
    bench.add_argument("-c", "--concurrency", type=int, default=8)
    bench.add_argument("-d", "--duration", type=float, default=10.0)
    bench.add_argument("--ops", type=int, default=0, help="总请求数（优先于时长）")
    bench.add_argument("--target", default="near", help="near|leader|all|节点名")
    bench.add_argument("--timeout", type=float, default=10.0)
    bench.add_argument("--csv", default="")
    bench.add_argument("--log-level", default="warn")
    bench.set_defaults(func=cmd_bench)

    inject = sub.add_parser("inject", help="注入时延/丢包/分区")
    inject.add_argument("--rtt", type=float, default=None, help="往返时延 ms")
    inject.add_argument("--loss", type=float, default=None, help="丢包率 0~1")
    inject.add_argument("--jitter", type=float, default=None, help="抖动 ms")
    inject.add_argument("--partition", default="", help="n1,n2|n3,n4（用 | 分组）")
    inject.add_argument("--isolate", default="", help="隔离指定节点（如 n3）")
    inject.add_argument("--heal", action="store_true", help="恢复所有链路")
    inject.set_defaults(func=cmd_inject)

    crash = sub.add_parser("crash", help="模拟节点崩溃")
    crash.add_argument("--node", required=True)
    crash.add_argument("--restart", action="store_true")
    crash.add_argument("--wait", type=float, default=1.0)
    crash.set_defaults(func=cmd_crash)

    ping = sub.add_parser("ping", help="节点之间空载探测 RTT（验证时延注入是否生效）")
    ping.add_argument("--from", dest="from_node", default="")
    ping.add_argument("--to", dest="to_node", default="")
    ping.add_argument("-n", "--count", type=int, default=10)
    ping.set_defaults(func=cmd_ping)

    verify = sub.add_parser("verify", help="比对所有节点的决定日志（安全性）")
    verify.add_argument("--settle", type=float, default=0.5)
    verify.add_argument("--json", action="store_true")
    verify.set_defaults(func=cmd_verify)

    smoke = sub.add_parser("smoke", help="环境自检（三种协议 + 崩溃恢复 + 安全性）")
    smoke.add_argument("--nodes", type=int, default=3, choices=[3, 4, 5])
    smoke.add_argument("--ops", type=int, default=40)
    smoke.add_argument("--base-port", type=int, default=17300, dest="base_port")
    smoke.set_defaults(func=cmd_smoke)

    reset = sub.add_parser("reset", help="清空数据与运行目录")
    reset.add_argument("--data-dir", default="data", dest="data_dir")
    reset.set_defaults(func=cmd_reset)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
