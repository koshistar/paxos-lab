"""在 Docker 四节点集群上跑一次状态检查 + 压测。

为什么单独放一个 .py 而不是嵌在 PowerShell 脚本里：``@"..."@`` 这种 here-string
要求结束符**顶格**，缩进进一个大 `switch` 里极易写错、报错信息还很难懂。
拆成独立文件后，Windows / Linux / macOS 都能直接用：

    python scripts/docker_bench.py
    python scripts/docker_bench.py --protocol fast --concurrency 32 --duration 20
    python scripts/docker_bench.py --kill n3        # 顺手演示一次故障恢复

默认对接 ``docker/docker-compose.yml`` 里规划好的宿主机端口：
Paxos 7001~7004，管理口 8101~8104。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

# 允许从工程根目录直接运行（把根目录放进 sys.path）
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paxos.cluster import Cluster, admin_call  # noqa: E402  必须在 sys.path 调整之后
from paxos.loadgen import LoadGen  # noqa: E402


def endpoints() -> list[tuple[str, str, int, int]]:
    return [(f"n{index}", "127.0.0.1", 7000 + index, 8100 + index) for index in range(1, 5)]


def show(cluster: Cluster, tag: str = "") -> dict[str, dict]:
    statuses = cluster.status_all()
    print(f"\n{'节点':<6}{'角色':<10}{'Leader':<8}{'已学slot':<10}{'轮次':<8}{'选举':<6}{'换主':<6}")
    print("-" * 58)
    for node, status in statuses.items():
        if not status.get("ok"):
            print(f"{node:<6}不可达（{status.get('err')}）")
            continue
        learner = status.get("learner") or {}
        print(
            f"{node:<6}{status.get('role', ''):<10}{status.get('leader') or '-'!s:<8}"
            f"{learner.get('chosen_slots', 0):<10}{status.get('max_round', 0):<8}"
            f"{(status.get('counters') or {}).get('elections', 0):<6}{status.get('leader_changes', 0):<6}"
        )
    if tag:
        print(f"（{tag}）")
    return statuses


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Docker 四节点集群：状态 + 压测 + 可选故障演示")
    parser.add_argument("--protocol", default="multipaxos")
    parser.add_argument("-c", "--concurrency", type=int, default=16)
    parser.add_argument("-d", "--duration", type=float, default=10.0)
    parser.add_argument("--target", default="n2", help="客户端接入的节点（默认 n2，非 Leader）")
    parser.add_argument("--kill", default="", help="压测中途 kill 掉某个容器（如 n3），5 秒后重启")
    args = parser.parse_args(argv)

    eps = endpoints()
    cluster = Cluster.external(eps, protocol=args.protocol)

    statuses = show(cluster, "压测前")
    if not any(s.get("ok") for s in statuses.values()):
        print("\n四个节点都不可达。检查：docker compose -f docker/docker-compose.yml ps")
        return 1

    target = next((ep for ep in eps if ep[0] == args.target), eps[1])
    print(f"\n压测：客户端 → {target[0]}，并发 {args.concurrency}，时长 {args.duration:.0f}s")

    gen = LoadGen([(target[0], target[1], target[2])], concurrency=args.concurrency, duration=args.duration)

    async def scenario() -> tuple:
        task = asyncio.create_task(gen.run())
        events: list[str] = []
        if args.kill:
            await asyncio.sleep(max(1.0, args.duration * 0.3))
            container = f"paxos-{args.kill}"
            print(f"  [故障注入] docker kill {container}")
            admin_call(target[1], target[2], {"cmd": "status"}, timeout=1.0)  # 让上面的日志先刷出来
            import subprocess

            subprocess.run(["docker", "kill", container], check=False, capture_output=True)
            events.append(f"kill {container}")
            await asyncio.sleep(5.0)
            print(f"  [恢复] docker start {container}")
            subprocess.run(["docker", "start", container], check=False, capture_output=True)
            events.append(f"start {container}")
        summary = await task
        return summary, events

    summary, events = asyncio.run(scenario())
    print("\n结果:")
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))

    time.sleep(1.5)
    show(cluster, "压测后")
    if events:
        print(f"\n故障时间线：{events} —— 注意看事件前后的成功数变化与换主次数")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
