"""把实验数据画成报告用的图（需要 matplotlib）。

用法::

    python -m experiments.plots                 # 生成 results/figs/*.png
    python -m experiments.plots --dpi 160

生成的图（报告里按图号引用）：

* ``fig1_protocol_throughput.png`` 三种机制 × 并发：吞吐 + P99
* ``fig2_rtt.png``                 RTT → 吞吐/P50
* ``fig3_loss.png``                丢包率 → 吞吐/错误率
* ``fig4_fsync.png``               fsync 开/关
* ``fig5_fast_conflict.png``       Fast Paxos：冲突率 vs 吞吐
* ``fig6_fault_timeline.png``      故障场景的逐请求延迟时间线
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

RESULTS = Path("results")
FIGS = RESULTS / "figs"

PROTOCOL_LABEL = {
    "basic": "Basic Paxos",
    "multipaxos": "Multi-Paxos",
    "fast": "Fast Paxos",
}
COLORS = {"basic": "#d9534f", "multipaxos": "#0275d8", "fast": "#5cb85c"}


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


def setup_matplotlib() -> Any:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib import font_manager

    # 让中文能正常显示（Windows/Linux 各试几个常见字体）
    candidates = [
        "Microsoft YaHei",
        "SimHei",
        "Noto Sans CJK SC",
        "WenQuanYi Zen Hei",
        "Source Han Sans SC",
        "PingFang SC",
        "Arial Unicode MS",
    ]
    available = {font.name for font in font_manager.fontManager.ttflist}
    chosen = next((name for name in candidates if name in available), None)
    if chosen:
        plt.rcParams["font.sans-serif"] = [chosen, "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["figure.autolayout"] = True
    return plt


def group_by(rows: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[str(row.get(key, ""))].append(row)
    return buckets


# --------------------------------------------------------------------- 各张图
def _series_plot(
    axes: Any,
    data: list[dict[str, Any]],
    x_key: str,
    y_key: str,
    *,
    x_scale: float = 1.0,
    x_label: str,
    y_label: str,
    title: str,
    log_x: bool = False,
    log_y: bool = False,
) -> None:
    """画一组"每种机制一条曲线"的折线图（三张图共用）。"""
    for protocol, group in group_by(data, "protocol").items():
        group.sort(key=lambda r: num(r[x_key]))
        xs = [num(r[x_key]) * x_scale for r in group]
        ys = [num(r[y_key]) for r in group]
        axes.plot(
            xs,
            ys,
            "o-",
            label=PROTOCOL_LABEL.get(protocol, protocol),
            color=COLORS.get(protocol),
        )
    axes.set_xlabel(x_label)
    axes.set_ylabel(y_label)
    axes.set_title(title)
    if log_x:
        axes.set_xscale("log", base=2)
    if log_y:
        axes.set_yscale("log")
    axes.grid(alpha=0.3)
    axes.legend()


def fig_protocol(plt: Any, rows: list[dict[str, Any]], dpi: int) -> Path | None:
    """图 1：三种机制 × 并发 —— 吞吐（左）与 P99（右）。"""
    data = [r for r in rows if r["group"] == "protocol"]
    if not data:
        return None
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    for protocol, group in group_by(data, "protocol").items():
        group.sort(key=lambda r: num(r["concurrency"]))
        xs = [num(r["concurrency"]) for r in group]
        axes[0].plot(
            xs,
            [num(r["throughput"]) for r in group],
            "o-",
            label=PROTOCOL_LABEL.get(protocol, protocol),
            color=COLORS.get(protocol),
        )
        axes[1].plot(
            xs,
            [num(r["p99_ms"]) for r in group],
            "o-",
            label=PROTOCOL_LABEL.get(protocol, protocol),
            color=COLORS.get(protocol),
        )
    axes[0].set_xlabel("并发提案数")
    axes[0].set_ylabel("吞吐 (ops/s)")
    axes[0].set_title("吞吐 vs 并发")
    axes[0].set_xscale("log", base=2)
    axes[0].grid(alpha=0.3)
    axes[0].legend()
    axes[1].set_xlabel("并发提案数")
    axes[1].set_ylabel("P99 延迟 (ms)")
    axes[1].set_title("P99 延迟 vs 并发")
    axes[1].set_xscale("log", base=2)
    axes[1].set_yscale("log")
    axes[1].grid(alpha=0.3)
    axes[1].legend()
    fig.suptitle("三种提案机制：3 节点，RTT≈1ms，fsync=on")
    path = FIGS / "fig1_protocol_throughput.png"
    fig.savefig(path, dpi=dpi)
    plt.close(fig)
    return path


def fig_rtt(plt: Any, rows: list[dict[str, Any]], dpi: int) -> Path | None:
    """图 2：RTT 对吞吐与 P50 的影响。"""
    data = [r for r in rows if r["group"] == "rtt"]
    if not data:
        return None
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    for protocol, group in group_by(data, "protocol").items():
        group.sort(key=lambda r: num(r["rtt_set_ms"]))
        xs = [num(r["rtt_set_ms"]) for r in group]
        label = PROTOCOL_LABEL.get(protocol, protocol)
        color = COLORS.get(protocol)
        axes[0].plot(xs, [num(r["throughput"]) for r in group], "o-", label=label, color=color)
        axes[1].plot(xs, [num(r["p50_ms"]) for r in group], "o-", label=label, color=color)
    for ax, ylabel, title in (
        (axes[0], "吞吐 (ops/s)", "吞吐 vs RTT"),
        (axes[1], "P50 延迟 (ms)", "P50 延迟 vs RTT"),
    ):
        ax.set_xlabel("注入 RTT (ms)")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.grid(alpha=0.3)
        ax.legend()
    fig.suptitle("网络 RTT 的影响（3 节点，并发 8）")
    path = FIGS / "fig2_rtt.png"
    fig.savefig(path, dpi=dpi)
    plt.close(fig)
    return path


def fig_loss(plt: Any, rows: list[dict[str, Any]], dpi: int) -> Path | None:
    """图 3：丢包率对吞吐与 P99 的影响。"""
    data = [r for r in rows if r["group"] == "loss"]
    if not data:
        return None
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    for protocol, group in group_by(data, "protocol").items():
        group.sort(key=lambda r: num(r["loss_set"]))
        xs = [num(r["loss_set"]) * 100 for r in group]
        label = PROTOCOL_LABEL.get(protocol, protocol)
        color = COLORS.get(protocol)
        axes[0].plot(xs, [num(r["throughput"]) for r in group], "o-", label=label, color=color)
        axes[1].plot(xs, [num(r["p99_ms"]) for r in group], "o-", label=label, color=color)
    axes[0].set_xlabel("丢包率 (%)")
    axes[0].set_ylabel("吞吐 (ops/s)")
    axes[0].set_title("吞吐 vs 丢包率")
    axes[1].set_xlabel("丢包率 (%)")
    axes[1].set_ylabel("P99 延迟 (ms)")
    axes[1].set_title("P99 延迟 vs 丢包率")
    for ax in axes:
        ax.grid(alpha=0.3)
        ax.legend()
    fig.suptitle("丢包的影响（3 节点，并发 8）")
    path = FIGS / "fig3_loss.png"
    fig.savefig(path, dpi=dpi)
    plt.close(fig)
    return path


def fig_fsync(plt: Any, rows: list[dict[str, Any]], dpi: int) -> Path | None:
    data = [r for r in rows if r["group"] == "fsync"]
    if not data:
        return None
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    for fsync, group in group_by(data, "fsync").items():
        group.sort(key=lambda r: num(r["concurrency"]))
        xs = [num(r["concurrency"]) for r in group]
        label = f"fsync={fsync}"
        axes[0].plot(xs, [num(r["throughput"]) for r in group], "o-", label=label)
        axes[1].plot(xs, [num(r["p50_ms"]) for r in group], "o-", label=label)
    axes[0].set_ylabel("吞吐 (ops/s)")
    axes[1].set_ylabel("P50 延迟 (ms)")
    for ax, title in ((axes[0], "吞吐 vs 并发"), (axes[1], "P50 延迟 vs 并发")):
        ax.set_xlabel("并发提案数")
        ax.set_title(title)
        ax.set_xscale("log", base=2)
        ax.grid(alpha=0.3)
        ax.legend()
    fig.suptitle("fsync 开/关的代价（Multi-Paxos，3 节点）")
    path = FIGS / "fig4_fsync.png"
    fig.savefig(path, dpi=dpi)
    plt.close(fig)
    return path


def fig_fast_conflict(plt: Any, rows: list[dict[str, Any]], dpi: int) -> Path | None:
    data = [r for r in rows if r["protocol"] == "fast" and num(r.get("conflicts")) > 0]
    if not data:
        return None
    fig, ax = plt.subplots(figsize=(6.4, 4.4))
    points = [(num(r["conflict_rate"]) * 100, num(r["throughput"]), num(r["concurrency"])) for r in data]
    points.sort()
    ax.plot([p[0] for p in points], [p[1] for p in points], "o-", color=COLORS["fast"])
    for conflict, throughput, concurrency in points:
        ax.annotate(
            f"c={int(concurrency)}",
            (conflict, throughput),
            textcoords="offset points",
            xytext=(4, 6),
            fontsize=8,
        )
    ax.set_xlabel("冲突率 (%)")
    ax.set_ylabel("吞吐 (ops/s)")
    ax.set_title("Fast Paxos：冲突率升高后吞吐反而下降")
    ax.grid(alpha=0.3)
    path = FIGS / "fig5_fast_conflict.png"
    fig.savefig(path, dpi=dpi)
    plt.close(fig)
    return path


def fig_fault_timeline(plt: Any, dpi: int) -> Path | None:
    """挑 3 个故障场景画延迟时间线（竖线标出故障/恢复时刻）。"""
    raw_dir = RESULTS / "raw"
    if not raw_dir.exists():
        return None
    candidates = sorted(raw_dir.glob("fault-*.csv"))
    if not candidates:
        return None
    picked = []
    for pattern in ("kill_leader", "kill_acceptor_3n", "partition"):
        for path in candidates:
            if pattern in path.name and path not in picked:
                picked.append(path)
                break
    picked = picked[:3] or candidates[:3]
    fig, axes = plt.subplots(len(picked), 1, figsize=(10, 2.8 * len(picked)), squeeze=False)
    for ax, path in zip(axes[:, 0], picked, strict=False):
        rows = read_rows(path)
        ok = [(num(r["ts_ms"]), num(r["latency_ms"])) for r in rows if r["ok"] == "1"]
        bad = [(num(r["ts_ms"]), num(r["latency_ms"])) for r in rows if r["ok"] != "1"]
        if ok:
            ax.plot([p[0] / 1000 for p in ok], [p[1] for p in ok], ".", markersize=2.5, label="成功", color="#0275d8")
        if bad:
            ax.plot([p[0] / 1000 for p in bad], [p[1] for p in bad], ".", markersize=2.5, label="失败", color="#d9534f")
        events_path = path.with_suffix(".events.json")
        if events_path.exists():
            events = json.loads(events_path.read_text(encoding="utf-8"))
            for event in events.get("events", []):
                ax.axvline(event["at_s"], color="#888", linestyle="--", linewidth=1)
                ax.annotate(
                    f"{event['action']} {event.get('node', '')}",
                    (event["at_s"], 0.95),
                    xycoords=("data", "axes fraction"),
                    fontsize=7,
                    rotation=90,
                    va="top",
                )
        ax.set_ylabel("延迟 (ms)")
        ax.set_xlabel("时间 (s)")
        ax.set_yscale("log")
        ax.grid(alpha=0.3)
        ax.legend(loc="upper right", fontsize=8)
        ax.set_title(path.stem.replace("fault-", ""), fontsize=10)
    fig.suptitle("故障场景：逐请求延迟时间线")
    out = FIGS / "fig6_fault_timeline.png"
    fig.savefig(out, dpi=dpi)
    plt.close(fig)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="生成实验图")
    parser.add_argument("--csv", default="results/summary.csv")
    parser.add_argument("--dpi", type=int, default=130)
    args = parser.parse_args(argv)

    plt = setup_matplotlib()
    FIGS.mkdir(parents=True, exist_ok=True)
    rows = read_rows(Path(args.csv))
    made: list[Path] = []
    for builder in (fig_protocol, fig_rtt, fig_loss, fig_fsync):
        path = builder(plt, rows, args.dpi)
        if path:
            made.append(path)
    path = fig_fast_conflict(plt, rows, args.dpi)
    if path:
        made.append(path)
    path = fig_fault_timeline(plt, args.dpi)
    if path:
        made.append(path)

    if not made:
        print("没有可画的数据，请先运行 python -m experiments.run_matrix")
        return 1
    for path in made:
        print(f"已生成 {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
