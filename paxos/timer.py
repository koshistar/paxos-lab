"""高精度定时：给"注入网络时延"用的 sleep。

**问题**：Windows 的默认定时器精度是 15.6 ms，``asyncio.sleep(0.001)`` 实际会睡
15.6 ms。而本实验注入的时延是 ``RTT/2``，RTT=1ms 时只有 0.5ms —— 直接睡的话，
"注入 1ms"会变成"注入 15.6ms"，整条 RTT 曲线的横轴就废了。

**更麻烦的是**：``winmm.timeBeginPeriod(1)`` 只在头几秒有效。实测（Windows 11）
调用后 1ms 的 sleep 确实是 1.5ms，但 3~4 秒之后系统会重新合并定时器，又退回
15.6ms，而且**重复调用 timeBeginPeriod 也压不住**（进程级有效、事件循环的
IOCP 等待仍被按 15.6ms 合并）。

**对策**：混合睡眠 —— 剩余时间超过一个"粗粒度"（16ms）就先睡掉大头（误差被
后面的自旋吸收），剩下不到 16ms 的部分用 ``await asyncio.sleep(0)`` 自旋等待，
靠 ``perf_counter``（微秒级）判断是否到点。代价是这段时间会占 CPU，但延迟精度
回到百微秒级。Linux 上不存在这个问题，``asyncio.sleep`` 本身就是微秒级精度，
所以这里在非 Windows 平台直接退化成普通 sleep。

**给做实验的同学**：这也解释了为什么报告里"设定 RTT=1ms"和"实测 RTT≈2~3ms"
对不上 —— 那 1~2ms 是用户态调度 + TCP 协议栈的固有开销，不是注入值没生效。
"""

from __future__ import annotations

import asyncio
import logging
import sys

log = logging.getLogger(__name__)

#: 系统定时器粗粒度（Windows 15.6ms，取 16ms 保险）
COARSE = 0.016
#: Windows 上是否需要自旋补偿（非 Windows 不需要）
NEEDS_SPIN = sys.platform == "win32"

_active = False


def enable_fine_timers() -> bool:
    """尽量把系统定时器精度提到 1ms（能提多少提多少，主要靠 sleep_precise 兜底）。"""
    global _active
    if _active:
        return True
    if sys.platform != "win32":
        return False
    try:
        import ctypes

        if ctypes.windll.winmm.timeBeginPeriod(1) == 0:  # type: ignore[attr-defined]
            _active = True
            log.debug("已启用 1ms 定时器精度")
            return True
    except Exception:  # pragma: no cover - 受限环境
        log.debug("无法启用高精度定时器", exc_info=True)
    return False


def restore_timers() -> None:
    """恢复系统默认定时器精度（退出前调用）。"""
    global _active
    if not _active or sys.platform != "win32":
        return
    try:
        import ctypes

        ctypes.windll.winmm.timeEndPeriod(1)  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover
        pass
    _active = False


async def sleep_precise(delay: float) -> None:
    """高精度 sleep：``delay`` 秒后返回，误差目标 < 1ms。"""
    if delay <= 0:
        return
    loop = asyncio.get_running_loop()
    if not NEEDS_SPIN:
        await asyncio.sleep(delay)
        return
    deadline = loop.time() + delay
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            return
        if remaining > COARSE:
            # 先睡掉大头，留一个粗粒度给自旋，避免被系统的 15.6ms 合并拖走
            await asyncio.sleep(remaining - COARSE)
        else:
            await asyncio.sleep(0)
