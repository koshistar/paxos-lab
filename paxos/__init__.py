"""Paxos 教学实验工程（《云计算技术》作业5）。

本包用 **纯 Python 标准库** 实现了一套 Paxos 协议原型，不依赖任何现成的
Paxos / Raft 库，包含三种提案机制：

* ``basic``      —— Basic Paxos（每个提案两轮 RTT，任意节点可提案）
* ``multipaxos`` —— Multi-Paxos（稳定 Leader 后跳过 Phase 1，支持批量）
* ``fast``       —— Fast Paxos（客户端单轮直投，冲突时回退经典轮）

另含：多数派确认、提案编号单调递增、超时重试、崩溃恢复（带 fsync 开关）、
可注入 RTT / 丢包 / 网络分区的自定义 TCP 报文传输层、Listener（只学不投票）。

模块划分::

    ballot.py     提案编号与多数派计算
    storage.py    追加写日志（fsync 开/关）
    transport.py  TCP JSON-Lines 报文、链路时延与丢包注入、消息计数
    acceptor.py   Acceptor（持久化 promised / accepted）
    learner.py    Learner（按 slot 顺序应用，缺口回填）
    kv.py         被复制的状态机（幂等 KV）
    proposer.py   三种提案机制的实现
    node.py       节点主循环：网络 + 角色黏合 + admin 控制
    cluster.py    多进程集群拉起/停止
    loadgen.py    压测客户端
    cli.py        命令行入口
"""

from __future__ import annotations

__version__ = "1.0.0"
__all__ = ["__version__"]
