"""被复制的状态机：一个幂等 KV。

Paxos 只保证"决定一个值"，业务语义要靠状态机来落地。这里用一个极简的 KV
做演示，并加入 **幂等键**（``req``）：客户端超时重试可能会让同一条命令在
不同 slot 上被决定两次，状态机按 ``req`` 去重，于是对业务而言是 exactly-once。

命令格式（JSON）::

    {"req": "<uuid>", "op": "set", "k": "a", "v": 1}
    {"req": "<uuid>", "op": "del", "k": "a"}
    {"req": "<uuid>", "op": "incr", "k": "cnt", "v": 1}
    {"req": "<uuid>", "op": "noop"}
"""

from __future__ import annotations

import json
from typing import Any


class KVStateMachine:
    """把已决定的命令按顺序应用到内存 KV 上。"""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}
        self.seen: set[str] = set()
        self.applied = 0
        self.duplicates = 0

    def apply(self, command: str) -> dict[str, Any]:
        """应用一条已决定的命令，返回执行摘要。"""
        try:
            msg = json.loads(command)
        except json.JSONDecodeError:
            self.applied += 1
            return {"skipped": "bad_json"}
        req = str(msg.get("req", ""))
        if req and req in self.seen:
            self.duplicates += 1
            return {"duplicate": True}
        if req:
            self.seen.add(req)
        op = msg.get("op", "noop")
        key = str(msg.get("k", ""))
        if op == "set":
            self.data[key] = msg.get("v")
        elif op == "del":
            self.data.pop(key, None)
        elif op == "incr":
            self.data[key] = int(self.data.get(key, 0)) + int(msg.get("v", 1))
        self.applied += 1
        return {"op": op, "k": key}

    def snapshot(self) -> dict[str, Any]:
        return {
            "applied": self.applied,
            "duplicates": self.duplicates,
            "keys": len(self.data),
            "checksum": checksum(self.data),
        }


def checksum(data: dict[str, Any]) -> int:
    """对 KV 内容做一个稳定校验和，用于多节点状态一致性比对。"""
    total = 0
    for key in sorted(data):
        total = (total * 131 + hash((key, json.dumps(data[key], sort_keys=True, ensure_ascii=False)))) % (2**61 - 1)
    return total
