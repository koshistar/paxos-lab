# 截图清单（作业要求"过程性考核"，截图是过程证据）

统一命名：`S01_短说明.png`。建议放在对应子目录下：

```
screenshots/
├── 01-env/          S01~S03   环境与自检
├── 02-protocol/     S04~S10   三种协议、冲突率、消息数
├── 03-fault/        S11~S21   故障、分区、fsync
├── 04-docker/       S22~S26   Docker 四节点
└── 05-data/         S27~S31   results 目录、表格、图表
```

## 必截（对应评分项）

| 编号 | 截什么 | 怎么产生 | 对应评分项 |
|---|---|---|---|
| S01 | 环境自检 8/8 通过 | `python -m paxos.cli smoke` | Paxos 实现 |
| S02 | 3 节点集群状态表 | `python -m paxos.cli up --nodes 3` 的输出 | 多节点部署 |
| S03 | 两种协议的状态对比（有/无 Leader） | `status`（basic vs multipaxos） | Paxos 实现 |
| S04 | Basic Paxos 压测 | `bench -c 8` | 实验与临界点 |
| S05 | Multi-Paxos 压测 | 同上，换协议 | 实验与临界点 |
| S06 | Fast Paxos 压测 | `bench -c 8 --target all` | 实验与临界点 |
| S07 | Fast Paxos 的冲突率 | `status --json` 里 `conflict_rate` | 实验与临界点 |
| S08 | 消息数明细 | `status --json` 里 `msg_by_type` | 实验与临界点 |
| S09 | RTT 实测（1ms / 50ms 对比） | `inject --rtt` + `ping` | 实验与临界点 |
| S10 | 丢包注入 | `inject --loss 0.1` + `bench` | 实验与临界点 |
| S11 | kill 一个 Acceptor 后仍可用 | `crash --node n3` + `bench` | 实验与临界点 |
| S12 | kill Leader + 新 Leader 出现 | `crash --node n1` + `status` | 实验与临界点 |
| S13 | 4 节点杀 2 个后不可用 | `crash` 两次 + `bench` | 实验与临界点 |
| S14 | 网络分区 | `inject --partition "n1,n2\|n3,n4"` | 实验与临界点 |
| S15 | 分区后 `verify` 仍通过 | `python -m paxos.cli verify` | 安全性 |
| S16 | fsync on/off 吞吐对比 | 两次 `bench -c 32` | 实验与临界点 |
| S17 | fsync off 后崩溃的日志残缺 | `crash` + `Get-Content data\n2\acceptor.log -Tail` | 实验与临界点 |
| S18 | 重启后 `promised_all` 回退 | `up --keep-data` + `status --json` | 崩溃恢复 |
| S19 | 崩溃恢复后 `chosen_slots` 还在 | 同上 | 崩溃恢复 |
| S20 | 集群消息计数（证明多数派通信） | `status --json` 里的 `messages` | Paxos 实现 |
| S21 | 4 节点启动时的多数派提示 | `up --nodes 4` 输出的那两行 | 临界点分析 |
| S22 | `docker compose ps` 四个容器 + IP | `scripts\02-docker.ps1 -Action up` | 多节点部署 |
| S23 | 容器独立文件系统 | `docker exec paxos-n1 ls /data` | 多节点部署 |
| S24 | `nc` 手敲 Paxos 报文 | 见手册 10.3 | 多节点部署 |
| S25 | `tc qdisc show` 内核级时延 | `scripts\02-docker.ps1 -Action netem` | 多节点部署 |
| S26 | `docker stats` 资源占用 | `docker stats --no-stream` | 多节点部署 |
| S27 | `results/` 目录结构 | `Get-ChildItem results -Recurse` | 原始数据 |
| S28 | `results/tables.md` 表 9 | 打开文件 | 临界点分析 |
| S29 | `figs/` 六张图 | 打开资源管理器 | 实验与临界点 |
| S30 | 单元测试通过 | `python -m pytest -q` | 代码质量 |
| S31 | GitHub 仓库首页 | 浏览器 | 提交 |

## 截图小技巧

1. **终端先放大字号**（Ctrl+滚轮），否则投影/打印看不清；
2. **一条命令一张图**，不要把整屏几十行都截进去，老师看不清重点；
3. 关键数字用红框/箭头标一下（Windows 截图工具 `Win+Shift+S` 后可以用"画图"标注）；
4. 每张图下面在报告里配一句"这张图说明了什么"，不要指望老师自己猜。
