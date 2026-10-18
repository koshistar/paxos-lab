# 复现指南

> 目标：任何人（包括助教）在你的机器或自己的机器上，**用 3 条命令、约 15 分钟**
> 复现 `results/` 里的全部数据与图表。

---

## 0. 前置条件

| 项 | 要求 | 检查命令 |
|---|---|---|
| Python | 3.10+ | `python --version` |
| 操作系统 | Windows / Linux / macOS | – |
| 端口 | 17100~17110、18100~18110 空闲 | `Get-NetTCPConnection -LocalPort 17100` |
| （可选）matplotlib | 只用于出图 | `python -c "import matplotlib"` |

**不需要**：Docker、Kubernetes、任何第三方 Paxos/Raft 库、任何 Python 包（除画图）。

---

## 1. 三步复现

```bash
cd paxos-lab

# ① 环境自检（约 1 分钟，8 项全 PASS 才继续）
python -m paxos.cli smoke

# ② 采集全部实验数据（约 12 分钟）
python -m experiments.run_matrix --profile all --save-raw --reset

# ③ 生成表格与图
python -m experiments.analyze
python -m experiments.plots
```

Windows 上也可以直接用脚本（会自动先停掉残留集群，避免端口冲突）：

```powershell
powershell -ExecutionPolicy Bypass -File scripts\00-check-env.ps1
powershell -ExecutionPolicy Bypass -File scripts\01-run-experiments.ps1 -Profile all -SaveRaw -Reset
```

产出：

```
results/
├── summary.csv       # 每个用例一行（协议/节点数/并发/RTT/丢包/fsync × P50/P95/P99/吞吐/消息数/冲突率/安全性）
├── faults.csv        # 8 个故障场景（恢复时间、黑洞时长、换主次数、安全性）
├── flat.csv          # 长表，便于自己透视
├── tables.md         # 9 张报告可用的 Markdown 表
├── meta.json         # 环境信息
├── raw/*.csv         # 逐条请求延迟（画分布、算分位数）
└── figs/*.png        # 6 张图
```

---

## 2. 分项复现（只想跑某一组实验）

| 想验证什么 | 命令 | 时长 |
|---|---|---|
| 三种协议谁快 | `python -m experiments.run_matrix --profile protocol` | ~3 min |
| 3/4/5 节点多数派 | `--profile nodes` | ~3 min |
| RTT / 丢包临界点 | `--profile rtt` / `--profile loss` | 各 ~2 min |
| fsync 的代价 | `--profile fsync` | ~2 min |
| 批处理窗口 | `--profile batch` | ~1 min |
| 故障与分区 | `--profile faults` | ~2 min |
| 快速冒烟（2 分钟出对比） | `--profile quick --duration 5` | ~2 min |

只要跑过任意 profile，`analyze.py` 就会自动生成对应部分：

```bash
python -m experiments.analyze      # 表 9（临界点速查）总是会生成
```

---

## 3. 手工复现单个用例（答辩演示用，30 秒）

```powershell
# 终端 A
python -m paxos.cli up --nodes 4 --protocol multipaxos

# 终端 B
python -m paxos.cli status
python -m paxos.cli ping --from n1 --to n2
python -m paxos.cli bench -c 16 -d 10
python -m paxos.cli inject --rtt 50
python -m paxos.cli bench -c 16 -d 10
python -m paxos.cli crash --node n3 --restart --wait 2
python -m paxos.cli verify
python -m paxos.cli down
```

---

## 4. Docker 四节点复现（作业"多节点部署"部分）

```powershell
powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action up
powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action status
powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action bench
powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action netem -DelayMs 50
powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action down
```

---

## 5. 复现结果的一致性

- **趋势**必须一致：Multi-Paxos 在并发 32 时明显高于 Basic；Fast Paxos 的低并发延迟最低；
  4 节点与 3 节点容错相同。
- **绝对值**会随机器变化：CPU 核数、磁盘 fsync 速度（本机约 2.3 ms/次）、是否开了杀毒软件。
  报告里已注明"本机 fsync 实测值"，对比时请同时看这个数。
- 如果 RTT=1ms 档的实测值明显大于 3ms，多半是 Windows 定时器精度问题（见
  `paxos/timer.py` 的注释）；此时请在报告里注明，或改用 Docker/WSL2 复现。

---

## 6. 常见偏差与处理

| 现象 | 原因 | 处理 |
|---|---|---|
| 吞吐只有预期的一半 | 机器在跑别的程序 / 省电模式 | 关掉浏览器、插电源 |
| `实测RTT` 远大于设定值 | Windows 定时器精度 | 见上一条；Linux 下更准 |
| Fast Paxos 吞吐极低且 `fast_paxos_exhausted` 很多 | 并发太高、3 节点快速仲裁凑不齐 | 降并发；用 4/5 节点 |
| 故障场景 `事件后未恢复` | 该场景本来就应该不可用（如 2/4 分区） | 看 `faults.csv` 的 `expect` 列 |
| `safety_conflicts > 0` | **安全性被破坏**，属于严重问题 | 保存 `results/raw/*` 与 `run/logs/*`，按 `实验操作手册.md` 第 12.3 节处理 |

---

## 7. 逐字节复现（可选）

想得到与 `results/` 里完全一致的数字几乎不可能（涉及真实时间、调度、磁盘），
但**随机种子固定**的部分是一致的：压测器的命令序列由 `LoadGen(seed=2025)` 决定，
KV 键空间固定为 97 个 key。差异只来自时序。
