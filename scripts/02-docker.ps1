<#
.SYNOPSIS
    Docker 四节点部署的一键脚本（起 / 看 / 压 / 停）。

.DESCRIPTION
    对应作业"3~4 台虚拟 Linux 或 Docker 节点、固定 IP、开放端口"的要求。
    每个容器有独立 IP（172.28.0.11~14）、独立数据卷，并带 NET_ADMIN 能力，
    可以用 tc netem 注入内核级网络时延。

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action up
    powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action status
    powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action bench
    powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action netem -DelayMs 50
    powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action down
#>
[CmdletBinding()]
param(
    [ValidateSet('up', 'status', 'bench', 'netem', 'clean-netem', 'logs', 'down')]
    [string]$Action = 'up',
    [string]$Protocol = 'multipaxos',
    [int]$DelayMs = 50,
    [int]$LossPercent = 0
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$env:PYTHONUTF8 = '1'
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }

$compose = 'docker/docker-compose.yml'

function Assert-Docker {
    try { docker info --format '{{.ServerVersion}}' | Out-Null }
    catch {
        Write-Host 'Docker 没有在运行。请先启动 Docker Desktop（托盘图标变绿）。' -ForegroundColor Red
        exit 1
    }
}

switch ($Action) {
    'up' {
        Assert-Docker
        $env:PAXOS_PROTOCOL = $Protocol
        Write-Host "正在构建并启动 4 个容器（protocol=$Protocol）..." -ForegroundColor Cyan
        docker compose -f $compose up -d --build
        Start-Sleep -Seconds 3
        docker compose -f $compose ps
        Write-Host @"

容器内网段固定：n1=172.28.0.11  n2=172.28.0.12  n3=172.28.0.13  n4=172.28.0.14
宿主机映射端口：7001~7004（Paxos）、8001~8004（管理口）

验证一下（应该能看到各自的 IP 与数据目录）：
  docker exec paxos-n1 hostname -i
  docker exec paxos-n1 ls /data

"@ -ForegroundColor Green
    }
    'status' {
        docker compose -f $compose ps
        Write-Host "`n（可选）看某个节点日志： docker compose -f $compose logs -f paxos-n1" -ForegroundColor Cyan
    }
    'bench' {
        Assert-Docker
        & python -c @'
from paxos.cluster import Cluster
from paxos.loadgen import LoadGen
import asyncio, json

cluster = Cluster.external(
    [('n1','127.0.0.1',7001,8001), ('n2','127.0.0.1',7002,8002),
     ('n3','127.0.0.1',7003,8003), ('n4','127.0.0.1',7004,8004)],
    protocol='multipaxos')
for node, status in cluster.status_all().items():
    print(node, status.get('role'), 'leader=', status.get('leader'),
          'learned=', (status.get('learner') or {}).get('chosen_slots'))
summary = asyncio.run(LoadGen([('n2','127.0.0.1',7002)], concurrency=16, duration=10).run())
print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))
'@
    }
    'netem' {
        Assert-Docker
        $cmd = "tc qdisc add dev eth0 root netem delay ${DelayMs}ms"
        if ($LossPercent -gt 0) { $cmd += " loss ${LossPercent}%" }
        foreach ($node in @('paxos-n1', 'paxos-n2', 'paxos-n3', 'paxos-n4')) {
            docker exec $node sh -c "tc qdisc del dev eth0 root 2>/dev/null; $cmd" 2>$null
            Write-Host "  $node : $cmd" -ForegroundColor Green
        }
        docker exec paxos-n1 tc qdisc show dev eth0
    }
    'clean-netem' {
        Assert-Docker
        foreach ($node in @('paxos-n1', 'paxos-n2', 'paxos-n3', 'paxos-n4')) {
            docker exec $node sh -c 'tc qdisc del dev eth0 root 2>/dev/null; true'
            Write-Host "  $node : 已清除" -ForegroundColor Green
        }
    }
    'logs' {
        docker compose -f $compose logs -f --tail 50
    }
    'down' {
        docker compose -f $compose down -v
        Write-Host '容器与数据卷已删除。' -ForegroundColor Green
    }
}
