<#
.SYNOPSIS
    Docker 四节点部署的一键脚本（起 / 看 / 压 / 内核级时延 / 停）。

.DESCRIPTION
    对应作业"3~4 台虚拟 Linux 或 Docker 节点、固定 IP、开放端口"的要求。
    每个容器有独立 IP（172.28.0.11~14）、独立数据卷，并带 NET_ADMIN 能力，
    可以用 tc netem 注入内核级网络时延。

    端口规划（详见 docker/docker-compose.yml 头部注释）：
      容器内   Paxos 口 7001、管理口 8001（四个容器一样）
      宿主机   Paxos 7001~7004、管理口 8101~8104
    admin 用 8101 而不是 8001，是为了不和作业4 的 game-rank(8000/8001) 撞车。

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action up
    powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action status
    powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action bench
    powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action netem -DelayMs 50
    powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action clean-netem
    powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action down
#>
[CmdletBinding()]
param(
    [ValidateSet('up', 'status', 'bench', 'netem', 'clean-netem', 'logs', 'down')]
    [string]$Action = 'up',
    [string]$Protocol = 'multipaxos',
    [int]$DelayMs = 50,
    [int]$LossPercent = 0,
    [string]$Node = 'paxos-n1'
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }

$compose = 'docker/docker-compose.yml'

function Assert-Docker {
    try { docker info --format '{{.ServerVersion}}' | Out-Null }
    catch {
        Write-Host 'Docker 没有在运行。请先启动 Docker Desktop（托盘图标变绿）。' -ForegroundColor Red
        exit 1
    }
}

function Wait-Healthy([int]$TimeoutSec = 60) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $rows = docker compose -f $compose ps --format '{{.Name}} {{.Status}}' 2>$null
        $up = ($rows | Select-String -Pattern 'healthy').Count
        if ($up -ge 4) { Write-Host "  4 个容器全部 healthy" -ForegroundColor Green; return $true }
        Start-Sleep -Seconds 2
    }
    Write-Host '  等待 healthy 超时，用 -Action logs 看看容器日志' -ForegroundColor Yellow
    return $false
}

switch ($Action) {
    'up' {
        Assert-Docker
        $env:PAXOS_PROTOCOL = $Protocol
        Write-Host "清理可能存在的旧容器（含失败残留）..." -ForegroundColor Cyan
        # 必须先 down：上一次失败的 up 会留下 Created 状态的容器占着宿主机端口
        docker compose -f $compose down -v --remove-orphans 2>$null | Out-Null
        Write-Host "正在构建并启动 4 个容器（protocol=$Protocol）..." -ForegroundColor Cyan
        docker compose -f $compose up -d --build
        Wait-Healthy | Out-Null
        docker compose -f $compose ps
        Write-Host @"

固定 IP：n1=172.28.0.11  n2=172.28.0.12  n3=172.28.0.13  n4=172.28.0.14
宿主机端口：Paxos 7001~7004（→容器 7001）、管理口 8101~8104（→容器 8001）

验证（这几条直接截图，对应作业的"多节点部署"）：
  docker exec paxos-n1 hostname -i          # 独立 IP
  docker exec paxos-n1 ls /data             # 独立文件系统
  docker exec paxos-n2 hostname             # 独立主机名
  powershell -ExecutionPolicy Bypass -File scripts\02-docker.ps1 -Action bench

"@ -ForegroundColor Green
    }
    'status' {
        docker compose -f $compose ps
        Write-Host "`n（可选）看某个节点日志： docker compose -f $compose logs -f paxos-n1" -ForegroundColor Cyan
    }
    'bench' {
        Assert-Docker
        & python scripts\docker_bench.py
    }
    'netem' {
        Assert-Docker
        $cmd = "tc qdisc add dev eth0 root netem delay ${DelayMs}ms"
        if ($LossPercent -gt 0) { $cmd += " loss ${LossPercent}%" }
        foreach ($name in @('paxos-n1', 'paxos-n2', 'paxos-n3', 'paxos-n4')) {
            docker exec $name sh -c "tc qdisc del dev eth0 root 2>/dev/null; $cmd" 2>$null
            Write-Host "  $name : $cmd" -ForegroundColor Green
        }
        docker exec paxos-n1 tc qdisc show dev eth0
        Write-Host @"

这就是"内核级"的时延注入（对照我们应用层的 paxos.cli inject）。
验证： python -m paxos.cli ping 连不上容器集群，直接用 bench 对比吞吐即可。

"@ -ForegroundColor Cyan
    }
    'clean-netem' {
        Assert-Docker
        foreach ($name in @('paxos-n1', 'paxos-n2', 'paxos-n3', 'paxos-n4')) {
            docker exec $name sh -c 'tc qdisc del dev eth0 root 2>/dev/null; true'
            Write-Host "  $name : 已清除" -ForegroundColor Green
        }
    }
    'logs' {
        docker compose -f $compose logs -f --tail 50
    }
    'down' {
        docker compose -f $compose down -v --remove-orphans
        Write-Host '容器与数据卷已删除。' -ForegroundColor Green
    }
}
