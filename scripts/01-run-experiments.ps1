<#
.SYNOPSIS
    作业5 一键跑实验 + 出表格 + 出图。

.DESCRIPTION
    profile 说明：
      quick    ~2 分钟：三种协议 × 3 档并发 + fsync 对照 + 一个 RTT 用例
      protocol 三种协议 × 5 档并发（回答"哪种机制快"）
      nodes    3/4/5 节点（回答"多数派与容错"）
      rtt      RTT 1/10/50/100ms
      loss     丢包 0/1/5/10%
      fsync    fsync 开/关
      batch    批处理窗口
      faults   kill/分区 等 8 个故障场景
      all      以上全部（约 10~12 分钟）

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\01-run-experiments.ps1
    powershell -ExecutionPolicy Bypass -File scripts\01-run-experiments.ps1 -Profile all -SaveRaw -Reset
#>
[CmdletBinding()]
param(
    [ValidateSet('quick', 'protocol', 'nodes', 'rtt', 'loss', 'fsync', 'batch', 'faults', 'all')]
    [string]$Profile = 'quick',
    [double]$Duration = 0,
    [int]$BasePort = 17100,
    [switch]$SaveRaw,
    [switch]$Reset,
    [switch]$NoPlots
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }

function Write-Step([string]$text) { Write-Host ''; Write-Host "=== $text ===" -ForegroundColor Cyan }

$python = 'python'
$started = Get-Date

# 先确保没有残留集群（否则端口冲突，整轮白跑）
& $python -m paxos.cli down 2>$null | Out-Null

Write-Step "跑实验矩阵 profile=$Profile"
$cliArgs = @('-m', 'experiments.run_matrix', '--profile', $Profile, '--base-port', $BasePort)
if ($Duration -gt 0) { $cliArgs += @('--duration', $Duration) }
if ($SaveRaw)        { $cliArgs += '--save-raw' }
if ($Reset)          { $cliArgs += '--reset' }
& $python @cliArgs
if ($LASTEXITCODE -ne 0) {
    Write-Host '实验矩阵执行失败，请看上面的报错。' -ForegroundColor Red
    exit 1
}

Write-Step '汇总成报告表格'
& $python -m experiments.analyze
if ($LASTEXITCODE -ne 0) { Write-Host 'analyze 失败（可能没有数据）' -ForegroundColor Yellow }

if (-not $NoPlots) {
    Write-Step '生成图表'
    & $python -m experiments.plots
    if ($LASTEXITCODE -ne 0) {
        Write-Host '出图失败——通常是没装 matplotlib。装一下再跑：' -ForegroundColor Yellow
        Write-Host '  python -m pip install matplotlib' -ForegroundColor Yellow
    }
}

$elapsed = (Get-Date) - $started
Write-Host ''
Write-Host '=============================================' -ForegroundColor Green
Write-Host (" 完成，用时 {0} 分 {1} 秒" -f [int]$elapsed.TotalMinutes, $elapsed.Seconds) -ForegroundColor Green
Write-Host ' 数据: results/summary.csv  results/faults.csv' -ForegroundColor Green
Write-Host ' 表格: results/tables.md' -ForegroundColor Green
Write-Host ' 图表: results/figs/' -ForegroundColor Green
Write-Host '=============================================' -ForegroundColor Green
Write-Host '下一步：把 results/tables.md 里的表格粘进 实验报告.md，再填上分析。' -ForegroundColor Cyan
