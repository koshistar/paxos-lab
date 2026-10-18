<#
.SYNOPSIS
    收工清理：停集群、停容器、清临时文件，并检查提交物是否齐全。

.DESCRIPTION
    打包上传前跑一遍，确保：
      1. 没有残留进程/容器占端口；
      2. data/ run/ __pycache__ 都清掉（.gitignore 也排除了，但本地清理更快）；
      3. results/ 里的原始数据与图表还在（这些是提交物！）；
      4. 报告里没有残留的【待填】。

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\99-cleanup.ps1
    powershell -ExecutionPolicy Bypass -File scripts\99-cleanup.ps1 -KeepData
#>
[CmdletBinding()]
param([switch]$KeepData)

$ErrorActionPreference = 'Continue'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$env:PYTHONUTF8 = '1'
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }

function Write-Step($text) { Write-Host "`n=== $text ===" -ForegroundColor Cyan }
function Write-Ok($text)   { Write-Host "  [OK]   $text" -ForegroundColor Green }
function Write-Warn2($text){ Write-Host "  [WARN] $text" -ForegroundColor Yellow }

Write-Step '1/5 停掉本地集群'
& python -m paxos.cli down 2>$null | Out-Null
$stale = Get-Process python -ErrorAction SilentlyContinue | Where-Object { $_.StartTime -lt (Get-Date).AddSeconds(-5) }
if ($stale) {
    Write-Warn2 ("还有 $($stale.Count) 个 python 进程在跑（可能是别的程序，请自行确认）")
} else {
    Write-Ok '没有残留的集群进程'
}

Write-Step '2/5 停掉 Docker 容器'
docker compose -f docker/docker-compose.yml down -v 2>$null | Out-Null
if ($LASTEXITCODE -eq 0) { Write-Ok '容器已清理（如果本来就没起，忽略）' }

Write-Step '3/5 清临时文件'
if (-not $KeepData) {
    Remove-Item -Recurse -Force data, run -ErrorAction SilentlyContinue
    Write-Ok '已删除 data/ 与 run/'
    Get-ChildItem -Recurse -Directory -Filter __pycache__ -ErrorAction SilentlyContinue |
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Write-Ok '已删除 __pycache__'
    Remove-Item -Recurse -Force .pytest_cache, .ruff_cache, .mypy_cache -ErrorAction SilentlyContinue
    Write-Ok '已删除 pytest/ruff 缓存'
} else {
    Write-Warn2 '按 -KeepData 保留 data/ 与 run/'
}

Write-Step '4/5 检查提交物'
$required = @(
    'README.md',
    '实验报告.md',
    '实验操作手册.md',
    'pyproject.toml',
    'paxos/cli.py',
    'experiments/run_matrix.py',
    'docs/开源项目架构分析.md',
    'results/tables.md'
)
foreach ($item in $required) {
    if (Test-Path $item) { Write-Ok $item } else { Write-Warn2 "缺少 $item" }
}
if (Test-Path 'results/summary.csv') {
    $rows = (Get-Content results/summary.csv | Measure-Object -Line).Lines - 1
    Write-Ok "results/summary.csv（$rows 个用例）"
} else { Write-Warn2 '缺少 results/summary.csv —— 先跑 scripts\01-run-experiments.ps1' }
if (Test-Path 'results/faults.csv') {
    $rows = (Get-Content results/faults.csv | Measure-Object -Line).Lines - 1
    Write-Ok "results/faults.csv（$rows 个故障场景）"
} else { Write-Warn2 '缺少 results/faults.csv —— 跑 --profile faults' }
if (Test-Path 'results/figs') {
    $figs = (Get-ChildItem results/figs -Filter *.png).Count
    Write-Ok "results/figs（$figs 张图）"
} else { Write-Warn2 '缺少 results/figs —— 跑 python -m experiments.plots' }

Write-Step '5/5 检查报告里的待填项'
if (Test-Path '实验报告.md') {
    $pending = (Select-String -Path '实验报告.md' -Pattern '【待填' -AllMatches).Count
    if ($pending -gt 0) {
        Write-Warn2 "实验报告.md 里还有 $pending 处【待填】，提交前请填完"
    } else {
        Write-Ok '报告里没有【待填】了'
    }
}

Write-Host @"

清理完毕。

打包（把 学号_姓名 换成你自己的）：
  Compress-Archive -Path * -DestinationPath ..\学号_姓名_Paxos实验.zip -Force

提交前最后确认：
  - 三种协议都跑过、data 里有 results/
  - 报告里的数字和 results/tables.md 一致
  - 截图齐全（screenshots/）

"@ -ForegroundColor Cyan
