<#
.SYNOPSIS
    作业5 环境自检：检查 Python / 端口 / 模块，并跑一遍 smoke。

.DESCRIPTION
    一次性确认"能不能开始做实验"：
      1. Python 版本 >= 3.10
      2. 工程模块能 import
      3. 端口段（默认 17100~17102 + 管理口 18100~18102）没被占用
      4. 清掉上次残留的集群
      5. 跑 python -m paxos.cli smoke

    说明：本脚本刻意不使用 PowerShell 的 here-string（@"..."@），
    因为它的结束符必须**顶格**，写进 if/try 块里极易出错、报错信息还很难懂。

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\00-check-env.ps1
    powershell -ExecutionPolicy Bypass -File scripts\00-check-env.ps1 -BasePort 17600 -Nodes 4
#>
[CmdletBinding()]
param(
    [int]$BasePort = 17100,
    [int]$Nodes = 3,
    [switch]$SkipSmoke
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }

function Write-Step([string]$text) { Write-Host ''; Write-Host "=== $text ===" -ForegroundColor Cyan }
function Write-Ok([string]$text)   { Write-Host "  [OK]   $text" -ForegroundColor Green }
function Write-Warn2([string]$text){ Write-Host "  [WARN] $text" -ForegroundColor Yellow }
function Write-Bad([string]$text)  { Write-Host "  [FAIL] $text" -ForegroundColor Red }

Write-Host '=============================================' -ForegroundColor Cyan
Write-Host ' Paxos 实验环境自检' -ForegroundColor Cyan
Write-Host " 工程目录: $root" -ForegroundColor Cyan
Write-Host '=============================================' -ForegroundColor Cyan

# ---------------------------------------------------------------- 1. Python
Write-Step '1/5 检查 Python'
$python = $null
foreach ($candidate in @('python', 'py')) {
    try {
        $version = & $candidate -c "import sys; print('.'.join(map(str, sys.version_info[:3])))" 2>$null
        if ($LASTEXITCODE -eq 0 -and $version) {
            $python = $candidate
            Write-Ok "Python $version （命令: $candidate）"
            break
        }
    } catch { }
}
if (-not $python) {
    Write-Bad '找不到 Python。请安装 Python 3.10+ 并勾选 Add python.exe to PATH。'
    exit 1
}
$majorMinor = & $python -c "import sys; print('%d.%d' % sys.version_info[:2])"
if ([version]$majorMinor -lt [version]'3.10') {
    Write-Bad "Python 版本过低（$majorMinor），需要 3.10+"
    exit 1
}

# ---------------------------------------------------------------- 2. 模块
Write-Step '2/5 检查工程模块'
& $python -c "import paxos, experiments; from paxos.cli import main; print('paxos', paxos.__version__)"
if ($LASTEXITCODE -ne 0) {
    Write-Bad '模块导入失败：请确认当前目录是 paxos-lab 根目录。'
    exit 1
}
Write-Ok 'paxos / experiments 都能 import'

$hasMpl = & $python -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('matplotlib') else 1)"
if ($LASTEXITCODE -eq 0) {
    Write-Ok 'matplotlib 已安装（可以出图）'
} else {
    Write-Warn2 'matplotlib 未安装：协议与实验不受影响，只是出不了图。装法: python -m pip install matplotlib'
}

# ---------------------------------------------------------------- 3. 端口
Write-Step '3/5 检查端口占用'
$busy = @()
foreach ($offset in 0..($Nodes - 1)) {
    foreach ($port in @(($BasePort + $offset), ($BasePort + 1000 + $offset))) {
        $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
        if ($conn) { $busy += $port }
    }
}
if ($busy.Count -gt 0) {
    Write-Bad ("端口被占用: " + ($busy -join ', '))
    Write-Host '        处理办法（二选一）：' -ForegroundColor Yellow
    Write-Host '          python -m paxos.cli down        # 停掉上次的集群' -ForegroundColor Yellow
    Write-Host "          换端口段: -BasePort 17600" -ForegroundColor Yellow
    exit 1
}
Write-Ok ("业务口 $BasePort~$($BasePort + $Nodes - 1) 与管理口 $($BasePort + 1000)~$($BasePort + 1000 + $Nodes - 1) 都空闲")

# ---------------------------------------------------------------- 4. 清理
Write-Step '4/5 清理上次残留'
& $python -m paxos.cli down 2>$null | Out-Null
Remove-Item -Recurse -Force data, run -ErrorAction SilentlyContinue
Write-Ok '已清理 data/ 与 run/'

# ---------------------------------------------------------------- 5. smoke
if (-not $SkipSmoke) {
    Write-Step "5/5 环境自检 (python -m paxos.cli smoke --nodes $Nodes)"
    & $python -m paxos.cli smoke --nodes $Nodes
    if ($LASTEXITCODE -ne 0) {
        Write-Bad 'smoke 未全部通过。请看上面的 FAIL 行，或 run/smoke-logs/*.err.log'
        Write-Host '        排错手册：实验操作手册.md 第 12 章' -ForegroundColor Yellow
        exit 1
    }
    Write-Host ''
    Write-Host '环境正常，可以开始做实验了。' -ForegroundColor Green
} else {
    Write-Step '5/5 已跳过 smoke'
}

Write-Host ''
Write-Host '下一步（任选）：' -ForegroundColor Cyan
Write-Host "  python -m paxos.cli up --nodes $Nodes        # 起集群（另开一个终端跑 bench）" -ForegroundColor Cyan
Write-Host '  python -m experiments.run_matrix --profile quick   # 2 分钟出一份对比数据' -ForegroundColor Cyan
Write-Host ''
