param(
  [string]$InstallRoot = $(if ($env:AISTOCK_GITHUB_RUNNER_ROOT) { $env:AISTOCK_GITHUB_RUNNER_ROOT } else { 'F:\Dev\github-actions-runner\aistock' }),
  [string]$WrapperName = 'run-aistock-runner-hidden.cmd',
  [string]$SupervisorName = 'supervise-aistock-runner.ps1',
  [string]$SupervisorStateName = '.aistock-runner-supervisor.json',
  [string]$StopSentinelName = '.aistock-runner-stop',
  [switch]$ClearStopRequest,
  [switch]$DryRun,
  [switch]$Json
)

$ErrorActionPreference = 'Stop'

function Write-Result {
  param([hashtable]$Payload)
  if ($Json) {
    $Payload | ConvertTo-Json -Depth 4 -Compress
  } else {
    "status=$($Payload.status) started=$($Payload.started) install_root=$($Payload.install_root)"
  }
}

function Get-RunnerProcess {
  param([string]$Root)
  $fullRoot = [System.IO.Path]::GetFullPath($Root).TrimEnd('\') + '\'
  $matches = @()
  foreach ($process in Get-Process -ErrorAction SilentlyContinue) {
    $path = $null
    try {
      $path = $process.Path
    } catch {
      $path = $null
    }

    $isUnderRoot = $false
    if ($path) {
      $isUnderRoot = $path.StartsWith($fullRoot, [System.StringComparison]::OrdinalIgnoreCase)
    }
    if ($isUnderRoot) {
      $matches += $process
    }
  }
  return $matches
}

$resolvedRoot = [System.IO.Path]::GetFullPath($InstallRoot)
$wrapper = Join-Path $resolvedRoot $WrapperName
$supervisor = Join-Path $resolvedRoot $SupervisorName
$supervisorState = Join-Path $resolvedRoot $SupervisorStateName
$stopSentinel = Join-Path $resolvedRoot $StopSentinelName

if (-not (Test-Path -LiteralPath $resolvedRoot -PathType Container)) {
  throw "GitHub runner install root not found: $resolvedRoot"
}
if (-not (Test-Path -LiteralPath $wrapper -PathType Leaf)) {
  throw "GitHub runner wrapper not found: $wrapper"
}
if (-not (Test-Path -LiteralPath $supervisor -PathType Leaf)) {
  throw "GitHub runner supervisor not found: $supervisor"
}

$existing = @(Get-RunnerProcess -Root $resolvedRoot)
$supervisorProcess = $null
if (Test-Path -LiteralPath $supervisorState -PathType Leaf) {
  try {
    $state = Get-Content -Raw -LiteralPath $supervisorState | ConvertFrom-Json
    if ($state.schema_version -eq 'aistock_github_runner_supervisor_state_v1' -and [int]$state.supervisor_pid -gt 0) {
      $supervisorProcess = Get-Process -Id ([int]$state.supervisor_pid) -ErrorAction SilentlyContinue
    }
  } catch {
    $supervisorProcess = $null
  }
}
if ($existing.Count -gt 0 -or $null -ne $supervisorProcess) {
  Write-Result @{
    schema_version = 'aistock_github_runner_start_v1'
    status = 'already_running'
    started = $false
    install_root = $resolvedRoot
    wrapper = $wrapper
    supervisor = $supervisor
    supervisor_pid = $(if ($null -ne $supervisorProcess) { $supervisorProcess.Id } else { $null })
    runner_process_count = $existing.Count
  }
  exit 0
}

if (Test-Path -LiteralPath $stopSentinel -PathType Leaf) {
  if (-not $ClearStopRequest) {
    Write-Result @{
      schema_version = 'aistock_github_runner_start_v1'
      status = 'stop_requested'
      started = $false
      install_root = $resolvedRoot
      wrapper = $wrapper
      supervisor = $supervisor
      stop_sentinel = $stopSentinel
      runner_process_count = 0
    }
    exit 0
  }
  if (-not $DryRun) {
    Remove-Item -LiteralPath $stopSentinel -Force
  }
}

if ($DryRun) {
  Write-Result @{
    schema_version = 'aistock_github_runner_start_v1'
    status = $(if (Test-Path -LiteralPath $stopSentinel -PathType Leaf) { 'would_clear_stop_and_start' } else { 'would_start' })
    started = $false
    install_root = $resolvedRoot
    wrapper = $wrapper
    supervisor = $supervisor
    stop_sentinel = $stopSentinel
    runner_process_count = 0
  }
  exit 0
}

$powerShell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
if (-not (Test-Path -LiteralPath $powerShell -PathType Leaf)) {
  throw "Windows PowerShell not found: $powerShell"
}
Start-Process `
  -FilePath $powerShell `
  -ArgumentList @('-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', ('"' + $supervisor + '"'), '-InstallRoot', ('"' + $resolvedRoot + '"')) `
  -WindowStyle Hidden `
  -WorkingDirectory $resolvedRoot

Write-Result @{
  schema_version = 'aistock_github_runner_start_v1'
  status = 'started'
  started = $true
  install_root = $resolvedRoot
  wrapper = $wrapper
  supervisor = $supervisor
  runner_process_count = 0
}
