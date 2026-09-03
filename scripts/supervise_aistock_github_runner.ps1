param(
  [string]$InstallRoot,
  [string]$WrapperName = 'run-aistock-runner-hidden.cmd',
  [string]$StateName = '.aistock-runner-supervisor.json',
  [string]$StopSentinelName = '.aistock-runner-stop',
  [string]$LogName = 'supervisor.jsonl',
  [int]$RestartDelaySeconds = 5,
  [int]$RestartWindowSeconds = 600,
  [int]$MaxRestarts = 12,
  [switch]$DryRun,
  [switch]$Json
)

$ErrorActionPreference = 'Stop'

function Write-Result {
  param([hashtable]$Payload)
  if ($Json) {
    $Payload | ConvertTo-Json -Depth 5 -Compress
  } else {
    "status=$($Payload.status) install_root=$($Payload.install_root)"
  }
}

function Write-SupervisorEvent {
  param([string]$Path, [string]$Event, [hashtable]$Details)
  $payload = @{
    schema_version = 'aistock_github_runner_supervisor_event_v1'
    timestamp = [DateTime]::UtcNow.ToString('o')
    event = $Event
    supervisor_pid = $PID
    details = $Details
  }
  Add-Content -LiteralPath $Path -Value ($payload | ConvertTo-Json -Depth 5 -Compress) -Encoding UTF8
}

function Write-SupervisorState {
  param([string]$Path, [hashtable]$Payload)
  $temporary = "$Path.$PID.tmp"
  $Payload | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $temporary -Encoding UTF8
  Move-Item -LiteralPath $temporary -Destination $Path -Force
}

if (-not $InstallRoot) {
  throw 'InstallRoot is required'
}
if ($RestartDelaySeconds -lt 1 -or $RestartDelaySeconds -gt 60) {
  throw 'RestartDelaySeconds must be between 1 and 60'
}
if ($RestartWindowSeconds -lt 60 -or $RestartWindowSeconds -gt 3600) {
  throw 'RestartWindowSeconds must be between 60 and 3600'
}
if ($MaxRestarts -lt 1 -or $MaxRestarts -gt 100) {
  throw 'MaxRestarts must be between 1 and 100'
}

$resolvedRoot = [System.IO.Path]::GetFullPath($InstallRoot).TrimEnd('\')
$wrapper = Join-Path $resolvedRoot $WrapperName
$statePath = Join-Path $resolvedRoot $StateName
$stopSentinel = Join-Path $resolvedRoot $StopSentinelName
$logPath = Join-Path $resolvedRoot $LogName

if (-not (Test-Path -LiteralPath $resolvedRoot -PathType Container)) {
  throw "GitHub runner install root not found: $resolvedRoot"
}
if (-not (Test-Path -LiteralPath $wrapper -PathType Leaf)) {
  throw "GitHub runner wrapper not found: $wrapper"
}

if ($DryRun) {
  Write-Result @{
    schema_version = 'aistock_github_runner_supervisor_v1'
    status = $(if (Test-Path -LiteralPath $stopSentinel -PathType Leaf) { 'stop_requested' } else { 'would_supervise' })
    install_root = $resolvedRoot
    wrapper = $wrapper
    state_path = $statePath
    stop_sentinel = $stopSentinel
    restart_delay_seconds = $RestartDelaySeconds
    restart_window_seconds = $RestartWindowSeconds
    max_restarts = $MaxRestarts
  }
  exit 0
}

$rootBytes = [Text.Encoding]::UTF8.GetBytes($resolvedRoot.ToLowerInvariant())
$sha256 = [Security.Cryptography.SHA256]::Create()
try {
  $rootHash = ([BitConverter]::ToString($sha256.ComputeHash($rootBytes))).Replace('-', '').ToLowerInvariant()
} finally {
  $sha256.Dispose()
}
$mutexName = "Local\AIstockGitHubRunner-$($rootHash.Substring(0, 24))"
$mutex = New-Object Threading.Mutex($false, $mutexName)
$ownsMutex = $false

try {
  $ownsMutex = $mutex.WaitOne(0)
  if (-not $ownsMutex) {
    Write-Result @{
      schema_version = 'aistock_github_runner_supervisor_v1'
      status = 'already_supervised'
      install_root = $resolvedRoot
      wrapper = $wrapper
      mutex = $mutexName
    }
    exit 0
  }
  if (Test-Path -LiteralPath $stopSentinel -PathType Leaf) {
    Write-SupervisorEvent -Path $logPath -Event 'stop_requested_before_start' -Details @{}
    Write-Result @{
      schema_version = 'aistock_github_runner_supervisor_v1'
      status = 'stop_requested'
      install_root = $resolvedRoot
      wrapper = $wrapper
    }
    exit 0
  }

  Write-SupervisorState -Path $statePath -Payload @{
    schema_version = 'aistock_github_runner_supervisor_state_v1'
    status = 'running'
    supervisor_pid = $PID
    started_at = [DateTime]::UtcNow.ToString('o')
    install_root = $resolvedRoot
    wrapper = $wrapper
    stop_sentinel = $stopSentinel
    restart_delay_seconds = $RestartDelaySeconds
    restart_window_seconds = $RestartWindowSeconds
    max_restarts = $MaxRestarts
  }
  Write-SupervisorEvent -Path $logPath -Event 'supervisor_started' -Details @{ wrapper = $wrapper }

  $restartTimes = New-Object System.Collections.Generic.List[DateTime]
  while (-not (Test-Path -LiteralPath $stopSentinel -PathType Leaf)) {
    $process = Start-Process `
      -FilePath 'cmd.exe' `
      -ArgumentList @('/d', '/c', ('"' + $wrapper + '"')) `
      -WindowStyle Hidden `
      -WorkingDirectory $resolvedRoot `
      -PassThru
    $process.WaitForExit()
    $exitCode = $process.ExitCode
    Write-SupervisorEvent -Path $logPath -Event 'runner_wrapper_exited' -Details @{
      wrapper_pid = $process.Id
      exit_code = $exitCode
    }

    if (Test-Path -LiteralPath $stopSentinel -PathType Leaf) {
      break
    }

    $now = [DateTime]::UtcNow
    for ($index = $restartTimes.Count - 1; $index -ge 0; $index--) {
      if (($now - $restartTimes[$index]).TotalSeconds -gt $RestartWindowSeconds) {
        $restartTimes.RemoveAt($index)
      }
    }
    $restartTimes.Add($now)
    if ($restartTimes.Count -gt $MaxRestarts) {
      Write-SupervisorEvent -Path $logPath -Event 'restart_budget_exhausted' -Details @{
        restart_count = $restartTimes.Count
        restart_window_seconds = $RestartWindowSeconds
        last_exit_code = $exitCode
      }
      throw "GitHub runner restart budget exhausted: $($restartTimes.Count) exits in $RestartWindowSeconds seconds"
    }
    Start-Sleep -Seconds $RestartDelaySeconds
  }

  Write-SupervisorEvent -Path $logPath -Event 'supervisor_stopped' -Details @{ reason = 'stop_sentinel' }
} finally {
  if (Test-Path -LiteralPath $statePath -PathType Leaf) {
    try {
      $state = Get-Content -Raw -LiteralPath $statePath | ConvertFrom-Json
      if ([int]$state.supervisor_pid -eq $PID) {
        Remove-Item -LiteralPath $statePath -Force
      }
    } catch {
      Write-Warning "Unable to finalize supervisor state: $($_.Exception.Message)"
    }
  }
  if ($ownsMutex) {
    $mutex.ReleaseMutex()
  }
  $mutex.Dispose()
}
