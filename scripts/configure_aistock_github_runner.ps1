param(
  [string]$InstallRoot = 'F:\Dev\github-actions-runner\aistock-security',
  [string]$AllowedRoot = 'F:\Dev\github-actions-runner',
  [string]$RunnerVersion = $env:AISTOCK_GITHUB_RUNNER_VERSION,
  [string]$ArchivePath = $env:AISTOCK_GITHUB_RUNNER_ARCHIVE_PATH,
  [string]$ArchiveSha256 = $env:AISTOCK_GITHUB_RUNNER_ARCHIVE_SHA256,
  [string]$TemplateWrapper = 'F:\Dev\github-actions-runner\aistock\run-aistock-runner-hidden.cmd',
  [string]$SupervisorSource = $(Join-Path $PSScriptRoot 'supervise_aistock_github_runner.ps1'),
  [string]$RepositoryUrl = 'https://github.com/licong01-cloud/AIstock',
  [string]$RunnerName = "$env:COMPUTERNAME-aistock-security",
  [string]$Role = 'security',
  [string[]]$Labels = @('aistock', 'aistock-ci', 'aistock-ci-security'),
  [string]$StartHelperPath,
  [switch]$AuditOnly,
  [switch]$Apply,
  [switch]$Start,
  [switch]$ScheduledDispatch,
  [string]$SchedulerPython = $(Join-Path ([Environment]::GetFolderPath('UserProfile')) 'miniconda3\envs\AIstock-CI\pythonw.exe'),
  [string]$SchedulerGhConfigRoot = $(if ($env:GH_CONFIG_DIR) { $env:GH_CONFIG_DIR } else { Join-Path $env:APPDATA 'GitHub CLI' }),
  [switch]$Json
)

$ErrorActionPreference = 'Stop'

function Write-Result {
  param([hashtable]$Payload)
  if ($Json) {
    $Payload | ConvertTo-Json -Depth 5 -Compress
  } else {
    "status=$($Payload.status) configured=$($Payload.configured) started=$($Payload.started) role=$($Payload.role) install_root=$($Payload.install_root)"
  }
}

function Resolve-BoundedPath {
  param([string]$Path, [string]$Boundary)
  $resolved = [System.IO.Path]::GetFullPath($Path).TrimEnd('\')
  $resolvedBoundary = [System.IO.Path]::GetFullPath($Boundary).TrimEnd('\')
  $prefix = $resolvedBoundary + '\'
  if (-not $resolved.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Runner install root must stay below ${resolvedBoundary}: $resolved"
  }
  if ($resolved.Equals($resolvedBoundary, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Runner install root cannot equal the runner boundary: $resolved"
  }
  return $resolved
}

function Resolve-ScheduledDispatchOwnerSid {
  param([Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$UserId)
  if ($UserId -match '^S-\d-') {
    return ([Security.Principal.SecurityIdentifier]$UserId).Value
  }
  return ([Security.Principal.NTAccount]$UserId).Translate([Security.Principal.SecurityIdentifier]).Value
}

# Separate infrastructure-only mode: no runner/service/DB lifecycle operations.
if ($ScheduledDispatch) {
  if ($Start -or ($AuditOnly -and $Apply)) {
    throw 'ScheduledDispatch cannot start a runner or combine AuditOnly with Apply'
  }
  $commonGit = & git -C (Join-Path $PSScriptRoot '..') rev-parse --path-format=absolute --git-common-dir
  if ($LASTEXITCODE -ne 0 -or -not $commonGit) { throw 'Cannot resolve canonical Git authority' }
  $canonical = Split-Path ([IO.Path]::GetFullPath($commonGit.Trim())) -Parent
  $python = [IO.Path]::GetFullPath($SchedulerPython)
  $script = Join-Path $canonical 'scripts\aistock_runner_health.py'
  $state = Join-Path (Split-Path $canonical -Parent) 'aistock-nightly-scheduler'
  $taskName = 'AIstock Nightly Scheduled Dispatch'
  $zone = Get-TimeZone
  if ($zone.BaseUtcOffset.TotalHours -ne 8 -or $zone.SupportsDaylightSavingTime) {
    throw 'Scheduler requires a fixed UTC+8 Windows timezone'
  }
  if (-not (Get-Command gh -ErrorAction SilentlyContinue)) { throw 'Existing gh CLI is required; not installed here' }
  $ghConfig = [IO.Path]::GetFullPath($SchedulerGhConfigRoot)
  if (-not (Test-Path -LiteralPath (Join-Path $ghConfig 'hosts.yml') -PathType Leaf)) {
    throw 'Existing authenticated gh config location is required; credentials are not copied'
  }
  $arguments = '"{0}" dispatch-scheduled --state-root "{1}" --gh-config-dir "{2}" --apply' -f $script, $state, $ghConfig
  foreach ($required in @($python, $script)) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) { throw "Missing scheduler entry: $required" }
  }
  $existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
  if ($existing -and ($existing.Actions.Count -ne 1 -or
      $existing.Actions[0].Execute -ne $python -or $existing.Actions[0].Arguments -ne $arguments)) {
    throw 'Existing scheduled task has unexpected ownership/action; not overwritten'
  }
  if ($existing) {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $ownerSid = Resolve-ScheduledDispatchOwnerSid -UserId $existing.Principal.UserId
    $triggerTimes = @($existing.Triggers | ForEach-Object { ([datetime]$_.StartBoundary).ToString('HH:mm') })
    # Task Scheduler CIM enums are numeric: Disabled=1, Interactive=3, Limited=0, IgnoreNew=2.
    if ([int]$existing.State -eq 1 -or $existing.Actions[0].WorkingDirectory -ne $canonical -or
        $ownerSid -ne $identity.User.Value -or
        [int]$existing.Principal.RunLevel -ne 0 -or -not $existing.Settings.Hidden -or
        -not $existing.Settings.StartWhenAvailable -or
        [int]$existing.Principal.LogonType -ne 3 -or $triggerTimes.Count -ne 3 -or
        @('02:30','03:07','04:27' | Where-Object { $_ -notin $triggerTimes }).Count -ne 0 -or
        $existing.Triggers[0].Repetition.Interval -ne 'PT5M' -or
        $existing.Triggers[0].Repetition.Duration -ne 'PT3H45M' -or
        [int]$existing.Settings.MultipleInstances -ne 2 -or
        $existing.Settings.ExecutionTimeLimit -ne 'PT4M') {
      throw 'Owned scheduler configuration drift; inspect before modifying'
    }
  }
  if ($Apply) {
    if ([IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..')).TrimEnd('\') -ne $canonical) {
      throw 'Apply scheduler only from the merged canonical script'
    }
    $branch = & git -C $canonical branch --show-current
    if ($LASTEXITCODE -ne 0 -or $branch -ne 'main') { throw 'Scheduler apply requires canonical main' }
    $dirty = & git -C $canonical status --porcelain
    if ($LASTEXITCODE -ne 0 -or $dirty) { throw 'Scheduler apply requires a clean canonical checkout' }
    $triggers = @()
    foreach ($at in @('02:30', '03:07', '04:27')) {
      $triggers += New-ScheduledTaskTrigger -Daily -At $at
    }
    # A bounded retry tick covers missed wakeup/network failures; no continuous daemon.
    $triggers[0].Repetition = New-CimInstance -ClientOnly -Namespace 'Root/Microsoft/Windows/TaskScheduler' `
      -ClassName MSFT_TaskRepetitionPattern -Property @{ Interval='PT5M'; Duration='PT3H45M'; StopAtDurationEnd=$false }
    $action = New-ScheduledTaskAction -Execute $python -Argument $arguments -WorkingDirectory $canonical
    $settings = New-ScheduledTaskSettingsSet -Hidden -StartWhenAvailable -MultipleInstances IgnoreNew `
      -ExecutionTimeLimit (New-TimeSpan -Minutes 4) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
    $principal = New-ScheduledTaskPrincipal -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) `
      -LogonType Interactive -RunLevel Limited
    if (-not $existing) {
      Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $triggers `
        -Settings $settings -Principal $principal -Description 'Only dispatch existing overnight workflows; no service control.' | Out-Null
    }
    # Existing exact owned action is left untouched, including while running.
    $existing = Get-ScheduledTask -TaskName $taskName
  }
  $info = if ($existing) { Get-ScheduledTaskInfo -TaskName $taskName } else { $null }
  Write-Result @{
    schema_version='aistock_scheduled_dispatch_install_v1'
    status=$(if ($existing) { 'installed' } else { 'planned' })
    configured=[bool]$existing; started=$false; role='scheduled_dispatch'
    task_name=$taskName; state_root=$state; execute=$python; arguments=$arguments
    next_run_time=$(if ($info) { $info.NextRunTime.ToString('o') } else { $null })
    last_task_result=$(if ($info) { $info.LastTaskResult } else { $null })
    requires_awake_signed_in_host=$true
  }
  exit 0
}

if ($Role -notin @('general', 'security')) {
  throw "Runner role must be general or security: $Role"
}
if ($AuditOnly -and ($Apply -or $Start)) {
  throw 'AuditOnly cannot be combined with Apply or Start'
}

function Get-RunnerProcess {
  param([string]$Root)
  $prefix = [System.IO.Path]::GetFullPath($Root).TrimEnd('\') + '\'
  $matches = @()
  foreach ($process in Get-Process -ErrorAction SilentlyContinue) {
    $path = $null
    try {
      $path = $process.Path
    } catch {
      $path = $null
    }
    if ($path -and $path.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
      $matches += $process
    }
  }
  return $matches
}
if (-not $Labels -or $Labels.Count -eq 0) {
  throw 'At least one explicit runner label is required'
}
$requiredRoleLabels = $(
  if ($Role -eq 'general') {
    @('aistock-ci')
  } else {
    @('aistock-ci', 'aistock-ci-security')
  }
)
foreach ($requiredRoleLabel in $requiredRoleLabels) {
  if ($Labels -notcontains $requiredRoleLabel) {
    throw "Runner labels for role $Role must include $requiredRoleLabel"
  }
}
if ($Role -eq 'general' -and $Labels -contains 'aistock-ci-security') {
  throw 'General runner labels must not include aistock-ci-security'
}

$resolvedRoot = Resolve-BoundedPath -Path $InstallRoot -Boundary $AllowedRoot
if ($AuditOnly) {
  $auditIdentityPath = Join-Path $resolvedRoot '.runner'
  $auditWrapperPath = Join-Path $resolvedRoot 'run-aistock-runner-hidden.cmd'
  $auditSupervisorPath = Join-Path $resolvedRoot 'supervise-aistock-runner.ps1'
  $auditSupervisorStatePath = Join-Path $resolvedRoot '.aistock-runner-supervisor.json'
  $blocking = @()
  $warnings = @()
  $identity = $null
  $supervisorPid = $null
  $supervisorAlive = $false

  if (-not (Test-Path -LiteralPath $resolvedRoot -PathType Container)) {
    $blocking += "runner install root is missing: $resolvedRoot"
  } elseif (-not (Test-Path -LiteralPath $auditIdentityPath -PathType Leaf)) {
    $blocking += "runner identity is missing: $auditIdentityPath"
  } else {
    try {
      $identity = Get-Content -Raw -LiteralPath $auditIdentityPath | ConvertFrom-Json
    } catch {
      $blocking += "runner identity is invalid: $auditIdentityPath"
    }
  }

  if ($null -ne $identity) {
    if ($identity.gitHubUrl.TrimEnd('/') -ne $RepositoryUrl.TrimEnd('/')) {
      $blocking += "configured runner repository mismatch: $($identity.gitHubUrl)"
    }
    if ($identity.workFolder -ne '_work') {
      $blocking += "configured runner work folder mismatch: $($identity.workFolder)"
    }
    if (-not [bool]$identity.disableUpdate) {
      $blocking += 'runner automatic update is enabled; disableUpdate=true is required'
    }
  }
  if (-not (Test-Path -LiteralPath $auditWrapperPath -PathType Leaf)) {
    $blocking += "runner wrapper is missing: $auditWrapperPath"
  } else {
    $auditWrapperText = Get-Content -Raw -LiteralPath $auditWrapperPath
    if ($auditWrapperText -notmatch '(?im)^set "GIT_ALTERNATE_OBJECT_DIRECTORIES="\s*$') {
      $blocking += 'runner wrapper must clear GIT_ALTERNATE_OBJECT_DIRECTORIES; external object caches are not durable runner dependencies'
    }
  }
  if (-not (Test-Path -LiteralPath $auditSupervisorPath -PathType Leaf)) {
    $blocking += "runner supervisor is missing: $auditSupervisorPath"
  }
  if (Test-Path -LiteralPath $auditSupervisorStatePath -PathType Leaf) {
    try {
      $state = Get-Content -Raw -LiteralPath $auditSupervisorStatePath | ConvertFrom-Json
      if ($state.schema_version -eq 'aistock_github_runner_supervisor_state_v1' -and [int]$state.supervisor_pid -gt 0) {
        $supervisorPid = [int]$state.supervisor_pid
        $supervisorAlive = $null -ne (Get-Process -Id $supervisorPid -ErrorAction SilentlyContinue)
      }
    } catch {
      $warnings += "runner supervisor state is invalid: $auditSupervisorStatePath"
    }
  }
  if ((Test-Path -LiteralPath $auditSupervisorPath -PathType Leaf) -and -not $supervisorAlive) {
    $warnings += 'runner supervisor is installed but not active'
  }

  $payload = @{
    schema_version = 'aistock_github_runner_install_audit_v1'
    status = $(if ($blocking.Count -eq 0) { 'ready' } else { 'blocked' })
    workflow_gate = $(if ($blocking.Count -eq 0) { 'ready' } else { 'blocked' })
    configured = $null -ne $identity
    started = $false
    role = $Role
    labels = $Labels
    runner_name = $(if ($null -ne $identity) { $identity.agentName } else { $null })
    install_root = $resolvedRoot
    automatic_update_disabled = $(if ($null -ne $identity) { [bool]$identity.disableUpdate } else { $false })
    wrapper_present = Test-Path -LiteralPath $auditWrapperPath -PathType Leaf
    supervisor_present = Test-Path -LiteralPath $auditSupervisorPath -PathType Leaf
    supervisor_pid = $supervisorPid
    supervisor_alive = $supervisorAlive
    blocking = $blocking
    warnings = $warnings
    process_control_performed = $false
    remediation = $(if ($blocking.Count -eq 0) { 'none' } else { 'run configure helper with pinned archive inputs only after explicit runner process-control authorization' })
  }
  Write-Result $payload
  exit $(if ($blocking.Count -eq 0) { 0 } else { 2 })
}

if (-not $RunnerVersion -or $RunnerVersion -notmatch '^\d+\.\d+\.\d+$') {
  throw 'RunnerVersion must be an explicit semantic version such as 2.337.0'
}
if (-not $ArchivePath) {
  throw 'ArchivePath or AISTOCK_GITHUB_RUNNER_ARCHIVE_PATH is required'
}
if (-not $ArchiveSha256 -or $ArchiveSha256 -notmatch '^[a-fA-F0-9]{64}$') {
  throw 'ArchiveSha256 must be an explicit 64-character SHA-256'
}

$resolvedArchive = [System.IO.Path]::GetFullPath($ArchivePath)
$resolvedTemplate = [System.IO.Path]::GetFullPath($TemplateWrapper)
$resolvedSupervisorSource = [System.IO.Path]::GetFullPath($SupervisorSource)
$resolvedStartHelper = if ($StartHelperPath) {
  [System.IO.Path]::GetFullPath($StartHelperPath)
} else {
  Join-Path $PSScriptRoot 'start_aistock_github_runner.ps1'
}
$wrapper = Join-Path $resolvedRoot 'run-aistock-runner-hidden.cmd'
$supervisor = Join-Path $resolvedRoot 'supervise-aistock-runner.ps1'
$supervisorState = Join-Path $resolvedRoot '.aistock-runner-supervisor.json'
$runnerIdentity = Join-Path $resolvedRoot '.runner'
$config = Join-Path $resolvedRoot 'config.cmd'
$alreadyConfigured = Test-Path -LiteralPath $runnerIdentity -PathType Leaf
$automaticUpdateDisabled = $false
$runnerProcesses = @()
$supervisorProcess = $null

if (-not (Test-Path -LiteralPath $resolvedArchive -PathType Leaf)) {
  throw "GitHub runner archive not found: $resolvedArchive"
}
$observedArchiveSha256 = (Get-FileHash -LiteralPath $resolvedArchive -Algorithm SHA256).Hash.ToLowerInvariant()
if (-not $ArchiveSha256 -or $observedArchiveSha256 -ne $ArchiveSha256.ToLowerInvariant()) {
  throw "GitHub runner archive SHA-256 mismatch: $resolvedArchive"
}
if (-not (Test-Path -LiteralPath $resolvedTemplate -PathType Leaf)) {
  throw "GitHub runner wrapper template not found: $resolvedTemplate"
}
if (-not (Test-Path -LiteralPath $resolvedSupervisorSource -PathType Leaf)) {
  throw "GitHub runner supervisor source not found: $resolvedSupervisorSource"
}
if ((Test-Path -LiteralPath $resolvedRoot) -and -not $alreadyConfigured) {
  $unexpected = @(Get-ChildItem -Force -LiteralPath $resolvedRoot -ErrorAction SilentlyContinue)
  if ($unexpected.Count -gt 0 -and -not (Test-Path -LiteralPath $config -PathType Leaf)) {
    throw "Unrecognized non-empty runner install root: $resolvedRoot"
  }
}
if ($alreadyConfigured) {
  try {
    $identity = Get-Content -Raw -LiteralPath $runnerIdentity | ConvertFrom-Json
  } catch {
    throw "GitHub runner identity is invalid: $runnerIdentity"
  }
  if ($identity.agentName -ne $RunnerName) {
    throw "Configured runner name mismatch: $($identity.agentName)"
  }
  if ($identity.gitHubUrl.TrimEnd('/') -ne $RepositoryUrl.TrimEnd('/')) {
    throw "Configured runner repository mismatch: $($identity.gitHubUrl)"
  }
  if ($identity.workFolder -ne '_work') {
    throw "Configured runner work folder mismatch: $($identity.workFolder)"
  }
  $automaticUpdateDisabled = [bool]$identity.disableUpdate
}
if (Test-Path -LiteralPath $resolvedRoot -PathType Container) {
  $runnerProcesses = @(Get-RunnerProcess -Root $resolvedRoot)
}
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

if (-not $Apply) {
  Write-Result @{
    schema_version = 'aistock_github_runner_configure_v1'
    status = $(if ($alreadyConfigured -and -not $automaticUpdateDisabled) { 'reconfiguration_required' } elseif ($alreadyConfigured) { 'already_configured' } else { 'would_configure' })
    configured = $alreadyConfigured
    automatic_update_disabled = $(if ($alreadyConfigured) { $automaticUpdateDisabled } else { $true })
    maintenance_process_count = $runnerProcesses.Count + $(if ($null -ne $supervisorProcess) { 1 } else { 0 })
    started = $false
    role = $Role
    labels = $Labels
    runner_name = $RunnerName
    runner_version = $RunnerVersion
    install_root = $resolvedRoot
    archive_path = $resolvedArchive
    archive_sha256 = $observedArchiveSha256
    supervisor_source = $resolvedSupervisorSource
  }
  exit 0
}

if ($alreadyConfigured -and -not $automaticUpdateDisabled) {
  throw 'Configured runner allows automatic updates; re-register it with --disableupdate before applying the supervised launcher'
}
if ($runnerProcesses.Count -gt 0 -or $null -ne $supervisorProcess) {
  throw 'Runner maintenance requires the selected role to have no active Listener, Worker, Updater, or supervisor process'
}

if (-not (Test-Path -LiteralPath $resolvedRoot -PathType Container)) {
  New-Item -ItemType Directory -Path $resolvedRoot | Out-Null
}
if (-not (Test-Path -LiteralPath $config -PathType Leaf)) {
  Expand-Archive -LiteralPath $resolvedArchive -DestinationPath $resolvedRoot
}
if (-not (Test-Path -LiteralPath $config -PathType Leaf)) {
  throw "GitHub runner config.cmd missing after archive expansion: $config"
}

if (-not $alreadyConfigured) {
  $token = $env:AISTOCK_GITHUB_RUNNER_REGISTRATION_TOKEN
  if (-not $token) {
    throw 'AISTOCK_GITHUB_RUNNER_REGISTRATION_TOKEN is required for first-time configuration'
  }
  & $config --unattended --url $RepositoryUrl --token $token --name $RunnerName --labels ($Labels -join ',') --work '_work' --disableupdate
  if ($LASTEXITCODE -ne 0) {
    throw "GitHub runner configuration failed with exit code $LASTEXITCODE"
  }
  $alreadyConfigured = Test-Path -LiteralPath $runnerIdentity -PathType Leaf
  if (-not $alreadyConfigured) {
    throw 'GitHub runner configuration completed without creating .runner identity'
  }
  $identity = Get-Content -Raw -LiteralPath $runnerIdentity | ConvertFrom-Json
  $automaticUpdateDisabled = [bool]$identity.disableUpdate
  if (-not $automaticUpdateDisabled) {
    throw 'GitHub accepted the runner registration without disableUpdate=true'
  }
}

Copy-Item -LiteralPath $resolvedSupervisorSource -Destination $supervisor -Force

$wrapperText = Get-Content -Raw -LiteralPath $resolvedTemplate
$runnerCallPattern = '(?im)^call run\.cmd(?:\s.*)?$'
if ($wrapperText -notmatch $runnerCallPattern) {
  throw "Runner wrapper template does not call run.cmd: $resolvedTemplate"
}
$wrapperText = [regex]::Replace(
  $wrapperText,
  '(?im)^cd /d ".*"\s*$',
  ('cd /d "' + $resolvedRoot + '"'),
  1
)
$gitAlternatePattern = '(?im)^set "GIT_ALTERNATE_OBJECT_DIRECTORIES=.*"\s*$'
$gitAlternateClear = 'set "GIT_ALTERNATE_OBJECT_DIRECTORIES="'
if ($wrapperText -match $gitAlternatePattern) {
  $wrapperText = [regex]::Replace($wrapperText, $gitAlternatePattern, $gitAlternateClear)
} else {
  $wrapperText = $wrapperText -replace '(?im)^call run\.cmd', ($gitAlternateClear + "`r`ncall run.cmd")
}
if ($wrapperText -match '(?im)^set "AISTOCK_RUNNER_ROLE=.*"\s*$') {
  $wrapperText = [regex]::Replace(
    $wrapperText,
    '(?im)^set "AISTOCK_RUNNER_ROLE=.*"\s*$',
    ('set "AISTOCK_RUNNER_ROLE=' + $Role + '"')
  )
} else {
  $roleLine = 'set "AISTOCK_RUNNER_ROLE=' + $Role + '"' + "`r`ncall run.cmd"
  $wrapperText = $wrapperText -replace '(?im)^call run\.cmd', $roleLine
}
Set-Content -LiteralPath $wrapper -Value $wrapperText -Encoding ASCII

$started = $false
if ($Start) {
  if (-not (Test-Path -LiteralPath $resolvedStartHelper -PathType Leaf)) {
    throw "GitHub runner start helper not found: $resolvedStartHelper"
  }
  try {
    $startPayload = & $resolvedStartHelper -InstallRoot $resolvedRoot -Json | ConvertFrom-Json
  } catch {
    throw "GitHub runner start helper failed: $($_.Exception.Message)"
  }
  if ($startPayload.schema_version -ne 'aistock_github_runner_start_v1') {
    throw 'GitHub runner start helper returned an invalid schema'
  }
  if ($startPayload.status -notin @('started', 'already_running')) {
    throw "GitHub runner start helper returned unexpected status: $($startPayload.status)"
  }
  $started = [bool]$startPayload.started
}

Write-Result @{
  schema_version = 'aistock_github_runner_configure_v1'
  status = $(if ($started) { 'configured_and_started' } else { 'configured' })
  configured = $true
  started = $started
  role = $Role
  labels = $Labels
  runner_name = $RunnerName
  runner_version = $RunnerVersion
  install_root = $resolvedRoot
  wrapper = $wrapper
  supervisor = $supervisor
  automatic_update_disabled = $automaticUpdateDisabled
}
