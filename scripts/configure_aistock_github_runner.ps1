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
  [string[]]$Labels = @('aistock', 'aistock-ci-security'),
  [string]$StartHelperPath,
  [switch]$Apply,
  [switch]$Start,
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

if ($Role -notin @('general', 'security')) {
  throw "Runner role must be general or security: $Role"
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
$requiredRoleLabel = $(if ($Role -eq 'general') { 'aistock-ci' } else { 'aistock-ci-security' })
if ($Labels -notcontains $requiredRoleLabel) {
  throw "Runner labels for role $Role must include $requiredRoleLabel"
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

$resolvedRoot = Resolve-BoundedPath -Path $InstallRoot -Boundary $AllowedRoot
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
