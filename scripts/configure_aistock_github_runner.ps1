param(
  [string]$InstallRoot = 'F:\Dev\github-actions-runner\aistock-security',
  [string]$AllowedRoot = 'F:\Dev\github-actions-runner',
  [string]$ArchivePath = 'F:\Dev\github-actions-runner\aistock\actions-runner-win-x64-2.334.0.zip',
  [string]$ArchiveSha256 = 'a0c896f3acf37841cc17f392a38111d39501e56f2990434567f027ee89cf8981',
  [string]$TemplateWrapper = 'F:\Dev\github-actions-runner\aistock\run-aistock-runner-hidden.cmd',
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

if ($Role -notmatch '^[a-z][a-z0-9-]*$') {
  throw "Invalid runner role: $Role"
}
if (-not $Labels -or $Labels.Count -eq 0) {
  throw 'At least one explicit runner label is required'
}
if ($Labels -notcontains "aistock-ci-$Role") {
  throw "Runner labels must include aistock-ci-$Role"
}

$resolvedRoot = Resolve-BoundedPath -Path $InstallRoot -Boundary $AllowedRoot
$resolvedArchive = [System.IO.Path]::GetFullPath($ArchivePath)
$resolvedTemplate = [System.IO.Path]::GetFullPath($TemplateWrapper)
$resolvedStartHelper = if ($StartHelperPath) {
  [System.IO.Path]::GetFullPath($StartHelperPath)
} else {
  Join-Path $PSScriptRoot 'start_aistock_github_runner.ps1'
}
$wrapper = Join-Path $resolvedRoot 'run-aistock-runner-hidden.cmd'
$runnerIdentity = Join-Path $resolvedRoot '.runner'
$config = Join-Path $resolvedRoot 'config.cmd'
$alreadyConfigured = Test-Path -LiteralPath $runnerIdentity -PathType Leaf

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
}

if (-not $Apply) {
  Write-Result @{
    schema_version = 'aistock_github_runner_configure_v1'
    status = $(if ($alreadyConfigured) { 'already_configured' } else { 'would_configure' })
    configured = $alreadyConfigured
    started = $false
    role = $Role
    labels = $Labels
    runner_name = $RunnerName
    install_root = $resolvedRoot
    archive_path = $resolvedArchive
    archive_sha256 = $observedArchiveSha256
  }
  exit 0
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
  & $config --unattended --url $RepositoryUrl --token $token --name $RunnerName --labels ($Labels -join ',') --work '_work'
  if ($LASTEXITCODE -ne 0) {
    throw "GitHub runner configuration failed with exit code $LASTEXITCODE"
  }
  $alreadyConfigured = Test-Path -LiteralPath $runnerIdentity -PathType Leaf
  if (-not $alreadyConfigured) {
    throw 'GitHub runner configuration completed without creating .runner identity'
  }
}

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
  install_root = $resolvedRoot
  wrapper = $wrapper
}
