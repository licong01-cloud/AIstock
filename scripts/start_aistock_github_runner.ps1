param(
  [string]$InstallRoot = $(if ($env:AISTOCK_GITHUB_RUNNER_ROOT) { $env:AISTOCK_GITHUB_RUNNER_ROOT } else { 'F:\Dev\github-actions-runner\aistock' }),
  [string]$WrapperName = 'run-aistock-runner-hidden.cmd',
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

if (-not (Test-Path -LiteralPath $resolvedRoot -PathType Container)) {
  throw "GitHub runner install root not found: $resolvedRoot"
}
if (-not (Test-Path -LiteralPath $wrapper -PathType Leaf)) {
  throw "GitHub runner wrapper not found: $wrapper"
}

$existing = @(Get-RunnerProcess -Root $resolvedRoot)
if ($existing.Count -gt 0) {
  Write-Result @{
    schema_version = 'aistock_github_runner_start_v1'
    status = 'already_running'
    started = $false
    install_root = $resolvedRoot
    wrapper = $wrapper
    runner_process_count = $existing.Count
  }
  exit 0
}

if ($DryRun) {
  Write-Result @{
    schema_version = 'aistock_github_runner_start_v1'
    status = 'would_start'
    started = $false
    install_root = $resolvedRoot
    wrapper = $wrapper
    runner_process_count = 0
  }
  exit 0
}

Start-Process `
  -FilePath 'cmd.exe' `
  -ArgumentList @('/d', '/c', ('"' + $wrapper + '"')) `
  -WindowStyle Hidden `
  -WorkingDirectory $resolvedRoot

Write-Result @{
  schema_version = 'aistock_github_runner_start_v1'
  status = 'started'
  started = $true
  install_root = $resolvedRoot
  wrapper = $wrapper
  runner_process_count = 0
}
