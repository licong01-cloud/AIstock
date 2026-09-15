param(
  [string]$SourceRoot = $env:AISTOCK_SELF_HOSTED_SOURCE,
  [string]$MirrorRoot = $env:AISTOCK_GIT_OBJECT_MIRROR_ROOT,
  [string]$AllowedRoot = $env:AISTOCK_GITHUB_RUNNER_PREBUILT_ROOT,
  [string]$Repository = 'licong01-cloud/AIstock',
  [switch]$Apply,
  [switch]$Json
)

$ErrorActionPreference = 'Stop'

if (-not $SourceRoot -or -not $MirrorRoot -or -not $AllowedRoot) {
  throw 'SourceRoot, MirrorRoot, and AllowedRoot must be supplied explicitly or through controlled AISTOCK_* environment variables'
}

function Invoke-Git {
  param([string]$WorkingDirectory, [string[]]$Arguments)
  $previousErrorAction = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    $output = & git -C $WorkingDirectory @Arguments 2>&1
    $exitCode = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $previousErrorAction
  }
  if ($exitCode -ne 0) {
    throw "git $($Arguments -join ' ') failed in ${WorkingDirectory}: $($output -join [Environment]::NewLine)"
  }
  return ($output -join "`n").Trim()
}

function Resolve-BoundedPath {
  param([string]$Path, [string]$Boundary)
  $resolved = [System.IO.Path]::GetFullPath($Path).TrimEnd('\')
  $resolvedBoundary = [System.IO.Path]::GetFullPath($Boundary).TrimEnd('\')
  $prefix = $resolvedBoundary + '\'
  if (-not $resolved.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Git mirror must stay below ${resolvedBoundary}: $resolved"
  }
  if ($resolved.Equals($resolvedBoundary, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Git mirror cannot equal the allowed root: $resolved"
  }
  return $resolved
}

function Write-Result {
  param([hashtable]$Payload)
  if ($Json) {
    $Payload | ConvertTo-Json -Depth 5 -Compress
  } else {
    "status=$($Payload.status) main_sha=$($Payload.main_sha) mirror_root=$($Payload.mirror_root)"
  }
}

$resolvedSource = [System.IO.Path]::GetFullPath($SourceRoot).TrimEnd('\')
$resolvedMirror = Resolve-BoundedPath -Path $MirrorRoot -Boundary $AllowedRoot
$mirrorParent = Split-Path -Parent $resolvedMirror
$manifestPath = $resolvedMirror + '.aistock-mirror.json'
$sourceGitDir = Invoke-Git -WorkingDirectory $resolvedSource -Arguments @('rev-parse', '--git-dir')
$sourceOrigin = Invoke-Git -WorkingDirectory $resolvedSource -Arguments @('config', '--get', 'remote.origin.url')
$expectedOriginSuffix = "/$Repository"
$normalizedOrigin = $sourceOrigin.TrimEnd('/')
if ($normalizedOrigin.EndsWith('.git', [System.StringComparison]::OrdinalIgnoreCase)) {
  $normalizedOrigin = $normalizedOrigin.Substring(0, $normalizedOrigin.Length - 4)
}
if (-not $normalizedOrigin.EndsWith($expectedOriginSuffix, [System.StringComparison]::OrdinalIgnoreCase)) {
  throw "Source origin does not match ${Repository}: $sourceOrigin"
}
$mainSha = Invoke-Git -WorkingDirectory $resolvedSource -Arguments @('rev-parse', '--verify', 'refs/heads/main^{commit}')
$originMainSha = Invoke-Git -WorkingDirectory $resolvedSource -Arguments @('rev-parse', '--verify', 'refs/remotes/origin/main^{commit}')
if ($mainSha -ne $originMainSha) {
  throw "Source main is not aligned with origin/main: main=$mainSha origin/main=$originMainSha"
}
$sourceStatus = Invoke-Git -WorkingDirectory $resolvedSource -Arguments @('status', '--short')
if ($sourceStatus) {
  throw "Source root must be clean before mirror maintenance: $resolvedSource"
}

if (-not $Apply) {
  Write-Result @{
    schema_version = 'aistock_git_object_mirror_maintenance_v1'
    status = $(if (Test-Path -LiteralPath $resolvedMirror -PathType Container) { 'would_update' } else { 'would_create' })
    source_root = $resolvedSource
    source_git_dir = $sourceGitDir
    repository = $Repository
    main_sha = $mainSha
    mirror_root = $resolvedMirror
    object_directory = Join-Path $resolvedMirror 'objects'
    manifest_path = $manifestPath
    network_accessed = $false
    process_control_performed = $false
  }
  exit 0
}

$sha256 = [System.Security.Cryptography.SHA256]::Create()
try {
  $mutexHash = [System.BitConverter]::ToString(
    $sha256.ComputeHash([Text.Encoding]::UTF8.GetBytes($resolvedMirror))
  ).Replace('-', '').Substring(0, 16)
} finally {
  $sha256.Dispose()
}
$mutexName = 'Global\AIstockGitObjectMirror-' + $mutexHash
$mutex = [Threading.Mutex]::new($false, $mutexName)
$lockAcquired = $false
try {
  $lockAcquired = $mutex.WaitOne([TimeSpan]::FromSeconds(30))
  if (-not $lockAcquired) {
    throw "Timed out waiting for Git mirror maintenance lock: $resolvedMirror"
  }
  if (-not (Test-Path -LiteralPath $mirrorParent -PathType Container)) {
    New-Item -ItemType Directory -Path $mirrorParent | Out-Null
  }
  if (-not (Test-Path -LiteralPath $resolvedMirror -PathType Container)) {
    & git init --quiet --bare $resolvedMirror 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
      throw "git init --bare failed: $resolvedMirror"
    }
  }
  $isBare = Invoke-Git -WorkingDirectory $resolvedMirror -Arguments @('rev-parse', '--is-bare-repository')
  if ($isBare -ne 'true') {
    throw "Git object mirror is not bare: $resolvedMirror"
  }
  $registeredRepository = (& git -C $resolvedMirror config --get aistock.repository 2>$null)
  if ($LASTEXITCODE -eq 0 -and $registeredRepository -and $registeredRepository.Trim() -ne $Repository) {
    throw "Git object mirror repository mismatch: $($registeredRepository.Trim())"
  }
  Invoke-Git -WorkingDirectory $resolvedMirror -Arguments @('config', 'aistock.repository', $Repository) | Out-Null
  Invoke-Git -WorkingDirectory $resolvedMirror -Arguments @(
    'fetch', '--quiet', '--force', '--no-tags', '--no-write-fetch-head', $resolvedSource,
    'refs/heads/main:refs/heads/main'
  ) | Out-Null
  $mirrorMainSha = Invoke-Git -WorkingDirectory $resolvedMirror -Arguments @('rev-parse', '--verify', 'refs/heads/main^{commit}')
  if ($mirrorMainSha -ne $mainSha) {
    throw "Git object mirror main mismatch: expected=$mainSha observed=$mirrorMainSha"
  }
  Invoke-Git -WorkingDirectory $resolvedMirror -Arguments @('fsck', '--connectivity-only', '--no-dangling') | Out-Null
  $manifest = [ordered]@{
    schema_version = 'aistock_git_object_mirror_v1'
    repository = $Repository
    main_sha = $mirrorMainSha
    mirror_root = $resolvedMirror
    object_directory = Join-Path $resolvedMirror 'objects'
    source_root = $resolvedSource
    updated_at = [DateTimeOffset]::Now.ToString('o')
    network_accessed = $false
  }
  $manifestTemp = $manifestPath + '.tmp'
  $manifest | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $manifestTemp -Encoding UTF8
  Move-Item -LiteralPath $manifestTemp -Destination $manifestPath -Force
  Write-Result @{
    schema_version = 'aistock_git_object_mirror_maintenance_v1'
    status = 'ready'
    source_root = $resolvedSource
    repository = $Repository
    main_sha = $mirrorMainSha
    mirror_root = $resolvedMirror
    object_directory = Join-Path $resolvedMirror 'objects'
    manifest_path = $manifestPath
    network_accessed = $false
    process_control_performed = $false
  }
} finally {
  if ($lockAcquired) {
    $mutex.ReleaseMutex()
  }
  $mutex.Dispose()
}
