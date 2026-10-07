<#
.SYNOPSIS
  Step 1 of the GitHub -> GitLab sync. Run on a machine WITH GitHub access, inside the GitHub clone.

.DESCRIPTION
  Creates a git bundle (one file) with GitHub 'main' and its release tag, ready to copy to the
  tenant VDI. Checks first that local 'main' equals GitHub 'main' and that it carries a tag,
  so the bundle is exactly what was tested and released.

  Uncommitted local changes are NOT in the bundle (a bundle only holds commits).

.EXAMPLE
  .\tools\gitlab-sync\export-bundle.ps1
  .\tools\gitlab-sync\export-bundle.ps1 -OutDir C:\Temp
#>
param(
  [string]$OutDir = [Environment]::GetFolderPath('Desktop')
)

function Fail($msg) { Write-Host "ERROR: $msg" -ForegroundColor Red; exit 1 }

git fetch origin --tags --quiet
if ($LASTEXITCODE -ne 0) { Fail "git fetch origin failed (no GitHub access?)." }

$local  = git rev-parse main
$remote = git rev-parse origin/main
if ($local -ne $remote) {
  Fail "local 'main' ($($local.Substring(0,7))) is not equal to GitHub 'main' ($($remote.Substring(0,7))). Push or pull first."
}

$tag = git tag --points-at main --list 'v*' | Sort-Object { [version]($_ -replace '^v','' -replace '-.*$','') } | Select-Object -Last 1
if (-not $tag) {
  Fail "'main' has no version tag. Tag and push it first, e.g.:`n  git tag -a v0.2.4 -m ""<what changed>""`n  git push origin v0.2.4"
}

if (git status --porcelain) {
  Write-Host "Note: you have uncommitted local changes. They are NOT included in the bundle." -ForegroundColor Yellow
}

$short = $local.Substring(0,7)
$file  = Join-Path $OutDir "rdnoc-alarm-app-$tag-$short.bundle"

git bundle create $file main "refs/tags/$tag" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { Fail "git bundle create failed." }

git bundle verify $file 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { Fail "git bundle verify failed for $file." }

$size = [math]::Round((Get-Item $file).Length / 1KB)
Write-Host ""
Write-Host "Bundle ready: $file  ($size KB)" -ForegroundColor Green
Write-Host "  version : $tag"
Write-Host "  commit  : $short  $(git log -1 --format=%s main)"
Write-Host ""
Write-Host "Next: copy it to the VDI and run tools\gitlab-sync\import-bundle.ps1 there."
