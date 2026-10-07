<#
.SYNOPSIS
  Step 2 of the GitHub -> GitLab sync. Run on the tenant VDI, inside the GitLab clone.

.DESCRIPTION
  Fetches GitHub 'main' from the bundle into the local branch 'github-main', makes the synced
  paths (default: src, pom.xml, tools/gitlab-sync) identical to it - including deleting files that GitHub removed -
  and commits + tags the result with the same version as GitHub (e.g. v0.2.4).

  Everything outside the synced paths (Dockerfile, .gitlab-ci.yml, ...) is left untouched.
  Nothing is pushed unless you pass -Push.

.EXAMPLE
  .\tools\gitlab-sync\import-bundle.ps1 -Bundle "$([Environment]::GetFolderPath('MyDocuments'))\rdnoc-alarm-app-v0.2.4-abc1234.bundle"
  .\tools\gitlab-sync\import-bundle.ps1 -Bundle <file> -Push
#>
param(
  [Parameter(Mandatory = $true)][string]$Bundle,
  [string[]]$Paths = @('src', 'pom.xml', 'tools/gitlab-sync'),
  [switch]$Push,
  [string]$GitExe = $(if (Test-Path 'C:\Program Files\Git\bin\git.exe') { 'C:\Program Files\Git\bin\git.exe' } else { 'git' })
)

function Fail($msg) { Write-Host "ERROR: $msg" -ForegroundColor Red; exit 1 }
function G { & $GitExe @args }

# ── Checks ────────────────────────────────────────────────────────────────────
$inRepo = G rev-parse --is-inside-work-tree 2>&1
if ($LASTEXITCODE -ne 0) {
  if ("$inRepo" -match 'dubious ownership') {
    Fail "git refuses this folder (network share). Run the 'git config --global --add safe.directory ...' command git printed, then retry."
  }
  Fail "run this inside the GitLab clone."
}

if (-not (Test-Path -LiteralPath $Bundle)) { Fail "bundle not found: $Bundle" }

if (G status --porcelain) { Fail "the working copy has uncommitted changes. Commit or stash them first." }

$branch = G rev-parse --abbrev-ref HEAD
if ($branch -ne 'main') {
  Write-Host "Note: you are on branch '$branch', not 'main'." -ForegroundColor Yellow
}

G bundle verify $Bundle 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { Fail "the bundle is damaged or incomplete (git bundle verify failed). Copy it again." }

# ── Which version is in the bundle? ───────────────────────────────────────────
$heads = G bundle list-heads $Bundle
$mainSha = ($heads | Where-Object { $_ -match ' refs/heads/main$' } | ForEach-Object { ($_ -split ' ')[0] }) | Select-Object -First 1
$tagLine = $heads | Where-Object { $_ -match ' refs/tags/(v[^ ^]+)$' } | Select-Object -First 1
if (-not $mainSha) { Fail "the bundle has no 'main' branch." }
if (-not $tagLine) { Fail "the bundle has no version tag. Re-create it with export-bundle.ps1." }
$tag = ($tagLine -split 'refs/tags/')[1]

# ── Fetch GitHub main into github-main ───────────────────────────────────────
# --no-tags: GitHub's own tags must not land in GitLab (they point at GitHub commits).
G fetch --quiet --no-tags $Bundle "+refs/heads/main:refs/heads/github-main" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { Fail "git fetch from the bundle failed." }
$short = (G rev-parse --short github-main)

# Keep only paths that exist on at least one side (git restore fails on unknown paths).
$Paths = @($Paths | Where-Object { (G ls-tree --name-only github-main -- $_) -or (G ls-files -- $_) })
if (-not $Paths) { Fail "none of the sync paths exist in GitHub main or in this repo." }

# ── Make the synced paths identical to GitHub (adds, changes AND deletions) ──
G restore --source=github-main --staged --worktree -- @Paths
if ($LASTEXITCODE -ne 0) { Fail "git restore failed." }

G diff --cached --quiet
if ($LASTEXITCODE -eq 0) {
  Write-Host "Already in sync with GitHub $tag ($short). Nothing to commit." -ForegroundColor Green
  exit 0
}

Write-Host "Changes coming from GitHub $tag ($short):" -ForegroundColor Cyan
G diff --cached --stat
Write-Host ""

G commit --quiet -m "Sync from GitHub $tag ($short)"
if ($LASTEXITCODE -ne 0) { Fail "git commit failed." }

# ── Tag with the same version as GitHub (never move an existing tag) ─────────
if (G tag --list $tag) {
  Write-Host "Note: GitLab already has tag $tag; it was NOT moved. Tag this commit manually if needed." -ForegroundColor Yellow
  $tagged = $false
} else {
  G tag -a $tag -m "GitHub $tag ($short)"
  $tagged = $true
}

# ── Check: synced paths now identical to GitHub ──────────────────────────────
$left = G diff --stat github-main -- @Paths
if ($left) { Fail "after the sync the paths still differ from GitHub:`n$left" }

Write-Host ""
Write-Host "Done: committed $(G rev-parse --short HEAD) = GitHub $tag ($short)$(if ($tagged) { ", tagged $tag" })." -ForegroundColor Green

if ($Push) {
  G push origin HEAD
  if ($tagged) { G push origin $tag }
} else {
  Write-Host "Review, then push:"
  Write-Host "  & `"$GitExe`" push origin HEAD"
  if ($tagged) { Write-Host "  & `"$GitExe`" push origin $tag" }
}
