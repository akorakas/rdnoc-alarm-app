<#
.SYNOPSIS
  Release a new version to GitHub: commit, push main, tag, push tag, and create the GitLab bundle.
  Run on your machine (GitHub access), inside the GitHub clone.

.DESCRIPTION
  1. Checks: -Version is vX.Y.Z, higher than the latest tag and not used yet (local or GitHub);
     you are on 'main' and 'main' is not behind GitHub.
  2. Stages the changes (all, or only -Paths), shows them and asks for confirmation.
  3. Runs the tests (mvnw test) - nothing is committed if they fail.
  4. Commits with -Message and pushes 'main'.
  5. Creates the annotated tag -Version (message = -Message) and pushes it.
  6. Creates the GitLab bundle (export-bundle.ps1).

  Stops at the first problem. Never force-pushes.
  Note: pushing 'main' and the tag starts the GitHub Actions workflow (build, GHCR image, deploy of main).

.EXAMPLE
  .\tools\gitlab-sync\publish-release.ps1 -Version v0.2.4 -Message "MV38 mapping + NSP active-site failover"
  .\tools\gitlab-sync\publish-release.ps1 -Version v0.2.4 -Message "Fix" -Paths src,pom.xml
  .\tools\gitlab-sync\publish-release.ps1 -Version v0.2.4 -Message "Fix" -SkipTests -NoBundle -Yes
#>
param(
  [Parameter(Mandatory = $true)][string]$Version,
  [Parameter(Mandatory = $true)][string]$Message,
  [string[]]$Paths,          # only stage these (default: all changes)
  [switch]$SkipTests,
  [switch]$NoBundle,
  [switch]$Yes,              # don't ask for confirmation
  [string]$BundleDir = [Environment]::GetFolderPath('Desktop')
)

function Fail($msg) { Write-Host "ERROR: $msg" -ForegroundColor Red; exit 1 }
function Step($msg) { Write-Host ""; Write-Host "== $msg" -ForegroundColor Cyan }

$repoRoot = git rev-parse --show-toplevel 2>$null
if ($LASTEXITCODE -ne 0) { Fail "run this inside the GitHub clone." }
Set-Location $repoRoot

# ── 1. Checks ────────────────────────────────────────────────────────────────
Step "Checks"

if ($Version -notmatch '^v(\d+)\.(\d+)\.(\d+)$') { Fail "-Version must look like v0.2.4 (got '$Version')." }
if ([string]::IsNullOrWhiteSpace($Message)) { Fail "-Message is empty." }
$newVer = [version]($Version.Substring(1))

$branch = git rev-parse --abbrev-ref HEAD
if ($branch -ne 'main') { Fail "you are on '$branch'. Switch to 'main' first." }

git fetch origin --tags --quiet
if ($LASTEXITCODE -ne 0) { Fail "git fetch origin failed (no GitHub access?)." }

if (git tag --list $Version) { Fail "tag $Version already exists. Use a higher version." }
if (git ls-remote --tags origin "refs/tags/$Version") { Fail "tag $Version already exists on GitHub." }

$latest = git tag --list 'v*' |
  Where-Object { $_ -match '^v\d+\.\d+\.\d+$' } |
  Sort-Object { [version]$_.Substring(1) } |
  Select-Object -Last 1
if ($latest -and $newVer -le [version]$latest.Substring(1)) {
  Fail "$Version is not higher than the latest tag $latest."
}

$behind = [int](git rev-list --count main..origin/main)
if ($behind -gt 0) { Fail "'main' is $behind commit(s) behind GitHub. Run 'git pull' first." }
$ahead = [int](git rev-list --count origin/main..main)

Write-Host "Version : $Version   (latest: $(if ($latest) { $latest } else { 'none' }))"
Write-Host "Message : $Message"
if ($ahead -gt 0) { Write-Host "Note: 'main' already has $ahead unpushed commit(s); they will be pushed too." -ForegroundColor Yellow }

# ── 2. Stage + confirm ───────────────────────────────────────────────────────
Step "Changes to commit"

if ($Paths) { git add -- @Paths } else { git add -A }
if ($LASTEXITCODE -ne 0) { Fail "git add failed." }

git diff --cached --quiet
$hasChanges = ($LASTEXITCODE -ne 0)

if ($hasChanges) {
  git diff --cached --stat
} elseif ($ahead -gt 0) {
  Write-Host "No new changes; releasing the $ahead unpushed commit(s)."
} else {
  Fail "nothing to release: no changes and no unpushed commits."
}

if ($Paths) {
  $left = git status --porcelain
  if ($left) { Write-Host "`nNot included (outside -Paths):" -ForegroundColor Yellow; $left | ForEach-Object { "  $_" } }
}

if (-not $Yes) {
  $answer = Read-Host "`nRelease $Version with these changes? (y/N)"
  if ($answer -notin @('y', 'Y', 'yes')) {
    if ($hasChanges) { git reset --quiet }
    Fail "cancelled. Nothing was committed or pushed (changes were unstaged)."
  }
}

# ── 3. Tests ─────────────────────────────────────────────────────────────────
if (-not $SkipTests) {
  Step "Tests (mvnw test)"
  & "$repoRoot\mvnw.cmd" -q -B test
  if ($LASTEXITCODE -ne 0) {
    if ($hasChanges) { git reset --quiet }
    Fail "tests failed. Nothing was committed or pushed (changes were unstaged)."
  }
  Write-Host "Tests passed." -ForegroundColor Green
}

# ── 4. Commit + push main ────────────────────────────────────────────────────
Step "Commit and push main"
if ($hasChanges) {
  git commit --quiet -m $Message
  if ($LASTEXITCODE -ne 0) { Fail "git commit failed." }
}
git push origin main
if ($LASTEXITCODE -ne 0) { Fail "git push origin main failed. The commit is local; fix the problem and push it yourself." }

# ── 5. Tag + push tag ────────────────────────────────────────────────────────
Step "Tag $Version"
git tag -a $Version -m $Message
if ($LASTEXITCODE -ne 0) { Fail "git tag failed." }
git push origin $Version
if ($LASTEXITCODE -ne 0) { Fail "git push of tag $Version failed. Retry: git push origin $Version" }

$short = git rev-parse --short HEAD
Write-Host "Released $Version = $short on GitHub." -ForegroundColor Green

# ── 6. Bundle for GitLab ─────────────────────────────────────────────────────
if (-not $NoBundle) {
  Step "GitLab bundle"
  & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot 'export-bundle.ps1') -OutDir $BundleDir
  if ($LASTEXITCODE -ne 0) { Fail "bundle creation failed. Run export-bundle.ps1 again later." }
}
