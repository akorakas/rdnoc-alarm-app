# GitHub → GitLab sync

GitHub is the source of truth: code is developed and tested there.
The tenant's GitLab can't reach GitHub, so each release travels as **one file**: a git bundle.

Synced into GitLab: `src/`, `pom.xml`, `tools/gitlab-sync/`. Everything else in GitLab (its `Dockerfile`, CI, …) is left alone.
GitLab gets **the same version tag** as GitHub (e.g. `v0.2.4`), so both sides show which code an image was built from.

---

## Every release

### A. On your machine (GitHub access), in the GitHub clone

1–3. **One command** releases the version and creates the bundle:
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\tools\gitlab-sync\publish-release.ps1 -Version v0.2.4 -Message "What changed"
   ```
   It does the following, and stops at the first problem:
   1. Checks that the version is `vX.Y.Z`, higher than the latest tag and not used yet, that you're on `main`, and that `main` isn't behind GitHub.
   2. Shows what will be committed and asks `y/N`.
   3. Runs `mvnw test`. Nothing is committed if the tests fail.
   4. Commits with your message and pushes `main`.
   5. Tags `v0.2.4` with the same message and pushes the tag.
   6. Creates the bundle on your Desktop as `rdnoc-alarm-app-v0.2.4-<commit>.bundle`.

   Options:
   - `-Paths src,pom.xml`: commit only these and leave the rest uncommitted.
   - `-SkipTests`: don't run the tests.
   - `-NoBundle`: don't create the bundle.
   - `-Yes`: don't ask for confirmation.

   Pushing `main` starts the GitHub Actions workflow (build, image, deploy of `main`).

   *Manual alternative:* commit and push yourself, then run `git tag -a v0.2.4 -m "…"` and `git push origin v0.2.4`, then `export-bundle.ps1`.
4. Copy the `.bundle` file to the VDI, into **Documents**.

### B. On the VDI, in the GitLab clone

5. Get the latest GitLab state first:
   ```powershell
   & "C:\Program Files\Git\bin\git.exe" pull
   ```
6. Import the bundle (replace the file name):
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\tools\gitlab-sync\import-bundle.ps1 -Bundle "$([Environment]::GetFolderPath('MyDocuments'))\rdnoc-alarm-app-v0.2.4-abc1234.bundle"
   ```
   The script:
   - checks the bundle;
   - makes `src`, `pom.xml` and `tools/gitlab-sync` identical to GitHub (added, changed **and deleted** files);
   - commits `Sync from GitHub v0.2.4 (<commit>)` and tags it `v0.2.4`;
   - checks that nothing differs from GitHub any more.

   It pushes nothing yet.
7. Look at the summary it printed, then push the commit and the tag:
   ```powershell
   & "C:\Program Files\Git\bin\git.exe" push origin HEAD
   & "C:\Program Files\Git\bin\git.exe" push origin v0.2.4
   ```
   You can instead add `-Push` to step 6, which runs these two pushes itself.
8. Build the image as usual and note which version it holds, e.g. image `1.0.4` = git `v0.2.4`.

Running step 6 again with the same bundle is harmless: it prints `Already in sync` and changes nothing.

---

## First time on the VDI

The scripts reach GitLab through the bundle itself, but the very first time they aren't there yet:

- Copy `tools\gitlab-sync\import-bundle.ps1` to the VDI by hand (e.g. into Documents), and run it from there with `-File <path>\import-bundle.ps1`.
- From then on it's part of GitLab (`tools\gitlab-sync\`) and is updated by every sync.

---

## Problems

| Message | Fix |
|---|---|
| `tag vX.Y.Z already exists` / `is not higher than the latest tag` | Pick the next version number. |
| `'main' is N commit(s) behind GitHub` | `git pull`, then run it again. |
| `git push origin main failed` | The commit is made locally. Fix the cause (credentials, network) and push yourself, then `git tag -a … ; git push origin <tag>` and `export-bundle.ps1`. |
| `running scripts is disabled on this system` | Use `powershell -ExecutionPolicy Bypass -File …` as above. |
| `detected dubious ownership` (repo on the network share) | Run the `git config --global --add safe.directory …` command that git prints, once. |
| `bundle not found` | Documents is on the network share: use the `GetFolderPath('MyDocuments')` form above, not `$HOME\Documents`. |
| `the working copy has uncommitted changes` | Commit or `git stash` local changes on the VDI first. |
| `GitLab already has tag vX.Y.Z; it was NOT moved` | That version was already used in GitLab. Pick the next number on GitHub, or delete the old GitLab tag if it was a mistake. |
| `the bundle is damaged` | It changed in transit (zipped, blocked). Copy it again; the size must match the original. |

## Why not just copy files?

Copying by hand is how GitLab ended up with an old `SyncMarkerFactory.java` next to a new `pom.xml`: one file missed, a broken build. The import makes the whole folder match GitHub, including deletions, and checks that nothing is left over.
