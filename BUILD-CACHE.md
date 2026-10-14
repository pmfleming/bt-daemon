# Build-cache maintenance

The standalone `scripts/trim-build-cache.py` applies a **4 GiB Cargo budget per
local target directory**. The packaged service additionally handles recognized
non-Cargo outputs and selected shared development caches, described below.
The `update-daemon` companion service runs it at system shutdown/reboot for
repositories discovered beneath configured workspace roots. Newly created or
cloned repositories are included on the next run without a rebuild. There is no
timer or automatic trimming on development-shell entry. This is a soft budget,
not a filesystem quota.

```sh
python3 scripts/trim-build-cache.py --dry-run
python3 scripts/trim-build-cache.py
CARGO_TARGET_MAX_GIB=8 python3 scripts/trim-build-cache.py
CARGO_TARGET_MAX_GIB=0 python3 scripts/trim-build-cache.py  # disable this invocation
```

`--max-gib N` overrides `CARGO_TARGET_MAX_GIB` (non-negative integer; `0` disables).
The old `SCRATCHPAD_TARGET_MAX_GIB` setting is a fallback for compatibility.
Python 3.9+, Git, and GNU `du` are required; locking uses Linux Cargo's `flock`
protocol. Existing `trim-target.sh` entry points delegate to this policy.

## Cargo retention and safety

1. Remove incremental caches first.
2. If still over budget, clear recognized `llvm-cov-target` build profiles.
3. If still over budget, clear recognized normal debug/release build profiles.

Each profile's `.cargo-lock` and containing directory are retained. Cleanup
acquires Cargo's locks non-blockingly and skips the entire target if any eligible
profile is building. Run between builds; it is not a concurrent-build scheduler.
Dry runs acquire/check locks but do not create, delete, or modify files. They
re-measure the retained tree, accounting for hard-linked binaries; estimates can
still change if another process modifies the tree.

Only recognized Cargo profiles are eligible. Tracked files, profiles containing
Git repositories or Cargo manifests, review snapshots, analysis/benchmark
reports, Node dependencies, and unrecognized directories are preserved. Cleanup
never follows symlinks and never uses `cargo clean` or `git clean`. **The budget is
soft:** protected content can keep a target above the limit; the script reports
this instead of deleting it. Custom profiles and custom review build directories
need manual review. Release binaries can be removed; rebuild them before use.

The default scan includes `target/` and targets next to tracked Cargo manifests
(including nested crates). `CARGO_TARGET_DIR`, when set, selects a single target;
relative paths are anchored to this repository. External/shared target paths,
symlinked paths, and paths inside nested Git repositories are refused.

## Other build systems and shared caches

The packaged service (`build-cache-trim`, not the standalone Cargo script) adds:

- **Node/TypeScript:** ignored `dist/` next to tracked `package.json` files whose
  build script declares Vite, tsc, webpack, Rollup, or esbuild.
- **Gradle/Android:** ignored `build/` with generated-output markers and `.gradle/`
  next to tracked Gradle build files.
- **CMake/Meson:** ignored `build`, `builddir`, or `_build` with generated metadata
  identifying the matching tracked source directory.
- **Python:** ignored bytecode-only `__pycache__` next to tracked Python files and
  standard pytest/mypy/ruff cache directories carrying `CACHEDIR.TAG`.

These outputs are removed only when **every entry is at least 14 days old**, even
when small. There is no arbitrary `build`/`dist` directory deletion, and no project
build/clean commands are executed. Any tracked content, nested Git checkout,
source manifest, symlink, unreadable tree, or foreign-owned entry protects the
whole candidate. Fresh artifacts remain untouched.

With `sharedCaches.enable`, a **1 GiB soft budget per cache group** applies to:

- npm's `_cacache` and `_npx`;
- Bun/node-gyp caches;
- Gradle's download/build caches (not its wrapper toolchains);
- QML test bytecode and pip caches;
- Android Studio indexes/caches (not LocalHistory);
- compiled rqlens helper profiles, including older version directories.

Shared caches must also satisfy the 14-day age floor before cleanup. The rqlens
bundled source/manifests and all lock-file inodes are retained. Cache groups with
recent or active entries can remain over budget; the service logs this. Clearing
package download caches may require downloads on a subsequent build. Downloaded
package manifests inside these fixed cache roots are cache objects, not protected
repository manifests; installed dependency trees outside the allowlist remain
untouched.

All additional policies check same-user tool processes and skip an ecosystem
while its build/runtime tools are running. Unreadable process state fails closed.
Existing lock files are acquired non-blockingly with both `flock` and POSIX range
locks (used by Gradle), then preserved. The process check is conservative and
best-effort, not an atomic build scheduler: do not deliberately start builds
concurrently with a manual cleanup. Shutdown ordering stops user sessions before
cleanup. Directories containing symlinks are preserved, including some npx caches.

Installed `node_modules`, virtual environments, SDKs, Gradle wrapper distributions,
Playwright browsers, editor LocalHistory, Nix-store objects, and review reports
are never cleanup candidates. Unknown build systems remain untouched.

## Shutdown service

`update-daemon` exports `nixosModules.buildCache` and `lib.mkBuildCachePackage`.
The system service runs as the explicitly configured non-root account. It is
independent of the privileged Rust update worker and uses immutable packaged
Python code, never scripts from writable checkouts. The host enables it in
`/etc/nixos/configuration.nix`, discovering repositories under `~/Projects` and
`~/Projects-p2`. Repositories do not need their own cleanup script.

```nix
imports = [ inputs.update-daemon.nixosModules.buildCache ];
services.build-cache-trim = {
  enable = true;
  user = "me";
  workspaceRoots = [ "/home/me/Projects" ];
  # Optional additional checkouts outside these workspaces:
  repositories = [ ];
  maxGiB = 4; # Cargo budget; 0 disables all trimming
  retentionDays = 14;
  sharedCaches = {
    enable = true;
    maxGiB = 1;
  };
};
```

Discovery walks grouping subdirectories and recognizes both `.git` directories
and worktree `.git` files. It stops at each checkout boundary, excludes hidden
folders and `target`, `node_modules`, `build`, `dist`, `dist-ssr`, `out`, and
`output`, and never follows symlinks. Bare repositories and directories owned by
other users are not scanned. It does not follow registered worktrees elsewhere
on disk or descend into nested submodules/vendor checkouts; add another workspace
root or an explicit repository path when those should be covered. Overlapping
roots and explicit checkouts are deduplicated. An empty workspace `.git` marker
does not hide its child repositories.

All discovered repositories are checked against the supported build-system
policies. The filesystem sandbox and mount dependencies cover the workspace roots, so new
repositories inside them are writable by the service without regenerating units.
Adding a new workspace root still requires a configuration rebuild.

After deploying the NixOS configuration:

```sh
build-cache-trim --dry-run          # run as the repository owner, not root
systemctl status build-cache-trim.service
journalctl -u build-cache-trim.service -b -1  # previous shutdown
```

The service arms at boot with `/bin/true` and `RemainAfterExit`; no process stays
resident. Its `ExecStop` performs cleanup during shutdown/reboot. Mount ordering
keeps repository filesystems available until the stop action completes. It does
not run on logout, suspend, or ordinary NixOS configuration switches. Explicitly
stopping or restarting the unit also invokes cleanup; use the CLI for manual
cleanup without disarming the shutdown hook.

Cleanup has a **60-second stop timeout**, after which the entire process group is
killed immediately rather than extending shutdown for another grace period.
Work is best-effort: large caches may need several shutdowns. Active Cargo builds
are skipped. Jobs use idle I/O, nice 19, a one-core CPU quota, and 512 MiB memory.
The sandbox is read-only outside configured workspaces/explicit repositories,
the fixed shared-cache allowlist (when enabled), and private temporary space.
No shutdown or cache deletion is required to validate the configuration.

Service runs ignore shell Cargo/Git overrides; configure their budget via the
module, not `CARGO_TARGET_MAX_GIB`. Missing roots/repositories are skipped; other errors
are logged, remaining repositories are checked, and the service exits nonzero.
Protected-content warnings do not fail the service. Set `maxGiB = 0` and redeploy
to disable trimming while leaving the hook armed.

## Maintaining the policy

The script is vendored identically into the 12 Cargo-build repositories so each
checkout works independently. The packaged reference and safety/scheduler tests
are in `update-daemon/scripts/`; propagate policy changes to the other copies.
`daemon-framework` also retains the mirrored policy safety suite.

```sh
python3 scripts/test_trim_build_cache.py -v  # in update-daemon
python3 scripts/test_build_cache_service.py -v
python3 scripts/test_ecosystem_cache.py -v
```

Tests operate only on temporary fixtures, including active-build locks, tracked
artifacts, dirty review checkouts, symlinks, dry runs, nested crates, and cleanup
ordering. The ecosystem-specific policies are packaged only in `update-daemon`;
the vendored standalone scripts remain Cargo-only. Nix-store garbage collection
and review-report retention remain separate concerns.
