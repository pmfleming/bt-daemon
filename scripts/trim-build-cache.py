#!/usr/bin/env python3
"""Bound local Cargo caches without removing review worktrees or reports.

Identical copies live in the Cargo repositories in this workspace. The reference
implementation and packaged tests live in update-daemon/scripts/.
"""

import argparse
from contextlib import ExitStack
import fcntl
import glob
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

GIB = 1024**3


def log(message):
    print(f"build-cache: {message}", flush=True)


def disk_bytes(path, excluded=()):
    # Re-scan the retained tree in dry runs so hard-linked binaries are counted
    # once, rather than subtracting the same inode's size multiple times.
    result = subprocess.run(
        ["du", "-sk", *("--exclude=" + glob.escape(str(p)) for p in excluded),
         "--", str(path)], check=True, capture_output=True, text=True
    )
    return int(result.stdout.split()[0]) * 1024


def inside(path, root):
    return path != root and path.is_relative_to(root)


def safe_target(target, root):
    # Reject symlinked path components, including external/shared target dirs.
    if not inside(target, root) or target.resolve() != target:
        return False
    return not any((p / ".git").exists() for p in (target, *target.parents) if inside(p, root))


def protected(path, tracked):
    if path.is_symlink():
        return True
    if any(p == path or p.is_relative_to(path) for p in tracked):
        return True
    if path.is_dir():
        for _, directories, files in os.walk(path, followlinks=False):
            if ".git" in directories or ".git" in files or "Cargo.toml" in files:
                return True
    return False


def profiles(target):
    # Deliberately do not recurse into named review snapshots or custom targets.
    # Recognize ordinary and cross-compiled Cargo profiles, plus llvm-cov builds.
    bases = [target]
    coverage = target / "llvm-cov-target"
    if coverage.is_dir() and not coverage.is_symlink():
        bases.append(coverage)
    for base in list(bases):
        for child in base.iterdir():
            if (child.is_dir() and not child.is_symlink()
                    and re.fullmatch(
                        r"(?:x86_64|i[3-6]86|aarch64|arm\w*|thumb\w*|wasm\d+|"
                        r"riscv\w*|powerpc\w*|s390x|mips\w*|loongarch\w*)-[\w.-]+",
                        child.name,
                    )):
                bases.append(child)
    return sorted({
        base / name
        for base in bases
        for name in ("debug", "release")
        if (base / name / ".cargo-lock").is_file()
        and not (base / name).is_symlink()
        and not (base / name / ".cargo-lock").is_symlink()
        and ((base / name / ".fingerprint").is_dir()
             or (base / name / "deps").is_dir())
    })


def trim_target(root, target, limit, dry_run, tracked):
    if not safe_target(target, root):
        log(f"refusing external or symlinked target: {target}")
        return
    if not target.is_dir():
        return
    size = disk_bytes(target)
    if size <= limit:
        return
    log(f"{target}: {size / GIB:.2f} GiB exceeds {limit / GIB:g} GiB")
    candidates = profiles(target)
    with ExitStack() as locks:
        # Cargo uses flock on these files on Linux. Preserve both the files and
        # profile directories so a new build cannot lock a replacement inode.
        for profile in candidates:
            if protected(profile, tracked):
                log(f"preserving source/tracked content: {profile}")
                continue
            try:
                handle = locks.enter_context((profile / ".cargo-lock").open("rb"))
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                log(f"active build; skipping target: {target}")
                return
        candidates = [p for p in candidates if not protected(p, tracked)]
        # Incremental caches first, coverage artifacts second, then normal
        # debug/release artifacts. No cargo clean: it would erase review data.
        batches = [[p / "incremental"] for p in candidates]
        for coverage_pass in (True, False):
            for profile in candidates:
                if ("llvm-cov-target" in profile.relative_to(target).parts) == coverage_pass:
                    batches.append([
                        child for child in sorted(profile.iterdir())
                        if child.name not in (".cargo-lock", "incremental")
                    ])
        remaining = size
        excluded = []
        for batch in batches:
            if remaining <= limit:
                break
            for path in batch:
                if not path.exists() or protected(path, tracked):
                    continue
                if dry_run:
                    log(f"would remove {path}")
                    excluded.append(path)
                else:
                    log(f"removing {path}")
                    if path.is_dir():
                        shutil.rmtree(path)
                    else:
                        path.unlink()
            remaining = disk_bytes(target, excluded)
        qualifier = "estimated after cleanup" if dry_run else "remaining"
        log(f"{target}: {remaining / GIB:.2f} GiB {qualifier}")
        if remaining > limit:
            log("limit still exceeded; preserved reports, snapshots, symlinks, "
                "and unrecognized content need manual review")


def git_files(root):
    # Do not let repository-local fsmonitor/hook configuration execute checkout
    # code when this policy is invoked by the packaged maintenance service.
    git = ["git", "-c", "core.fsmonitor=false", "-c", "core.hooksPath=/dev/null",
           "-C", str(root)]
    top = subprocess.check_output(git + ["rev-parse", "--show-toplevel"], text=True).strip()
    if Path(top).resolve() != root:
        raise ValueError("expected repository root does not match Git worktree root")
    data = subprocess.check_output(git + ["ls-files", "-z"])
    return {root / os.fsdecode(name) for name in data.split(b"\0") if name}


def target_paths(root, tracked):
    override = os.environ.get("CARGO_TARGET_DIR")
    if override:
        # Relative overrides are anchored to this repository, not the caller's cwd.
        return [Path(os.path.abspath(root / override))]
    targets = {root / "target"}
    targets.update(p.parent / "target" for p in tracked if p.name == "Cargo.toml")
    # Do not descend into a target already covered by the outer scan.
    return [p for p in sorted(targets) if not any(inside(p, q) for q in targets)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report only; change no files")
    parser.add_argument("--max-gib", default=os.environ.get(
        "CARGO_TARGET_MAX_GIB", os.environ.get("SCRATCHPAD_TARGET_MAX_GIB", "4")
    ), help="limit per target directory (default: 4; 0 disables cleanup)")
    args = parser.parse_args()
    if not str(args.max_gib).isascii() or not str(args.max_gib).isdigit():
        parser.error("cache limit must be a non-negative integer")
    limit = int(args.max_gib) * GIB
    if not limit:
        return
    root = Path(__file__).resolve().parent.parent
    tracked = git_files(root)
    for target in target_paths(root, tracked):
        trim_target(root, target, limit, args.dry_run, tracked)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        log(f"cleanup stopped: {error}")
        sys.exit(1)
