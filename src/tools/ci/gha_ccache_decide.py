#!/usr/bin/env python3

import os
import re
import shutil
import subprocess

def run(cmd, check=True):
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    ).stdout

def parse_ccache_stats():
    out = run(["ccache", "--print-stats"])
    hits = 0
    misses = 0

    for line in out.splitlines():
        line = line.strip()
        m = re.match(r"^local_storage_hit\s+(\d+)$", line)
        if m:
            hits = int(m.group(1))
            continue
        m = re.match(r"^local_storage_miss\s+(\d+)$", line)
        if m:
            misses = int(m.group(1))
            continue

    return hits, misses

def append_github_output(key, value):
    output_path = os.environ["GITHUB_OUTPUT"]
    with open(output_path, "a", encoding="utf-8") as f:
        f.write(f"{key}={value}\n")

def main():
    on_default_branch = os.environ["ON_DEFAULT_BRANCH"] == "true"
    ccache_dir = os.environ["CCACHE_DIR"]

    # Decide the target hit percentage below which we decide to upload a new
    # cache. On non-default branches a few misses aren't that bad. But, as the
    # caches of the default branch are shared with all branches, it's worth
    # aiming for a higher ratio there.
    target_rate = 95 if on_default_branch else 80

    # Log ccache stats, useful for more in-depth understanding. The avoid it
    # swamping the output, collapse it in a group.
    print("::group::ccache_stats")
    print(run(["ccache", "-s", "-vv"]))
    print("::endgroup::")

    # compute cache hit ratio
    hits, misses = parse_ccache_stats()
    total = hits + misses
    hit_pct = int(( hits / total) * 100) if total > 0 else 100

    print(f"hits: {hits}, misses: {misses}, hit_pct: {hit_pct}, target rate: {target_rate}")

    # If there were either barely any misses, or the cache hit ratio was high,
    # there no point in generating a new cache entry. We have limited cache
    # space.
    should_save = misses > 10 and hit_pct < target_rate

    append_github_output("should_save", str(should_save).lower())

    if not should_save:
        print(f"hit rate {hit_pct} is above target of {target_rate}, skip creating new cache entry")
        return 0

    print(f"hit rate {hit_pct} is below target of {target_rate}, create new cache entry")

    # It's not worth persisting old cache entries (e.g. from before a
    # change to a central header, or from the default branch if this
    # branch differs a lot). Therefore evict ccache entries that are a
    # bit older. The cutoff here is fairly arbitrary, it could
    # probably be improved.
    print("::group::ccache_shrink")
    print(run(["ccache", "--evict-older-than", f"{45*60}s"]))
    print(run(["ccache", "-X", "10"]))

    # Don't store ccache stats , otherwise we'd need to reset the cache access
    # data after restoring the cache in the next run, to be able to get the
    # hit ratio of the CI run.
    print(run(["ccache", "-z"]))
    print("::endgroup::")

    # Before continuing, try to kill all ccache instances, otherwise
    # it's possible that on cancellations there is still running
    # ccaches that cause the upload to fail.
    if shutil.which("killall"):
        print(run(["killall", "ccache"], check=False))

    return 0

if __name__ == "__main__":
    exit(main())
