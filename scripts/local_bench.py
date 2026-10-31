#!/usr/bin/env python3
"""Run DataFusion benchmarks locally, comparing a baseline against a changed
checkout, and produce a report shaped like the one the GKE runner posts on PRs
(see controller/src/runner/bench_datafusion.rs).

Pipeline (mirrors the runner):

    local datafusion repo
      ├── worktree  <workdir>/baseline   (merge-base | --baseline-ref | --baseline-branch)
      ├── worktree  <workdir>/changed    (HEAD | --changed-ref | --changed-branch | --changed-in-place)
      └── worktree  <workdir>/harness    (bench.sh + compare.py from <upstream>/main)
                     benchmarks/queries ──symlink──► each side's own queries while it runs

For each requested benchmark:
  * a Criterion `[[bench]]` target  → `cargo bench --bench <name> -- --save-baseline <side>`
  * tpch / tpch10 / tpch_mem / tpch_mem10 → prebuilt `dfbench tpch` directly
  * anything else                    → `bench.sh run <name> [query]`
Then `bench.sh compare` (min), `bench.sh compare_detail` (mean ±stddev) and
`critcmp` are run over whatever artifacts each side produced.

The benchmark command can be given in any of these forms:

    local_bench.py tpch_mem clickbench_partitioned        # benchmark names
    local_bench.py run benchmarks tpch_mem                # trigger-comment form
    local_bench.py ./bench.sh run tpch_mem 5              # bench.sh form (query 5)
    local_bench.py cargo bench --bench sql_planner        # raw command (auto-detected)
    local_bench.py --raw -- my_script.sh {results_name}   # raw command (forced)

Raw commands run in each side's checkout with SIDE, RESULTS_NAME, DATAFUSION_DIR,
DATA_DIR, RESULTS_DIR and CARGO_TARGET_DIR set; `{results_name}`, `{side_dir}`,
`{results_dir}` and `{data_dir}` placeholders are substituted. A `cargo bench`
command without `--save-baseline` gets `--save-baseline <side>` appended.

Options must come before the benchmark command. Only the Python standard
library is needed. bench.sh's compare step needs `uv`,
Criterion comparison needs `critcmp` (`cargo install critcmp`).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path

BASE_LABEL = "baseline"
CHANGED_LABEL = "changed"
WORKLOAD_DIRS = ("benchmarks/queries", "benchmarks/sql_benchmarks")
TPCH_DIRECT = {
    # name: (scale factor, in memory)
    "tpch": ("1", False),
    "tpch10": ("10", False),
    "tpch_mem": ("1", True),
    "tpch_mem10": ("10", True),
}
# Datasets a Criterion bench target needs before it can run.
CRITERION_DATASETS = {"sql_planner": ["clickbench_partitioned"]}
# Build settings the GKE runner forces (controller/src/runner/build_env.rs).
THIN_LTO_ENV = {"CARGO_PROFILE_RELEASE_LTO": "thin", "CARGO_PROFILE_BENCH_LTO": "thin"}

# Runs a command and records its resource usage. ru_maxrss of RUSAGE_CHILDREN
# covers every descendant reaped below this fresh process, so it is per-run.
RUSAGE_WRAPPER = r"""
import json, resource, subprocess, sys, time
out, cmd = sys.argv[1], sys.argv[2:]
t0 = time.time()
rc = subprocess.call(cmd)
ru = resource.getrusage(resource.RUSAGE_CHILDREN)
scale = 1 if sys.platform == "darwin" else 1024
json.dump({"rc": rc, "wall": time.time() - t0, "user": ru.ru_utime,
           "sys": ru.ru_stime, "maxrss": ru.ru_maxrss * scale}, open(out, "w"))
sys.exit(rc)
"""


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def log(msg: str) -> None:
    print(f"\033[1;34m==>\033[0m {msg}", file=sys.stderr, flush=True)


def warn(msg: str) -> None:
    print(f"\033[1;33mwarning:\033[0m {msg}", file=sys.stderr, flush=True)


class CommandError(RuntimeError):
    pass


DRY_RUN = False


def show(cmd: list[str], cwd: Path, env_overrides: dict[str, str] | None = None) -> str:
    env_s = " ".join(f"{k}={shlex.quote(v)}" for k, v in (env_overrides or {}).items())
    return f"(cd {shlex.quote(str(cwd))} && {env_s + ' ' if env_s else ''}{shlex.join(cmd)})"


def run(
    cmd: list[str],
    cwd: Path,
    env: dict[str, str] | None = None,
    check: bool = True,
    capture: bool = True,
) -> str:
    """Run a short command, returning stdout."""
    proc = subprocess.run(
        cmd,
        cwd=cwd,
        env={**os.environ, **(env or {})},
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    if check and proc.returncode != 0:
        raise CommandError(
            f"{show(cmd, cwd)} failed ({proc.returncode})\n{proc.stderr or ''}".rstrip()
        )
    return proc.stdout or ""


def git(repo: Path, *args: str, check: bool = True) -> str:
    return run(["git", *args], repo, check=check).strip()


@dataclass
class RunStats:
    wall: float = 0.0
    user: float = 0.0
    sys: float = 0.0
    maxrss: int = 0
    rc: int = 0


def run_logged(
    cmd: list[str],
    cwd: Path,
    env_overrides: dict[str, str],
    log_path: Path,
    quiet: bool,
) -> RunStats:
    """Run a long command, teeing output to `log_path`, and measure it."""
    log(show(cmd, cwd, env_overrides))
    if DRY_RUN:
        return RunStats()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    stats_path = log_path.with_suffix(".rusage.json")
    full = [sys.executable, "-c", RUSAGE_WRAPPER, str(stats_path), *cmd]
    with open(log_path, "w") as lf:
        lf.write(show(cmd, cwd, env_overrides) + "\n")
        proc = subprocess.Popen(
            full,
            cwd=cwd,
            env={**os.environ, **env_overrides},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            lf.write(line)
            if not quiet:
                sys.stdout.write(line)
        proc.wait()
    try:
        data = json.loads(stats_path.read_text())
        stats = RunStats(**data)
    except (OSError, ValueError, TypeError):
        stats = RunStats(rc=proc.returncode)
    if proc.returncode != 0:
        raise CommandError(f"exit code {proc.returncode}; log: {log_path}")
    return stats


def sanitize(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name)


def human_bytes(n: float | None) -> str:
    if n is None:
        return "-"
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return str(n)


def human_secs(s: float) -> str:
    m, sec = divmod(int(round(s)), 60)
    return f"{m}m {sec:02d}s" if m else f"{s:.1f}s"


# ---------------------------------------------------------------------------
# Command parsing
# ---------------------------------------------------------------------------


@dataclass
class Plan:
    benches: list[str] = field(default_factory=list)
    query: str | None = None
    raw: str | None = None  # raw shell command (raw mode)


RAW_PROGRAMS = {"cargo", "env", "dfbench", "bash", "sh", "python", "python3", "uv"}


def parse_command(tokens: list[str], force_raw: bool, query: str | None) -> Plan:
    if tokens and tokens[0] == "--":
        tokens = tokens[1:]
    if not tokens:
        raise SystemExit("error: no benchmark command given")
    if force_raw:
        return Plan(raw=shlex.join(tokens) if len(tokens) > 1 else tokens[0])

    # A single quoted argument such as "run benchmarks tpch_mem" or a raw command.
    if len(tokens) == 1 and " " in tokens[0]:
        tokens = shlex.split(tokens[0])

    # Trigger-comment form: `/run benchmarks a b` or `run benchmark a`.
    if (
        len(tokens) >= 2
        and tokens[0].lstrip("/") == "run"
        and tokens[1]
        in (
            "benchmark",
            "benchmarks",
        )
    ):
        return Plan(benches=tokens[2:], query=query)

    # bench.sh form: `./bench.sh run <name> [query]`.
    if Path(tokens[0]).name == "bench.sh":
        if len(tokens) < 3 or tokens[1] != "run":
            raise SystemExit(
                "error: only `bench.sh run <benchmark> [query]` is supported"
            )
        return Plan(benches=[tokens[2]], query=tokens[3] if len(tokens) > 3 else query)

    first = tokens[0]
    if first in RAW_PROGRAMS or "/" in first or "=" in first:
        return Plan(raw=shlex.join(tokens))
    return Plan(benches=tokens, query=query)


# ---------------------------------------------------------------------------
# Checkouts
# ---------------------------------------------------------------------------


def detect_upstream_remote(repo: Path) -> str:
    remotes = git(repo, "remote").split()
    for r in remotes:
        url = git(repo, "remote", "get-url", r, check=False)
        if re.search(r"apache/(arrow-)?datafusion(\.git)?$", url):
            return r
    return "origin" if "origin" in remotes else remotes[0]


def resolve(repo: Path, ref: str) -> str:
    try:
        return git(repo, "rev-parse", "--verify", f"{ref}^{{commit}}")
    except CommandError:
        raise SystemExit(f"error: cannot resolve git ref {ref!r} in {repo}")


def resolve_branch(
    repo: Path, remote: str, branch: str, fetch: bool
) -> tuple[str, str]:
    """Resolve a branch name, preferring <remote>/<branch> (fetched) over a local one."""
    if fetch:
        log(f"Fetching {remote} {branch}")
        if not DRY_RUN:
            run(["git", "fetch", remote, branch], repo, check=False)
    for candidate in (f"{remote}/{branch}", branch):
        sha = git(
            repo, "rev-parse", "--verify", "-q", f"{candidate}^{{commit}}", check=False
        )
        if sha:
            return candidate, sha
    raise SystemExit(f"error: branch {branch!r} not found locally or on {remote}")


def prepare_worktree(repo: Path, path: Path, sha: str) -> None:
    """Create (or reuse) a detached worktree at `path` checked out at `sha`."""
    if (path / ".git").exists():
        # Undo the harness's queries symlink before checking out over it.
        q = path / "benchmarks" / "queries"
        if q.is_symlink():
            q.unlink()
        shutil.rmtree(path / "benchmarks" / "queries.main", ignore_errors=True)
        log(f"Updating worktree {path} -> {sha[:10]}")
        if not DRY_RUN:
            git(
                path,
                "-c",
                "advice.detachedHead=false",
                "checkout",
                "--force",
                "--detach",
                sha,
            )
    else:
        log(f"Creating worktree {path} at {sha[:10]}")
        if not DRY_RUN:
            path.parent.mkdir(parents=True, exist_ok=True)
            git(repo, "worktree", "prune")
            git(repo, "worktree", "add", "--force", "--detach", str(path), sha)


@dataclass
class Side:
    name: str  # BASE_LABEL / CHANGED_LABEL
    label: str  # human description, e.g. "abc1234 (merge-base)"
    dir: Path
    sha: str
    target_dir: Path
    env: dict[str, str] = field(default_factory=dict)
    stats: list[tuple[str, RunStats]] = field(default_factory=list)
    failures: list[tuple[str, str]] = field(default_factory=list)
    criterion_ran: bool = False


# ---------------------------------------------------------------------------
# Benchmark execution
# ---------------------------------------------------------------------------


def criterion_targets(benchmarks_dir: Path) -> dict[str, list[str]]:
    try:
        out = run(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"], benchmarks_dir
        )
        meta = json.loads(out)
    except (CommandError, OSError, ValueError) as e:
        warn(f"cargo metadata failed; treating all names as bench.sh suites: {e}")
        return {}
    targets: dict[str, list[str]] = {}
    for pkg in meta.get("packages", []):
        for t in pkg.get("targets", []):
            if "bench" not in t.get("kind", []):
                continue
            feats = targets.setdefault(t["name"], [])
            for f in t.get("required-features", []) or []:
                if f not in feats:
                    feats.append(f)
    return targets


def clear_criterion_baseline(target_dir: Path, label: str) -> None:
    """Remove stale `<target>/criterion/**/<label>` dirs from earlier runs."""
    root = target_dir / "criterion"
    if not root.is_dir() or DRY_RUN:
        return
    for p in sorted(root.rglob(label), key=lambda p: -len(p.parts)):
        if p.is_dir() and p.name == label:
            shutil.rmtree(p, ignore_errors=True)


def has_criterion_baseline(target_dir: Path, label: str) -> bool:
    root = target_dir / "criterion"
    return root.is_dir() and any(p.is_dir() for p in root.rglob(label))


def point_queries(harness_bench: Path, target: Path) -> None:
    link = harness_bench / "queries"
    if DRY_RUN:
        return
    if link.is_symlink():
        link.unlink()
    elif link.exists():
        raise CommandError(f"{link} is not a symlink")
    link.symlink_to(target)


def prepare_harness_queries(harness_bench: Path) -> Path:
    q, main = harness_bench / "queries", harness_bench / "queries.main"
    if not DRY_RUN and q.is_dir() and not q.is_symlink():
        shutil.rmtree(main, ignore_errors=True)
        q.rename(main)
    point_queries(harness_bench, main)
    return main


def side_queries(side: Side, harness_main_queries: Path) -> Path:
    q = side.dir / "benchmarks" / "queries"
    return q if q.is_dir() else harness_main_queries


def results_dir(harness_bench: Path, side: Side) -> Path:
    return harness_bench / "results" / side.name


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def results_json_files(d: Path) -> dict[str, Path]:
    return {p.name: p for p in sorted(d.glob("*.json"))} if d.is_dir() else {}


def query_ids(path: Path) -> list[str]:
    try:
        data = json.loads(path.read_text())
        return [str(q.get("query")) for q in data.get("queries", [])]
    except (OSError, ValueError, AttributeError):
        return []


def pool_peaks(path: Path) -> dict[str, int]:
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    peaks = {}
    for q in data.get("queries", []) if isinstance(data, dict) else []:
        if isinstance(q, dict) and isinstance(q.get("pool_peak_bytes"), (int, float)):
            peaks[str(q.get("query"))] = int(q["pool_peak_bytes"])
    return peaks


def coverage_section(base_dir: Path, changed_dir: Path) -> list[str]:
    lines = []
    base, changed = results_json_files(base_dir), results_json_files(changed_dir)
    for name in sorted(set(base) | set(changed)):
        if name not in base:
            lines.append(f"- `{name}`: changed side only (no baseline results)")
            continue
        if name not in changed:
            lines.append(f"- `{name}`: baseline side only")
            continue
        b, c = query_ids(base[name]), query_ids(changed[name])
        only_b = [q for q in b if q not in c]
        only_c = [q for q in c if q not in b]
        if only_b:
            lines.append(f"- `{name}`: queries only on baseline: {', '.join(only_b)}")
        if only_c:
            lines.append(f"- `{name}`: queries only on changed: {', '.join(only_c)}")
        if b != c and not only_b and not only_c:
            lines.append(
                f"- `{name}`: query order differs; positional pairing is misaligned"
            )
        elif (only_b or only_c) and b[: min(len(b), len(c))] != c[
            : min(len(b), len(c))
        ]:
            lines.append(
                f"- `{name}`: compare.py pairs queries by position — rows past the first "
                "difference are mispaired"
            )
    return lines


def workload_diff(repo: Path, base: Side, changed: Side, in_place: bool) -> list[str]:
    args = ["diff", "--name-status", base.sha]
    if not in_place:
        args.append(changed.sha)
    out = git(
        repo if in_place else changed.dir, *args, "--", *WORKLOAD_DIRS, check=False
    )
    rows = []
    for line in filter(None, out.splitlines()):
        status, *paths = line.split("\t")
        rows.append(f"- `{status}` {' → '.join(paths)}")
    return rows


def pool_section(harness_bench: Path, base: Side, changed: Side) -> str:
    bfiles = results_json_files(results_dir(harness_bench, base))
    cfiles = results_json_files(results_dir(harness_bench, changed))
    rows = []
    for name in sorted(set(bfiles) | set(cfiles)):
        bp = pool_peaks(bfiles[name]) if name in bfiles else {}
        cp = pool_peaks(cfiles[name]) if name in cfiles else {}
        for q in sorted(set(bp) | set(cp), key=lambda s: (len(s), s)):
            b, c = bp.get(q), cp.get(q)
            change = ""
            if b and c is not None:
                change = f"{(c - b) / b * 100:+.1f}%"
            rows.append(
                f"| {name} | {q} | {human_bytes(b)} | {human_bytes(c)} | {change} |"
            )
    if not rows:
        return ""
    return (
        "| File | Query | Baseline | Changed | Change |\n|---|---|---|---|---|\n"
        + "\n".join(rows)
        + "\n"
    )


def resource_section(sides: list[Side]) -> str:
    rows = []
    for s in sides:
        for bench, st in s.stats:
            rows.append(
                f"| {bench} | {s.name} | {human_secs(st.wall)} | {st.user:.1f}s | "
                f"{st.sys:.1f}s | {human_bytes(st.maxrss)} |"
            )
    if not rows:
        return "_No resource data recorded._\n"
    return (
        "Peak RSS is the largest single descendant process, which includes any "
        "compilation the benchmark command performed.\n\n"
        "| Benchmark | Side | Wall | User CPU | Sys CPU | Peak RSS |\n"
        "|---|---|---|---|---|---|\n" + "\n".join(rows) + "\n"
    )


def machine_info() -> tuple[str, str]:
    uname = " ".join(platform.uname()[:3] + (platform.machine(),))
    if sys.platform == "darwin":
        cpu = run(
            ["sysctl", "-n", "machdep.cpu.brand_string"], Path("/"), check=False
        ).strip()
        ncpu = run(["sysctl", "-n", "hw.ncpu"], Path("/"), check=False).strip()
        mem = run(["sysctl", "-n", "hw.memsize"], Path("/"), check=False).strip()
        mem_s = human_bytes(int(mem)) if mem.isdigit() else "?"
        details = run(
            ["sysctl", "machdep.cpu", "hw.perflevel0", "hw.perflevel1"],
            Path("/"),
            check=False,
        )
        return f"{cpu} ({ncpu} CPU / {mem_s}) | `{uname}`", details.strip()
    details = run(["lscpu"], Path("/"), check=False) if shutil.which("lscpu") else ""
    return f"{os.cpu_count()} CPU | `{uname}`", details.strip()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def parse_env(pairs: list[str], flag: str) -> dict[str, str]:
    env = {}
    for p in pairs:
        if "=" not in p:
            raise SystemExit(f"error: {flag} expects KEY=VALUE, got {p!r}")
        k, v = p.split("=", 1)
        env[k] = v
    return env


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n\n".join(__doc__.split("\n\n")[3:]),
    )
    p.add_argument(
        "command", nargs=argparse.REMAINDER, help="benchmark command (see forms below)"
    )
    p.add_argument(
        "--repo",
        type=Path,
        default=None,
        help="local datafusion repo (default: $DATAFUSION_REPO, cwd if it is one, "
        "else ~/GitHub/datafusion)",
    )
    p.add_argument(
        "--workdir",
        type=Path,
        default=None,
        help="where worktrees/logs/reports live (default: <repo>/../datafusion-bench-local)",
    )
    p.add_argument(
        "--remote",
        default=None,
        help="remote holding upstream main (default: auto-detect apache/datafusion)",
    )
    p.add_argument(
        "--no-fetch", action="store_true", help="do not git fetch before resolving refs"
    )

    b = p.add_argument_group(
        "baseline (default: merge-base of changed and <remote>/main)"
    )
    bx = b.add_mutually_exclusive_group()
    bx.add_argument(
        "--baseline-ref", help="any git ref: sha, tag, local branch, remote/branch"
    )
    bx.add_argument(
        "--baseline-branch", help="branch name, fetched from --remote first"
    )

    c = p.add_argument_group("changed side (default: HEAD of --repo)")
    cx = c.add_mutually_exclusive_group()
    cx.add_argument("--changed-ref", help="any git ref")
    cx.add_argument("--changed-branch", help="branch name, fetched from --remote first")
    cx.add_argument(
        "--changed-in-place",
        action="store_true",
        help="benchmark the repo's working tree as-is (includes uncommitted changes, "
        "reuses its target dir)",
    )

    r = p.add_argument_group("run options")
    r.add_argument(
        "--raw", action="store_true", help="treat the command as a raw shell command"
    )
    r.add_argument("--query", help="single query for bench.sh suites (e.g. 5)")
    r.add_argument("--bench-filter", default="", help="Criterion test-name filter")
    r.add_argument(
        "--harness-ref",
        default=None,
        help="ref providing bench.sh/compare.py (default: <remote>/main)",
    )
    r.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="benchmark data dir, shared by all runs (default: <repo>/benchmarks/data)",
    )
    r.add_argument(
        "--skip-data", action="store_true", help="skip `bench.sh data <bench>`"
    )
    r.add_argument(
        "--env", action="append", default=[], metavar="K=V", help="env for both sides"
    )
    r.add_argument("--baseline-env", action="append", default=[], metavar="K=V")
    r.add_argument("--changed-env", action="append", default=[], metavar="K=V")
    r.add_argument(
        "--no-thin-lto",
        action="store_true",
        help="keep DataFusion's fat LTO (the GKE runner forces thin)",
    )
    r.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="CARGO_BUILD_JOBS (default: 1 to avoid OOM; GKE runner uses 5)",
    )
    r.add_argument(
        "--sequential-build",
        action="store_true",
        help="build dfbench for the two sides one after another",
    )
    r.add_argument(
        "--changed-first",
        action="store_true",
        help="run the changed side before the baseline for each benchmark",
    )
    r.add_argument(
        "--output", type=Path, help="report path (default: <workdir>/reports/<ts>.md)"
    )
    r.add_argument(
        "--quiet", action="store_true", help="only write benchmark output to log files"
    )
    r.add_argument(
        "--dry-run", action="store_true", help="print the plan and commands only"
    )
    return p


def default_repo() -> Path:
    if os.environ.get("DATAFUSION_REPO"):
        return Path(os.environ["DATAFUSION_REPO"])
    cwd = Path.cwd()
    top = run(["git", "rev-parse", "--show-toplevel"], cwd, check=False).strip()
    if top and (Path(top) / "benchmarks" / "bench.sh").exists():
        return Path(top)
    return Path.home() / "GitHub" / "datafusion"


def main() -> int:
    global DRY_RUN
    args = build_parser().parse_args()
    DRY_RUN = args.dry_run
    plan = parse_command(args.command, args.raw, args.query)

    repo = (args.repo or default_repo()).expanduser().resolve()
    if not (repo / "benchmarks" / "bench.sh").exists():
        raise SystemExit(f"error: {repo} does not look like a datafusion checkout")
    workdir = (
        (args.workdir or repo.parent / "datafusion-bench-local").expanduser().resolve()
    )
    data_dir = (args.data_dir or repo / "benchmarks" / "data").expanduser().resolve()
    remote = args.remote or detect_upstream_remote(repo)
    fetch = not args.no_fetch

    for tool in ("git", "cargo"):
        if not shutil.which(tool):
            raise SystemExit(f"error: `{tool}` not found on PATH")

    # --- resolve changed -------------------------------------------------
    if args.changed_branch:
        changed_display, changed_sha = resolve_branch(
            repo, remote, args.changed_branch, fetch
        )
    elif args.changed_ref:
        changed_display, changed_sha = args.changed_ref, resolve(repo, args.changed_ref)
    else:
        changed_sha = resolve(repo, "HEAD")
        changed_display = git(repo, "rev-parse", "--abbrev-ref", "HEAD")
        if changed_display == "HEAD":
            changed_display = changed_sha[:7]
        dirty = git(repo, "status", "--porcelain", "--untracked-files=no")
        if args.changed_in_place:
            if dirty:
                changed_display += " + uncommitted changes"
        elif dirty:
            warn(
                "repo has uncommitted changes; they are NOT benchmarked "
                "(use --changed-in-place to include them)"
            )

    # --- resolve baseline ------------------------------------------------
    main_ref = f"{remote}/main"
    if fetch:
        log(f"Fetching {remote} main")
        if not DRY_RUN:
            run(["git", "fetch", remote, "main"], repo, check=False)
    if args.baseline_branch:
        baseline_label, base_sha = resolve_branch(
            repo, remote, args.baseline_branch, fetch
        )
    elif args.baseline_ref:
        if fetch and not DRY_RUN:
            run(["git", "fetch", remote, "--tags"], repo, check=False)
        base_sha = resolve(repo, args.baseline_ref)
        baseline_label = args.baseline_ref
    else:
        resolve(repo, main_ref)
        base_sha = git(repo, "merge-base", changed_sha, main_ref)
        baseline_label = f"{base_sha[:7]} (merge-base)"

    harness_ref = args.harness_ref or main_ref
    harness_sha = resolve(repo, harness_ref)

    # --- worktrees --------------------------------------------------------
    base = Side(
        BASE_LABEL,
        baseline_label,
        workdir / "baseline",
        base_sha,
        workdir / "baseline" / "target",
    )
    if args.changed_in_place:
        target = Path(os.environ.get("CARGO_TARGET_DIR", repo / "target"))
        changed = Side(CHANGED_LABEL, changed_display, repo, changed_sha, target)
    else:
        changed = Side(
            CHANGED_LABEL,
            changed_display,
            workdir / "changed",
            changed_sha,
            workdir / "changed" / "target",
        )
    harness = workdir / "harness"
    harness_bench = harness / "benchmarks"

    shared_env = parse_env(args.env, "--env")
    base.env = {**shared_env, **parse_env(args.baseline_env, "--baseline-env")}
    changed.env = {**shared_env, **parse_env(args.changed_env, "--changed-env")}
    forced = {} if args.no_thin_lto else dict(THIN_LTO_ENV)
    forced["CARGO_BUILD_JOBS"] = str(args.jobs)

    log(f"Repo:      {repo} (upstream remote: {remote})")
    log(f"Workdir:   {workdir}")
    log(f"Changed:   {changed.label} ({changed_sha[:10]}) at {changed.dir}")
    log(f"Baseline:  {baseline_label} ({base_sha[:10]})")
    log(f"Harness:   {harness_ref} ({harness_sha[:10]})")
    log(f"Command:   {plan.raw if plan.raw else ' '.join(plan.benches)}")
    if base_sha == changed_sha and not args.changed_in_place:
        warn(
            "baseline and changed are the same commit (A/A run: differences are noise)"
        )

    prepare_worktree(repo, base.dir, base_sha)
    if not args.changed_in_place:
        prepare_worktree(repo, changed.dir, changed_sha)
    prepare_worktree(repo, harness, harness_sha)
    main_queries = prepare_harness_queries(harness_bench)
    if not DRY_RUN:
        data_dir.mkdir(parents=True, exist_ok=True)
        for s in (base, changed):
            shutil.rmtree(results_dir(harness_bench, s), ignore_errors=True)
            results_dir(harness_bench, s).mkdir(parents=True, exist_ok=True)
            clear_criterion_baseline(s.target_dir, s.name)

    run_id = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    logs = workdir / "logs" / run_id

    def side_env(s: Side, extra: dict[str, str] | None = None) -> dict[str, str]:
        # Order matters: user env first, then runner-controlled values.
        return {
            **s.env,
            "CARGO_TARGET_DIR": str(s.target_dir),
            **(extra or {}),
            **forced,
        }

    order = [changed, base] if args.changed_first else [base, changed]

    # --- run ---------------------------------------------------------------
    if plan.raw:
        for s in order:
            rdir = results_dir(harness_bench, s)
            subs = {
                "results_name": s.name,
                "side_dir": str(s.dir),
                "results_dir": str(rdir),
                "data_dir": str(data_dir),
            }
            cmd = plan.raw
            for k, v in subs.items():
                cmd = cmd.replace("{" + k + "}", v)
            if "cargo bench" in cmd and "--save-baseline" not in cmd:
                cmd += (
                    f" --save-baseline {s.name}"
                    if " -- " in cmd
                    else f" -- --save-baseline {s.name}"
                )
            env = side_env(
                s,
                {
                    "SIDE": s.name,
                    "RESULTS_NAME": s.name,
                    "DATAFUSION_DIR": str(s.dir),
                    "DATA_DIR": str(data_dir),
                    "RESULTS_DIR": str(rdir),
                },
            )
            try:
                st = run_logged(
                    ["bash", "-c", cmd],
                    s.dir,
                    env,
                    logs / f"raw-{s.name}.log",
                    args.quiet,
                )
                s.stats.append(("raw", st))
            except CommandError as e:
                s.failures.append(("raw", str(e)))
                warn(f"{s.name} failed: {e}")
        requested = ["raw"]
    else:
        requested = plan.benches
        targets = criterion_targets_cache(changed)
        shell_benches = [b for b in requested if b not in targets]

        if shell_benches:
            build = ["cargo", "build", "--release", "--bin", "dfbench"]
            log(
                "Building dfbench for both sides"
                + ("" if args.sequential_build else " in parallel")
            )
            errors: dict[str, str] = {}

            def do_build(s: Side) -> None:
                try:
                    run_logged(
                        build,
                        s.dir / "benchmarks",
                        side_env(s),
                        logs / f"build-{s.name}.log",
                        quiet=not args.sequential_build or args.quiet,
                    )
                except CommandError as e:
                    errors[s.name] = str(e)

            if args.sequential_build or DRY_RUN:
                for s in (base, changed):
                    do_build(s)
            else:
                threads = [
                    threading.Thread(target=do_build, args=(s,))
                    for s in (base, changed)
                ]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()
            if CHANGED_LABEL in errors:
                raise SystemExit(
                    f"error: changed-side build failed: {errors[CHANGED_LABEL]}"
                )
            if BASE_LABEL in errors:
                warn(f"baseline build failed: {errors[BASE_LABEL]}")

        for bench in requested:
            if bench in targets:
                feats = ["parquet"] + [f for f in targets[bench] if f != "parquet"]
                if not args.skip_data:
                    for ds in CRITERION_DATASETS.get(bench, []):
                        point_queries(harness_bench, main_queries)
                        run_logged(
                            ["./bench.sh", "data", ds],
                            harness_bench,
                            {
                                "DATA_DIR": str(data_dir),
                                "DATAFUSION_DIR": str(changed.dir),
                            },
                            logs / f"data-{ds}.log",
                            args.quiet,
                        )
                    # Criterion targets read <checkout>/benchmarks/data.
                    for s in (base, changed):
                        link = s.dir / "benchmarks" / "data"
                        if (
                            not link.exists()
                            and not DRY_RUN
                            and CRITERION_DATASETS.get(bench)
                        ):
                            link.symlink_to(data_dir)
                for s in order:
                    cmd = [
                        "cargo",
                        "bench",
                        f"--features={','.join(feats)}",
                        "--bench",
                        bench,
                        "--",
                        "--save-baseline",
                        s.name,
                    ]
                    if args.bench_filter:
                        cmd.append(args.bench_filter)
                    try:
                        st = run_logged(
                            cmd,
                            s.dir,
                            side_env(s),
                            logs / f"{bench}-{s.name}.log",
                            args.quiet,
                        )
                        s.stats.append((bench, st))
                        s.criterion_ran = True
                    except CommandError as e:
                        s.failures.append((bench, str(e)))
                        warn(f"{bench} ({s.name}) failed: {e}")
                continue

            if not args.skip_data:
                point_queries(harness_bench, main_queries)
                data_bench = bench
                if bench in TPCH_DIRECT:
                    data_bench = "tpch10" if TPCH_DIRECT[bench][0] == "10" else "tpch"
                try:
                    run_logged(
                        ["./bench.sh", "data", data_bench],
                        harness_bench,
                        {
                            "DATA_DIR": str(data_dir),
                            "DATAFUSION_DIR": str(changed.dir),
                            "CARGO_TARGET_DIR": str(changed.target_dir),
                            **forced,
                        },
                        logs / f"data-{bench}.log",
                        args.quiet,
                    )
                except CommandError as e:
                    warn(f"data generation for {bench} failed: {e}")

            for s in order:
                rdir = results_dir(harness_bench, s)
                point_queries(harness_bench, side_queries(s, main_queries))
                if bench in TPCH_DIRECT:
                    sf, mem = TPCH_DIRECT[bench]
                    out = rdir / (
                        f"tpch_mem_sf{sf}.json" if mem else f"tpch_sf{sf}.json"
                    )
                    cmd = [
                        str(s.target_dir / "release" / "dfbench"),
                        "tpch",
                        "--iterations",
                        "5",
                        "--scale-factor",
                        sf,
                        "--format",
                        "parquet",
                        "--prefer_hash_join",
                        "true",
                        "--path",
                        str(data_dir / f"tpch_sf{sf}"),
                        "-o",
                        str(out),
                    ]
                    if mem:
                        cmd.append("-m")
                    if plan.query:
                        cmd += ["--query", plan.query]
                    cwd, env = harness_bench, side_env(s)
                else:
                    cmd = ["./bench.sh", "run", bench] + (
                        [plan.query] if plan.query else []
                    )
                    env = side_env(
                        s,
                        {
                            "DATAFUSION_DIR": str(s.dir),
                            "RESULTS_NAME": s.name,
                            "DATA_DIR": str(data_dir),
                            "SQL_CARGO_COMMAND": f"cargo bench --bench sql -- --save-baseline {s.name}",
                        },
                    )
                    cwd = harness_bench
                try:
                    st = run_logged(
                        cmd, cwd, env, logs / f"{bench}-{s.name}.log", args.quiet
                    )
                    s.stats.append((bench, st))
                except CommandError as e:
                    s.failures.append((bench, str(e)))
                    warn(f"{bench} ({s.name}) failed: {e}")
        point_queries(harness_bench, main_queries)

    if DRY_RUN:
        log("Dry run complete")
        return 0

    # --- compare -----------------------------------------------------------
    report: list[str] = []
    base_results = results_dir(harness_bench, base)
    if results_json_files(base_results) and results_json_files(
        results_dir(harness_bench, changed)
    ):
        if not shutil.which("uv"):
            report.append("`uv` not found: bench.sh compare skipped\n")
        else:
            summary = run(
                ["./bench.sh", "compare", base.name, changed.name],
                harness_bench,
                check=False,
            )
            detail = run(
                ["./bench.sh", "compare_detail", base.name, changed.name],
                harness_bench,
                check=False,
            )
            report.append(summary)
            report.append(
                "\nDistribution per query (min / mean ±stddev / max):\n\n" + detail
            )

    base_crit = has_criterion_baseline(base.target_dir, base.name)
    changed_crit = has_criterion_baseline(changed.target_dir, changed.name)
    if changed_crit:
        if not shutil.which("critcmp"):
            report.append(
                "\n`critcmp` not found (cargo install critcmp): Criterion comparison skipped\n"
            )
        else:
            exports = workdir / "criterion-exports" / run_id
            exports.mkdir(parents=True, exist_ok=True)
            files = []
            for s, ok in ((base, base_crit), (changed, changed_crit)):
                if not ok:
                    continue
                f = exports / f"{s.name}.json"
                f.write_text(
                    run(
                        [
                            "critcmp",
                            "--target-dir",
                            str(s.target_dir),
                            "--export",
                            s.name,
                        ],
                        s.dir,
                    )
                )
                files.append(str(f))
            out = run(["critcmp", *files], workdir, check=False)
            prefix = (
                ""
                if base_crit
                else "New benchmark — changed-only Criterion results:\n\n"
            )
            report.append(("\n" if report else "") + prefix + out)

    report_text = "".join(report) or "No comparable results were produced.\n"

    # --- assemble markdown -------------------------------------------------
    machine, cpu_details = machine_info()
    config_yaml = [
        f"command: {plan.raw if plan.raw else 'run benchmarks ' + ' '.join(plan.benches)}"
    ]
    if plan.query:
        config_yaml.append(f"query: {plan.query}")
    if shared_env:
        config_yaml.append("env:")
        config_yaml += [f"  {k}: {json.dumps(v)}" for k, v in shared_env.items()]
    for title, side, ref, extra in (
        (
            "baseline",
            base,
            args.baseline_ref or args.baseline_branch,
            args.baseline_env,
        ),
        ("changed", changed, args.changed_ref or args.changed_branch, args.changed_env),
    ):
        if ref or extra:
            config_yaml.append(f"{title}:")
            if ref:
                config_yaml.append(f"  ref: {json.dumps(ref)}")
            if extra:
                config_yaml.append("  env:")
                config_yaml += [
                    f"    {k}: {json.dumps(v)}"
                    for k, v in parse_env(extra, title).items()
                ]
    if forced:
        config_yaml.append("build: " + " ".join(f"{k}={v}" for k, v in forced.items()))

    md = [
        f"🤖 Benchmark completed (local) | {dt.datetime.now():%Y-%m-%d %H:%M}\n",
        f"**Machine:** {machine}\n",
        f"Comparing `{changed.label}` ({changed_sha[:7]}) to `{baseline_label}` ({base_sha[:7]})\n",
        "<details><summary>Run configuration</summary>\n\n```yaml\n"
        + "\n".join(config_yaml)
        + "\n```\n\n</details>\n",
    ]

    workload: list[str] = []
    if not plan.raw and any(
        b not in criterion_targets_cache(changed) for b in requested
    ):
        workload.append(
            f"**Workload:** `bench.sh` from `{harness_ref}` ({harness_sha[:7]}); "
            "each side runs its own query files.\n"
        )
    failures = [(s, b, e) for s in (base, changed) for b, e in s.failures]
    if failures:
        workload.append("**Failures:**\n")
        workload += [
            f"- `{b}` on **{s.name}**: {e.splitlines()[0]}" for s, b, e in failures
        ]
        workload.append("")
    cov = coverage_section(base_results, results_dir(harness_bench, changed))
    if cov:
        workload.append("**Result coverage:**\n")
        workload += cov + [""]
    wd = workload_diff(repo, base, changed, args.changed_in_place)
    if wd:
        workload.append("**Workload files differing between sides:**\n")
        workload += (
            wd[:25] + ([f"- … and {len(wd) - 25} more"] if len(wd) > 25 else []) + [""]
        )
    if workload:
        md.append("\n".join(workload))

    if cpu_details:
        md.append(
            f"<details><summary>CPU Details</summary>\n\n```\n{cpu_details}\n```\n\n</details>\n"
        )
    md.append(
        f"<details><summary>Details</summary>\n<p>\n\n```\n{report_text.rstrip()}\n```\n\n</p>\n</details>\n"
    )
    pool = pool_section(harness_bench, base, changed)
    if pool:
        md.append(
            f"<details><summary>Memory Pool Peaks</summary>\n\n{pool}\n</details>\n"
        )
    md.append(
        f"<details><summary>Resource Usage</summary>\n\n{resource_section([base, changed])}\n</details>\n"
    )
    md.append(f"_Logs: `{logs}`_\n")

    out_path = args.output or workdir / "reports" / f"{run_id}.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(md))

    print("\n" + "=" * 78)
    print(report_text.rstrip())
    print("=" * 78)
    log(f"Markdown report: {out_path}")
    return 1 if changed.failures else 0


_TARGETS_CACHE: dict[Path, dict[str, list[str]]] = {}


def criterion_targets_cache(side: Side) -> dict[str, list[str]]:
    key = side.dir / "benchmarks"
    if key not in _TARGETS_CACHE:
        _TARGETS_CACHE[key] = criterion_targets(key)
    return _TARGETS_CACHE[key]


if __name__ == "__main__":
    try:
        sys.exit(main())
    except CommandError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)
