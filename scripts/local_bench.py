#!/Users/kosiew/GitHub/scripts_venv/.venv/bin/python3
"""Run DataFusion benchmarks locally, comparing a baseline against a changed
checkout, and produce a report shaped like the one the GKE runner posts on PRs
(see controller/src/runner/bench_datafusion.rs).

\b
Pipeline (mirrors the runner):
    local datafusion repo
      ├── worktree  <workdir>/baseline   (merge-base | --baseline-ref | --baseline-branch)
      ├── worktree  <workdir>/changed    (HEAD | --changed-ref | --changed-branch | --changed-in-place)
      └── worktree  <workdir>/harness    (bench.sh + compare.py: --harness-ref | baseline ref | <upstream>/main)
                     benchmarks/queries ──symlink──► each side's own queries while it runs

\b
For each requested benchmark:
  * a Criterion `[[bench]]` target  → `cargo bench --bench <name> -- --save-baseline <side>`
  * tpch / tpch10 / tpch_mem / tpch_mem10 → prebuilt `dfbench tpch` directly
  * anything else                    → `bench.sh run <name> [query]`
Then `bench.sh compare` (min), `bench.sh compare_detail` (mean ±stddev) and
`critcmp` are run over whatever artifacts each side produced.

\b
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

Options must come before the benchmark command; everything after it is passed
through verbatim (including `--`). Runs with the scripts venv
(/Users/kosiew/GitHub/scripts_venv/.venv/bin/python3), which provides Typer and
Rich. bench.sh's compare step needs `uv`, Criterion comparison needs `critcmp`
(`cargo install critcmp`).
"""

from __future__ import annotations

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
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Iterator, Optional

import typer
from rich.console import Console
from rich.markup import escape
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.text import Text

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


class UI:
    """Progress display on stderr: an overall bar plus one spinner line per
    running command (two at once during parallel builds). Benchmark output and
    log messages are printed above the live display."""

    def __init__(self) -> None:
        self.console = Console(stderr=True, highlight=False)
        self.progress: Progress | None = None
        self.overall: TaskID | None = None
        self.lock = threading.Lock()

    def start(self, total: int) -> None:
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(bar_width=24),
            TextColumn("{task.fields[count]}"),
            TimeElapsedColumn(),
            TextColumn("[dim]{task.fields[detail]}"),
            console=self.console,
        )
        self.progress.start()
        self.overall = self.progress.add_task(
            "[bold]Overall", total=total, count=f"0/{total}", detail=""
        )

    def set_total(self, total: int) -> None:
        if self.progress and self.overall is not None:
            done = int(self.progress.tasks[self.overall].completed)
            self.progress.update(self.overall, total=total, count=f"{done}/{total}")

    def advance(self) -> None:
        if not self.progress or self.overall is None:
            return
        with self.lock:
            self.progress.advance(self.overall)
            t = self.progress.tasks[self.overall]
            total = int(t.total or 0)
            done = min(int(t.completed), total)
            self.progress.update(self.overall, count=f"{done}/{total}")

    def stop(self) -> None:
        if self.progress:
            self.progress.stop()
            self.progress = None

    def print(self, renderable: object) -> None:
        self.console.print(renderable)

    @contextmanager
    def step(self, label: str) -> Iterator[TaskID | None]:
        """A spinner line for one step; advances the overall bar when done."""
        if not self.progress:
            yield None
            return
        tid = self.progress.add_task(label, total=None, count="", detail="")
        try:
            yield tid
        finally:
            self.progress.remove_task(tid)
            self.advance()

    def detail(self, tid: TaskID | None, text: str) -> None:
        if self.progress and tid is not None:
            text = text.strip()
            if len(text) > 70:
                text = text[:67] + "..."
            self.progress.update(tid, detail=escape(text))


ui = UI()


def log(msg: str) -> None:
    ui.print(Text.assemble(("==> ", "bold blue"), msg))


def warn(msg: str) -> None:
    ui.print(Text.assemble(("warning: ", "bold yellow"), msg))


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
    label: str | None = None,
) -> RunStats:
    """Run a long command, teeing output to `log_path`, and measure it."""
    log(show(cmd, cwd, env_overrides))
    if DRY_RUN:
        return RunStats()
    with ui.step(label or log_path.stem) as tid:
        return _run_logged(cmd, cwd, env_overrides, log_path, quiet, tid)


def _run_logged(
    cmd: list[str],
    cwd: Path,
    env_overrides: dict[str, str],
    log_path: Path,
    quiet: bool,
    tid: TaskID | None,
) -> RunStats:
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
            if line.strip():
                ui.detail(tid, Text.from_ansi(line).plain)
            if not quiet:
                ui.print(Text.from_ansi(line.rstrip("\n")))
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
    # Click hands unknown options to the command; no command form starts with
    # `-`, so this is a typo'd option, which would otherwise be silently ignored.
    if tokens[0].startswith("-"):
        raise SystemExit(
            f"error: unknown option {tokens[0]!r} (see --help; use --raw for a "
            "command starting with '-')"
        )

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


def disable_submodule_recursion() -> None:
    """Make every git command this script starts (and those bench.sh starts)
    ignore a global `submodule.recurse = true`.

    With it, `worktree add`/`checkout` recurse into datafusion's submodules,
    which inside a linked worktree leaves submodule `.git` files pointing at
    `.git/worktrees/<name>/modules/...` dirs that were never populated, and every
    later checkout fails with "could not reset submodule index". The benchmarks
    do not need the submodules (the GKE runner never initializes them).
    """
    n = int(os.environ.get("GIT_CONFIG_COUNT", "0") or 0)
    os.environ[f"GIT_CONFIG_KEY_{n}"] = "submodule.recurse"
    os.environ[f"GIT_CONFIG_VALUE_{n}"] = "false"
    os.environ["GIT_CONFIG_COUNT"] = str(n + 1)


def repair_submodule_links(path: Path) -> None:
    """Remove submodule `.git` files whose gitdir no longer exists (left behind
    by an earlier recursive checkout), so git treats them as uninitialized."""
    if not (path / ".gitmodules").exists():
        return
    out = git(
        path, "config", "-f", ".gitmodules", "--get-regexp", r"\.path$", check=False
    )
    for line in out.splitlines():
        sub = path / line.split(maxsplit=1)[-1]
        dotgit = sub / ".git"
        if not dotgit.is_file():
            continue
        # The gitdir can exist yet be a stub (just a `config`), so ask git.
        probe = subprocess.run(
            ["git", "rev-parse", "--git-dir"], cwd=sub, capture_output=True
        )
        if probe.returncode != 0:
            log(f"Removing stale submodule link {dotgit}")
            if not DRY_RUN:
                dotgit.unlink()


def prepare_worktree(repo: Path, path: Path, sha: str) -> None:
    """Create (or reuse) a detached worktree at `path` checked out at `sha`."""
    if (path / ".git").exists():
        repair_submodule_links(path)
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


def default_repo() -> Path:
    if os.environ.get("DATAFUSION_REPO"):
        return Path(os.environ["DATAFUSION_REPO"])
    cwd = Path.cwd()
    top = run(["git", "rev-parse", "--show-toplevel"], cwd, check=False).strip()
    if top and (Path(top) / "benchmarks" / "bench.sh").exists():
        return Path(top)
    return Path.home() / "GitHub" / "datafusion"


def count_steps(plan: Plan, targets: dict[str, list[str]], skip_data: bool) -> int:
    """Commands `main` will run (one progress step each), plus compare/report."""
    if plan.raw:
        return 2 + 1
    steps = 2 if any(b not in targets for b in plan.benches) else 0  # dfbench builds
    for bench in plan.benches:
        if bench in targets:
            steps += 0 if skip_data else len(CRITERION_DATASETS.get(bench, []))
        else:
            steps += 0 if skip_data else 1
        steps += 2
    return steps + 1


def main(args: SimpleNamespace) -> int:
    global DRY_RUN
    DRY_RUN = args.dry_run
    disable_submodule_recursion()
    plan = parse_command(args.command or [], args.raw, args.query)
    exclusive = (
        ("--baseline-ref", "--baseline-branch"),
        ("--changed-ref", "--changed-branch", "--changed-in-place"),
    )
    for group in exclusive:
        given = [o for o in group if getattr(args, o[2:].replace("-", "_"))]
        if len(given) > 1:
            raise SystemExit(f"error: {' and '.join(given)} are mutually exclusive")

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

    # bench.sh/compare.py follow an explicit baseline, so an old baseline is not
    # driven with flags its dfbench does not know; otherwise main, like the runner.
    if args.harness_ref:
        harness_ref = args.harness_ref
    elif args.baseline_ref or args.baseline_branch:
        harness_ref = baseline_label  # the ref, or the resolved <remote>/<branch>
    else:
        harness_ref = main_ref
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

    if not DRY_RUN:
        ui.start(total=count_steps(plan, {}, args.skip_data) + 1)
    with ui.step("Preparing worktrees"):
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
                    label=f"Running command on [bold]{s.name}[/]",
                )
                s.stats.append(("raw", st))
            except CommandError as e:
                s.failures.append(("raw", str(e)))
                warn(f"{s.name} failed: {e}")
        requested = ["raw"]
    else:
        requested = plan.benches
        targets = criterion_targets_cache(changed)
        ui.set_total(count_steps(plan, targets, args.skip_data) + 1)
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
                        label=f"Building dfbench ([bold]{s.name}[/])",
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
                            label=f"Generating data {ds}",
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
                            label=f"{bench} on [bold]{s.name}[/] (criterion)",
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
                        label=f"Generating data for {bench}",
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
                        cmd,
                        cwd,
                        env,
                        logs / f"{bench}-{s.name}.log",
                        args.quiet,
                        label=f"{bench} on [bold]{s.name}[/]",
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
    with ui.step("Comparing results"):
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
    ui.stop()

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


app = typer.Typer(
    add_completion=False,
    rich_markup_mode=None,
    context_settings={"help_option_names": ["-h", "--help"]},
)

BASELINE = "Baseline (default: merge-base of changed and <remote>/main)"
CHANGED = "Changed side (default: HEAD of --repo)"
RUN = "Run options"


@app.command(
    help=__doc__,
    context_settings={
        # Everything after the first positional token is the benchmark command,
        # passed through verbatim (`cargo bench ... -- --flag` included).
        "allow_extra_args": True,
        "ignore_unknown_options": True,
        "allow_interspersed_args": False,
    },
)
def cli(
    command: Annotated[
        Optional[list[str]],
        typer.Argument(help="Benchmark command (see forms above).", show_default=False),
    ] = None,
    repo: Annotated[
        Optional[Path],
        typer.Option(
            help="Local datafusion repo (default: $DATAFUSION_REPO, cwd if it is "
            "one, else ~/GitHub/datafusion)."
        ),
    ] = None,
    workdir: Annotated[
        Optional[Path],
        typer.Option(
            help="Where worktrees/logs/reports live "
            "(default: <repo>/../datafusion-bench-local)."
        ),
    ] = None,
    remote: Annotated[
        Optional[str],
        typer.Option(
            help="Remote holding upstream main (default: auto-detect apache/datafusion)."
        ),
    ] = None,
    no_fetch: Annotated[
        bool, typer.Option("--no-fetch", help="Do not git fetch before resolving refs.")
    ] = False,
    baseline_ref: Annotated[
        Optional[str],
        typer.Option(
            help="Any git ref: sha, tag, local branch, remote/branch.",
            rich_help_panel=BASELINE,
        ),
    ] = None,
    baseline_branch: Annotated[
        Optional[str],
        typer.Option(
            help="Branch name, fetched from --remote first.", rich_help_panel=BASELINE
        ),
    ] = None,
    changed_ref: Annotated[
        Optional[str], typer.Option(help="Any git ref.", rich_help_panel=CHANGED)
    ] = None,
    changed_branch: Annotated[
        Optional[str],
        typer.Option(
            help="Branch name, fetched from --remote first.", rich_help_panel=CHANGED
        ),
    ] = None,
    changed_in_place: Annotated[
        bool,
        typer.Option(
            "--changed-in-place",
            help="Benchmark the repo's working tree as-is (includes uncommitted "
            "changes, reuses its target dir).",
            rich_help_panel=CHANGED,
        ),
    ] = False,
    raw: Annotated[
        bool,
        typer.Option(
            "--raw",
            help="Treat the command as a raw shell command.",
            rich_help_panel=RUN,
        ),
    ] = False,
    query: Annotated[
        Optional[str],
        typer.Option(
            help="Single query for bench.sh suites (e.g. 5).", rich_help_panel=RUN
        ),
    ] = None,
    bench_filter: Annotated[
        str, typer.Option(help="Criterion test-name filter.", rich_help_panel=RUN)
    ] = "",
    harness_ref: Annotated[
        Optional[str],
        typer.Option(
            help="Ref providing bench.sh/compare.py (default: --baseline-ref or "
            "--baseline-branch if given, else <remote>/main).",
            rich_help_panel=RUN,
        ),
    ] = None,
    data_dir: Annotated[
        Optional[Path],
        typer.Option(
            help="Benchmark data dir, shared by all runs "
            "(default: <repo>/benchmarks/data).",
            rich_help_panel=RUN,
        ),
    ] = None,
    skip_data: Annotated[
        bool,
        typer.Option(
            "--skip-data", help="Skip `bench.sh data <bench>`.", rich_help_panel=RUN
        ),
    ] = False,
    env: Annotated[
        Optional[list[str]],
        typer.Option(
            metavar="K=V", help="Env for both sides (repeatable).", rich_help_panel=RUN
        ),
    ] = None,
    baseline_env: Annotated[
        Optional[list[str]],
        typer.Option(
            metavar="K=V", help="Env for the baseline only.", rich_help_panel=RUN
        ),
    ] = None,
    changed_env: Annotated[
        Optional[list[str]],
        typer.Option(
            metavar="K=V", help="Env for the changed side only.", rich_help_panel=RUN
        ),
    ] = None,
    no_thin_lto: Annotated[
        bool,
        typer.Option(
            "--no-thin-lto",
            help="Keep DataFusion's fat LTO (the GKE runner forces thin).",
            rich_help_panel=RUN,
        ),
    ] = False,
    jobs: Annotated[
        int,
        typer.Option(
            help="CARGO_BUILD_JOBS (1 avoids OOM; the GKE runner uses 5).",
            rich_help_panel=RUN,
        ),
    ] = 1,
    sequential_build: Annotated[
        bool,
        typer.Option(
            "--sequential-build",
            help="Build dfbench for the two sides one after another.",
            rich_help_panel=RUN,
        ),
    ] = False,
    changed_first: Annotated[
        bool,
        typer.Option(
            "--changed-first",
            help="Run the changed side before the baseline for each benchmark.",
            rich_help_panel=RUN,
        ),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option(
            help="Report path (default: <workdir>/reports/<ts>.md).",
            rich_help_panel=RUN,
        ),
    ] = None,
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            help="Only write benchmark output to log files (progress still shown).",
            rich_help_panel=RUN,
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run", help="Print the plan and commands only.", rich_help_panel=RUN
        ),
    ] = False,
) -> None:
    args = SimpleNamespace(**locals())
    args.env, args.baseline_env, args.changed_env = (
        env or [],
        baseline_env or [],
        changed_env or [],
    )
    try:
        code = main(args)
    except CommandError as e:
        ui.stop()
        ui.print(Text.assemble(("error: ", "bold red"), str(e)))
        code = 1
    except KeyboardInterrupt:
        ui.stop()
        code = 130
    finally:
        ui.stop()
    raise typer.Exit(code)


if __name__ == "__main__":
    app()
