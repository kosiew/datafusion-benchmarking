# bench.sh suites run main's workload definitions instead of each side's checkout

## Problem

`run benchmark h2o_small_window` does not execute SQL benchmark cases added by the PR.

Observed run: https://github.com/apache/datafusion/pull/26923#issuecomment-6520552596

The report contains only Q1–Q29. PR #26923 adds Q30–Q37 to both workload formats in the changed revision:

- `benchmarks/queries/h2o/window.sql` (read by `dfbench h2o`)
- `benchmarks/sql_benchmarks/h2o/benchmarks/window/q30.benchmark`–`q37.benchmark` (read by `benchmark_runner`)

## Why it matters

A successful report can silently omit the cases intended to measure a PR's change, giving misleading performance coverage. This affects every non-Criterion suite whose workload files change in a PR, not only H2O window.

## Root cause

In `controller/src/runner/bench_datafusion.rs`:

- Non-Criterion names run through `bench.sh run <name>` (`run_shell_side`).
- `bench.sh` runs from a third checkout pinned to `origin/main` (`/workspace/datafusion-bench`).
- `DATAFUSION_DIR` points at the baseline/changed checkout, so `bench.sh` builds and runs that side's binary.
- But `bench.sh` resolves workload files relative to its own `SCRIPT_DIR`. For example, `h2o_runner` passes `--queries-path "${SCRIPT_DIR}/queries/h2o/${RUN_Type}.sql"`, which is main's file.

Result: both sides' binaries run main's workload. Workload changes in the PR (added, removed or edited queries) never reach either side.

## Invariant / desired behavior

Each side runs the workload definitions from its own checked-out revision. There is no silent fallback to main's definitions.

Comparison reports flag cases present on only one side and do not fabricate a comparison for them. A case present on both sides is compared only when its definition matches. Differing definitions are reported, not compared by query ID alone.

## Proposed direction

Make each side's workload files the source of truth. Options, smallest first:

1. **Per-side workload paths, shared `bench.sh`.** Keep main's `bench.sh` but point workload inputs (`--queries-path`, suite dirs) at `${DATAFUSION_DIR}/benchmarks/...`. This needs `bench.sh` to honor an override, either upstream or via direct invocation as the TPC-H shortcut already does (`tpch_direct_args`).
2. **Per-side `bench.sh`.** Run `${side_dir}/benchmarks/bench.sh` with a shared `DATA_DIR`/`RESULTS_DIR`. This is simplest, but lets a PR change the harness itself and may break on old merge-bases whose `bench.sh` lacks newer suites or flags.
3. **`benchmark_runner` path for SQL suites.** Build `benchmark_runner` per side and run it from the side's `benchmarks` dir against `sql_benchmarks/`. This is the largest change. It is only needed if the legacy `dfbench` query files diverge from `sql_benchmarks`.

Whichever option is chosen must:

- Keep shared and per-side `env:` propagation (e.g. optimizer flags) and `build_env` overrides.
- Keep data generation/caching shared across sides.
- Write JSON results into the existing `results/<side>/` layout used by `compare_detail` and `pool_peak`.
- Define behavior when the baseline lacks the suite, the query file or the binary. Report it explicitly and do not fail silently.

Decide on the trade-off between harness pinning and workload fidelity and record it in the module doc. The current pin to main is likely deliberate, so that old baselines keep running.

## Scope

### In

- Workload-file resolution for `bench.sh`-routed suites, starting with `h2o_small_window`.
- Reporting of one-sided and definition-mismatched cases.
- Compatibility with Criterion targets and the TPC-H direct path.

### Out

- Arbitrary shell commands in PR comments.
- Query-range and iteration configuration.
- Rewriting every suite onto `benchmark_runner`.

## Acceptance criteria

- [ ] Re-running PR #26923 with `h2o_small_window` reports Q30–Q37 from the changed revision.
- [ ] Q30–Q37 are marked as changed-only (no baseline), not compared against fabricated or missing data.
- [ ] A query whose SQL differs between sides is flagged, not compared silently.
- [ ] A suite or query file missing on the baseline produces an explicit report note, not a job failure or a silent drop.
- [ ] Per-side `env:` settings reach the benchmark process.
- [ ] The report states the workload source (revision) for each side.
- [ ] Criterion targets, TPC-H direct runs and unchanged-workload suites produce the same results as before.

## Tests / verification

- Unit: workload paths/args resolve to the side checkout, not the bench checkout.
- Unit: comparison classifies cases as both-sides / changed-only / baseline-only / definition-mismatch.
- Regression fixture: Q30–Q37 present only on the changed side.
- Unit: `env:` propagation still holds (extend the `shell_side_env_*` tests).
- Manual: re-run on PR #26923 and on a PR with no workload changes; confirm no regression in the latter.
