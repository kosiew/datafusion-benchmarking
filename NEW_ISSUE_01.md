# Controller runs legacy H2O workload instead of branch-local SQL benchmark suite

## Problem

`run benchmark h2o_small_window` does not execute new SQL benchmark cases added by the PR.

Observed run: https://github.com/apache/datafusion/pull/26923#issuecomment-6520552596

The report contains only Q1–Q29. Expected additional Q30–Q37, defined as `q30.benchmark`–`q37.benchmark` in the checked-out revision’s H2O window suite.

## Why it matters

A successful benchmark report can omit the cases intended to measure the PR’s optimization, giving misleading coverage of its performance impact.

## Root cause

In `controller/src/runner/bench_datafusion.rs`:

- Non-Criterion benchmark names route through `bench.sh`.
- Controller builds `dfbench`, not `benchmark_runner`.
- `bench.sh` and legacy query files come from a separate `origin/main` checkout.
- `DATAFUSION_DIR` selects the baseline/changed Rust checkout, but does not select its workload definitions.

Consequently, the job compares branch binaries against an older workload. Building `benchmark_runner` alone is insufficient; the controller must invoke it against the intended suite files.

## Invariant / desired behavior

SQL-suite runs use `benchmark_runner` and discover workload definitions from the intended checked-out revision—not silently substitute main’s legacy SQL.

Comparison reports must identify missing cases when baseline and changed revisions contain different queries. A query present on both sides must represent a comparable workload; matching query IDs alone is insufficient if their definitions differ.

## Proposed direction

Add a SQL-suite execution path that:

- Builds `benchmark_runner` for baseline and changed revisions.
- Runs from each revision’s `benchmarks` directory with appropriate dataset paths.
- Generates/caches the required dataset.
- Preserves shared and per-side environment variables.
- Writes JSON results into the controller’s comparison/reporting layout.
- Defines explicit handling for queries or runner binaries missing on the baseline.

Keep existing Criterion and legacy-only suites working. Preserve the existing trigger name or document any required migration.

## Scope

### In

- H2O window SQL-suite routing, dataset setup, execution, and result comparison.
- Explicit handling of missing or non-comparable baseline cases.
- Compatibility with existing Criterion and legacy-only benchmark routes.

### Out

- Arbitrary shell commands in PR comments.
- Query-range and iteration configuration; these can be separate work.

## Acceptance criteria

- [ ] H2O window runs discover added Q30–Q37 from the changed revision.
- [ ] No silent fallback to main’s Q1–Q29 workload.
- [ ] Missing baseline cases are reported explicitly, without fabricated comparisons.
- [ ] Different workload definitions are not silently compared solely by query ID.
- [ ] Per-side optimizer settings reach the benchmark process.
- [ ] Reports identify runner, revisions, executed cases, and iteration count.
- [ ] Existing Criterion and legacy-only benchmark routes remain functional.

## Tests / verification

- Routing test distinguishes SQL suites, Criterion targets, and legacy suites.
- Regression fixture adds Q30–Q37 only on the changed side.
- Integration test verifies discovery, environment propagation, JSON collection, and missing-baseline reporting.
- Test differing query definitions and a baseline without `benchmark_runner` to verify explicit reporting rather than silent fallback.
- Re-run PR #26923 and confirm Q30–Q37 appear in results.
