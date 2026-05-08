# Benchmark Behaviour for Unmerged PR Benchmark Runs

## What happens when a PR comment runs a new benchmark not yet merged to `main`

- The PR comment is detected by the GitHub poller and inserted as a benchmark job.
- The runner clones:
  - the PR branch,
  - the merge-base between the PR branch and `origin/main`,
  - and for standard benchmarks, `origin/main` again to fetch a trusted `bench.sh`.

## New benchmark behavior

- The comparison is performed against the merge-base, not directly against `main`.
- If the benchmark is new on the PR branch and did not exist in the merge-base, there is no baseline result to compare.
- For Criterion-style benchmarks, the runner still executes the benchmark, but reports it as branch-only.
- The PR result comment is marked with:
  - `**New benchmark — branch-only results (no baseline comparison)**`

## Key point

If the benchmark is new and not merged to `main`, the system still runs it, but the result is a branch-only run with no baseline comparison.
