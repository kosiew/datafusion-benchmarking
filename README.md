# DataFusion Benchmarking

Automated benchmarking infrastructure for [Apache DataFusion](https://github.com/apache/datafusion) and [Apache Arrow-rs](https://github.com/apache/arrow-rs). Triggered via PR comments, executed on GKE Autopilot, results posted back to the PR.

## Architecture

```
GitHub PR comment                        GitHub PR
  "run benchmark tpch"                   (results posted)
        │                                       ▲
        ▼                                       │
┌──────────────────────────────────────────────────────┐
│  Controller Pod (StatefulSet)                        │
│                                                      │
│  ┌──────────────┐       ┌───────────────────────┐   │
│  │ GitHub Poller │       │ Job Reconciler        │   │
│  │ (poll_loop)   │       │ (reconcile_loop)      │   │
│  │               │       │                       │   │
│  │ fetch comments│       │ pending  → create Job │   │
│  │ detect trigger│       │ running  → check K8s  │   │
│  │ insert job    │       │ done     → post result│   │
│  └──────┬────────┘       └───────────┬───────────┘   │
│         │    SQLite                  │               │
│         └──────────┤ benchmark.db ├──┘               │
└──────────────────────────────────────────────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │  K8s Job     │
                    │  (spot node) │
                    │              │
                    │ clone repo   │
                    │ build & run  │
                    │ post results │
                    └──────────────┘
```

## How It Works

1. The **GitHub poller** scans PR comments every 30s for trigger phrases
2. Valid triggers insert a row into SQLite with status `pending`
3. The **job reconciler** picks up pending rows and creates K8s Jobs
4. Each Job runs on a Performance-class spot node, builds the project, runs benchmarks, and posts results back to the PR
5. A **scheduled CI workflow** benchmarks the latest `main` every 6 hours

## Triggering Benchmarks

Comment on a PR in `apache/datafusion` or `apache/arrow-rs`:

```
run benchmarks
```

Or run specific benchmarks:

```
run benchmark tpch_mem clickbench_partitioned
```

With environment variables:

```
run benchmark tpch_mem
DATAFUSION_RUNTIME_MEMORY_LIMIT=1G
```

View the queue:

```
show benchmark queue
```

## Supported Benchmarks

### DataFusion (standard)

| Benchmark | Description |
| --- | --- |
| `tpch` | TPC-H SF=1 |
| `tpch10` | TPC-H SF=10 |
| `tpch_mem` | TPC-H SF=1 in-memory |
| `tpch_mem10` | TPC-H SF=10 in-memory |
| `clickbench_partitioned` | ClickBench partitioned Parquet |
| `clickbench_extended` | ClickBench extended queries |
| `clickbench_1` | ClickBench single-file Parquet |
| `clickbench_pushdown` | ClickBench pushdown queries |
| `external_aggr` | External aggregation |
| `tpcds` | TPC-DS |

### DataFusion (criterion)

`sql_planner`, `in_list`, `case_when`, `aggregate_vectorized`, `aggregate_query_sql`, `with_hashes`, `range_and_generate_series`, `sort`, `left`, `strpos`, `substr_index`, `character_length`, `reset_plan_states`, `replace`, `plan_reuse`

### Arrow-rs (criterion)

`arrow_reader`, `arrow_reader_clickbench`, `arrow_reader_row_filter`, `arrow_statistics`, `arrow_writer`, `array_iter`, `array_from`, `bitwise_kernel`, `boolean_kernels`, `buffer_bit_ops`, `builder`, `cast_kernels`, `comparison_kernels`, `csv_writer`, `coalesce_kernels`, `encoding`, `metadata`, `json-reader`, `ipc_reader`, `take_kernels`, `sort_kernel`, `interleave_kernels`, `union_array`, `variant_builder`, `variant_kernels`, `view_types`, `variant_validation`, `filter_kernels`, `concatenate_kernel`, `row_format`, `zip_kernels`

## Project Structure

Monorepo with npm workspaces (TypeScript) and a Cargo workspace (Rust).

```
package.json               Root npm workspace (infra + services)
Cargo.toml                 Root Cargo workspace (controller)
.github/workflows/         GitHub Actions (deploy, build, CI, scheduled benchmarks)
infra/                     Pulumi stack: GCP resources (GKE, Artifact Registry, IAM)
services/                  Pulumi stack: K8s deployments (controller StatefulSet)
controller/                Rust controller crate
  src/
    main.rs                Entry point — spawns poller + reconciler
    config.rs              Environment-based configuration
    models.rs              SQLite row types, GitHub API types
    github.rs              GitHub REST API client
    github_poller.rs       Comment polling loop
    job_manager.rs         K8s Job lifecycle reconciler
    db.rs                  SQLite queries (jobs, seen comments, scan state)
    benchmarks.rs          Trigger parsing, benchmark allowlists
  migrations/              SQLite schema
runner/                    Benchmark runner container (builds project, runs benchmarks, posts results)
queries/                   SQL query files for ClickBench
scripts/                   Legacy benchmark scripts (reference)
```

## Building

```bash
cargo build --manifest-path controller/Cargo.toml
cargo fmt --check --manifest-path controller/Cargo.toml
cargo clippy --manifest-path controller/Cargo.toml -- -D warnings
```

## Testing

```bash
cargo test --manifest-path controller/Cargo.toml
```

## Running Locally

The controller needs a GitHub token and a runner image. SQLite is used for state (auto-created).

```bash
export GITHUB_TOKEN="ghp_..."
export RUNNER_IMAGE="us-docker.pkg.dev/.../runner:latest"

# Optional overrides (shown with defaults):
export DATABASE_URL="sqlite:///data/benchmark.db"
export WATCHED_REPOS="apache/datafusion:apache/arrow-rs"
export POLL_INTERVAL_SECS=2
export RECONCILE_INTERVAL_SECS=3
export K8S_NAMESPACE="benchmarking"
export DEFAULT_CPU=30
export DEFAULT_MEMORY="60Gi"

cargo run --manifest-path controller/Cargo.toml
```

Note: the reconciler requires in-cluster K8s access. Outside a cluster it will fail to create jobs. The poller works standalone for testing comment detection.

## Infrastructure

Managed with [Pulumi](https://www.pulumi.com/) (TypeScript), split into two stacks connected via StackReference:

- **`infra`** — GCP resources: GKE Autopilot cluster, Artifact Registry, IAM (controller SA + WI binding)
- **`services`** — K8s resources: namespace, service account, secrets, controller StatefulSet

```bash
npm install          # install all workspace deps from root
cd infra && pulumi preview    # dry-run infra
cd services && pulumi preview # dry-run services
```

The dependency chain is: `infra (pulumi up)` → `build images (docker push)` → `services (pulumi up)`.

Key resources:
- **GKE Autopilot** cluster with Performance-class spot instances
- **Hyperdisk-balanced** ephemeral volumes for fast compilation I/O
- **Artifact Registry** for runner container images
- **Workload Identity Federation** for keyless auth (GitHub Actions + controller pod)

### Bootstrapping

One-time setup for GitHub Actions → GCP authentication:

1. Run `infra/bootstrap.sh` (creates WIF pool, OIDC provider, gha-deployer SA via gcloud)
2. Set the 3 GitHub repo variables printed by the script (`GCP_PROJECT_ID`, `GCP_WORKLOAD_IDENTITY_PROVIDER`, `GCP_SERVICE_ACCOUNT_EMAIL`)
3. `gcloud auth application-default login && cd infra && pulumi up --stack dev`
4. Push to main — GHA takes over from there

## Design

### Job Lifecycle

```
                     ┌─────────┐
   insert_job() ───► │ pending │
                     └────┬────┘
                          │ reconcile_pending()
                          │ create K8s Job
                          ▼
                     ┌─────────┐
                     │ running │
                     └────┬────┘
                          │ reconcile_active()
                          │ check K8s Job status
                    ┌─────┴──────┐
                    ▼            ▼
              ┌───────────┐ ┌────────┐
              │ completed │ │ failed │
              └───────────┘ └────────┘
```

### Component Responsibilities

| Component | Role |
| --- | --- |
| `github_poller` | Poll GitHub API, detect triggers, insert jobs |
| `job_manager` | Create K8s Jobs, monitor status, post results |
| `db` | SQLite persistence (jobs, seen comments, scan state) |
| `benchmarks` | Trigger parsing, per-repo allowlists, classification |
| `github` | GitHub REST API client (comments, reactions) |
| `config` | Environment-based configuration |

## Observability

We use [Logfire](https://logfire-us.pydantic.dev/l/join-adriangb/bavCVFu0iw) for tracing and metrics.

---

## How Results Are Generated

### Runner workflow (inside the K8s Job pod)

The runner binary dispatches to one of three workflows based on benchmark type:

| JobType | Workflow | Comparison tool |
|---|---|---|
| `Standard` | Shell-script benchmarks (TPC-H, ClickBench) | `bench.sh compare_detail` |
| `Criterion` | Criterion.rs microbenchmarks for DataFusion | `critcmp` |
| `ArrowCriterion` | Criterion.rs microbenchmarks for Arrow-rs | `critcmp` |

---

### Standard workflow (`bench.sh`)

```
Clone PR branch     → /workspace/datafusion-branch
Clone merge-base    → /workspace/datafusion-base
Clone main          → /workspace/datafusion-bench  (provides a trusted bench.sh)
        │
        ▼
Parallel: cargo build --release --bin dfbench (both sides)
        │
        ▼
Post "🔄 Benchmark running..." comment with SHA + diff link
        │
        ▼
For each benchmark (e.g. tpch_mem, clickbench):
  1. Download/generate data (GCS cache → local fallback)
  2. Run bench.sh   DATAFUSION_DIR=base_dir    RESULTS_NAME=HEAD
  3. Run bench.sh   DATAFUSION_DIR=branch_dir  RESULTS_NAME=<branch>
        │
        ▼
bench.sh compare_detail HEAD <branch_name>
→ reads results/HEAD/ and results/<branch>/ JSON files
→ outputs comparison table to stdout
        │
        ▼
Post result comment to PR
```

> **Security**: `bench.sh` is always taken from `origin/main` (the third clone), not from the PR branch. This prevents a PR from modifying the benchmark runner script.

---

### Criterion workflow (`critcmp`)

```
Clone PR branch     → /workspace/datafusion-branch
Clone merge-base    → /workspace/datafusion-base
        │
        ▼
Parallel: cargo bench --bench <name> --no-run  (compile only, both sides)
        │
        ▼
Run baseline:
  cargo bench --bench <name> -- --save-baseline main
  → writes JSON to base_dir/target/criterion/main/

Run branch:
  cargo bench --bench <name> -- --save-baseline <branch_name>
  → writes JSON to branch_dir/target/criterion/<branch>/
        │
        ▼
cp -r base_dir/target/criterion/. branch_dir/target/criterion/
  (merge both result trees into one directory)
        │
        ▼
critcmp main <branch_name>    ← produces the diff table
        │
        ▼
Post result comment to PR
```

---

## How to Interpret the Results

### Standard benchmark comment

The result comment looks like:

```
🤖 Benchmark completed (GKE) | trigger

<details><summary>Details</summary>

Query 1: HEAD=120ms  branch=115ms  (-4.2%)
Query 2: HEAD=340ms  branch=351ms  (+3.2%)
...

</details>

<details><summary>Resource Usage</summary>

**tpch_mem — base (merge-base)**
| Metric      | Value   |
|-------------|---------|
| Wall time   | 42.3s   |
| Peak memory | 1.2 GiB |
| Avg memory  | 800 MiB |
| CPU user    | 38.1s   |
| CPU sys     | 2.4s    |
| Disk read   | 512 MiB |
| Disk write  | 12.3 MiB|

**tpch_mem — branch**
...

</details>
```

- **Percentages**: negative = PR is **faster**, positive = PR is **slower**.
- **Resource Usage**: cgroup v2 measurements (wall clock, peak/avg RSS, CPU time, I/O) sampled every second during the run. Useful for catching memory regressions even when query time looks similar.

---

### Criterion benchmark comment

```
group                      main               my_branch
-----                      ----               ---------
sql_planner/tpch_q1        1.00  2.5±0.01ms  0.97  2.4±0.01ms
sql_planner/tpch_q2        1.00 12.3±0.05ms  1.05 12.9±0.06ms
```

- **Baseline column** (`1.00`): the merge-base is always normalized to 1.00.
- **PR column** (e.g. `0.97`): the PR's relative performance. **< 1.00 = faster**, **> 1.00 = slower**.
- **`±` value**: Criterion's measured noise (standard deviation). Large `±` values mean noisy results — treat differences smaller than the noise with caution, especially on spot nodes.
- **New benchmarks**: if the benchmark didn't exist in the merge-base, `critcmp` runs in single-baseline mode and the comment is marked *"branch-only results (no baseline comparison)"*.

---

### Error comment

If the runner fails at any step, it posts:

```
❌ Benchmark failed
<last 20 lines of build/test output>
```

The output file (`/tmp/benchmark_output.txt`) captures all stdout/stderr from every command in the job, so the tail typically shows the exact build or runtime error.
