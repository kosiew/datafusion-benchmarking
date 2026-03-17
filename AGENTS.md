# AGENTS.md — datafusion-benchmarking

Automated benchmarking infrastructure for [Apache DataFusion](https://github.com/apache/datafusion) and [Apache Arrow-rs](https://github.com/apache/arrow-rs). The system listens for trigger comments on GitHub PRs, spins up ephemeral Kubernetes Jobs on GKE Autopilot spot nodes, runs the requested benchmarks comparing the PR branch against its merge-base, and posts results back to the PR.

---

## Repository Structure

```
.
├── controller/          # Rust crate — long-running controller + runner binaries
│   ├── migrations/      # SQLite schema (sqlx embedded migrations)
│   └── src/
│       ├── bin/
│       │   ├── controller.rs   # Main entry: health + poller + reconciler + cleanup
│       │   └── runner.rs       # Runner binary executed inside K8s Jobs
│       ├── runner/             # Sub-module: benchmark execution logic
│       └── *.rs                # Domain: benchmarks, config, db, github, job_manager, models
├── runner/              # Rust crate — bench-cache binary (GCS data cache upload/download)
├── infra/               # Pulumi TypeScript — GCP resources (GKE, GCS, Artifact Registry, IAM)
├── services/            # Pulumi TypeScript — Kubernetes resources (namespace, SAs, RBAC, controller StatefulSet)
└── queries/             # ClickBench SQL query files
    └── clickbench/
        ├── queries/     # q0.sql – q42.sql
        └── extended/    # q0.sql – q7.sql
```

---

## Technology Stack

| Layer | Technology |
|---|---|
| Primary language | Rust 2021 edition, async with Tokio |
| Database | SQLite via `sqlx` 0.8 (WAL mode, embedded migrations) |
| K8s client | `kube` 0.98 + `k8s-openapi` 0.24 (v1_32) |
| HTTP client | `reqwest` 0.12 |
| Observability | `tracing` + Logfire (`logfire` 0.9) |
| Trigger parsing | `regex` + `serde_yaml` |
| Cloud object store | `object_store` 0.11 (GCP feature) |
| Infrastructure | Pulumi TypeScript — `@pulumi/gcp`, `@pulumi/kubernetes` |
| Cloud platform | GCP (GKE Autopilot, Artifact Registry, GCS, IAM) |
| Node.js | 20 (infra/services npm workspaces) |

---

## Build, Test, and Lint Commands

### Rust

```bash
# Build
cargo build --manifest-path controller/Cargo.toml
cargo build --manifest-path runner/Cargo.toml

# Format check (CI enforces this)
cargo fmt --check --manifest-path controller/Cargo.toml

# Lint (CI enforces -D warnings)
cargo clippy --manifest-path controller/Cargo.toml -- -D warnings

# Tests
cargo test --manifest-path controller/Cargo.toml
```

### TypeScript / Pulumi

```bash
# Install all workspace deps (from repo root)
npm install

# Dry-run infrastructure changes
cd infra && pulumi preview

# Dry-run service changes
cd services && pulumi preview
```

### Docker

```bash
docker build -f controller/Dockerfile -t controller .
docker build -f runner/Dockerfile -t runner .
```

---

## Code Conventions

### Rust patterns to follow

- **Module doc comments**: every `.rs` file has a leading `//!` block describing its role; include ASCII-art state machine diagrams where helpful.
- **Tracing**: use `#[tracing::instrument(skip_all)]` on all async I/O functions; export spans to Logfire.
- **Cooperative shutdown**: use `CancellationToken` (`tokio-util`) across all async tasks; do not put `select!(ctrl_c…)` in individual loops.
- **GitHub API retries**: wrap all GitHub calls with `backon` exponential backoff; retry on 5xx, 429, and network errors; respect `Retry-After` headers.
- **Compiled regexes**: define with `once_cell::sync::Lazy`.
- **YAML config in comments**: parse with `serde_yaml` using `#[serde(deny_unknown_fields)]` — strict validation to catch typos.
- **SQLite**: WAL mode, `max_connections(5)` pool; all schema changes go in a new numbered file under `controller/migrations/`.
- **State machine**: the job lifecycle is `pending → running → completed | failed`; status transitions are explicit calls to `db::update_job_status`.
- **Error handling**: use `anyhow::Result` for application errors; propagate with `?`; add context with `.context()`/`.with_context()`.

### TypeScript / Pulumi patterns to follow

- Use `StackReference` to consume outputs from the `infra` stack inside the `services` stack — never hard-code resource names.
- Export all resource identifiers (cluster name, registry URL, service-account emails) from the `infra` stack `index.ts`.

### Docker layer-caching pattern

The Dockerfiles use a stub `main.rs` trick: dependency layers are compiled before the real source is copied in. Preserve this pattern when modifying the Dockerfiles to keep CI image builds fast.

---

## Key Domain Concepts

### Job lifecycle

```
GitHub PR comment
      │
      ▼
github_poller  →  inserts BenchmarkJob (status=pending) into SQLite
      │
      ▼
job_manager (reconcile_pending)  →  creates K8s Job, sets status=running
      │
      ▼
job_manager (reconcile_active)   →  polls K8s Job conditions
      │
  ┌───┴───┐
failed   completed
```

### Trigger syntax (in PR comments)

```
/run benchmarks tpch_mem

# or with YAML config block:
/run benchmarks tpch_mem
\`\`\`yaml
env:
  DATAFUSION_RUNTIME_ENV: "1"
baseline:
  ref: main
changed:
  ref: my-branch
\`\`\`
```

Allowed benchmark names are configured per-repo in the `BENCHMARK_CONFIG` environment variable (JSON with `standard`, `criterion`, `allowed_users`, `default_standard` fields).

### JobType enum

| Variant | Description |
|---|---|
| `Standard` | Runs `bench.sh` shell scripts; results compared with a custom reporter |
| `Criterion` | Runs Criterion.rs benchmarks; compared with `critcmp` |
| `ArrowCriterion` | Same as Criterion but targeting the arrow-rs repo |

### GKE spot nodes

Runner Jobs use `Performance`-class spot nodes (`cloud.google.com/gke-spot` toleration). Default resources: 12 vCPU, 65 GiB RAM, 128 GiB ephemeral storage (Hyperdisk-balanced). Jobs have a 2-hour deadline (`activeDeadlineSeconds: 7200`).

---

## Database Migrations

Add new migrations as sequentially numbered SQL files in `controller/migrations/` (e.g. `003_my_change.sql`). They are embedded at compile time via `sqlx::migrate!()` and applied automatically on startup. Never edit existing migration files.

---

## Environment Variables (controller)

| Variable | Description |
|---|---|
| `GITHUB_TOKEN` | Personal access token or GitHub App token |
| `RUNNER_IMAGE` | Full image URI for the runner container |
| `DATABASE_URL` | SQLite path, e.g. `sqlite:///data/benchmark.db` |
| `WATCHED_REPOS` | Colon-separated `owner/repo` list, e.g. `apache/datafusion:apache/arrow-rs` |
| `BENCHMARK_CONFIG` | JSON blob defining per-repo benchmark allowlists and allowed users |
| `LOGFIRE_TOKEN` | Pydantic Logfire ingest token (optional) |

---

## CI/CD Overview

| Workflow | Trigger | Purpose |
|---|---|---|
| `ci.yml` | PR / push to `main` | `cargo fmt`, `cargo clippy -D warnings`, `cargo test`, Pulumi preview |
| `build-and-deploy.yml` | Push to `main` or manual | Build multi-arch images (amd64 + arm64), push to Artifact Registry, `pulumi up` services |
| `deploy-infra.yml` | Push to `main` (infra paths) or manual | `pulumi up` infra stack |
| `benchmark-main.yml` | Schedule every 6 hours or manual | Create K8s Job directly via `kubectl apply` to benchmark `main` |

Authentication uses OIDC Workload Identity Federation — no long-lived GCP credentials in CI.

---

## Security Notes

- Never commit `GITHUB_TOKEN`, GCP service account keys, or other secrets.
- Secrets are injected via K8s Secret objects (`github-token`, `logfire-token`) created by the `services` Pulumi stack.
- The `allowed_users` list in `BENCHMARK_CONFIG` gates who may trigger benchmark runs — always validate this is set correctly before deploying.
- The YAML config block embedded in PR comments is parsed with `deny_unknown_fields`; unknown keys are rejected to prevent injection of unexpected configuration.
