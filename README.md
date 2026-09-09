# MongoDB Migration & Replication Tool (with Web Console for Firestore)

This Go application migrates and replicates data from a source MongoDB database to another MongoDB database **or to *Firestore with MongoDB compatibility***.

> ### 🌐 New: browser-based migration console
>
> Beyond the config-file CLI, the tool now ships a **web console** that drives a migration end-to-end from the browser — **assess → migrate (with live per-collection progress) → verify** — with no hand-written config file. It is the recommended way to migrate to Firestore. Launch it with `./migrate -mode=console` and open http://localhost:9090/.
>
> **See the [Web Console](#web-console) section for full details.** It covers:
> - **Pre-migration assessment & remediation** — a read-only compatibility scan (collection names, `_id` types, field names, document/index budgets…) with opt-in fixes applied at write time; the source is never modified.
> - **Live migration & progress** — `migrate` (full) or `live` (full + change-stream/oplog tailing), per-collection progress/throughput/lag, and Pause/Resume/Stop.
> - **Dead Letter Queue** — bad documents don't block the migration; failures are captured with reasons and can be recycled.
> - **Post-migration verification** — source↔target count (and optional content-hash) comparison, reconciling converted `_id`s.
> - **Index handling for Firestore** — the `_id` index is always created; secondary indexes are optional and built after data.

The tool supports these operation modes (`-mode`): **`console`** (web UI), **`migrate`** (one-time full migration), **`live`** (full migration + continuous change-stream/oplog replication), **`live-only`**, **`retry-dlq`**, **`capture-resume-token`**, **`assess`** (headless compatibility check), **`verify`** (headless verification), and **`wizard`** (interactive config generator).

## Documentation

This page covers what you need to **run a migration**: requirements, supported source versions, and the web console. Everything else lives in the detailed references:

- **[docs/CONFIGURATION.md](docs/CONFIGURATION.md)** — full config-file reference: every JSON example and parameter, all CLI modes and flags, index sync, tuning/parallelism/checkpoint/retry internals, replica-set setup, and project structure. Read this only if you drive the tool from the CLI with a hand-written config, or need to tune advanced parameters.
- **[使用说明.md](使用说明.md)** — step-by-step user guide for the web console (Chinese), including the Firestore endpoint format, the assess→migrate→verify flow, and troubleshooting.
- **[CONSOLE_UI_DESIGN.md](CONSOLE_UI_DESIGN.md)** — the web console's UI interaction contract (severity model, live progress, DLQ, index strategy).

## Requirements

### General

- Go 1.21 or later
- MongoDB servers running and accessible (both source and target)

### Supported Source Versions & Replication Methods

Live replication supports three methods, each with different source prerequisites. **All live replication methods require the source MongoDB to be running as a replica set** (the oplog only exists on replica sets). A one-time `migrate` (full copy) has no replica-set requirement.

#### 1. Change Stream Replication (Default — `replicationMethod: "changestream"`)
- **Source MongoDB**: Version 3.6 or later
- **Replica Set**: Source MongoDB **must** be running as a replica set
- **Recommended for**: Modern MongoDB deployments (3.6+)
- **Advantages**:
  - High-level API with server-side filtering
  - Structured change events
  - Official MongoDB feature with long-term support

#### 2. Oplog Replication (`replicationMethod: "oplog"`)
- **Source MongoDB**: Any version with replica set support (2.0+)
- **Replica Set**: Source MongoDB **must** be running as a replica set
- **Wire Protocol**: Modern wire protocol (version 6+)
- **Recommended for**:
  - MongoDB 3.0, 3.2, 3.4 (wire protocol version 6)
  - Scenarios requiring low-level oplog access
- **Advantages**:
  - Works with older MongoDB versions that don't support change streams
  - Direct access to operation log

#### 3. Legacy Oplog Replication (`replicationMethod: "oplog-legacy"`)
- **Source MongoDB**: MongoDB 3.0, 3.2, 3.4 (wire protocol version 3)
- **Replica Set**: Source MongoDB **must** be running as a replica set
- **Target MongoDB**: Modern MongoDB (3.6+) or MongoDB-compatible databases (e.g., Firestore)
- **Recommended for**:
  - Migrating from very old MongoDB versions (3.0/3.2) to modern MongoDB
  - Bridging the gap between legacy and modern MongoDB versions
- **Implementation**:
  - Uses dual-driver architecture (mgo for source, mongo-driver for target)
  - Leverages GTM legacy library for oplog tailing
  - Full initial migration + incremental replication support

#### Target Requirements
- **Migrate mode**: Any MongoDB version, or *Firestore with MongoDB compatibility*
- **Live mode**: No specific version requirements (receives standard insert/update/delete operations)

#### Quick Decision Guide

| Your Source MongoDB | Recommended Method | Configuration |
|-------------------|-------------------|---------------|
| MongoDB 3.6 or later | Change Streams | `"replicationMethod": "changestream"` (default) |
| MongoDB 3.0, 3.2, 3.4 (wire v6) | Oplog | `"replicationMethod": "oplog"` |
| MongoDB 3.0, 3.2, 3.4 (wire v3) | Legacy Oplog | `"replicationMethod": "oplog-legacy"` |
| Single-node deployment | Migrate mode only | Not applicable (live mode requires replica set) |

> For local development, see [docs/CONFIGURATION.md → Setting Up a Single-Node Replica Set](docs/CONFIGURATION.md#setting-up-a-single-node-replica-set-for-development).

## Quick Start

1. Clone and build:

   ```bash
   git clone https://github.com/gsbingo17/mongodb-to-new.git
   cd mongodb-to-new
   go mod tidy
   go build -o migrate ./cmd/migrate
   ```

2. Launch the web console and open the printed URL:

   ```bash
   ./migrate -mode=console          # serves on http://localhost:9090
   ```

   The console needs **no config file** — it builds the configuration from a form and runs the migration in-process. From there, follow **assess → migrate → verify** in the browser. See the [Web Console](#web-console) section below.

To drive the tool from the CLI with a hand-written config file instead, see [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## Web Console

In addition to the config-file-driven CLI, the tool ships a browser-based control console that walks an operator through a migration end-to-end — **assess → migrate (with live progress) → verify** — without hand-writing a config file. It is the recommended way to drive a migration to *Firestore with MongoDB compatibility*.

Launch it with:

```bash
./migrate -mode=console                 # serves on http://localhost:9090
./migrate -mode=console -metrics-addr=:8080   # custom listen address
```

Console mode needs **no pre-existing config file** — it builds the configuration from the form and runs the migration **in-process**. Open the printed URL in a browser.

> A full step-by-step user guide (in Chinese) is available in [`使用说明.md`](使用说明.md).

#### Running the console under a watchdog (optional)

For long-running migrations, two helper scripts make the console more robust to accidental process death and binary rebuilds. They are optional — `./migrate -mode=console` alone is enough to launch the UI.

- **[`run-console.sh`](run-console.sh)** — a supervisor that relaunches the console within ~2s if it exits for any reason, so the UI never gets stuck at "Failed to fetch". Logs to `/tmp/console.log`; override the binary with `MIGRATE_BIN`.
  ```bash
  go build -o migrate ./cmd/migrate
  ./run-console.sh :9090          # default binary ./migrate, default addr :9090
  ```
- **[`restart-console.sh`](restart-console.sh)** — safely restart the console to pick up a freshly rebuilt binary. It **refuses to restart while a migration is in flight** (a running job lives in the process's memory and would be lost); pass `--force` only if you knowingly accept discarding the running job.
  ```bash
  ./restart-console.sh            # refuses if a job is running
  ./restart-console.sh --force    # discard the running job and restart
  ```

> ⚠️ A migration job's runtime state lives in the console process's memory, so killing/restarting the process discards an in-flight job (re-launch it from the form). For a planned halt, use the **Pause** button in the UI rather than killing the process.

### Phase 1 — Assessment & Remediation (read-only)

The console first runs a **read-only** scan of the source and target and produces a pre-migration health report plus a tuning recommendation. **The source database is never modified during assessment.**

Findings are graded by severity, which determines what the UI offers:

| Severity | Meaning | Console action |
|----------|---------|----------------|
| **A** (auto-fix) | The migrator handles it automatically at write time (e.g. invalid `_id` type conversion, empty/`__x__` field-name rewrite, over-long nested field-name stringify) | Shown as "fixed automatically during migration / no action needed" — **no button** |
| **B** (blocking) | Will block that collection's migration; must be handled | "Choose fix" button when a remediation is available, otherwise "manual handling required" |
| **C** (warning) | Non-blocking advisory | Same as B: choose a fix or handle manually |

Remediation is **opt-in and decoupled from scanning**:

1. **Register intent:** clicking "Choose fix" records the choice into `remediation-plan.json` and re-draws that row locally — it does **not** re-scan or touch the source.
2. **Re-detect (user-driven):** after selecting all desired fixes, the operator clicks "re-detect (simulate N selected fixes)" to re-scan and see what would remain unresolved.

All fixes are **plans** applied by the transform layer when writing to the target — the source documents are never altered. `remediation-plan.json` persists across runs; the headless `assess` mode honors it so the CLI re-check matches the console.

### Phase 2 — Migration & Live Progress

Choose the migration mode in the form:

- **`migrate`** — one-time full migration (initial load only).
- **`live`** — full migration followed by continuous change-stream / oplog replication.

Live progress is reported **per collection** (`db.collection`), never as a per-database rollup, with columns for phase (`initial` / `live` / `live-stopped`), progress, throughput, ETA, and replication lag (computed from oplog position, so an idle stream shows lag ≈ 0). **Pause / Resume / Stop** controls are always present and enabled per the running / paused / finished state; Stop truly interrupts in-flight work via context cancellation.

**Index handling:** the `_id` index (`{_id:1}`) is **always created** on every target collection (Firestore with MongoDB compatibility does not auto-create it). Secondary indexes are copied by default and can be turned off with the "replicate source secondary indexes" toggle. Indexes are always built **after** data (for `live`, once replication lag has caught up) to avoid per-insert index rebuilds on Firestore.

**Dead Letter Queue (non-blocking):** documents that fail basic transformation (oversized fields, too-deep nesting, reserved keys/collection names, etc.) are written to a DLQ with the detailed error reason while the rest of the migration proceeds. The failed count surfaces as a red "DLQ N" chip in the task header; the panel breaks it down per collection with expandable document snapshots. After fixing the data offline, recycle the DLQ:

- **Replay snapshots** (default) — rewrites the stored DLQ document snapshots to the target (best for config/rule fixes).
- **Resync from source** — re-reads each failed document fresh from the source by `_id`, picking up a source-side fix; source-deleted docs are treated as resolved. Triggered from the DLQ panel, or on the CLI via `./migrate -mode=retry-dlq -dlq-resync-from-source`.

### Phase 3 — Verification

After the load completes, the "verify migration result" button compares **document counts** for every source→target collection, with an optional "also compare content hash (slow)" checkbox. Documents whose `_id` was converted during migration are reconciled through `id-mapping.jsonl`, so a converted `_id` matches instead of being falsely reported as a mismatch.

> The full UI interaction contract is documented in [`CONSOLE_UI_DESIGN.md`](CONSOLE_UI_DESIGN.md).

### Headless Assessment & Verification (CLI)

The same assessment and verification are available without the browser, for scripting and CI:

```bash
# Read-only pre-migration assessment; exits non-zero if any hard-blocking issue is found.
./migrate -mode=assess

# Post-migration verification (counts only); exits non-zero on any discrepancy.
./migrate -mode=verify

# Also compare content hashes and reconcile converted _ids.
./migrate -mode=verify -verify-hash -id-map=id-mapping.jsonl
```

The interactive configuration wizard generates a config file and exits:

```bash
./migrate -mode=wizard          # writes mongodb_replication_config.json (or the -config path)
```

## Migrating to Firestore

When the target is *Firestore with MongoDB compatibility*, a few target-specific notes apply. The web console's form handles all of these for you; they matter mainly when hand-writing a config file (see [docs/CONFIGURATION.md](docs/CONFIGURATION.md)):

- **Connection string:** use the Firestore MongoDB-compatibility endpoint and append `retryWrites=false` (Firestore does not support retryable writes). The exact endpoint format is in [`使用说明.md`](使用说明.md).
- **Indexes:** the `_id` index is always created on each target collection; secondary index builds are serialized (`indexConcurrency: 1`) to avoid cross-transaction contention, and are built after data.
- **`_id` compatibility:** invalid `_id` types are converted automatically at write time (`convertInvalidIds`, on by default) and reconciled during verification via `id-mapping.jsonl`.
- **Billing:** Firestore is a billed Google Cloud service. Remember to tear down any test target and its data when you're done.

For the full parameter reference (config file, all CLI modes and flags, tuning/index/checkpoint/retry internals, project structure), see **[docs/CONFIGURATION.md](docs/CONFIGURATION.md)**.
