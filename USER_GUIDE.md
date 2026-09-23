# MongoDB to Firestore Migration Setup & Run Guide

This guide compiles the VM configuration, pre-flight performance benchmarking, and operational runbook instructions for running the Go-based MongoDB migration and replication tool in **`live`** mode. This mode captures a change stream watermark, performs an initial backfill to the destination, and continuously applies live CDC changes.

> **Note**: The tool can also run a one-time offline migration (`--mode=migrate`) or a decoupled replication (`--mode=capture-resume-token` followed by `--mode=live-only`).

---

## 1. Migration VM & Infrastructure Setup

### VM Specifications
- **Machine Type**: `n2-highmem-64` (64 vCPUs, 256 GB Memory) — recommended to handle multi-level parallelism during incremental replication.
- **OS Image**: `Ubuntu 24.04 LTS` or above
- **Boot Disk**: `500 GB SSD persistent disk`
- **High Performance Networking**: `total_egress_bandwidth_tier = "TIER_1"`

```hcl
machine_type = "n2-highmem-64"
boot_disk {
  initialize_params {
    image = "ubuntu-os-cloud/ubuntu-2404-lts-amd64"
    size  = 500
    type  = "pd-ssd"
  }
}

# UNLOCKS HIGH PERFORMANCE NETWORKING
network_performance_config {
  total_egress_bandwidth_tier = "TIER_1"
}
```

### Network Configuration & Firewall Rules
- **Source MongoDB (e.g., MongoDB Atlas)**: The VM requires outbound access to port `27017` (or custom Atlas port). Ensure the VM's external IP is added to the source IP allowlist.
- **Target Firestore**: Hosted as a standard Google API on port `443` (HTTPS), accessible by default on GCE via standard outbound internet access.
- **Inbound SSH**: Allow SSH for administration via GCP Identity-Aware Proxy (IAP) `35.235.240.0/20` or your organization's secure SUP ranges.

### Software Setup & Tool Build
```bash
# 1. Install Git and Go 1.21+
sudo apt-get update && sudo apt-get install -y git golang-go

# 2. Clone and build the migration tool (oplog branch)
git clone -b oplog https://github.com/gsbingo17/mongodb-to-new.git
cd mongodb-to-new
go mod tidy && go build -o migrate ./cmd/migrate
```

---

## 2. Migration Tool Configuration

Create `mongodb_replication_config.json` in the tool directory:
- **Target Firestore**: Find connection string in Google Cloud Console → **Firestore database → Security** page.
- **Source MongoDB**: Append `readPreference=secondary` to read from a secondary replica if available, improving throughput and offloading the primary.

### Production Configuration Template

```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "<CONNECTION_STRING>&readPreference=secondary",
        "database": "<SOURCE_DATABASE>"
      },
      "target": {
        "connectionString": "<TARGET_FIRESTORE_CONNECTION_STRING>",
        "database": "<TARGET_DATABASE>",
        "syncAllIndexes": false,
        "indexOnly": false,
        "collections": [
          {
            "sourceCollection": "<SOURCE_COLLECTION>",
            "targetCollection": "<TARGET_COLLECTION>"
          }
        ]
      }
    }
  ],
  "concurrentCollections": 1,
  "saveThreshold": 1000000,
  "forceOrderedOperations": false,
  "checkpointIntervalMinutes": 5,
  "flushIntervalMs": 1,
  "incrementalReadBatchSize": 12000,
  "incrementalWriteBatchSize": 1,
  "incrementalWorkerCount": 25000,
  "incrementalIncomingQueueSize": 16384,
  "incrementalProcessingQueueSize": 8192,
  "incrementalStreamPartitions": 40,
  "targetMinPoolSize": 25000,
  "targetMaxPoolSize": 25000,
  "groupOpsByDistinctId": true,
  "statsIntervalMinutes": 1,

  "parallelReadsEnabled": true,
  "initialReadBatchSize": 10000,
  "initialWriteBatchSize": 128,
  "initialChannelBufferSize": 64,
  "initialMigrationWorkers": 32,
  "maxReadPartitions": 64,
  "workersPerPartition": 16,
  "sampleSize": 100000,

  "backfillRampUp": {
    "enabled": true,
    "strategy": "adaptive",
    "startQps": 60000.0,
    "rampRatePerMin": 60000.0,
    "updateIntervalMs": 1000,
    "workerDelayMs": 0
  }
}
```

---

## 3. Pre-Flight Performance & Sizing Tests

Before testing, clear any saved state from previous runs:

```bash
# Caution: DO NOT run if you have already captured active migration resume tokens!
rm -f resumeToken* initialMigrationState* dlq* backfillCheckpoint*
```

### Benchmark Commands

| Test Objective | Mode & Command | Notes / Guidelines |
|---|---|---|
| **Pure Read Throughput** (at "Now") | `./migrate --config=mongodb_replication_config.json --mode=live-only -dry-run` | Measures change stream read throughput in isolation. Target read rate $\ge 4\times$ average change rate (e.g. 25K events/sec with 16 partitions for 6K events/sec change rate). |
| **Historical Replay** ($t-12\text{h}$ or $t-24\text{h}$) | `./migrate -mode=live-only -dry-run -live-start-timestamp="2026-06-01T06:11:47+05:30"` | Verifies read throughput when reading older oplog segments. |
| **Replication Lag Validation** | `./migrate --config=mongodb_replication_config.json --mode=live-only` | Runs against destination Firestore for **20 minutes** to measure steady-state lag without backfill. |

### Sample Benchmark Log Output
```text
INFO[2026-05-28T01:44:20Z] Change stream statistics in dry-run live-only mode (last 1m0s):
  - Read: 1720261 (28671.11 events/sec) | Global Next Latency: avg 139µs | Event Size: avg 1477.9 bytes (total 2.37 GB)
  - Connection Pool: Source Open: 384, In-Use: 4 | Target Open: 127, In-Use: 0
```

### Testing Oplog Retention Window
Run in `mongosh` on the source cluster to find the earliest readable oplog event:
```javascript
db.aggregate([{$changeStream: {startAtOperationTime: Timestamp()}}])
```
Note the `clusterTime` and `wallTime` of the first batch to ensure your backfill duration comfortably fits within the oplog retention window.

---

## 4. Live Migration Runbook

### Pre-Migration Verification & State Cleanup
If starting a fresh migration, clear state files from previous runs:
- **DLQ Files (`dlq*.json`)**: Must be removed; the tool refuses to start if active failures exist.
- **Initial Migration State (`initialMigrationState*.json`)**: Must be removed; otherwise the tool assumes backfill is complete and bypasses it.
- **Resume Tokens (`resumeToken-*.json`)**: Remove for fresh unified migration. *(DO NOT remove if running decoupled pipeline after `capture-resume-token`)*.
- **Backfill Checkpoints (`backfillCheckpoint-*.json`)**: Remove only to force a backfill from scratch. If left, the tool automatically resumes from where it stopped.

### Execution Pipelines

#### Option A: Unified Pipeline (`--mode=live`)
Runs all phases sequentially in a single automated process:
```bash
./migrate --config=mongodb_replication_config.json --mode=live
```
1. **Watermark Capture**: Captures starting change stream resume token (`resumeToken-*.json`).
2. **Index Sync**: Replicates index definitions to target collections.
3. **Data Backfill**: Parallel scan of source collections with duplicate key checks to safely skip records concurrently modified by live writes. Progress is saved to `backfillCheckpoint-*.json`.
4. **Live Replication (CDC)**: Upon backfill completion with zero errors, automatically transitions into real-time streaming from the captured watermark.

#### Option B: Decoupled Pipeline
Use when backfill is managed externally:
```bash
# 1. Capture watermark immediately before backfill
./migrate --config=mongodb_replication_config.json --mode=capture-resume-token

# 2. Run backfill via external tool or offline job

# 3. Start live replication from the captured watermark
./migrate --config=mongodb_replication_config.json --mode=live-only
```

### Reprocessing DLQ Failures
If backfill completes with failures, it enters `CompletedWithFailures` and halts:
```bash
./migrate --config=mongodb_replication_config.json --mode=retry-dlq
```
Once `failedCount == 0`, state updates to `Completed`. Restart `--mode=live` to proceed to live replication.

### Final Cutover & Verification
1. Monitor live replication stats until lag is near-zero.
2. **Pause write traffic on the source database.**
3. Allow live replicator to drain remaining changes until replication lag reaches `0`, then stop (`Ctrl+C`).
4. Reprocess any final DLQ records:
   ```bash
   ./migrate --config=mongodb_replication_config.json --mode=retry-dlq
   ```
5. Verify document counts match between source and target:
   ```bash
   mongosh "mongodb://<host>:<port>/<db>?socketTimeoutMS=720000" --quiet --eval "db.<collection>.countDocuments()"
   ```
6. Direct client application traffic to the target Firestore database.

---

## 5. Handling Interruptions with Active DLQ Failures

### Scenario A: Interrupted during Backfill
- **Automated DLQ Backup**: Restarting `--mode=live` automatically backs up the active DLQ to `dlq.json.backup-<timestamp>`.
- **Option 1: Resume from Checkpoints**: Restart `./migrate --config=mongodb_replication_config.json --mode=live`. Resumes from the last saved checkpoint per partition.
- **Option 2: Fresh Restart from Scratch**: Delete `backfillCheckpoint-*.json` before restarting `--mode=live` to re-scan from scratch.

### Scenario B: Interrupted during Replication
- **Automatic Resume**: Since backfill is marked `Completed`, restarting `--mode=live` skips backfill and resumes streaming from the last watermark.
- **Manual DLQ Cleanup**: Replicator will not auto-reprocess historical DLQ on startup. Run manually:
  ```bash
  ./migrate --config=mongodb_replication_config.json --mode=retry-dlq
  ```
> [!CAUTION]
> Do **NOT** manually delete the DLQ file during replication. Since backfill will not rerun, deleting the DLQ causes **permanent data loss** for those records.

---

## 6. Scaling and Tuning Parameters

### Scaling Change Stream Partitions
You can change the partition count without losing data:
1. Stop the replicator (`Ctrl+C`).
2. Adjust `"incrementalStreamPartitions": <new_count>` in `mongodb_replication_config.json`.
3. Restart `./migrate --config=mongodb_replication_config.json --mode=live`.
4. The tool automatically resolves the oldest timestamp among all existing partition checkpoints (safe minimum watermark), initializes the new partitions with it, cleans up old checkpoints, and resumes replication safely.

### Adjusting Initial Backfill Parameters
You can adjust `workersPerPartition`, `maxReadPartitions`, and `parallelReadsEnabled` between restarts. The tool resolves safe progress boundaries per BSON type and resumes without data duplication.

---

## 7. Monitoring Statistics Logs Reference

The tool periodically logs rolling metrics blocks (configured via `statsIntervalMinutes`).

### Initial Backfill Statistics
```text
Initial Backfill statistics (duration: 30s):
  - Progress:         45.2% (45200 / 100000 docs) [Remaining: 36s]
  - Throttler:        Active (Limit: 5000.00 QPS)
  - Ingestion Stalls: [Cursor blocked waiting for workers: 12ms]
  - Read:             12500 (416.67 docs/sec) | Processed: 12000 (400.00 docs/sec)
  - BulkWrite Latency: [p50: 12ms, p90: 45ms, p99: 120ms, p100: 350ms] (avg: 18ms)
  - Errors:           Duplicate Key Errors: 1000 (33.33/sec) | DLQ'ed: 50 (Resolved: 0)
```
- **Progress**: Percent complete and estimated remaining time.
- **Skipped Duplicates**: Expected when resuming or handling concurrent live writes.
- **Ingestion Stalls**: If cursor blocked time is high (`> 500ms`), target writes are bottlenecked (backpressure).
- **BulkWrite Latency**: If p99 exceeds 500ms, target database is under heavy load.
- **Sequential Retries**: If high, batches are failing and splitting into individual writes, degrading throughput.

### Live Replication (CDC) Statistics
```text
Change stream statistics (last 30s):
  - Read:           340 (11.33 events/sec) | Processed: 340 (11.33 events/sec)
  - Lags:           Event-to-Read: 120ms | End-to-end: 159ms
  - Worker QPS:     [Active: 4] [p50: 2.50, p90: 3.00, p99: 3.50]
  - Group Flushes:  [batchfull: 8 (0.27/sec), timeout: 0 (0.00/sec)]
  - Errors:         DLQ'ed: 0 (Resolved: 0) [Active Failed: 0]
```
- **End-to-End Lag**: Delay from source mutation to target apply. Should remain stable at steady state (typically low single-digit seconds).
- **Lags Rising Steadily**: Target write rate is lower than source mutation rate; scale target capacity or increase `incrementalStreamPartitions`.
- **DLQ'ed / Active Failed**: If non-zero, operations are actively failing. Investigate DLQ immediately.
