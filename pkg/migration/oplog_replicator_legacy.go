package migration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"github.com/gsbingo17/mongodb-migration/pkg/db"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"github.com/gsbingo17/mongodb-migration/pkg/metrics"
	"github.com/rwynn/gtm"
	modernbson "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mongod "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// OplogReplicatorLegacy handles replication using oplog tailing via GTM legacy library with mgo
type OplogReplicatorLegacy struct {
	sourceDB          *db.MongoDBLegacy
	targetDB          *db.MongoDB
	config            *config.Config
	log               *logger.Logger
	collectionMap     map[string]map[string]string                  // Map of database -> source collection -> target collection
	collectionConfigs map[string]map[string]config.CollectionConfig // Map of database -> source collection -> full config
	mu                sync.Mutex                                    // Mutex for thread-safe operations
	dlq               DLQ                                           // Dead Letter Queue for failed documents
	retryManager      *RetryManager                                 // Retry manager for transient errors
	transformer       *FieldTransformer                             // Field transformer
	DryRun            bool                                          // Dry run flag
	migrator          *Migrator                                     // Back-reference for console progress reporting (optional)
	lastInitialReport map[string]time.Time                          // Per-collection throttle for initial-load console updates
	initialStart      map[string]time.Time                          // Per-collection start time for initial-load rate calc
	liveEvents        int64                                         // Cumulative oplog events applied (all collections; atomic)
	lastEventUnix     int64                                         // Oplog seconds of the most recent applied event (for lag; atomic)
	liveDBName        string                                        // Source database name for the console live row
	liveStats         sync.Map                                      // Per-collection live stats: namespace "db.coll" -> *liveCollStat
	pair              config.DatabasePair                           // The pair being replicated (needed for the deferred index build)
	deferredIndex     *DeferredIndexController                      // Shared "build indexes after load, once lag settles" controller
}

// liveCollStat holds the incremental (增量) counters for a single collection so
// the console can render one live row per collection instead of one aggregate
// row per database. All numeric fields are updated atomically.
type liveCollStat struct {
	db            string
	coll          string
	events        int64 // Cumulative oplog events applied to this collection (atomic)
	lastEventUnix int64 // Oplog seconds of the most recent event for this collection (atomic; for lag)
}

// NewOplogReplicatorLegacy creates a new oplog-based replicator using GTM legacy
func NewOplogReplicatorLegacy(sourceDB *db.MongoDBLegacy, targetDB *db.MongoDB, cfg *config.Config, log *logger.Logger) *OplogReplicatorLegacy {
	return &OplogReplicatorLegacy{
		sourceDB:          sourceDB,
		targetDB:          targetDB,
		config:            cfg,
		log:               log,
		collectionMap:     make(map[string]map[string]string),
		collectionConfigs: make(map[string]map[string]config.CollectionConfig),
		transformer:       NewFieldTransformer(cfg.DropEmptyFieldNames, cfg.ConvertLongFieldNamesInNestedDocs, cfg.RetryConfig.ConvertInvalidIds, log),
	}
}

// SetDLQ sets the Dead Letter Queue writer for this replicator
func (r *OplogReplicatorLegacy) SetDLQ(dlq DLQ) {
	r.dlq = dlq
}

// AddCollection adds a collection to be watched
func (r *OplogReplicatorLegacy) AddCollection(sourceDB, targetDB string, collConfig config.CollectionConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.collectionMap[sourceDB] == nil {
		r.collectionMap[sourceDB] = make(map[string]string)
	}
	if r.collectionConfigs == nil {
		r.collectionConfigs = make(map[string]map[string]config.CollectionConfig)
	}
	if r.collectionConfigs[sourceDB] == nil {
		r.collectionConfigs[sourceDB] = make(map[string]config.CollectionConfig)
	}

	r.collectionMap[sourceDB][collConfig.SourceCollection] = collConfig.TargetCollection
	r.collectionConfigs[sourceDB][collConfig.SourceCollection] = collConfig
	r.log.Infof("Added collection mapping: %s.%s -> %s.%s (UpsertMode: %t)",
		sourceDB, collConfig.SourceCollection, targetDB, collConfig.TargetCollection, collConfig.UpsertMode)
}

// getCurrentOplogTimestamp gets the current oplog timestamp from the oplog collection
func (r *OplogReplicatorLegacy) getCurrentOplogTimestamp() (*primitive.Timestamp, error) {
	// Get mgo session
	session := r.sourceDB.GetSession()
	defer session.Close()

	// Access the local.oplog.rs collection
	oplogCollection := session.DB("local").C("oplog.rs")

	// Find the most recent oplog entry
	var oplogEntry bson.M
	err := oplogCollection.Find(nil).Sort("-$natural").Limit(1).One(&oplogEntry)
	if err != nil {
		return nil, fmt.Errorf("failed to query oplog: %w", err)
	}

	// Extract timestamp from oplog entry
	// The 'ts' field in oplog is a BSON Timestamp (bson.MongoTimestamp in mgo)
	tsValue, ok := oplogEntry["ts"]
	if !ok {
		return nil, fmt.Errorf("oplog entry missing 'ts' field")
	}

	mongoTs, ok := tsValue.(bson.MongoTimestamp)
	if !ok {
		return nil, fmt.Errorf("oplog 'ts' field is not a MongoTimestamp, got %T", tsValue)
	}

	// Convert bson.MongoTimestamp (int64) to primitive.Timestamp
	// High 32 bits are seconds (T), low 32 bits are increment (I)
	t := uint32(mongoTs >> 32)
	i := uint32(mongoTs & 0xFFFFFFFF)

	timestamp := &primitive.Timestamp{T: t, I: i}
	return timestamp, nil
}

// StartReplication starts the oplog-based replication using GTM legacy
func (r *OplogReplicatorLegacy) StartReplication(ctx context.Context, globalTimestamp interface{}, timestampPath string, initialMigrationState *InitialMigrationState, initialMigrationStatePath string, pair config.DatabasePair, liveOnly bool, fullOnly bool, liveStartTime *primitive.Timestamp, migrator *Migrator) error {
	// Keep a back-reference so the initial-load and oplog-tailing paths can push
	// per-collection progress to the web console (no-op when unattached).
	r.migrator = migrator
	r.liveDBName = pair.Source.Database
	r.pair = pair

	// Shared controller for the "indexes are built after the load, once replication
	// lag has settled" rule — identical code to the modern change-stream path so the
	// two can never drift apart again. The injected build primitive is the legacy
	// _id+secondary index sync.
	r.deferredIndex = NewDeferredIndexController(migrator, r.targetDB, r.config, r.log, func(ctx context.Context) {
		r.syncIndexesLegacy(ctx, pair)
	})

	// Stream deferred index-build progress ("创建索引 M/N") to the console for the
	// whole run. Cheap no-op when detached; only emits once a build has launched.
	if r.migrator != nil && r.migrator.controlPlaneAttached() {
		go r.deferredIndex.PollProgress(ctx)
	}

	// Abort if the initial migration state was completed with failures, or if DLQ has entries
	if initialMigrationState != nil && initialMigrationState.Status == StatusCompletedWithFailures {
		return fmt.Errorf("cannot start replication: initial migration completed with failures in a previous run")
	}
	if r.dlq != nil {
		if _, isNop := r.dlq.(*NopDLQWriter); !isNop {
			if r.dlq.Count() > 0 {
				return fmt.Errorf("cannot start replication: DLQ contains failed documents")
			}
		}
	}

	// Enforce safety invariants between Initial Migration State and Oplog Timestamp Checkpoint
	if liveStartTime != nil && globalTimestamp != nil {
		return fmt.Errorf("safety violation: a custom live-start-timestamp is specified, but a global oplog timestamp checkpoint already exists. Clean up checkpoint file or omit live-start-timestamp to resume from the last checkpoint")
	}

	if liveStartTime == nil {
		if initialMigrationState == nil {
			if globalTimestamp != nil {
				return fmt.Errorf("safety violation: initial migration state file does not exist, but a global oplog timestamp checkpoint exists. Clean up checkpoint file or ensure state is in sync before proceeding")
			}
		} else if initialMigrationState.Status == StatusCompleted || initialMigrationState.Status == StatusSkipped {
			if globalTimestamp == nil {
				return fmt.Errorf("safety violation: initial migration state is marked as %s, but no global oplog timestamp checkpoint was found. Clean up state file or restore checkpoint before proceeding", initialMigrationState.Status)
			}
		}
	}

	var needsInitialMigration bool
	var afterTimestamp bson.MongoTimestamp

	// We need to run initial migration if no state file exists OR if it is not marked completed
	if initialMigrationState == nil || !initialMigrationState.IsCompleted() {
		if liveOnly {
			r.log.Info("Live-only mode enabled. Skipping initial migration phase.")
			if err := SaveInitialMigrationState(initialMigrationStatePath, StatusSkipped, 0); err != nil {
				r.log.Errorf("Error saving initial migration state as skipped: %v", err)
			}
			needsInitialMigration = false
			initialMigrationState = &InitialMigrationState{
				Status: StatusSkipped,
			}
		} else {
			needsInitialMigration = true
		}
	}

	// Load saved timestamp if exists
	var savedTimestamp *OplogTimestamp
	if globalTimestamp != nil {
		if ts, ok := globalTimestamp.(*OplogTimestamp); ok {
			savedTimestamp = ts
		} else if tsMap, ok := globalTimestamp.(map[string]interface{}); ok {
			if t, ok := tsMap["t"].(float64); ok {
				// Only have T component, construct MongoTimestamp
				afterTimestamp = bson.MongoTimestamp(int64(t) << 32)
				savedTimestamp = &OplogTimestamp{}
			}
		}
	}

	// If no saved legacy timestamp checkpoint is found on disk:
	// - If a custom liveStartTime was supplied, initialize the legacy Tail afterTimestamp
	//   by shifting the seconds (T) 32 bits to the left and bitwise-ORing with the increment (I).
	// - Otherwise, fallback to fetching the current oplog timestamp from the source DB.
	if savedTimestamp == nil {
		if liveStartTime != nil {
			r.log.Infof("Using user-provided liveStartTime: %s", time.Unix(int64(liveStartTime.T), 0).UTC().Format(time.RFC3339))
			initialOplogTimestamp := &primitive.Timestamp{T: liveStartTime.T, I: liveStartTime.I}
			savedTimestamp = &OplogTimestamp{Timestamp: *initialOplogTimestamp}
			// Legacy bson.MongoTimestamp is represented as a 64-bit int where the upper 32 bits
			// are the Unix epoch seconds and the lower 32 bits are the increment counter.
			afterTimestamp = bson.MongoTimestamp((int64(liveStartTime.T) << 32) | int64(liveStartTime.I))
		} else {
			if liveOnly {
				r.log.Info("No oplog timestamp found in live-only mode. Obtaining current oplog position to start incremental replication.")
			} else {
				r.log.Info("No oplog timestamp found. Will get current oplog position.")
			}

			// Get current oplog timestamp BEFORE initial migration to prevent data loss
			// This follows the same pattern as change stream mode
			currentOplogTimestamp, err := r.getCurrentOplogTimestamp()
			if err != nil {
				return fmt.Errorf("failed to get current oplog timestamp: %w", err)
			}

			r.log.Infof("Obtained current oplog timestamp before migration: T=%d, I=%d",
				currentOplogTimestamp.T, currentOplogTimestamp.I)

			// Save this timestamp BEFORE performing initial migration
			initialTimestamp := OplogTimestamp{
				Timestamp: *currentOplogTimestamp,
			}
			if err := SaveOplogTimestamp(timestampPath, initialTimestamp); err != nil {
				r.log.Errorf("Error saving initial oplog timestamp: %v", err)
			} else {
				r.log.Info("Saved initial oplog timestamp before migration")
			}

			// Convert timestamp to bson.MongoTimestamp for GTM
			// MongoTimestamp is int64 where high 32 bits are T (seconds), low 32 bits are I (increment)
			afterTimestamp = bson.MongoTimestamp((int64(currentOplogTimestamp.T) << 32) | int64(currentOplogTimestamp.I))
		}
	} else {
		// Use saved timestamp - properly combine T and I components
		afterTimestamp = bson.MongoTimestamp((int64(savedTimestamp.Timestamp.T) << 32) | int64(savedTimestamp.Timestamp.I))
	}

	// Create retry manager from config
	r.retryManager = NewRetryManagerFromConfig(r.config, r.log)

	// Perform initial migration if needed
	if needsInitialMigration {
		// Mark initial migration state as incomplete before starting
		if err := SaveInitialMigrationState(initialMigrationStatePath, StatusInProgress, 0); err != nil {
			r.log.Errorf("Error saving initial migration state as incomplete: %v", err)
		}

		_, totalFailedCount, err := r.performInitialMigration(ctx, pair, migrator)
		if err != nil {
			return fmt.Errorf("initial migration failed: %w", err)
		}

		// A cancelled context means the run was INTERRUPTED, not finished. The
		// un-migrated remainder gets counted as "failed" (batchSize - succeeded)
		// but is NOT written to the DLQ (cancellation is not a per-document
		// error). Marking that as completed_with_failures would ban the whole
		// database on the next run with an empty DLQ and no recovery path. Leave
		// the state in-progress so a re-run cleanly re-migrates it.
		if ctx.Err() != nil {
			r.log.Warnf("Initial migration for %s interrupted (context cancelled); ~%d documents remain un-migrated. Leaving state in-progress for a clean re-run.",
				pair.Source.Database, totalFailedCount)
			return ctx.Err()
		}

		// Determine the terminal status. completed_with_failures is NON-BLOCKING:
		// failed documents are captured in the DLQ with their error reason and
		// recovered later via retry-dlq; every other document migrated normally
		// and replication proceeds. (Shared across all replication paths.)
		status, _ := resolveInitialMigrationOutcome(r.dlq, totalFailedCount, r.log)

		// Mark initial migration state as complete
		if err := SaveInitialMigrationState(initialMigrationStatePath, status, totalFailedCount); err != nil {
			r.log.Errorf("Error saving initial migration state as complete: %v", err)
		}

		r.log.Info("Initial migration completed. Starting incremental replication.")
	} else {
		r.log.Info("Initial migration already marked as completed. Skipping.")
	}

	// Index-Only mode: sync indexes (if not already done during initial migration) and exit.
	// Always syncs when the checkpoint already existed — performInitialMigration was
	// skipped, so no indexes (not even _id) have been built yet. syncIndexesLegacy
	// always creates _id regardless of SyncAllIndexes, so this runs unconditionally.
	if pair.Target.IndexOnly {
		if !needsInitialMigration {
			// Checkpoint exists, so performInitialMigration was skipped — sync indexes now
			r.log.Info("IndexOnly mode: checkpoint exists, performing index sync directly")

			// Configure index build concurrency before launching any async builds
			if r.config.IndexConcurrency > 0 {
				r.targetDB.SetIndexConcurrency(r.config.IndexConcurrency)
			}

			r.syncIndexesLegacy(ctx, pair)
			r.log.Info("IndexOnly mode: waiting for all async index creation to complete...")
			r.targetDB.WaitForIndexCreation()
			migrator.logFailedIndexes(r.targetDB)
		}
		r.log.Info("IndexOnly mode: skipping oplog tailing. Index replication complete.")
		return nil
	}

	// Full-only (migrate) mode: the initial full load is done; stop here without
	// tailing the oplog. This is a terminal, one-shot copy — writes made to the
	// source during/after the scan are NOT captured (use live mode for those).
	//
	// Indexes are built NOW, after the full load — never before or during it.
	// Building indexes up front makes Firestore re-index on every inserted
	// document, which is far slower than a single build over the finished data.
	// syncIndexesLegacy always creates _id (independent of SyncAllIndexes) and is
	// idempotent, so this is safe on a fullOnly re-run over an existing checkpoint.
	if fullOnly {
		r.log.Info("Full-only mode: initial migration complete. Building indexes now (after full load).")
		// Same shared "build after load" primitive the live/modern paths use.
		// Synchronous: the one-shot job must not report done until indexes exist.
		r.deferredIndex.BuildNow(ctx)
		r.log.Info("Full-only mode: indexes complete. Skipping oplog tailing (no incremental replication).")
		return nil
	}

	// Start oplog tailing using GTM legacy
	return r.tailOplog(ctx, afterTimestamp, timestampPath)
}

// performInitialMigration performs the initial migration using mgo for source
func (r *OplogReplicatorLegacy) performInitialMigration(ctx context.Context, pair config.DatabasePair, migrator *Migrator) (int64, int64, error) {
	initialMigrationStart := time.Now()
	r.log.Info("Performing initial migration for all collections")

	// Index-Only mode: build indexes (including _id) and return without migrating
	// data. syncIndexesLegacy always creates _id regardless of SyncAllIndexes, so
	// this runs unconditionally.
	if pair.Target.IndexOnly {
		r.log.Info("IndexOnly mode: syncing indexes only, skipping data migration (legacy mode)")
		if r.config.IndexConcurrency > 0 {
			r.targetDB.SetIndexConcurrency(r.config.IndexConcurrency)
		}
		r.syncIndexesLegacy(ctx, pair)
		r.log.Info("IndexOnly mode: waiting for all async index creation to complete...")
		r.targetDB.WaitForIndexCreation()
		migrator.logFailedIndexes(r.targetDB)
		r.log.Info("IndexOnly mode: all indexes synced successfully. Skipping data migration.")
		return 0, 0, nil
	}

	// NOTE: Secondary and _id indexes are intentionally NOT built here. Creating
	// indexes before (or during) the full load forces Firestore to re-index on
	// every inserted document. Instead the build happens AFTER the load:
	//   - full-only mode: immediately after this returns (see StartReplication)
	//   - live mode:      deferred until replication lag settles (see reportLiveLoop)

	// Use ConcurrentCollections for collection-level concurrency (separate from per-collection worker count)
	concurrentCollections := r.config.ConcurrentCollections
	if concurrentCollections <= 0 {
		concurrentCollections = 4
	}
	r.log.Infof("Processing up to %d collections concurrently", concurrentCollections)
	semaphore := make(chan struct{}, concurrentCollections)
	var wg sync.WaitGroup

	var totalMigratedCount int64
	var totalFailedCount int64
	var completedCollections int64
	var mu sync.Mutex

	// Pre-compute total collection count before launching goroutines
	// so the progress log always shows the correct total
	totalCollections := 0
	for _, colls := range r.collectionConfigs {
		totalCollections += len(colls)
	}

	for sourceDB, collections := range r.collectionConfigs {
		for sourceCollection, collConfig := range collections {
			wg.Add(1)

			semaphore <- struct{}{}

			go func(sourceDB, sourceCollection string, collConfig config.CollectionConfig) {
				defer wg.Done()
				defer func() { <-semaphore }()

				// Apply an operator-approved rename-collection remediation (e.g. the
				// Firestore-reserved __x__ → _x_) so the full load lands on the SAME
				// legal target name the index build and the incremental workers use.
				// Without this, reserved-named source collections write straight to the
				// illegal name and Firestore rejects every document. Idempotent: a name
				// that needs no rename is returned unchanged.
				targetCollection := collConfig.TargetCollection
				if r.transformer != nil {
					targetCollection = r.transformer.SanitizeTargetName(sourceDB, sourceCollection, targetCollection)
				}
				r.log.Infof("Starting initial migration for %s.%s to %s (UpsertMode: %t)",
					sourceDB, sourceCollection, targetCollection, collConfig.UpsertMode)

				// Get source collection using mgo
				sourceCol := r.sourceDB.GetCollection(sourceCollection)
				defer sourceCol.Database.Session.Close()

				// Get target collection using modern driver
				targetCol := r.targetDB.GetCollection(targetCollection)

				// Count documents
				count, err := sourceCol.Count()
				if err != nil {
					r.log.Errorf("Error counting documents in %s.%s: %v", sourceDB, sourceCollection, err)
					return
				}

				r.log.Infof("Found %d documents to migrate in %s.%s", count, sourceDB, sourceCollection)

				if count == 0 {
					r.log.Infof("No documents to migrate for %s.%s", sourceDB, sourceCollection)
					return
				}

				successCount, failedCount := r.migrateCollection(ctx, sourceCol, targetCol, count, sourceDB, sourceCollection)

				// Update overall statistics and log overall progress
				mu.Lock()
				totalMigratedCount += successCount
				totalFailedCount += failedCount
				completedCollections++
				r.log.Infof("Overall progress: %d/%d collections completed", completedCollections, totalCollections)
				mu.Unlock()
			}(sourceDB, sourceCollection, collConfig)
		}
	}

	wg.Wait()

	initialMigrationDuration := time.Since(initialMigrationStart)
	totalAttempted := totalMigratedCount + totalFailedCount
	var failurePercentage float64
	if totalAttempted > 0 {
		failurePercentage = (float64(totalFailedCount) * 100.0) / float64(totalAttempted)
	}
	r.log.Infof("Initial migration completed in %.2f seconds. Total collections: %d, Total documents: %d (Success: %d, Failed: %d, Failure Rate: %.2f%%)",
		initialMigrationDuration.Seconds(), totalCollections, totalAttempted, totalMigratedCount, totalFailedCount, failurePercentage)

	return totalMigratedCount, totalFailedCount, nil
}

// migrateCollection migrates a single collection from mgo source to modern driver target.
// It includes cursor resumption logic: if the cursor becomes invalid (e.g., server-side timeout),
// it re-queries from the last successfully read _id and continues the migration.
func (r *OplogReplicatorLegacy) migrateCollection(ctx context.Context, sourceCol *mgo.Collection, targetCol *mongod.Collection, count int, sourceDB, sourceCollection string) (int64, int64) {
	readBatchSize := r.config.InitialReadBatchSize
	writeBatchSize := r.config.InitialWriteBatchSize

	const maxCursorResumes = 10 // Maximum number of cursor resumption attempts

	var batch []interface{}
	var successCount int64
	var failedCount int64
	var lastLoggedPercentage int = -1 // Start at -1 to ensure 0% is logged
	var lastID interface{}            // Track last successfully read _id for cursor resumption
	var cursorResumeCount int

	for {
		// Build query: on first pass, read all documents sorted by _id.
		// On resumption, read from after the last successfully read _id.
		var query *mgo.Query
		if lastID == nil {
			query = sourceCol.Find(nil).Sort("_id").Batch(readBatchSize)
		} else {
			r.log.Infof("[%s.%s] Resuming cursor from _id=%v (resume attempt %d/%d)",
				sourceDB, sourceCollection, lastID, cursorResumeCount, maxCursorResumes)
			query = sourceCol.Find(bson.M{"_id": bson.M{"$gt": lastID}}).Sort("_id").Batch(readBatchSize)
		}

		iter := query.Iter()
		var doc bson.M
		cursorFailed := false

		for iter.Next(&doc) {
			// Track last _id for cursor resumption
			if id, ok := doc["_id"]; ok {
				lastID = id
			}

			// Convert mgo bson.M to interface{} for modern driver
			batch = append(batch, convertMgoBSONToInterface(doc))

			if len(batch) >= writeBatchSize {
				batchSize := int64(len(batch))
				succeeded := r.insertBatchWithRetry(ctx, targetCol, batch, sourceDB, sourceCollection)
				successCount += succeeded
				failedCount += batchSize - succeeded
				batch = nil

				// Log progress at every 10% threshold
				if count > 0 {
					currentCount := successCount + failedCount
					currentPercentage := int(float64(currentCount) / float64(count) * 10)
					if currentPercentage > lastLoggedPercentage {
						lastLoggedPercentage = currentPercentage
						r.log.Infof("Collection %s.%s progress: %d/%d documents (%.0f%%) - Successful: %d, Failed: %d",
							sourceDB, sourceCollection, currentCount, count, float64(currentPercentage)*10, successCount, failedCount)
					}
				}
				// Push a live progress row to the web console on every batch so the
				// 全量 (initial) bar advances smoothly (no-op for the CLI).
				r.reportInitialProgress(sourceDB, sourceCollection, int64(count), successCount+failedCount)
			}

			// Reset doc for next iteration
			doc = bson.M{}
		}

		// Check for cursor errors
		if err := iter.Err(); err != nil {
			currentCount := successCount + failedCount
			r.log.Warnf("[%s.%s] Cursor error after %d documents: %v", sourceDB, sourceCollection, currentCount, err)
			iter.Close()

			// Check if context is canceled
			if ctx.Err() != nil {
				r.log.Infof("[%s.%s] Context canceled, stopping migration", sourceDB, sourceCollection)
				break
			}

			// Attempt cursor resumption if we have a last _id and haven't exceeded max resumes
			cursorResumeCount++
			if lastID != nil && cursorResumeCount <= maxCursorResumes {
				r.log.Infof("[%s.%s] Will attempt cursor resumption from last _id=%v", sourceDB, sourceCollection, lastID)

				// Insert any pending batch before resuming
				if len(batch) > 0 {
					batchSize := int64(len(batch))
					succeeded := r.insertBatchWithRetry(ctx, targetCol, batch, sourceDB, sourceCollection)
					successCount += succeeded
					failedCount += batchSize - succeeded
					batch = nil
				}

				cursorFailed = true
			} else {
				currentCount = successCount + failedCount
				if cursorResumeCount > maxCursorResumes {
					r.log.Errorf("[%s.%s] Exceeded maximum cursor resume attempts (%d). Stopping migration at %d documents.",
						sourceDB, sourceCollection, maxCursorResumes, currentCount)
				} else {
					r.log.Errorf("[%s.%s] Cursor error with no last _id to resume from. Stopping migration at %d documents.",
						sourceDB, sourceCollection, currentCount)
				}
				break
			}
		} else {
			iter.Close()
		}

		// If cursor didn't fail, we've finished iterating successfully
		if !cursorFailed {
			break
		}
		// Otherwise, the loop continues with a new cursor from lastID
	}

	// Insert remaining documents
	if len(batch) > 0 {
		batchSize := int64(len(batch))
		succeeded := r.insertBatchWithRetry(ctx, targetCol, batch, sourceDB, sourceCollection)
		successCount += succeeded
		failedCount += batchSize - succeeded
	}

	totalCount := successCount + failedCount
	// Final progress row so the console bar lands on 100%.
	r.reportInitialProgress(sourceDB, sourceCollection, int64(count), totalCount)
	if failedCount > 0 {
		r.log.Warnf("Migration for %s.%s completed with %d failures! Successful: %d, Failed: %d, Total: %d",
			sourceDB, sourceCollection, failedCount, successCount, failedCount, totalCount)
	} else {
		r.log.Infof("Migration for %s.%s completed successfully! Total documents: %d",
			sourceDB, sourceCollection, totalCount)
	}
	return successCount, failedCount
}

// reportInitialProgress mirrors this collection's initial-load progress into the
// web console as a per-collection 全量 row, throttled to avoid flooding the
// registry. No-op when no console is attached.
func (r *OplogReplicatorLegacy) reportInitialProgress(sourceDB, sourceCollection string, total, done int64) {
	if r.migrator == nil || !r.migrator.controlPlaneAttached() {
		return
	}
	now := time.Now()
	key := sourceDB + "." + sourceCollection
	r.mu.Lock()
	if r.lastInitialReport == nil {
		r.lastInitialReport = make(map[string]time.Time)
		r.initialStart = make(map[string]time.Time)
	}
	if r.initialStart[key].IsZero() {
		r.initialStart[key] = now
	}
	start := r.initialStart[key]
	last, seen := r.lastInitialReport[key]
	// Throttle to ~1 update/sec per collection, but always let the final
	// (done==total) update through.
	if seen && done < total && now.Sub(last) < time.Second {
		r.mu.Unlock()
		return
	}
	r.lastInitialReport[key] = now
	r.mu.Unlock()

	var rate float64
	if elapsed := now.Sub(start).Seconds(); elapsed > 0 {
		rate = float64(done) / elapsed
	}
	r.migrator.reportInitial(sourceDB, sourceCollection, total, done, rate)
}

// insertBatchWithRetry inserts a batch of documents with sophisticated error handling
// Returns the count of successfully inserted documents
func (r *OplogReplicatorLegacy) insertBatchWithRetry(ctx context.Context, targetCol *mongod.Collection, batch []interface{}, sourceDB, sourceCollection string) int64 {
	transformedBatch, err := r.transformer.TransformBatch(batch, sourceDB, sourceCollection)
	if err != nil {
		r.log.Errorf("Field name transformation failed for batch in %s.%s: %v", sourceDB, sourceCollection, err)
		for _, doc := range batch {
			docID := extractDocID(doc)
			if r.dlq != nil {
				r.dlq.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", doc, time.Time{})
			}
		}
		return 0
	}
	batch = transformedBatch

	var successCount int64

	collConfig, exists := r.collectionConfigs[sourceDB][sourceCollection]
	useUpsert := exists && collConfig.UpsertMode

	if useUpsert {
		var models []mongod.WriteModel
		for _, doc := range batch {
			docID := extractDocID(doc)
			if docID != nil {
				filter := modernbson.M{"_id": docID}
				model := mongod.NewReplaceOneModel().
					SetFilter(filter).
					SetReplacement(doc).
					SetUpsert(true)
				models = append(models, model)
			}
		}

		if len(models) > 0 {
			if _, err := targetCol.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
				// Use errors.As instead of direct type assertion (err.(mongod.BulkWriteException))
				// because the driver or retry wrapper may wrap the underlying BulkWriteException.
				var bulkWriteException mongod.BulkWriteException
				ok := errors.As(err, &bulkWriteException)
				if ok {
					successCount = int64(len(batch) - len(bulkWriteException.WriteErrors))
					r.log.Errorf("Bulk upsert partially failed for %s.%s: %d succeeded, %d failed",
						sourceDB, sourceCollection, successCount, len(bulkWriteException.WriteErrors))

					for _, writeErr := range bulkWriteException.WriteErrors {
						var errDocID interface{}
						if writeErr.Index < len(batch) {
							errDocID = extractDocID(batch[writeErr.Index])
						}
						r.log.Errorf("[%s.%s] Upsert error at index %d, _id=%v: %v", sourceDB, sourceCollection, writeErr.Index, errDocID, writeErr.Message)
						if r.dlq != nil && writeErr.Index < len(batch) {
							r.dlq.WriteFailed(sourceDB, sourceCollection, errDocID, fmt.Errorf("upsert failed: %s", writeErr.Message), "initial", "insert", batch[writeErr.Index], time.Time{})
						}
					}
				} else {
					// Global failure
					if err == context.Canceled {
						r.log.Debugf("Bulk upsert canceled for %s.%s due to context cancellation", sourceDB, sourceCollection)
					} else {
						r.log.Errorf("Error performing bulk upsert for %s.%s: %v", sourceDB, sourceCollection, err)
					}
					// Fall back to individual upserts
					for _, doc := range batch {
						docID := extractDocID(doc)
						if docID != nil {
							filter := modernbson.M{"_id": docID}
							if _, err := targetCol.ReplaceOne(ctx, filter, doc, options.Replace().SetUpsert(true)); err != nil {
								if err != context.Canceled {
									r.log.Errorf("Error fallback upserting document %v in %s.%s: %v", docID, sourceDB, sourceCollection, err)
									if r.dlq != nil {
										r.dlq.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", doc, time.Time{})
									}
								}
							} else {
								successCount++
							}
						}
					}
				}
			} else {
				successCount = int64(len(batch))
				r.log.Debugf("Bulk upserted %d documents successfully in %s.%s", len(batch), sourceDB, sourceCollection)
			}
		}
		return successCount
	}

	if _, err := targetCol.InsertMany(ctx, batch, options.InsertMany().SetOrdered(false)); err != nil {
		// Use errors.As to unwrap any nested BulkWriteException. This ensures that
		// individual write errors (like duplicate keys) are handled gracefully.
		var bulkWriteException mongod.BulkWriteException
		ok := errors.As(err, &bulkWriteException)
		if ok {
			// Calculate successful inserts
			successCount = int64(len(batch) - len(bulkWriteException.WriteErrors))

			if len(bulkWriteException.WriteErrors) > 0 {
				r.log.Debugf("Bulk insert partially failed for %s.%s: %d succeeded, %d failed",
					sourceDB, sourceCollection, successCount, len(bulkWriteException.WriteErrors))
			}

			// Process individual errors
			for _, writeErr := range bulkWriteException.WriteErrors {
				// Check if it's a duplicate key error (code 11000)
				if writeErr.Code == 11000 {
					// Use upsert for duplicate key errors
					if writeErr.Index < len(batch) {
						doc := batch[writeErr.Index]

						// Extract document ID for filter
						var docID interface{}
						if docMap, ok := doc.(map[string]interface{}); ok {
							docID = docMap["_id"]
						}

						if docID != nil {
							filter := modernbson.M{"_id": docID}
							if _, err := targetCol.ReplaceOne(ctx, filter, doc, options.Replace().SetUpsert(true)); err != nil {
								r.log.Debugf("Upsert fallback failed for document %v in %s.%s: %v",
									docID, sourceDB, sourceCollection, err)
								if r.dlq != nil {
									r.dlq.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", doc, time.Time{})
								}
							} else {
								r.log.Debugf("Successfully upserted document %v in %s.%s after duplicate key error",
									docID, sourceDB, sourceCollection)
								successCount++
							}
						}
					}
				} else {
					// For non-duplicate key errors, log and retry with individual insert
					r.log.Debugf("Insert error at index %d in %s.%s: %v",
						writeErr.Index, sourceDB, sourceCollection, writeErr.Message)

					if writeErr.Index < len(batch) {
						// Extract document ID for logging
						var retryDocID interface{}
						if docMap, ok := batch[writeErr.Index].(map[string]interface{}); ok {
							retryDocID = docMap["_id"]
						}

						if _, err := targetCol.InsertOne(ctx, batch[writeErr.Index]); err != nil {
							r.log.Errorf("[%s.%s] Retry insert failed for document _id=%v: %v",
								sourceDB, sourceCollection, retryDocID, err)
							if r.dlq != nil {
								r.dlq.WriteFailed(sourceDB, sourceCollection, retryDocID, err, "initial", "insert", batch[writeErr.Index], time.Time{})
							}
						} else {
							successCount++
						}
					}
				}
			}
		} else {
			// Handle non-bulk write errors
			if err == context.Canceled {
				r.log.Debugf("Bulk insert canceled for %s.%s due to context cancellation", sourceDB, sourceCollection)
			} else {
				r.log.Errorf("Error performing bulk insert for %s.%s: %v", sourceDB, sourceCollection, err)
			}

			// For transient errors, retry the bulk operation with backoff before falling back
			bulkRetrySucceeded := false
			if r.retryManager != nil && err != context.Canceled {
				errType := r.retryManager.ClassifyError(err)
				if errType == ErrorTypeConnection || errType == ErrorTypeContention {
					r.log.Infof("Transient error detected for %s.%s. Retrying bulk insert with backoff...", sourceDB, sourceCollection)
					retryErr := r.retryManager.RetryWithBackoff(ctx, func() error {
						_, retryInsertErr := targetCol.InsertMany(ctx, batch, options.InsertMany().SetOrdered(false))
						return retryInsertErr
					})
					if retryErr == nil {
						r.log.Infof("Bulk insert for %s.%s succeeded after retry", sourceDB, sourceCollection)
						bulkRetrySucceeded = true
						successCount = int64(len(batch))
					} else {
						r.log.Warnf("Bulk insert for %s.%s still failed after retries: %v. Falling back to individual operations.", sourceDB, sourceCollection, retryErr)
					}
				}
			}

			if !bulkRetrySucceeded {
				// Fall back to individual operations with upsert for all documents
				for _, doc := range batch {
					// Try insert first
					if _, err := targetCol.InsertOne(ctx, doc); err != nil {
						// If insert fails, try upsert
						var docID interface{}
						if docMap, ok := doc.(map[string]interface{}); ok {
							docID = docMap["_id"]
						}

						if docID != nil {
							filter := modernbson.M{"_id": docID}
							if _, err := targetCol.ReplaceOne(ctx, filter, doc, options.Replace().SetUpsert(true)); err != nil {
								if err == context.Canceled {
									r.log.Debugf("Upserting document %v in %s.%s canceled due to context cancellation",
										docID, sourceDB, sourceCollection)
								} else {
									r.log.Errorf("Error upserting document %v in %s.%s: %v",
										docID, sourceDB, sourceCollection, err)
									if r.dlq != nil {
										r.dlq.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", doc, time.Time{})
									}
								}
							} else {
								r.log.Debugf("Successfully upserted document %v in %s.%s after insert failed",
									docID, sourceDB, sourceCollection)
								successCount++
							}
						}
					} else {
						successCount++
					}
				}
			} // end if !bulkRetrySucceeded
		}
	} else {
		// All documents inserted successfully
		successCount = int64(len(batch))
		r.log.Debugf("Bulk inserted %d documents successfully in %s.%s", len(batch), sourceDB, sourceCollection)
	}

	return successCount
}

// tailOplog starts tailing the oplog using GTM legacy with parallel processing
func (r *OplogReplicatorLegacy) tailOplog(ctx context.Context, afterTimestamp bson.MongoTimestamp, timestampPath string) error {
	// Extract T and I for logging
	t := uint32(afterTimestamp >> 32)
	i := uint32(afterTimestamp & 0xFFFFFFFF)
	r.log.Infof("Starting oplog tailing from timestamp: T=%d, I=%d", t, i)

	// Build allowed namespaces map for O(1) filtering (important for databases with many collections)
	nsFilterMap := make(map[string]bool)
	for sourceDB, collections := range r.collectionMap {
		for sourceCollection := range collections {
			namespace := fmt.Sprintf("%s.%s", sourceDB, sourceCollection)
			nsFilterMap[namespace] = true
		}
	}
	r.log.Infof("Watching %d namespaces for oplog events", len(nsFilterMap))

	// Get mgo session for GTM
	session := r.sourceDB.GetSession()
	defer session.Close()

	// Configure GTM options for legacy version
	oplogDB := "local"
	oplogColl := "oplog.rs"
	gtmOpts := &gtm.Options{
		After: func(session *mgo.Session, options *gtm.Options) bson.MongoTimestamp {
			// Return the full timestamp with both T and I components
			return afterTimestamp
		},
		NamespaceFilter: func(op *gtm.Op) bool {
			return nsFilterMap[op.Namespace]
		},
		OpLogDatabaseName:   &oplogDB,
		OpLogCollectionName: &oplogColl,
		ChannelSize:         r.config.IncrementalReadBatchSize,
		BufferDuration:      time.Duration(r.config.FlushIntervalMs) * time.Millisecond,
	}

	// Start GTM
	gtmCtx := gtm.Start(session, gtmOpts)
	defer gtmCtx.Stop()

	r.log.Info("GTM oplog tailing started successfully")

	// Initialize StatsManager for comprehensive worker-level telemetry
	statsInterval := time.Duration(r.config.StatsIntervalMinutes) * time.Minute
	statsManager := NewIncrementalStatsManager(r.log, statsInterval, r.config.GroupOpsByDistinctId)
	statsManager.Start(ctx)

	// Initialize parallel workers
	r.log.Infof("Starting parallel oplog processing with %d workers", r.config.IncrementalWorkerCount)

	// Pre-populate active failed document IDs from the DLQ file on startup
	var activeFailedIDs map[string]string
	var activeFailedMu sync.RWMutex
	if r.dlq != nil && r.dlq.FilePath() != "" {
		r.log.Info("DLQ resolution ledger: performing pre-scan startup check...")
		var scanErr error
		activeFailedIDs, scanErr = PopulateActiveFailedIDs(r.dlq.FilePath(), r.log)
		if scanErr != nil {
			r.log.Warnf("DLQ resolution ledger: failed to pre-scan DLQ file: %v (continuing without resolution logging)", scanErr)
		} else {
			r.log.Infof("DLQ resolution ledger: loaded %d active failed IDs on startup", len(activeFailedIDs))
		}
	}

	workers := make([]*Worker, r.config.IncrementalWorkerCount)
	for i := 0; i < r.config.IncrementalWorkerCount; i++ {
		workers[i] = NewWorker(i, ctx, r.log, r.targetDB, r.collectionConfigs, r.config.IncrementalWriteBatchSize, r.config.ForceOrderedOperations, r.dlq, r.retryManager, statsManager, r.config.GroupOpsByDistinctId, time.Duration(r.config.FlushIntervalMs)*time.Millisecond, r.config.IncrementalIncomingQueueSize, r.config.IncrementalProcessingQueueSize, r.transformer)
		if activeFailedIDs != nil {
			workers[i].SetActiveFailedIDs(activeFailedIDs, &activeFailedMu)
		}
	}

	// Set up context cancellation handling for workers
	go func() {
		<-ctx.Done()
		r.log.Info("Context canceled. Shutting down workers...")
		for _, worker := range workers {
			worker.Shutdown()
		}
	}()

	// Set up periodic flushing (matching EventDistributor pattern)
	flushInterval := time.Duration(r.config.FlushIntervalMs) * time.Millisecond
	StartPeriodicFlushLoop(ctx, workers, flushInterval, r.log)

	// Web console: transition the job to the live phase. No-op for CLI.
	if r.migrator != nil && r.migrator.controlPlaneAttached() {
		r.migrator.markState(metrics.StateLive)
	}
	// Always run the live loop. It streams 增量 (incremental) rows with throughput
	// and replication lag to the console (every migrator call inside is nil-guarded,
	// so it is a cheap no-op in CLI mode) AND — crucially — it drives the deferred
	// index build via DeferredIndexController.Observe. The build must happen in every
	// mode, not only when a console is attached, so this launch is unconditional
	// (matching the modern change-stream path, which also builds indexes regardless
	// of the console).
	go r.reportLiveLoop(ctx)

	// Statistics tracking
	var processedCount int
	var lastCheckpoint time.Time = time.Now()
	var eventsSinceLastStats int
	var lastStatsTime time.Time = time.Now()

	// Track latest oplog timestamp for checkpoint saving
	var latestOplogTimestamp primitive.Timestamp

	// Set up periodic statistics reporting
	statsTicker := time.NewTicker(statsInterval)
	defer statsTicker.Stop()

	go func() {
		for {
			select {
			case <-statsTicker.C:
				// Calculate and log statistics
				r.mu.Lock()
				eventCount := eventsSinceLastStats
				duration := time.Since(lastStatsTime)
				eventsSinceLastStats = 0
				lastStatsTime = time.Now()
				r.mu.Unlock()

				if duration > 0 && eventCount > 0 {
					rate := float64(eventCount) / duration.Seconds()
					r.log.Infof("Oplog replication statistics: Processed %d events in the last %v (%.2f events/second)",
						eventCount, duration.Round(time.Second), rate)
				} else if eventCount > 0 {
					r.log.Infof("Oplog replication statistics: Processed %d events since last report", eventCount)
				} else {
					r.log.Info("Oplog replication statistics: No events processed since last report")
				}

				if r.dlq != nil {
					count := r.dlq.Count()
					if count > 100 {
						r.log.Warnf("DLQ WARNING: The Dead Letter Queue contains %d failed documents! Please check the DLQ file.", count)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case op := <-gtmCtx.OpC:
			if op == nil {
				continue
			}

			// Debug log for GTM operations
			r.log.Debugf("GTM received operation: type=%s, namespace=%s, id=%v", op.Operation, op.Namespace, op.Id)

			// Update latest oplog timestamp from GTM operation
			// GTM provides timestamp as bson.MongoTimestamp, convert to primitive.Timestamp
			if op.Timestamp != 0 {
				// bson.MongoTimestamp is int64 where high 32 bits are seconds, low 32 bits are increment
				t := uint32(op.Timestamp >> 32)
				i := uint32(op.Timestamp & 0xFFFFFFFF)
				latestOplogTimestamp = primitive.Timestamp{T: t, I: i}
			}

			// Convert oplog event to worker event format and distribute to workers
			if r.DryRun {
				continue
			}

			r.distributeOplogEvent(ctx, op, workers)

			r.mu.Lock()
			processedCount++
			eventsSinceLastStats++
			r.mu.Unlock()

			// Track cumulative events and the newest event's oplog time for the
			// console live rows + lag calculation (atomic; read by reportLiveLoop).
			atomic.AddInt64(&r.liveEvents, 1)
			var evUnix int64
			if op.Timestamp != 0 {
				evUnix = int64(uint32(op.Timestamp >> 32))
				atomic.StoreInt64(&r.lastEventUnix, evUnix)
			}
			// Per-collection counters so the console shows one live row per
			// collection (whichever collection is actually receiving changes)
			// instead of a single "增量汇总" aggregate.
			r.trackLiveEvent(op.Namespace, evUnix)

			// Periodic checkpoint
			r.mu.Lock()
			shouldCheckpoint := processedCount >= r.config.SaveThreshold || time.Since(lastCheckpoint) >= time.Duration(r.config.CheckpointIntervalMinutes)*time.Minute
			currentProcessedCount := processedCount
			r.mu.Unlock()

			if shouldCheckpoint {
				// Save the actual oplog timestamp
				timestamp := OplogTimestamp{
					Timestamp: latestOplogTimestamp,
				}
				if err := SaveOplogTimestamp(timestampPath, timestamp); err != nil {
					r.log.Errorf("Failed to save oplog timestamp: %v", err)
				} else {
					r.log.Infof("Checkpoint saved (%d operations processed, timestamp T=%d I=%d)",
						currentProcessedCount, latestOplogTimestamp.T, latestOplogTimestamp.I)
				}
				r.mu.Lock()
				processedCount = 0
				lastCheckpoint = time.Now()
				r.mu.Unlock()
			}

		case err := <-gtmCtx.ErrC:
			if err != nil {
				r.log.Errorf("GTM error: %v", err)
			}

		case <-ctx.Done():
			r.log.Info("Oplog replication stopped due to context cancellation")

			// Wait for all workers to finish processing
			for _, worker := range workers {
				worker.WaitForCompletion()
			}

			// Save final oplog timestamp before exiting
			if latestOplogTimestamp.T > 0 || latestOplogTimestamp.I > 0 {
				finalTimestamp := OplogTimestamp{
					Timestamp: latestOplogTimestamp,
				}
				if err := SaveOplogTimestamp(timestampPath, finalTimestamp); err != nil {
					r.log.Errorf("Failed to save final oplog timestamp on shutdown: %v", err)
				} else {
					r.log.Infof("Saved final oplog timestamp on shutdown: T=%d, I=%d",
						latestOplogTimestamp.T, latestOplogTimestamp.I)
				}
			}

			return nil
		}
	}
}

// trackLiveEvent increments the per-collection incremental counters for the
// given oplog namespace ("db.collection"). Namespaces that don't split cleanly
// (e.g. command ops) are ignored for the per-collection view. evUnix is the
// event's oplog seconds (0 if unknown).
func (r *OplogReplicatorLegacy) trackLiveEvent(namespace string, evUnix int64) {
	parts := strings.SplitN(namespace, ".", 2)
	if len(parts) != 2 || parts[1] == "" {
		return
	}
	v, ok := r.liveStats.Load(namespace)
	if !ok {
		v, _ = r.liveStats.LoadOrStore(namespace, &liveCollStat{db: parts[0], coll: parts[1]})
	}
	st := v.(*liveCollStat)
	atomic.AddInt64(&st.events, 1)
	if evUnix > 0 {
		atomic.StoreInt64(&st.lastEventUnix, evUnix)
	}
}

// reportLiveLoop streams one incremental (增量) progress row per collection to
// the web console every 2 seconds: cumulative events applied, current
// events/sec, and the replication lag (tailer's newest consumed oplog position
// minus that collection's newest applied event). Only collections that have actually
// received changes appear. Exits when ctx is cancelled; only started when a
// console is attached.
func (r *OplogReplicatorLegacy) reportLiveLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	prevEvents := make(map[string]int64)
	prevT := time.Now()

	// Deferred index build: indexes are built only once the live catch-up burst has
	// drained (lag settled). The per-cycle worst lag is fed to the shared
	// DeferredIndexController, which owns the threshold/streak/once logic — the very
	// same code the modern change-stream path uses, so the two cannot drift.

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			dt := now.Sub(prevT).Seconds()
			// Worst per-collection lag this cycle; -1 (idle/unknown) counts as
			// caught up. An idle source with no live rows leaves maxLag at -1.
			maxLag := -1.0
			r.liveStats.Range(func(key, value interface{}) bool {
				ns := key.(string)
				st := value.(*liveCollStat)
				events := atomic.LoadInt64(&st.events)
				var rate float64
				if dt > 0 {
					rate = float64(events-prevEvents[ns]) / dt
				}
				prevEvents[ns] = events

				// Replication lag = how far this collection's newest applied event
				// trails the tailer's newest CONSUMED oplog position (any namespace,
				// r.lastEventUnix). Measured against the oplog stream — NOT wall
				// clock — so a caught-up tailer with no new source writes reports ~0
				// instead of a gap that grows by one second every second while idle.
				lag := -1.0
				if lastEv := atomic.LoadInt64(&st.lastEventUnix); lastEv > 0 {
					head := atomic.LoadInt64(&r.lastEventUnix)
					if head < lastEv {
						head = lastEv // this collection holds the freshest op
					}
					lag = float64(head - lastEv)
				}
				if lag > maxLag {
					maxLag = lag
				}
				// Legacy path does not track per-collection failed counts; the
				// console's file-based DLQ view surfaces any errors instead.
				r.migrator.reportLive(st.db, st.coll, events, 0, rate, lag)
				return true
			})
			prevT = now

			// Build indexes once lag has settled (shared with the modern path).
			r.deferredIndex.Observe(ctx, maxLag)
		}
	}
}

// distributeOplogEvent converts oplog event to worker format and distributes to appropriate worker
func (r *OplogReplicatorLegacy) distributeOplogEvent(ctx context.Context, op *gtm.Op, workers []*Worker) {
	// Debug log for distribution
	r.log.Debugf("Distributing event: op=%s, namespace=%s", op.Operation, op.Namespace)

	// Extract namespace parts
	parts := strings.SplitN(op.Namespace, ".", 2)
	if len(parts) != 2 {
		r.log.Warnf("Invalid namespace: %s", op.Namespace)
		return
	}

	sourceDB := parts[0]
	sourceCollection := parts[1]

	// Convert mgo data to interface{} for modern driver
	var fullDoc interface{}
	if op.Data != nil {
		fullDoc = convertMgoBSONToInterface(op.Data)
	}

	// Debug log for insert and update operations
	if op.Operation == "i" {
		r.log.Debugf("Insert operation: op.Data isNil=%v, fullDoc isNil=%v, fullDoc type=%T",
			op.Data == nil, fullDoc == nil, fullDoc)
	}

	if op.Operation == "u" {
		r.log.Debugf("Update operation: op.Data isNil=%v, fullDoc isNil=%v, fullDoc type=%T, fullDoc=%+v",
			op.Data == nil, fullDoc == nil, fullDoc, fullDoc)
	}

	// Convert GTM operation to change event format expected by workers (use modern driver's bson.M)
	var changeEvent modernbson.M

	switch op.Operation {
	case "i": // insert
		changeEvent = modernbson.M{
			"operationType": "insert",
			"ns": modernbson.M{
				"db":   sourceDB,
				"coll": sourceCollection,
			},
			"documentKey": modernbson.M{
				"_id": convertMgoValue(op.Id),
			},
			"fullDocument": fullDoc,
		}

	case "u": // update
		// Check if this is a modifier update or full document replacement
		var hasModifiers bool
		if docMap, ok := fullDoc.(map[string]interface{}); ok {
			for k := range docMap {
				if strings.HasPrefix(k, "$") {
					hasModifiers = true
					break
				}
			}
		}

		if hasModifiers {
			// Modifier update - convert to update event with updateDescription
			changeEvent = modernbson.M{
				"operationType": "update",
				"ns": modernbson.M{
					"db":   sourceDB,
					"coll": sourceCollection,
				},
				"documentKey": modernbson.M{
					"_id": convertMgoValue(op.Id),
				},
				"updateDescription": fullDoc,
			}
		} else {
			// Full document replacement
			changeEvent = modernbson.M{
				"operationType": "replace",
				"ns": modernbson.M{
					"db":   sourceDB,
					"coll": sourceCollection,
				},
				"documentKey": modernbson.M{
					"_id": convertMgoValue(op.Id),
				},
				"fullDocument": fullDoc,
			}
		}

	case "d": // delete
		changeEvent = modernbson.M{
			"operationType": "delete",
			"ns": modernbson.M{
				"db":   sourceDB,
				"coll": sourceCollection,
			},
			"documentKey": modernbson.M{
				"_id": convertMgoValue(op.Id),
			},
		}

	default:
		r.log.Warnf("Unknown operation type: %s", op.Operation)
		return
	}

	// Determine worker based on document ID hash
	docID := convertMgoValue(op.Id)
	workerIndex := hashDocumentID(docID) % len(workers)
	if workerIndex < 0 {
		workerIndex = -workerIndex
	}

	changeEvent["readTime"] = time.Now()

	// Send raw event to appropriate worker concurrently
	workers[workerIndex].batchingQueue <- changeEvent
}

// syncIndexesLegacy syncs indexes from the legacy mgo source to the modern driver target.
// This uses the mgo driver's Indexes() method to list source indexes and converts them
// to the format expected by the modern driver's CreateIndexFromDefinitionAsync.
// Index creation is fire-and-forget — goroutines run in the background with dedicated clients.
func (r *OplogReplicatorLegacy) syncIndexesLegacy(ctx context.Context, pair config.DatabasePair) {
	var indexCount int

	// Always create the _id index on every target collection, independent of
	// SyncAllIndexes. Firestore does NOT auto-create it (real MongoDB does), so
	// without this there is no index backing _id lookups/ordering. Idempotent:
	// skipped when the target already has "_id_".
	for dbName, colls := range r.collectionMap {
		for srcColl, tgtColl := range colls {
			// Apply any rename-collection remediation so indexes land on the SAME
			// legal target name the data was written to (idempotent).
			if r.transformer != nil {
				tgtColl = r.transformer.SanitizeTargetName(dbName, srcColl, tgtColl)
			}
			if r.targetHasIndexLegacy(ctx, tgtColl, "_id_") {
				r.log.Infof("_id index already exists on target collection '%s', skipping", tgtColl)
				continue
			}
			r.log.Infof("Launching async _id index creation on target collection '%s'", tgtColl)
			r.targetDB.CreateIDIndexAsync(pair.Target.ConnectionString, tgtColl)
			indexCount++
		}
	}

	if pair.Target.SyncAllIndexes {
		r.log.Info("SyncAllIndexes enabled: launching async index creation (excluding _id_) for all collections")

		for dbName, colls := range r.collectionMap {
			for srcColl, tgtColl := range colls {
				// Apply any rename-collection remediation (idempotent) so secondary
				// indexes land on the same legal target name as the data.
				if r.transformer != nil {
					tgtColl = r.transformer.SanitizeTargetName(dbName, srcColl, tgtColl)
				}
				mgoIndexes, err := r.sourceDB.ListIndexes(srcColl)
				if err != nil {
					r.log.Warnf("Failed to list indexes for %s: %v (continuing anyway)", srcColl, err)
					continue
				}

				// List existing indexes on the target to skip already-created ones
				existingIndexNames := make(map[string]bool)
				targetIndexes, err := r.targetDB.ListIndexes(ctx, tgtColl)
				if err != nil {
					r.log.Debugf("Could not list target indexes for %s: %v (will attempt all)", tgtColl, err)
				} else {
					for _, idx := range targetIndexes {
						if name, ok := idx["name"].(string); ok {
							existingIndexNames[name] = true
						}
					}
				}

				for _, idx := range mgoIndexes {
					if idx.Name == "_id_" {
						continue
					}

					// Skip if index already exists on target
					if existingIndexNames[idx.Name] {
						r.log.Infof("Index '%s' already exists on target collection '%s', skipping", idx.Name, tgtColl)
						continue
					}

					indexDef := convertMgoIndexToModernBsonM(idx)

					r.log.Infof("Launching async index creation: '%s' on target collection '%s'", idx.Name, tgtColl)
					r.targetDB.CreateIndexFromDefinitionAsync(pair.Target.ConnectionString, tgtColl, indexDef)
					indexCount++
				}
			}
		}
	}

	// Also process explicit index configs if provided
	for _, indexConfig := range pair.Target.Indexes {
		mgoIndexes, err := r.sourceDB.ListIndexes(indexConfig.SourceCollection)
		if err != nil {
			r.log.Warnf("Failed to list indexes for %s: %v (continuing anyway)", indexConfig.SourceCollection, err)
			continue
		}

		// Look up target collection name from collectionMap, then apply any
		// rename-collection remediation (idempotent) so indexes match the data.
		tgtColl := indexConfig.SourceCollection // default same name
		for dbName, colls := range r.collectionMap {
			if mapped, ok := colls[indexConfig.SourceCollection]; ok {
				tgtColl = mapped
				if r.transformer != nil {
					tgtColl = r.transformer.SanitizeTargetName(dbName, indexConfig.SourceCollection, tgtColl)
				}
				break
			}
		}

		// List existing indexes on the target to skip already-created ones
		existingIndexNames := make(map[string]bool)
		targetIndexes, err := r.targetDB.ListIndexes(ctx, tgtColl)
		if err != nil {
			r.log.Debugf("Could not list target indexes for %s: %v (will attempt all)", tgtColl, err)
		} else {
			for _, idx := range targetIndexes {
				if name, ok := idx["name"].(string); ok {
					existingIndexNames[name] = true
				}
			}
		}

		for _, idx := range mgoIndexes {
			if idx.Name == "_id_" {
				continue
			}

			found := false
			for _, requestedName := range indexConfig.IndexNames {
				if idx.Name == requestedName {
					found = true
					break
				}
			}
			if !found {
				continue
			}

			// Skip if index already exists on target
			if existingIndexNames[idx.Name] {
				r.log.Infof("Index '%s' already exists on target collection '%s', skipping", idx.Name, tgtColl)
				continue
			}

			indexDef := convertMgoIndexToModernBsonM(idx)

			r.log.Infof("Launching async index creation: '%s' on target collection '%s'", idx.Name, tgtColl)
			r.targetDB.CreateIndexFromDefinitionAsync(pair.Target.ConnectionString, tgtColl, indexDef)
			indexCount++
		}
	}

	r.log.Infof("Launched %d async index creation tasks (legacy mode).", indexCount)
}

// targetHasIndexLegacy reports whether the target collection already has an index
// with the given name. Errors (e.g. collection not yet created) are treated as
// "no" so the caller attempts creation. Keeps index creation idempotent.
func (r *OplogReplicatorLegacy) targetHasIndexLegacy(ctx context.Context, collection, indexName string) bool {
	idxs, err := r.targetDB.ListIndexes(ctx, collection)
	if err != nil {
		return false
	}
	for _, idx := range idxs {
		if n, ok := idx["name"].(string); ok && n == indexName {
			return true
		}
	}
	return false
}

// convertMgoIndexToModernBsonM converts an mgo.Index to a modern driver bson.M
// definition compatible with MongoDB.CreateIndexFromDefinition.
// mgo Key format: ["field1", "-field2"] where "-" prefix means descending.
// Special prefixes: "$text:", "$2d:", "$2dsphere:", "$geoHaystack:", "$hashed:"
func convertMgoIndexToModernBsonM(idx mgo.Index) modernbson.M {
	// Convert key fields to bson.D (ordered)
	var keys modernbson.D
	for _, k := range idx.Key {
		switch {
		case strings.HasPrefix(k, "-"):
			keys = append(keys, modernbson.E{Key: k[1:], Value: int32(-1)})
		case strings.HasPrefix(k, "$text:"):
			keys = append(keys, modernbson.E{Key: k[6:], Value: "text"})
		case strings.HasPrefix(k, "$2dsphere:"):
			keys = append(keys, modernbson.E{Key: k[10:], Value: "2dsphere"})
		case strings.HasPrefix(k, "$2d:"):
			keys = append(keys, modernbson.E{Key: k[4:], Value: "2d"})
		case strings.HasPrefix(k, "$geoHaystack:"):
			keys = append(keys, modernbson.E{Key: k[13:], Value: "geoHaystack"})
		case strings.HasPrefix(k, "$hashed:"):
			keys = append(keys, modernbson.E{Key: k[8:], Value: "hashed"})
		default:
			keys = append(keys, modernbson.E{Key: k, Value: int32(1)})
		}
	}

	indexDef := modernbson.M{
		"name": idx.Name,
		"key":  keys,
	}

	if idx.Unique {
		indexDef["unique"] = true
	}
	if idx.Background {
		indexDef["background"] = true
	}
	if idx.Sparse {
		indexDef["sparse"] = true
	}
	if idx.ExpireAfter > 0 {
		indexDef["expireAfterSeconds"] = int32(idx.ExpireAfter.Seconds())
	}
	if idx.DefaultLanguage != "" {
		indexDef["default_language"] = idx.DefaultLanguage
	}
	if idx.LanguageOverride != "" {
		indexDef["language_override"] = idx.LanguageOverride
	}
	if len(idx.Weights) > 0 {
		weights := modernbson.M{}
		for field, weight := range idx.Weights {
			weights[field] = weight
		}
		indexDef["weights"] = weights
	}

	return indexDef
}

// convertMgoBSONToInterface converts mgo bson.M to interface{} for modern driver
// This function recursively converts mgo BSON types to modern driver compatible types
func convertMgoBSONToInterface(doc bson.M) interface{} {
	result := make(map[string]interface{})
	for k, v := range doc {
		result[k] = convertMgoValue(v)
	}
	return result
}

// convertMgoValue recursively converts mgo BSON values to modern driver compatible types
func convertMgoValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case bson.ObjectId:
		// Convert mgo ObjectId to modern primitive.ObjectID
		// mgo ObjectId is a string type, we need to convert it to [12]byte
		if len(val) == 12 {
			var oid [12]byte
			copy(oid[:], []byte(val))
			return primitive.ObjectID(oid)
		}
		return val

	case bson.M:
		// Recursively convert nested documents
		result := make(map[string]interface{})
		for k, v := range val {
			result[k] = convertMgoValue(v)
		}
		return result

	case map[string]interface{}:
		// Recursively convert map
		result := make(map[string]interface{})
		for k, v := range val {
			result[k] = convertMgoValue(v)
		}
		return result

	case []interface{}:
		// Recursively convert arrays
		result := make([]interface{}, len(val))
		for i, item := range val {
			result[i] = convertMgoValue(item)
		}
		return result

	case []bson.M:
		// Convert array of documents
		result := make([]interface{}, len(val))
		for i, item := range val {
			result[i] = convertMgoValue(item)
		}
		return result

	default:
		// Return primitive types as-is (string, int, float, bool, time.Time, etc.)
		return val
	}
}

// sourceDocFetcher re-reads one document from the source by _id during retry-dlq
// source-resync. Returns (doc, true, nil) when the document is found in the
// source, (nil, false, nil) when the source no longer has it (deleted — treated
// as resolved and skipped), or (nil, false, err) on a genuine read error (the
// document is kept in the DLQ so it can be retried again later).
type sourceDocFetcher func(collection string, id interface{}) (interface{}, bool, error)

// newLegacySourceFetcher opens a legacy (mgo) connection to the source and
// returns a fetcher plus a close func. Used when the source is an oplog-legacy
// (MongoDB 3.0/3.2) server the modern driver cannot connect to. The returned
// documents are converted to modern-driver-compatible types (via
// convertMgoBSONToInterface) so they can be written straight to the target.
func newLegacySourceFetcher(connectionString, database string) (sourceDocFetcher, func(), error) {
	src, err := db.NewMongoDBLegacy(connectionString, database)
	if err != nil {
		return nil, nil, err
	}
	fetch := func(collection string, id interface{}) (interface{}, bool, error) {
		var doc bson.M
		ferr := src.GetCollection(collection).FindId(modernIDToMgo(id)).One(&doc)
		if ferr == mgo.ErrNotFound {
			return nil, false, nil
		}
		if ferr != nil {
			return nil, false, ferr
		}
		return convertMgoBSONToInterface(doc), true, nil
	}
	return fetch, func() { src.Close() }, nil
}

// modernIDToMgo converts a modern-driver _id value (as decoded from the DLQ file,
// which is read with the modern bson codec) back into the mgo representation so
// FindId matches on a legacy source. ObjectIDs need explicit mapping; other
// primitive types (string, int, etc.) marshal fine under mgo unchanged.
func modernIDToMgo(id interface{}) interface{} {
	if oid, ok := id.(primitive.ObjectID); ok {
		return bson.ObjectId(string(oid[:]))
	}
	return id
}
