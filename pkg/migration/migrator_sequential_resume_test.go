package migration

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestMigrator_SequentialResumption_DirectPlan(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "test_coll"

	oid := primitive.NewObjectID()
	cp := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          0,
		TotalSplits:             1,
		ApproximateDocsMigrated: 15000,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {
				BSONType:    BSONTypeObjectID,
				SavedLastID: oid,
			},
		},
		UpdatedAt: time.Now().UTC(),
	}

	checkpointPath := GetPartitionCheckpointPath(tmpDir, dbName, collName, 0, 1)
	if err := SavePartitionCheckpoint(checkpointPath, cp); err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	plan, err := DetermineBackfillResumptionPlan(tmpDir, dbName, collName, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Mode != ResumptionModeDirect {
		t.Fatalf("expected ResumptionModeDirect, got %v", plan.Mode)
	}
	if len(plan.PartitionFilters) != 1 {
		t.Fatalf("expected 1 partition filter, got %d", len(plan.PartitionFilters))
	}
	if plan.TotalDocsMigrated() != 15000 {
		t.Fatalf("expected TotalDocsMigrated 15000, got %d", plan.TotalDocsMigrated())
	}

	loadedCP, err := LoadPartitionCheckpoint(checkpointPath)
	if err != nil {
		t.Fatalf("failed to load checkpoint: %v", err)
	}
	if loadedCP.ApproximateDocsMigrated != 15000 {
		t.Errorf("expected loadedCP ApproximateDocsMigrated 15000, got %d", loadedCP.ApproximateDocsMigrated)
	}
}

func TestMigrator_SequentialResumption_GlobalMinPlan(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "test_coll"

	oid1 := primitive.NewObjectID()
	time.Sleep(5 * time.Millisecond)
	oid2 := primitive.NewObjectID()

	// Prior run had 2 partitions, new run is sequential (1 partition)
	cp0 := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          0,
		TotalSplits:             2,
		ApproximateDocsMigrated: 5000,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, SavedLastID: oid1},
		},
		UpdatedAt: time.Now().UTC(),
	}
	cp1 := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          1,
		TotalSplits:             2,
		ApproximateDocsMigrated: 8000,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, SavedLastID: oid2},
		},
		UpdatedAt: time.Now().UTC(),
	}

	_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, dbName, collName, 0, 2), cp0)
	_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, dbName, collName, 1, 2), cp1)

	plan, err := DetermineBackfillResumptionPlan(tmpDir, dbName, collName, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Mode != ResumptionModeResampleWithGlobalMin {
		t.Fatalf("expected ResumptionModeResampleWithGlobalMin, got %v", plan.Mode)
	}
	if plan.TotalDocsMigrated() != 13000 {
		t.Fatalf("expected TotalDocsMigrated 13000, got %d", plan.TotalDocsMigrated())
	}
	if plan.GlobalMinSafeIDs[BSONTypeObjectID] != oid1 {
		t.Fatalf("expected global min safe ID %v, got %v", oid1, plan.GlobalMinSafeIDs[BSONTypeObjectID])
	}

	rebuiltCP := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          0,
		TotalSplits:             1,
		TypeProgress:            make(map[BSONType]*TypeRangeBoundary),
		ApproximateDocsMigrated: plan.TotalDocsMigrated(),
		UpdatedAt:               time.Now().UTC(),
	}
	for bType, minID := range plan.GlobalMinSafeIDs {
		rebuiltCP.TypeProgress[bType] = &TypeRangeBoundary{BSONType: bType, SavedLastID: minID}
	}

	filter, filterErr := BuildPartitionFilterFromCheckpoint(rebuiltCP)
	if filterErr != nil {
		t.Fatalf("failed to build partition filter: %v", filterErr)
	}
	if len(filter) == 0 {
		t.Fatalf("expected non-empty resume filter")
	}
}

func TestMigrator_SequentialResumption_CleanupOnCompletion(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "test_coll"

	cp := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          0,
		TotalSplits:             1,
		ApproximateDocsMigrated: 20000,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeString: {BSONType: BSONTypeString, SavedLastID: "user_9999"},
		},
		UpdatedAt: time.Now().UTC(),
	}

	checkpointPath := GetPartitionCheckpointPath(tmpDir, dbName, collName, 0, 1)
	if err := SavePartitionCheckpoint(checkpointPath, cp); err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	if _, err := os.Stat(checkpointPath); os.IsNotExist(err) {
		t.Fatalf("checkpoint file should exist before cleanup")
	}

	if err := DeletePartitionCheckpoints(tmpDir, dbName, collName); err != nil {
		t.Fatalf("failed to delete partition checkpoints: %v", err)
	}

	if _, err := os.Stat(checkpointPath); !os.IsNotExist(err) {
		t.Fatalf("checkpoint file should have been deleted")
	}
}

func TestMigrator_SequentialResumption_RecordBatchProgress(t *testing.T) {
	cp := &PartitionCheckpoint{
		Database:                "test_db",
		Collection:              "test_coll",
		PartitionIndex:          0,
		TotalSplits:             1,
		ApproximateDocsMigrated: 0,
		TypeProgress:            make(map[BSONType]*TypeRangeBoundary),
		UpdatedAt:               time.Now().UTC(),
	}

	oid1 := primitive.NewObjectID()
	oid2 := primitive.NewObjectID()
	binData := primitive.Binary{Subtype: 0x04, Data: []byte("uuid-bytes-12345")}

	batch := []interface{}{
		bson.D{{Key: "_id", Value: oid1}, {Key: "val", Value: "first"}},
		bson.D{{Key: "_id", Value: "user_abc"}, {Key: "val", Value: "second"}},
		bson.D{{Key: "_id", Value: int64(123456789)}, {Key: "val", Value: "third"}},
		bson.D{{Key: "_id", Value: binData}, {Key: "val", Value: "fourth"}},
		bson.D{{Key: "_id", Value: oid2}, {Key: "val", Value: "fifth"}},
	}

	cp.RecordBatchProgress(batch, int64(len(batch)))

	if cp.ApproximateDocsMigrated != 5 {
		t.Errorf("expected ApproximateDocsMigrated 5, got %d", cp.ApproximateDocsMigrated)
	}

	if cp.TypeProgress[BSONTypeObjectID] == nil || cp.TypeProgress[BSONTypeObjectID].SavedLastID != oid2 {
		t.Errorf("expected ObjectID SavedLastID %v, got %v", oid2, cp.TypeProgress[BSONTypeObjectID])
	}
	if cp.TypeProgress[BSONTypeString] == nil || cp.TypeProgress[BSONTypeString].SavedLastID != "user_abc" {
		t.Errorf("expected String SavedLastID 'user_abc', got %v", cp.TypeProgress[BSONTypeString])
	}
	if cp.TypeProgress[BSONTypeNumber] == nil || cp.TypeProgress[BSONTypeNumber].SavedLastID != int64(123456789) {
		t.Errorf("expected Number SavedLastID 123456789, got %v", cp.TypeProgress[BSONTypeNumber])
	}
	if cp.TypeProgress[BSONTypeBinary] == nil {
		t.Errorf("expected Binary progress entry")
	} else {
		savedBin, ok := cp.TypeProgress[BSONTypeBinary].SavedLastID.(primitive.Binary)
		if !ok || savedBin.Subtype != binData.Subtype || !bytes.Equal(savedBin.Data, binData.Data) {
			t.Errorf("expected Binary SavedLastID %v, got %v", binData, cp.TypeProgress[BSONTypeBinary].SavedLastID)
		}
	}
}

func TestMigrator_SequentialResumption_DryRunSkipsCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	migrator := &Migrator{
		config: &config.Config{
			SaveThreshold:             100,
			CheckpointIntervalMinutes: 1,
		},
		log:           logger.New(),
		DryRun:        true,
		CheckpointDir: tmpDir,
	}

	if !migrator.DryRun {
		t.Fatalf("expected DryRun to be true")
	}

	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to read temp dir: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected 0 checkpoint files in dry run mode, found %d", len(files))
	}
}

func TestMigrator_SequentialResumption_DefaultCheckpointDir(t *testing.T) {
	migrator := NewMigrator(&config.Config{}, logger.New())
	if migrator.getCheckpointDir() != "." {
		t.Errorf("expected default checkpoint dir '.', got '%s'", migrator.getCheckpointDir())
	}

	migrator.CheckpointDir = "/custom/path"
	if migrator.getCheckpointDir() != "/custom/path" {
		t.Errorf("expected custom checkpoint dir '/custom/path', got '%s'", migrator.getCheckpointDir())
	}

	migrator.CheckpointDir = ""
	if migrator.getCheckpointDir() != "." {
		t.Errorf("expected empty checkpoint dir to resolve to '.', got '%s'", migrator.getCheckpointDir())
	}
}

