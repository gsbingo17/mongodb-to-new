package migration

import (
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestInitialMigrator_DeterminePlanDirectResumption(t *testing.T) {
	tmpDir := t.TempDir()
	db := "testdb"
	coll := "testcoll"
	oidSaved, _ := primitive.ObjectIDFromHex("60a000000000000000000045")

	cp := &PartitionCheckpoint{
		Database:                db,
		Collection:              coll,
		PartitionIndex:          0,
		TotalSplits:             1,
		ApproximateDocsMigrated: 1500,
		UpdatedAt:               time.Now().UTC(),
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {
				BSONType:    BSONTypeObjectID,
				SavedLastID: oidSaved,
			},
		},
	}

	checkpointPath := GetPartitionCheckpointPath(tmpDir, db, coll, 0, 1)
	if err := SavePartitionCheckpoint(checkpointPath, cp); err != nil {
		t.Fatalf("failed to save test checkpoint: %v", err)
	}

	plan, err := DetermineBackfillResumptionPlan(tmpDir, db, coll, 1)
	if err != nil {
		t.Fatalf("unexpected error determining plan: %v", err)
	}

	if plan.Mode != ResumptionModeDirect {
		t.Errorf("expected ResumptionModeDirect, got %v", plan.Mode)
	}
	if plan.TotalDocsMigrated() != 1500 {
		t.Errorf("expected TotalDocsMigrated to be 1500, got %d", plan.TotalDocsMigrated())
	}
	if len(plan.PartitionFilters) != 1 {
		t.Fatalf("expected 1 partition filter, got %d", len(plan.PartitionFilters))
	}

	expectedFilter := bson.D{
		{Key: "_id", Value: bson.D{
			{Key: "$type", Value: "objectId"},
			{Key: "$gte", Value: oidSaved},
		}},
	}
	if diff := cmp.Diff(expectedFilter, plan.PartitionFilters[0]); diff != "" {
		t.Errorf("direct resumption filter mismatch (-want +got):\n%s", diff)
	}
}

func TestInitialMigrator_DeterminePlanGlobalMinResumption(t *testing.T) {
	tmpDir := t.TempDir()
	db := "testdb"
	coll := "testcoll"
	historicalSplits := 2
	oid1, _ := primitive.ObjectIDFromHex("60a000000000000000000010")
	oid2, _ := primitive.ObjectIDFromHex("60a000000000000000000050")

	cp0 := &PartitionCheckpoint{
		Database:                db,
		Collection:              coll,
		PartitionIndex:          0,
		TotalSplits:             historicalSplits,
		ApproximateDocsMigrated: 1000,
		UpdatedAt:               time.Now().UTC(),
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeEndID: oid2, SavedLastID: oid1},
		},
	}
	cp1 := &PartitionCheckpoint{
		Database:                db,
		Collection:              coll,
		PartitionIndex:          1,
		TotalSplits:             historicalSplits,
		ApproximateDocsMigrated: 2000,
		UpdatedAt:               time.Now().UTC(),
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid2, SavedLastID: oid2},
		},
	}

	if err := SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, db, coll, 0, historicalSplits), cp0); err != nil {
		t.Fatalf("failed to save cp0: %v", err)
	}
	if err := SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, db, coll, 1, historicalSplits), cp1); err != nil {
		t.Fatalf("failed to save cp1: %v", err)
	}

	plan, err := DetermineBackfillResumptionPlan(tmpDir, db, coll, 1)
	if err != nil {
		t.Fatalf("unexpected error determining plan: %v", err)
	}

	if plan.Mode != ResumptionModeResampleWithGlobalMin {
		t.Errorf("expected ResumptionModeResampleWithGlobalMin, got %v", plan.Mode)
	}
	if plan.TotalDocsMigrated() != 3000 {
		t.Errorf("expected TotalDocsMigrated to be 3000, got %d", plan.TotalDocsMigrated())
	}
	if plan.GlobalMinSafeIDs[BSONTypeObjectID] != oid1 {
		t.Errorf("expected GlobalMinSafeID %v, got %v", oid1, plan.GlobalMinSafeIDs[BSONTypeObjectID])
	}

	// Build single partition filter using the global min
	sequentialCP := &PartitionCheckpoint{
		Database:       db,
		Collection:     coll,
		PartitionIndex: 0,
		TotalSplits:    1,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, SavedLastID: plan.GlobalMinSafeIDs[BSONTypeObjectID]},
		},
	}
	filter, err := BuildPartitionFilterFromCheckpoint(sequentialCP)
	if err != nil {
		t.Fatalf("unexpected error building filter from global min: %v", err)
	}

	expectedFilter := bson.D{
		{Key: "_id", Value: bson.D{
			{Key: "$type", Value: "objectId"},
			{Key: "$gte", Value: oid1},
		}},
	}
	if diff := cmp.Diff(expectedFilter, filter); diff != "" {
		t.Errorf("resample filter mismatch (-want +got):\n%s", diff)
	}
}

func TestInitialMigrator_DeterminePlanFreshFallback(t *testing.T) {
	tmpDir := t.TempDir()
	db := "testdb"
	coll := "testcoll"

	plan, err := DetermineBackfillResumptionPlan(tmpDir, db, coll, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Mode != ResumptionModeFresh {
		t.Errorf("expected ResumptionModeFresh for empty dir, got %v", plan.Mode)
	}
	if plan.TotalDocsMigrated() != 0 {
		t.Errorf("expected TotalDocsMigrated to be 0, got %d", plan.TotalDocsMigrated())
	}
}

func TestInitialMigrator_DryRunSkipsCheckpointCreation(t *testing.T) {
	tmpDir := t.TempDir()
	log := logger.New()
	cfg := &config.Config{
		InitialReadBatchSize:  100,
		InitialWriteBatchSize: 10,
	}

	migrator := &InitialMigrator{
		config:        cfg,
		log:           log,
		DryRun:        true,
		CheckpointDir: tmpDir,
	}

	if !migrator.DryRun {
		t.Errorf("expected DryRun to be true")
	}

	// In dry run, no checkpoint files should be written
	checkpoints, err := ListPartitionCheckpoints(tmpDir, "testdb", "testcoll")
	if err != nil {
		t.Fatalf("unexpected error listing checkpoints: %v", err)
	}
	if len(checkpoints) != 0 {
		t.Errorf("expected 0 checkpoints in dry run directory, got %d", len(checkpoints))
	}
}

func TestInitialMigrator_PeriodicSaveAndResumeSimulation(t *testing.T) {
	tmpDir := t.TempDir()
	db := "testdb"
	coll := "testcoll"
	checkpointPath := GetPartitionCheckpointPath(tmpDir, db, coll, 0, 1)

	checkpoint := &PartitionCheckpoint{
		Database:                db,
		Collection:              coll,
		PartitionIndex:          0,
		TotalSplits:             1,
		TypeProgress:            make(map[BSONType]*TypeRangeBoundary),
		ApproximateDocsMigrated: 0,
		UpdatedAt:               time.Now().UTC(),
	}

	// Simulate batch 1 with ObjectIDs
	oid1, _ := primitive.ObjectIDFromHex("60a000000000000000000001")
	oid2, _ := primitive.ObjectIDFromHex("60a000000000000000000010")
	batch1 := []bson.D{
		{{Key: "_id", Value: oid1}},
		{{Key: "_id", Value: oid2}},
	}

	for _, doc := range batch1 {
		docID := extractDocID(doc)
		bType := GetBSONType(docID)
		if checkpoint.TypeProgress[bType] == nil {
			checkpoint.TypeProgress[bType] = &TypeRangeBoundary{BSONType: bType}
		}
		checkpoint.TypeProgress[bType].SavedLastID = docID
	}
	checkpoint.ApproximateDocsMigrated += int64(len(batch1))

	if err := SavePartitionCheckpoint(checkpointPath, checkpoint); err != nil {
		t.Fatalf("failed to save periodic checkpoint: %v", err)
	}

	// Load checkpoint back and verify progress
	loaded, err := LoadPartitionCheckpoint(checkpointPath)
	if err != nil {
		t.Fatalf("failed to load checkpoint: %v", err)
	}
	if loaded.ApproximateDocsMigrated != 2 {
		t.Errorf("expected ApproximateDocsMigrated to be 2, got %d", loaded.ApproximateDocsMigrated)
	}
	if loaded.TypeProgress[BSONTypeObjectID].SavedLastID != oid2 {
		t.Errorf("expected SavedLastID to be %v, got %v", oid2, loaded.TypeProgress[BSONTypeObjectID].SavedLastID)
	}

	// Simulate batch 2 with string IDs (mixed type collection)
	batch2 := []bson.D{
		{{Key: "_id", Value: "user_001"}},
		{{Key: "_id", Value: "user_002"}},
	}
	for _, doc := range batch2 {
		docID := extractDocID(doc)
		bType := GetBSONType(docID)
		if checkpoint.TypeProgress[bType] == nil {
			checkpoint.TypeProgress[bType] = &TypeRangeBoundary{BSONType: bType}
		}
		checkpoint.TypeProgress[bType].SavedLastID = docID
	}
	checkpoint.ApproximateDocsMigrated += int64(len(batch2))

	if err := SavePartitionCheckpoint(checkpointPath, checkpoint); err != nil {
		t.Fatalf("failed to save updated checkpoint: %v", err)
	}

	loaded2, err := LoadPartitionCheckpoint(checkpointPath)
	if err != nil {
		t.Fatalf("failed to load updated checkpoint: %v", err)
	}
	if loaded2.ApproximateDocsMigrated != 4 {
		t.Errorf("expected ApproximateDocsMigrated to be 4, got %d", loaded2.ApproximateDocsMigrated)
	}
	if loaded2.TypeProgress[BSONTypeString].SavedLastID != "user_002" {
		t.Errorf("expected string SavedLastID to be 'user_002', got %v", loaded2.TypeProgress[BSONTypeString].SavedLastID)
	}
	if loaded2.TypeProgress[BSONTypeObjectID].SavedLastID != oid2 {
		t.Errorf("expected objectId SavedLastID to remain %v, got %v", oid2, loaded2.TypeProgress[BSONTypeObjectID].SavedLastID)
	}
}

func TestInitialMigrator_CleanupOnCompletion(t *testing.T) {
	tmpDir := t.TempDir()
	db := "testdb"
	coll := "testcoll"
	checkpointPath := GetPartitionCheckpointPath(tmpDir, db, coll, 0, 1)

	cp := &PartitionCheckpoint{
		Database:       db,
		Collection:     coll,
		PartitionIndex: 0,
		TotalSplits:    1,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, SavedLastID: primitive.NewObjectID()},
		},
	}
	if err := SavePartitionCheckpoint(checkpointPath, cp); err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	if _, err := os.Stat(checkpointPath); os.IsNotExist(err) {
		t.Fatalf("expected checkpoint file to exist")
	}

	if err := DeletePartitionCheckpoints(tmpDir, db, coll); err != nil {
		t.Fatalf("failed to delete partition checkpoints: %v", err)
	}

	if _, err := os.Stat(checkpointPath); !os.IsNotExist(err) {
		t.Errorf("expected checkpoint file to be deleted, but it still exists")
	}
}
