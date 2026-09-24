package migration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// TestExtractEventTime validates the extraction of standard event timestamps from different potential change event fields (clusterTime and wallTime) with various data typings.
func TestExtractEventTime(t *testing.T) {
	// Arrange: initialize current second boundaries
	nowSec := time.Now().Unix()
	expectedTime := time.Unix(nowSec, 0)

	// Table-driven mock events: contains standard valid and invalid types/formats
	tests := []struct {
		name     string
		event    bson.M
		wantZero bool
	}{
		{
			name: "primitive.Timestamp clusterTime",
			event: bson.M{
				"clusterTime": primitive.Timestamp{T: uint32(nowSec), I: 1},
			},
			wantZero: false,
		},
		{
			name: "primitive.DateTime wallTime",
			event: bson.M{
				"wallTime": primitive.NewDateTimeFromTime(expectedTime),
			},
			wantZero: false,
		},
		{
			name: "nested/interface primitive.Timestamp clusterTime",
			event: bson.M{
				"clusterTime": interface{}(primitive.Timestamp{T: uint32(nowSec), I: 1}),
			},
			wantZero: false,
		},
		{
			name:     "empty event",
			event:    bson.M{},
			wantZero: true,
		},
		{
			name: "invalid types",
			event: bson.M{
				"clusterTime": "not-a-timestamp",
				"wallTime":    12345,
			},
			wantZero: true,
		},
	}

	// Act & Assert loops: run table tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractEventTime(tt.event)
			if tt.wantZero {
				if !got.IsZero() {
					t.Errorf("ExtractEventTime() = %v, want zero time", got)
				}
			} else {
				if got.IsZero() {
					t.Fatalf("ExtractEventTime() returned zero time, want %v", expectedTime)
				}
				if got.Unix() != nowSec {
					t.Errorf("ExtractEventTime() = %v, want %v", got, expectedTime)
				}
			}
		})
	}
}

// TestBuildPartitionPipelineRedundantStreamsCount verifies that when totalStreams <= 1 and no filters are set, an empty pipeline is safely returned.
func TestBuildPartitionPipelineRedundantStreamsCount(t *testing.T) {
	// Act: build pipeline for single-stream partition index 0 of 1 total stream
	p1 := BuildPartitionPipeline(0, 1, "", nil)
	// Assert: pipeline must be completely empty
	if len(p1) != 0 {
		t.Errorf("BuildPartitionPipeline(0, 1) returned len %d, want empty", len(p1))
	}
	// Act 2: build pipeline for boundary stream count 0
	p0 := BuildPartitionPipeline(0, 0, "", nil)
	// Assert: pipeline must be completely empty
	if len(p0) != 0 {
		t.Errorf("BuildPartitionPipeline(0, 0) returned len %d, want empty", len(p0))
	}
}

// TestBuildPartitionPipelineValidStructure verifies the exact BSON stage operators structure for valid partitions.
func TestBuildPartitionPipelineValidStructure(t *testing.T) {
	// Arrange: set up target partition index (2) and total partition streams (4)
	streamIndex := 2
	totalStreams := 4
	// Act: build the partition aggregation query match stage
	pipeline := BuildPartitionPipeline(streamIndex, totalStreams, "", nil)

	// Assert 1: Must contain exactly one match stage
	if len(pipeline) != 1 {
		t.Fatalf("BuildPartitionPipeline returned len %d, want 1 stage", len(pipeline))
	}

	// Marshal and unmarshal to generic BSON map to easily traverse the BSON tree paths
	var doc bson.M
	bytes, err := bson.Marshal(pipeline[0])
	if err != nil {
		t.Fatalf("Failed to marshal BSON pipeline stage: %v", err)
	}
	if err := bson.Unmarshal(bytes, &doc); err != nil {
		t.Fatalf("Failed to unmarshal BSON pipeline stage: %v", err)
	}

	// Assert 2: Verify root stage is a '$match' operator
	matchVal, exists := doc["$match"]
	if !exists {
		t.Fatalf("Pipeline stage does not contain '$match' operator: %v", doc)
	}

	matchDoc, ok := matchVal.(bson.M)
	if !ok {
		t.Fatalf("'$match' value is not a BSON document: %T", matchVal)
	}

	// Assert 3: Verify match contains '$expr' expression operator
	exprVal, exists := matchDoc["$expr"]
	if !exists {
		t.Fatalf("'$match' doc does not contain '$expr' condition: %v", matchDoc)
	}

	exprDoc, ok := exprVal.(bson.M)
	if !ok {
		t.Fatalf("'$expr' value is not a BSON document: %T", exprVal)
	}

	// Assert 4: Verify expression matches equality via '$eq'
	eqVal, exists := exprDoc["$eq"]
	if !exists {
		t.Fatalf("'$expr' doc does not contain '$eq' operator: %v", exprDoc)
	}

	eqArray, ok := eqVal.(primitive.A)
	if !ok {
		t.Fatalf("'$eq' value is not a BSON array: %T", eqVal)
	}

	if len(eqArray) != 2 {
		t.Fatalf("'$eq' array len is %d, want 2 operands", len(eqArray))
	}

	// Assert 5: Verify the target stream index parameter is mapped correctly as the second operand
	var indexVal int
	switch gotVal := eqArray[1].(type) {
	case int32:
		indexVal = int(gotVal)
	case int64:
		indexVal = int(gotVal)
	case int:
		indexVal = gotVal
	default:
		t.Fatalf("Second operand is not an integer: %T (%v)", eqArray[1], eqArray[1])
	}

	if indexVal != streamIndex {
		t.Errorf("Second operand value is %d, want streamIndex %d", indexVal, streamIndex)
	}
}

// TestBuildPartitionPipelineWithNamespaceAndCollections verifies that namespace filtering and ID partitioning are composed correctly.
func TestBuildPartitionPipelineWithNamespaceAndCollections(t *testing.T) {
	sourceDB := "ecommerce"
	collections := []config.CollectionConfig{
		{SourceCollection: "orders", TargetCollection: "orders"},
		{SourceCollection: "users", TargetCollection: "users"},
	}

	// Case 1: Partitioned stream (totalStreams = 4) -> 2 stages (match namespace + match partition hash)
	pipeline := BuildPartitionPipeline(1, 4, sourceDB, collections)
	if len(pipeline) != 2 {
		t.Fatalf("Expected 2 stages for partitioned stream with collections, got %d", len(pipeline))
	}

	// Check stage 0 (namespace filtering)
	var stage0 bson.M
	b0, err := bson.Marshal(pipeline[0])
	if err != nil {
		t.Fatalf("Failed to marshal stage 0: %v", err)
	}
	if err := bson.Unmarshal(b0, &stage0); err != nil {
		t.Fatalf("Failed to unmarshal stage 0: %v", err)
	}

	matchDoc0, ok := stage0["$match"].(bson.M)
	if !ok {
		t.Fatalf("Stage 0 is not a $match doc: %v", stage0)
	}
	if matchDoc0["ns.db"] != "ecommerce" {
		t.Errorf("Expected ns.db 'ecommerce', got %v", matchDoc0["ns.db"])
	}
	collDoc, ok := matchDoc0["ns.coll"].(bson.M)
	if !ok {
		t.Fatalf("Expected ns.coll to be bson.M, got %T (%v)", matchDoc0["ns.coll"], matchDoc0["ns.coll"])
	}
	inList, ok := collDoc["$in"].(primitive.A)
	if !ok {
		t.Fatalf("Expected $in to be primitive.A, got %T", collDoc["$in"])
	}
	if len(inList) != 2 || inList[0] != "orders" || inList[1] != "users" {
		t.Errorf("Unexpected $in list: %v", inList)
	}

	// Case 2: Single stream (totalStreams = 1) -> 1 stage (match namespace only, no partition hash)
	singlePipeline := BuildPartitionPipeline(0, 1, sourceDB, collections)
	if len(singlePipeline) != 1 {
		t.Fatalf("Expected 1 stage for single stream with collections, got %d", len(singlePipeline))
	}
}

// TestBuildPartitionPipelineDatabaseOnly verifies full database mode without collection restriction.
func TestBuildPartitionPipelineDatabaseOnly(t *testing.T) {
	sourceDB := "ecommerce"

	pipeline := BuildPartitionPipeline(0, 1, sourceDB, nil)
	if len(pipeline) != 1 {
		t.Fatalf("Expected 1 stage, got %d", len(pipeline))
	}

	var stage bson.M
	b, err := bson.Marshal(pipeline[0])
	if err != nil {
		t.Fatalf("Failed to marshal stage: %v", err)
	}
	if err := bson.Unmarshal(b, &stage); err != nil {
		t.Fatalf("Failed to unmarshal stage: %v", err)
	}

	matchDoc, ok := stage["$match"].(bson.M)
	if !ok {
		t.Fatalf("Stage is not a $match doc: %v", stage)
	}
	if matchDoc["ns.db"] != "ecommerce" {
		t.Errorf("Expected ns.db 'ecommerce', got %v", matchDoc["ns.db"])
	}
	if _, exists := matchDoc["ns.coll"]; exists {
		t.Errorf("Expected no ns.coll in full database mode, but got %v", matchDoc["ns.coll"])
	}
}

// buildPartitionPipelineFull represents a helper model of the full-string hashing aggregation pipeline (our baseline)
func buildPartitionPipelineFull(streamIndex, totalStreams int) mongo.Pipeline {
	if totalStreams <= 1 {
		return mongo.Pipeline{}
	}

	const asciiString = " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"

	stringHash := bson.D{
		bson.E{Key: "$reduce", Value: bson.D{
			bson.E{Key: "input", Value: bson.D{
				bson.E{Key: "$split", Value: bson.A{
					bson.D{bson.E{Key: "$toString", Value: "$documentKey._id"}},
					"",
				}},
			}},
			bson.E{Key: "initialValue", Value: int64(2166136261)},
			bson.E{Key: "in", Value: bson.D{
				bson.E{Key: "$mod", Value: bson.A{
					bson.D{
						bson.E{Key: "$add", Value: bson.A{
							bson.D{bson.E{Key: "$multiply", Value: bson.A{int64(16777619), "$$value"}}},
							bson.D{
								bson.E{Key: "$add", Value: bson.A{
									32,
									bson.D{
										bson.E{Key: "$indexOfCP", Value: bson.A{
											asciiString,
											"$$this",
										}},
									},
								}},
							},
						}},
					},
					int64(4294967296),
				}},
			}},
		}},
	}

	return mongo.Pipeline{
		bson.D{
			bson.E{Key: "$match", Value: bson.D{
				bson.E{Key: "$expr", Value: bson.D{
					bson.E{Key: "$eq", Value: bson.A{
						bson.D{
							bson.E{Key: "$mod", Value: bson.A{
								stringHash,
								totalStreams,
							}},
						},
						streamIndex,
					}},
				}},
			}},
		},
	}
}

// BenchmarkBuildPartitionPipelineFull evaluates the client-side CPU compile performance of the full-string baseline BSON pipeline builder in Go memory.
func BenchmarkBuildPartitionPipelineFull(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = buildPartitionPipelineFull(i%4, 4)
	}
}

// BenchmarkBuildPartitionPipelineTrailing4 evaluates the Go-side CPU compile performance of the highly optimized flat loopless FNV-inspired BSON partition pipeline builder.
func BenchmarkBuildPartitionPipelineTrailing4(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = BuildPartitionPipeline(i%4, 4, "testdb", nil)
	}
}

// TestExtractNamespaceFromRawEvent validates BSON lookup parsing, data typing constraints, and field boundary coverage for namespace paths in raw change events.
func TestExtractNamespaceFromRawEvent(t *testing.T) {
	tests := []struct {
		name     string
		event    bson.M
		expected string
	}{
		{
			name: "valid namespace",
			event: bson.M{
				"ns": bson.M{
					"db":   "testdb",
					"coll": "testcoll",
				},
			},
			expected: "testdb.testcoll",
		},
		{
			name:     "missing ns key",
			event:    bson.M{},
			expected: "",
		},
		{
			name: "non-document ns type",
			event: bson.M{
				"ns": "not-a-document",
			},
			expected: "",
		},
		{
			name: "missing db key inside ns",
			event: bson.M{
				"ns": bson.M{
					"coll": "testcoll",
				},
			},
			expected: "",
		},
		{
			name: "missing coll key inside ns",
			event: bson.M{
				"ns": bson.M{
					"db": "testdb",
				},
			},
			expected: "",
		},
		{
			name: "non-string db type inside ns",
			event: bson.M{
				"ns": bson.M{
					"db":   12345,
					"coll": "testcoll",
				},
			},
			expected: "",
		},
		{
			name: "non-string coll type inside ns",
			event: bson.M{
				"ns": bson.M{
					"db":   "testdb",
					"coll": true,
				},
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rawBytes, err := bson.Marshal(tt.event)
			if err != nil {
				t.Fatalf("failed to marshal target BSON: %v", err)
			}
			rawEvent := bson.Raw(rawBytes)
			got := ExtractNamespaceFromRawEvent(rawEvent)
			if got != tt.expected {
				t.Errorf("ExtractNamespaceFromRawEvent() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// TestBuildPartitionPipelineCustomShardKey verifies pipeline construction with custom and compound shard keys.
func TestBuildPartitionPipelineCustomShardKey(t *testing.T) {
	// Case 1: Single custom shard key ("order_id")
	colls1 := []config.CollectionConfig{
		{SourceCollection: "orders", TargetCollection: "orders", ShardKey: "order_id"},
	}
	p1 := BuildPartitionPipeline(0, 4, "testdb", colls1)
	if len(p1) != 2 {
		t.Fatalf("Expected 2 stages ($match + hash), got %d", len(p1))
	}
	stageBytes1, err := bson.Marshal(p1[1])
	if err != nil {
		t.Fatalf("Failed to marshal stage: %v", err)
	}
	stageStr1 := string(stageBytes1)
	if !strings.Contains(stageStr1, "$documentKey.order_id") {
		t.Errorf("Expected pipeline to reference $documentKey.order_id for custom shard key, got: %s", stageStr1)
	}

	// Case 2: Compound shard key ("customer_id,order_id")
	colls2 := []config.CollectionConfig{
		{SourceCollection: "orders", TargetCollection: "orders", ShardKey: "customer_id,order_id"},
	}
	p2 := BuildPartitionPipeline(0, 4, "testdb", colls2)
	stageBytes2, err := bson.Marshal(p2[1])
	if err != nil {
		t.Fatalf("Failed to marshal stage: %v", err)
	}
	stageStr2 := string(stageBytes2)
	if !strings.Contains(stageStr2, "$documentKey.customer_id") || !strings.Contains(stageStr2, "$documentKey.order_id") || !strings.Contains(stageStr2, "$concat") {
		t.Errorf("Expected compound pipeline to use $concat on customer_id and order_id, got: %s", stageStr2)
	}

	// Case 3: Mixed collections (orders with order_id, users with default _id)
	colls3 := []config.CollectionConfig{
		{SourceCollection: "orders", TargetCollection: "orders", ShardKey: "order_id"},
		{SourceCollection: "users", TargetCollection: "users", ShardKey: ""},
	}
	p3 := BuildPartitionPipeline(0, 4, "testdb", colls3)
	stageBytes3, err := bson.Marshal(p3[1])
	if err != nil {
		t.Fatalf("Failed to marshal stage: %v", err)
	}
	stageStr3 := string(stageBytes3)
	if !strings.Contains(stageStr3, "$switch") || !strings.Contains(stageStr3, "$documentKey.order_id") || !strings.Contains(stageStr3, "$documentKey._id") {
		t.Errorf("Expected mixed collections pipeline to use $switch, got: %s", stageStr3)
	}
}

// TestGetCollectionsToProcessShardKeyHierarchy verifies the 3-tier precedence hierarchy for shard keys
// (explicit config > auto-detected > default _id).
func TestGetCollectionsToProcessShardKeyHierarchy(t *testing.T) {
	migrator := &Migrator{
		log: logger.New(),
	}
	ctx := context.Background()

	// Case 1: Explicit collection config with custom shardKey should be preserved (Tier 1)
	inputCollections := []config.CollectionConfig{
		{
			SourceCollection: "orders",
			TargetCollection: "orders",
			ShardKey:         "order_id",
		},
		{
			SourceCollection: "users",
			TargetCollection: "users",
			ShardKey:         "", // Unspecified, should default to _id (Tier 3 fallback)
		},
	}

	result, err := migrator.getCollectionsToProcess(ctx, nil, inputCollections)
	if err != nil {
		t.Fatalf("getCollectionsToProcess failed: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("Expected 2 collections, got %d", len(result))
	}

	if result[0].ShardKey != "order_id" {
		t.Errorf("Expected orders ShardKey to be 'order_id', got %q", result[0].ShardKey)
	}

	if result[1].ShardKey != "_id" {
		t.Errorf("Expected users ShardKey to default to '_id', got %q", result[1].ShardKey)
	}
}

// TestAutoDetectSourceShardKeysGracefulFallback verifies that when sourceDB or client is nil,
// autoDetectSourceShardKeys returns an empty map without panicking.
func TestAutoDetectSourceShardKeysGracefulFallback(t *testing.T) {
	ctx := context.Background()

	res1 := autoDetectSourceShardKeys(ctx, nil, "testdb")
	if len(res1) != 0 {
		t.Errorf("Expected empty map for nil sourceDB, got %v", res1)
	}

	res2 := autoDetectSourceShardKeys(ctx, nil, "")
	if len(res2) != 0 {
		t.Errorf("Expected empty map for empty dbName, got %v", res2)
	}
}

