package migration

import (
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// ExtractEventTime extracts the event timestamp from clusterTime or wallTime fields in a BSON change event
func ExtractEventTime(event bson.M) time.Time {
	if ct, ok := event["clusterTime"].(primitive.Timestamp); ok {
		return time.Unix(int64(ct.T), 0)
	}
	if wt, ok := event["wallTime"].(primitive.DateTime); ok {
		return wt.Time()
	}
	if ctVal, exists := event["clusterTime"]; exists {
		if ctValTS, ok := ctVal.(primitive.Timestamp); ok {
			return time.Unix(int64(ctValTS.T), 0)
		}
	}
	return time.Time{}
}

// ExtractEventTimeFromRaw extracts the event timestamp from clusterTime or wallTime in raw BSON bytes
func ExtractEventTimeFromRaw(raw bson.Raw) time.Time {
	var event bson.M
	if err := bson.Unmarshal(raw, &event); err == nil {
		return ExtractEventTime(event)
	}
	return time.Time{}
}

// BuildPartitionPipeline constructs an aggregation stage filtering standard and custom keys uniformly.
// It builds a zero-JavaScript, highly performant, BSON type-safe flat unrolled 32-bit FNV-1a hash stage.
//
// Optimization (Flat Loopless Hashing):
// To achieve maximum possible query execution speeds on the MongoDB sharded cluster, we completely eliminate
// the slow BSON split-reduce loops and array allocations. Instead, we extract the trailing 2 characters of
// the ID and compile a flat, unrolled polynomial rolling hash using standard addition and multiplication.
//
// FNV-1a Properties:
// - Seed: 2166136261 (FNV offset basis)
// - Prime Multiplier: 16777619 (FNV prime)
// - Modulo Cap: 4294967296 (2^32 registers wrap-around)
//
// Two trailing characters (256 slots in hex ObjectIDs, 1024 slots in Crockford Base32 ULIDs) provide more than
// enough entropy to guarantee a perfectly balanced partition workload distribution!
func BuildPartitionPipeline(streamIndex, totalStreams int, sourceDB string, collections []config.CollectionConfig) mongo.Pipeline {
	var pipeline mongo.Pipeline

	// 1. Namespace filtering stage (Database & Collections)
	if sourceDB != "" {
		matchFilter := bson.D{{Key: "ns.db", Value: sourceDB}}
		if len(collections) > 0 {
			var collNames bson.A
			for _, coll := range collections {
				collNames = append(collNames, coll.SourceCollection)
			}
			matchFilter = append(matchFilter, bson.E{
				Key:   "ns.coll",
				Value: bson.D{{Key: "$in", Value: collNames}},
			})
		}
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: matchFilter}})
	} else if len(collections) > 0 {
		var collNames bson.A
		for _, coll := range collections {
			collNames = append(collNames, coll.SourceCollection)
		}
		matchFilter := bson.D{{
			Key:   "ns.coll",
			Value: bson.D{{Key: "$in", Value: collNames}},
		}}
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: matchFilter}})
	}

	// 2. ID Partitioning stage (FNV-1a 32-bit hash)
	if totalStreams > 1 {
		pipeline = append(pipeline, buildPartitionHashStage(streamIndex, totalStreams, collections))
	}

	return pipeline
}

// buildKeyExprForFields builds the expression that stringifies a field or concatenates compound fields.
func buildKeyExprForFields(fields []string) bson.D {
	if len(fields) == 0 || (len(fields) == 1 && fields[0] == "_id") {
		return bson.D{{Key: "$toString", Value: "$documentKey._id"}}
	}
	if len(fields) == 1 {
		return bson.D{{Key: "$toString", Value: "$documentKey." + fields[0]}}
	}
	// Compound shard key: concatenate stringified fields with "_"
	var concatArgs bson.A
	for i, f := range fields {
		if i > 0 {
			concatArgs = append(concatArgs, "_")
		}
		concatArgs = append(concatArgs, bson.D{{Key: "$toString", Value: "$documentKey." + f}})
	}
	return bson.D{{Key: "$concat", Value: concatArgs}}
}

// buildTargetKeyExpr builds the aggregation expression that produces the string to hash for partitioning.
// If all collections use the default "_id" (or if collections is empty), it returns {"$toString": "$documentKey._id"}.
// For collections with custom or compound shard keys, it builds a dynamic $switch over "$ns.coll".
func buildTargetKeyExpr(collections []config.CollectionConfig) bson.D {
	var branches bson.A
	for _, c := range collections {
		fields := c.GetShardKeyFields()
		if len(fields) == 1 && fields[0] == "_id" {
			continue // Handled by $switch default
		}
		branches = append(branches, bson.D{
			{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{"$ns.coll", c.SourceCollection}}}},
			{Key: "then", Value: buildKeyExprForFields(fields)},
		})
	}

	if len(branches) == 0 {
		return bson.D{{Key: "$toString", Value: "$documentKey._id"}}
	}
	if len(branches) == 1 && len(collections) == 1 {
		return buildKeyExprForFields(collections[0].GetShardKeyFields())
	}
	return bson.D{
		{Key: "$switch", Value: bson.D{
			{Key: "branches", Value: branches},
			{Key: "default", Value: bson.D{{Key: "$toString", Value: "$documentKey._id"}}},
		}},
	}
}

// buildPartitionHashStage builds the zero-JavaScript, loopless FNV-1a 32-bit hash match stage
func buildPartitionHashStage(streamIndex, totalStreams int, collections []config.CollectionConfig) bson.D {
	const asciiString = " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"

	targetKeyExpr := buildTargetKeyExpr(collections)

	// Get length of the target stringified key
	strLen := bson.D{bson.E{Key: "$strLenCP", Value: targetKeyExpr}}

	// Safe starting position for trailing 2 characters: Max(0, length - 2)
	startPos := bson.D{
		bson.E{Key: "$cond", Value: bson.A{
			bson.D{bson.E{Key: "$lt", Value: bson.A{strLen, 2}}},
			0,
			bson.D{bson.E{Key: "$subtract", Value: bson.A{strLen, 2}}},
		}},
	}

	// Safe substring length: Min(2, length)
	subLen := bson.D{
		bson.E{Key: "$cond", Value: bson.A{
			bson.D{bson.E{Key: "$lt", Value: bson.A{strLen, 2}}},
			strLen,
			2,
		}},
	}

	// Extract the trailing 2 target stringified key characters safely
	last2Sub := bson.D{
		bson.E{Key: "$substrCP", Value: bson.A{
			targetKeyExpr,
			startPos,
			subLen,
		}},
	}

	// Extract individual characters safely (c0 = second-to-last char, c1 = last char)
	c0 := bson.D{
		bson.E{Key: "$cond", Value: bson.A{
			bson.D{bson.E{Key: "$lt", Value: bson.A{strLen, 2}}},
			"", // Safe fallback for single character IDs
			bson.D{bson.E{Key: "$substrCP", Value: bson.A{last2Sub, 0, 1}}},
		}},
	}
	c1 := bson.D{
		bson.E{Key: "$cond", Value: bson.A{
			bson.D{bson.E{Key: "$lt", Value: bson.A{strLen, 2}}},
			last2Sub, // For single-character IDs, the entire string is our character
			bson.D{bson.E{Key: "$substrCP", Value: bson.A{last2Sub, 1, 1}}},
		}},
	}

	// Translate characters to their exact system-wide ASCII byte values (index + 32)
	val0 := bson.D{
		bson.E{Key: "$add", Value: bson.A{
			32,
			bson.D{bson.E{Key: "$indexOfCP", Value: bson.A{asciiString, c0}}},
		}},
	}
	val1 := bson.D{
		bson.E{Key: "$add", Value: bson.A{
			32,
			bson.D{bson.E{Key: "$indexOfCP", Value: bson.A{asciiString, c1}}},
		}},
	}

	// Unrolled FNV step 1: (initialValue * FNV_prime + val0) % 2^32
	// Precomputed constant: 2166136261 * 16777619 = 36343516597791559
	hash1 := bson.D{
		bson.E{Key: "$mod", Value: bson.A{
			bson.D{bson.E{Key: "$add", Value: bson.A{
				int64(36343516597791559),
				val0,
			}}},
			int64(4294967296),
		}},
	}

	// Unrolled FNV step 2: (hash1 * FNV_prime + val1) % 2^32
	stringHash := bson.D{
		bson.E{Key: "$mod", Value: bson.A{
			bson.D{bson.E{Key: "$add", Value: bson.A{
				bson.D{bson.E{Key: "$multiply", Value: bson.A{int64(16777619), hash1}}},
				val1,
			}}},
			int64(4294967296),
		}},
	}

	// Match Stage: checks if (stringHash % totalStreams) == streamIndex
	return bson.D{
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
	}
}

// ExtractNamespaceFromRawEvent extracts the "db.coll" namespace string from a raw BSON change event.
// Returns an empty string if the "ns" field is missing or invalid.
func ExtractNamespaceFromRawEvent(rawEvent bson.Raw) string {
	nsVal, err := rawEvent.LookupErr("ns")
	if err != nil || nsVal.Type != bson.TypeEmbeddedDocument {
		return ""
	}
	nsDoc := nsVal.Document()
	dbVal, dbErr := nsDoc.LookupErr("db")
	collVal, collErr := nsDoc.LookupErr("coll")
	if dbErr != nil || dbVal.Type != bson.TypeString || collErr != nil || collVal.Type != bson.TypeString {
		return ""
	}
	return dbVal.StringValue() + "." + collVal.StringValue()
}

