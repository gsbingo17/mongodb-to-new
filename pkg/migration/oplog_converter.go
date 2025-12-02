package migration

import (
	"fmt"
	"strings"

	"github.com/rwynn/gtm/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// GTMOpToChangeEvent converts a GTM operation to a unified change event format
// compatible with the existing EventDistributor
func GTMOpToChangeEvent(op *gtm.Op) (map[string]interface{}, error) {
	if op == nil {
		return nil, fmt.Errorf("operation is nil")
	}

	// Split namespace into database and collection
	parts := strings.SplitN(op.Namespace, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid namespace format: %s", op.Namespace)
	}
	database := parts[0]
	collection := parts[1]

	// Create base event structure
	event := map[string]interface{}{
		"ns": map[string]interface{}{
			"db":   database,
			"coll": collection,
		},
		"documentKey": map[string]interface{}{
			"_id": op.Id,
		},
	}

	// Convert operation type and set operation-specific fields
	switch op.Operation {
	case "i": // insert
		event["operationType"] = "insert"
		event["fullDocument"] = op.Data

	case "u": // update
		event["operationType"] = "update"
		// GTM provides the full document after update in op.Data
		if op.Data != nil {
			event["fullDocument"] = op.Data
		}
		// Create update description
		event["updateDescription"] = map[string]interface{}{
			"updatedFields": op.Data,
			"removedFields": []string{},
		}

	case "d": // delete
		event["operationType"] = "delete"
		// For delete, we only have the documentKey

	case "c": // command (like drop, rename, etc.)
		// For now, we'll skip command operations
		// Could be extended to handle collection drops, renames, etc.
		return nil, fmt.Errorf("command operations not supported: %s", op.Namespace)

	default:
		return nil, fmt.Errorf("unsupported operation type: %s", op.Operation)
	}

	return event, nil
}

// ConvertGTMOpToBSON converts a GTM operation to BSON document for processing
func ConvertGTMOpToBSON(op *gtm.Op) (bson.M, error) {
	event, err := GTMOpToChangeEvent(op)
	if err != nil {
		return nil, err
	}

	// Convert to bson.M
	return bson.M(event), nil
}

// ExtractTimestampFromGTMOp extracts the timestamp from a GTM operation
func ExtractTimestampFromGTMOp(op *gtm.Op) primitive.Timestamp {
	return op.Timestamp
}

// GetNamespaceFromGTMOp returns the full namespace (database.collection) from GTM op
func GetNamespaceFromGTMOp(op *gtm.Op) string {
	return op.Namespace
}

// ShouldProcessGTMOp determines if a GTM operation should be processed
// based on the namespace and operation type
func ShouldProcessGTMOp(op *gtm.Op, allowedNamespaces map[string]bool) bool {
	if op == nil {
		return false
	}

	// Check if namespace is in allowed list
	if !allowedNamespaces[op.Namespace] {
		return false
	}

	// Only process insert, update, and delete operations
	switch op.Operation {
	case "i", "u", "d":
		return true
	default:
		return false
	}
}
