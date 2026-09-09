package migration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestClassifyError(t *testing.T) {
	log := logger.New()
	r := NewRetryManager(3, 10*time.Millisecond, 100*time.Millisecond, true, 2, true, log)

	// Connection errors (case-insensitive checks)
	connectionCases := []string{
		"socket was unexpectedly closed",
		"Socket was unexpectedly closed",
		"EOF",
		"eof",
		"connection reset by peer",
		"Connection Reset By Peer",
		"broken pipe",
		"Broken Pipe",
		"i/o timeout",
		"I/O Timeout",
		"DeadlineExceeded",
		"Deadline exceeded",
		"deadline exceeded",
		"DEADLINE_EXCEEDED",
	}
	for _, msg := range connectionCases {
		if r.ClassifyError(errors.New(msg)) != ErrorTypeConnection {
			t.Errorf("expected ErrorTypeConnection for %q", msg)
		}
	}

	// Contention errors (case-insensitive checks)
	contentionCases := []string{
		"too much contention",
		"Too much contention on these documents. Please try again.",
		"cross-transaction contention",
		"Cross-Transaction Contention",
		"lock timeout",
		"Lock Timeout",
		"TransientTransactionError",
		"transienttransactionerror",
		"transient transaction error",
		"WriteConflict",
		"write conflict",
		"schema change",
		"Schema Change",
		"request was aborted due to a schema change involving the indexes used in the request. Retry the request to run against the updated schema",
		"OperationFailed: operation was Aborted",
		"operationfailed: aborted",
		"exceeded time limit",
		"Exceeded Time Limit",
	}
	for _, msg := range contentionCases {
		if r.ClassifyError(errors.New(msg)) != ErrorTypeContention {
			t.Errorf("expected ErrorTypeContention for %q", msg)
		}
	}

	// Invalid _id type error (case-insensitive checks)
	invalidIdCases := []string{
		"_id must be an objectId, string, long",
		"_id must be an ObjectId, string, long",
		"_ID MUST BE AN OBJECTID, STRING, LONG",
	}
	for _, msg := range invalidIdCases {
		if r.ClassifyError(errors.New(msg)) != ErrorTypeInvalidIdType {
			t.Errorf("expected ErrorTypeInvalidIdType for %q", msg)
		}
	}

	// Other errors
	if r.ClassifyError(errors.New("some other random database error")) != ErrorTypeOther {
		t.Error("expected ErrorTypeOther")
	}
}

func TestRetryWithBackoff(t *testing.T) {
	log := logger.New()
	r := NewRetryManager(3, 5*time.Millisecond, 20*time.Millisecond, false, 1, false, log)
	ctx := context.Background()

	// 1. Successful on second attempt
	attempts := 0
	err := r.RetryWithBackoff(ctx, func() error {
		attempts++
		if attempts < 2 {
			return errors.New("temporary connection error")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}

	// 2. Exceed max retries
	attempts = 0
	err = r.RetryWithBackoff(ctx, func() error {
		attempts++
		return errors.New("persistent failure")
	})

	if err == nil || err.Error() != "persistent failure" {
		t.Errorf("expected persistent failure error, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}

	// 3. Context canceled before retrying
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // Cancel immediately

	err = r.RetryWithBackoff(cancelCtx, func() error {
		return errors.New("should not be called multiple times")
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestRetryWithSplit(t *testing.T) {
	log := logger.New()
	r := NewRetryManager(3, 5*time.Millisecond, 20*time.Millisecond, true, 1, true, log)
	ctx := context.Background()

	batch := []interface{}{
		bson.M{"_id": 1.23, "name": "invalid float _id"}, // Invalid type (float64)
		bson.M{"_id": "valid_str", "name": "valid string _id"},
	}

	// Test _id conversion capability
	err := r.RetryWithSplit(ctx, batch, "test_coll", func(b []interface{}) error {
		for _, doc := range b {
			m := doc.(bson.M)
			id := m["_id"]
			switch id.(type) {
			case string, int64, primitive.ObjectID:
				// Acceptable types
			default:
				return errors.New("_id must be an objectId, string, long")
			}
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected success after string conversion, got %v", err)
	}

	// Verify first document was converted to string
	res := r.convertInvalidIds(batch, nil, "test_coll")
	firstID := res[0].(bson.M)["_id"]
	if _, ok := firstID.(string); !ok {
		t.Errorf("expected first _id to be converted to string, got %T (%v)", firstID, firstID)
	}
}
