package migration

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/mongo"
)

// ErrorType categorizes different types of errors
type ErrorType int

const (
	ErrorTypeConnection ErrorType = iota
	ErrorTypeContention
	ErrorTypeOther
)

// RetryManager handles retrying operations with different strategies
type RetryManager struct {
	MaxRetries       int
	BaseDelay        time.Duration
	MaxDelay         time.Duration
	EnableBatchSplit bool
	MinBatchSize     int
	Logger           *logger.Logger
}

// NewRetryManager creates a new retry manager
func NewRetryManager(maxRetries int, baseDelay, maxDelay time.Duration, enableBatchSplit bool, minBatchSize int, log *logger.Logger) *RetryManager {
	return &RetryManager{
		MaxRetries:       maxRetries,
		BaseDelay:        baseDelay,
		MaxDelay:         maxDelay,
		EnableBatchSplit: enableBatchSplit,
		MinBatchSize:     minBatchSize,
		Logger:           log,
	}
}

// ClassifyError determines the type of error
func (r *RetryManager) ClassifyError(err error) ErrorType {
	if err == nil {
		return ErrorTypeOther
	}

	errStr := err.Error()

	// Check for connection errors
	if strings.Contains(errStr, "socket was unexpectedly closed") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "i/o timeout") {
		return ErrorTypeConnection
	}

	// Check for contention errors
	if strings.Contains(errStr, "too much contention") ||
		strings.Contains(errStr, "lock timeout") ||
		strings.Contains(errStr, "OperationFailed") && strings.Contains(errStr, "Aborted") ||
		strings.Contains(errStr, "TransientTransactionError") ||
		strings.Contains(errStr, "WriteConflict") ||
		strings.Contains(errStr, "exceeded time limit") {
		return ErrorTypeContention
	}

	return ErrorTypeOther
}

// RetryWithBackoff retries an operation with exponential backoff
func (r *RetryManager) RetryWithBackoff(ctx context.Context, operation func() error) error {
	var err error

	for attempt := 0; attempt < r.MaxRetries; attempt++ {
		err = operation()
		if err == nil {
			return nil
		}

		// Check for context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Special handling for contention errors - use a fixed 5-second delay
		if r.ClassifyError(err) == ErrorTypeContention {
			r.Logger.Infof("Contention error detected. Waiting 5 seconds before retry %d/%d...",
				attempt+1, r.MaxRetries)

			// Wait for 5 seconds or until context is canceled
			select {
			case <-time.After(5 * time.Second):
				// Continue to next attempt
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		// Calculate delay with exponential backoff and jitter for non-contention errors
		delay := r.calculateBackoff(attempt)

		r.Logger.Debugf("Operation failed (attempt %d/%d): %v. Retrying in %v...",
			attempt+1, r.MaxRetries, err, delay)

		// Wait for the delay or until context is canceled
		select {
		case <-time.After(delay):
			// Continue to next attempt
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return err
}

// RetryWithSplit retries a batch operation with progressive splitting
func (r *RetryManager) RetryWithSplit(ctx context.Context, batch []interface{},
	processBatch func([]interface{}) error) error {

	// Try processing the full batch first
	err := processBatch(batch)
	if err == nil {
		return nil
	}

	// If batch splitting is not enabled or batch is already small, use backoff
	if !r.EnableBatchSplit || len(batch) <= r.MinBatchSize {
		return r.RetryWithBackoff(ctx, func() error {
			return processBatch(batch)
		})
	}

	// If it's a contention error, split the batch and retry
	if r.ClassifyError(err) == ErrorTypeContention {
		r.Logger.Debugf("Contention error detected. Splitting batch of size %d", len(batch))

		// Split the batch in half
		mid := len(batch) / 2
		firstHalf := batch[:mid]
		secondHalf := batch[mid:]

		// Process each half
		err1 := r.RetryWithSplit(ctx, firstHalf, processBatch)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err2 := r.RetryWithSplit(ctx, secondHalf, processBatch)

		// If both halves succeeded, return nil
		if err1 == nil && err2 == nil {
			return nil
		}

		// Otherwise, return the first non-nil error
		if err1 != nil {
			return err1
		}
		return err2
	}

	// For connection errors, use backoff with the full batch
	if r.ClassifyError(err) == ErrorTypeConnection {
		r.Logger.Debugf("Connection error detected. Retrying batch of size %d with backoff", len(batch))
		return r.RetryWithBackoff(ctx, func() error {
			return processBatch(batch)
		})
	}

	// For duplicate key errors, use upsert instead of retrying
	var bulkWriteErr mongo.BulkWriteException
	if errors.As(err, &bulkWriteErr) {
		// Check if all errors are duplicate key errors
		allDuplicateKeyErrors := true
		for _, writeErr := range bulkWriteErr.WriteErrors {
			if writeErr.Code != 11000 { // 11000 is the code for duplicate key error
				allDuplicateKeyErrors = false
				break
			}
		}

		if allDuplicateKeyErrors {
			r.Logger.Debugf("Bulk write error detected with %d duplicate key errors. Using upsert instead.",
				len(bulkWriteErr.WriteErrors))

			// Extract the documents that failed due to duplicate keys
			failedIndices := make(map[int]bool)
			for _, writeErr := range bulkWriteErr.WriteErrors {
				failedIndices[writeErr.Index] = true
			}

			// Create a new batch with only the failed documents
			var failedBatch []interface{}
			for i, doc := range batch {
				if failedIndices[i] {
					failedBatch = append(failedBatch, doc)
				}
			}

			// Use upsert for the failed documents
			return processBatch(failedBatch)
		}

		// For other bulk write errors, retry with backoff
		r.Logger.Debugf("Bulk write error detected with %d non-duplicate key errors. Retrying with backoff.",
			len(bulkWriteErr.WriteErrors))
	}

	// For other errors, use backoff with the full batch
	return r.RetryWithBackoff(ctx, func() error {
		return processBatch(batch)
	})
}

// calculateBackoff calculates the backoff delay with jitter
func (r *RetryManager) calculateBackoff(attempt int) time.Duration {
	// Calculate exponential backoff: baseDelay * 2^attempt
	backoff := float64(r.BaseDelay) * math.Pow(2, float64(attempt))

	// Add jitter: random value between 0.5 and 1.5 of the calculated backoff
	jitter := 0.5 + rand.Float64()
	backoff = backoff * jitter

	// Cap at max delay
	if backoff > float64(r.MaxDelay) {
		backoff = float64(r.MaxDelay)
	}

	return time.Duration(backoff)
}
