package migration

import (
	"encoding/json"
	"fmt"
	"os"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// OplogTimestamp represents an oplog timestamp for resume functionality
type OplogTimestamp struct {
	Timestamp primitive.Timestamp `json:"ts"`
	Term      int64               `json:"t,omitempty"`
}

// SaveOplogTimestamp saves the oplog timestamp to a file
func SaveOplogTimestamp(path string, timestamp OplogTimestamp) error {
	// Create backup of existing file if it exists
	if _, err := os.Stat(path); err == nil {
		backupPath := path + ".backup"
		if err := os.Rename(path, backupPath); err != nil {
			return fmt.Errorf("failed to create backup of oplog timestamp file: %w", err)
		}
	}

	// Marshal timestamp to JSON
	data, err := json.MarshalIndent(timestamp, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal oplog timestamp: %w", err)
	}

	// Write to file
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write oplog timestamp file: %w", err)
	}

	return nil
}

// LoadOplogTimestamp loads the oplog timestamp from a file
func LoadOplogTimestamp(path string) (*OplogTimestamp, error) {
	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, nil
	}

	// Read file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read oplog timestamp file: %w", err)
	}

	// Unmarshal JSON
	var timestamp OplogTimestamp
	if err := json.Unmarshal(data, &timestamp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal oplog timestamp: %w", err)
	}

	return &timestamp, nil
}

// DeleteOplogTimestamp deletes the oplog timestamp file
func DeleteOplogTimestamp(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete oplog timestamp file: %w", err)
	}
	return nil
}
