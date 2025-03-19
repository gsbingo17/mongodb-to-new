package migration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ResumeToken represents a MongoDB change stream resume token
type ResumeToken struct {
	Data string `json:"_data"`
}

// GetResumeTokenPath returns the path for a resume token file
func GetResumeTokenPath(database, collection string) string {
	return fmt.Sprintf("resumeToken-%s-%s.json", database, collection)
}

// tryLoadToken attempts to load a resume token from a file
func tryLoadToken(filePath string) (interface{}, error) {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("file does not exist: %s", filePath)
	}

	// Read file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read resume token file: %w", err)
	}

	// Parse JSON
	var resumeToken ResumeToken
	if err := json.Unmarshal(data, &resumeToken); err != nil {
		return nil, fmt.Errorf("failed to parse resume token: %w", err)
	}

	// If the _data field is empty, return nil
	if resumeToken.Data == "" {
		return nil, nil
	}

	// Extract the actual value if it's a string representation of a map
	dataValue := resumeToken.Data
	if strings.HasPrefix(dataValue, "map[_data:") {
		// Extract the value after "map[_data:" and before the closing "]"
		start := len("map[_data:")
		end := strings.LastIndex(dataValue, "]")
		if end > start {
			dataValue = dataValue[start:end]
		}
	}

	// Return the token in the format expected by MongoDB
	tokenDoc := map[string]interface{}{
		"_data": dataValue,
	}
	return tokenDoc, nil
}

// LoadResumeToken loads a resume token from a file with fallback to versioned backups
func LoadResumeToken(filePath string) (interface{}, error) {
	// fmt.Printf("Loading resume token from %s\n", filePath)
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// Create directory if it doesn't exist
		dir := filepath.Dir(filePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}

		// Create empty resume token file
		if err := SaveResumeToken(filePath, nil); err != nil {
			return nil, err
		}

		return nil, nil
	}

	// Try to load the main token file
	token, err := tryLoadToken(filePath)
	if err == nil {
		return token, nil
	}

	fmt.Printf("Failed to load resume token from main file: %v. Trying backups...\n", err)

	// Try versioned backups
	for i := 1; i <= 10; i++ {
		backupPath := fmt.Sprintf("%s.%d", filePath, i)
		token, err := tryLoadToken(backupPath)
		if err == nil {
			fmt.Printf("Successfully loaded resume token from backup version %d\n", i)

			// Restore this backup as the main file
			if copyErr := copyFile(backupPath, filePath); copyErr != nil {
				fmt.Printf("Warning: Failed to restore backup as main token file: %v\n", copyErr)
			} else {
				fmt.Printf("Restored backup version %d as main resume token file\n", i)
			}

			return token, nil
		}
	}

	// If all backups failed, return nil (start fresh)
	fmt.Printf("All resume token backups failed or don't exist. Starting fresh.\n")
	return nil, nil
}

// DeleteResumeToken deletes a resume token file and all its versioned backups
func DeleteResumeToken(filePath string) error {
	// Check if main file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// Main file doesn't exist, but we'll still try to delete backups
		fmt.Printf("Main resume token file %s doesn't exist, checking for backups\n", filePath)
	} else {
		// Delete the main file
		if err := os.Remove(filePath); err != nil {
			return fmt.Errorf("failed to delete resume token file: %w", err)
		}
		fmt.Printf("Deleted resume token file: %s\n", filePath)
	}

	// Delete all versioned backups
	for i := 1; i <= 10; i++ {
		backupPath := fmt.Sprintf("%s.%d", filePath, i)
		if _, err := os.Stat(backupPath); err == nil {
			if err := os.Remove(backupPath); err != nil {
				fmt.Printf("Warning: Failed to delete backup version %d: %v\n", i, err)
			} else {
				fmt.Printf("Deleted backup resume token file: %s\n", backupPath)
			}
		}
	}

	return nil
}

// SaveResumeToken saves a resume token to a file and maintains up to 10 backup versions
func SaveResumeToken(filePath string, token interface{}) error {
	// Create backup of existing token file if it exists
	if _, err := os.Stat(filePath); err == nil {
		// Rotate existing backups (10 -> delete, 9 -> 10, 8 -> 9, etc.)
		// Delete the oldest backup (version 10) if it exists
		backupPath10 := filePath + ".10"
		os.Remove(backupPath10) // Ignore errors if file doesn't exist

		// Shift all other backups down
		for i := 9; i >= 1; i-- {
			oldPath := fmt.Sprintf("%s.%d", filePath, i)
			newPath := fmt.Sprintf("%s.%d", filePath, i+1)

			// If this backup exists, rename it
			if _, err := os.Stat(oldPath); err == nil {
				os.Rename(oldPath, newPath)
			}
		}

		// Create new backup (version 1) from current token
		backupPath1 := filePath + ".1"
		if err := copyFile(filePath, backupPath1); err != nil {
			return fmt.Errorf("failed to create backup of resume token: %w", err)
		}
	}

	// If token is nil, save an empty token
	if token == nil {
		resumeToken := ResumeToken{
			Data: "",
		}
		data, err := json.MarshalIndent(resumeToken, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal resume token: %w", err)
		}
		return os.WriteFile(filePath, data, 0644)
	}

	// Print the token type and value for debugging
	// fmt.Printf("SaveResumeToken token type: %T, value: %v\n", token, token)

	// Extract the _data field from the token
	var dataValue string

	// Handle different token types
	switch v := token.(type) {
	case map[string]interface{}: // This handles bson.M as well since it's a type alias for map[string]interface{}
		// If token is a map with _data field, extract it
		if data, ok := v["_data"]; ok {
			switch d := data.(type) {
			case string:
				// If _data is a string, use it directly
				dataValue = d
			case map[string]interface{}:
				// If _data is a nested map, check if it has an _data field
				if innerData, ok := d["_data"]; ok {
					dataValue = fmt.Sprintf("%v", innerData)
				} else {
					// Otherwise, convert the map to a string and parse it
					mapStr := fmt.Sprintf("%v", d)
					if strings.HasPrefix(mapStr, "map[_data:") {
						// Extract the value after "map[_data:" and before the closing "]"
						start := len("map[_data:")
						end := strings.LastIndex(mapStr, "]")
						if end > start {
							dataValue = mapStr[start:end]
						} else {
							dataValue = mapStr
						}
					} else {
						dataValue = mapStr
					}
				}
			default:
				// For any other type, convert to string and check if it's a map representation
				mapStr := fmt.Sprintf("%v", d)
				if strings.HasPrefix(mapStr, "map[_data:") {
					// Extract the value after "map[_data:" and before the closing "]"
					start := len("map[_data:")
					end := strings.LastIndex(mapStr, "]")
					if end > start {
						dataValue = mapStr[start:end]
					} else {
						dataValue = mapStr
					}
				} else {
					dataValue = mapStr
				}
			}
		} else {
			// If no _data field, convert the map to a string and check if it contains _data
			mapStr := fmt.Sprintf("%v", v)
			if strings.HasPrefix(mapStr, "map[_data:") {
				// Extract the value after "map[_data:" and before the closing "]"
				start := len("map[_data:")
				end := strings.LastIndex(mapStr, "]")
				if end > start {
					dataValue = mapStr[start:end]
				} else {
					dataValue = mapStr
				}
			} else {
				dataValue = mapStr
			}
		}
	case string:
		// If token is a string, try to parse it as JSON
		var tokenMap map[string]interface{}
		if err := json.Unmarshal([]byte(v), &tokenMap); err == nil {
			// If it has _data field, use that
			if data, ok := tokenMap["_data"]; ok {
				switch d := data.(type) {
				case string:
					dataValue = d
				default:
					dataValue = fmt.Sprintf("%v", d)
				}
			} else {
				// Otherwise use the original string
				dataValue = v
			}
		} else {
			// If it's not valid JSON, use the string directly
			dataValue = v
		}
	default:
		// For any other type, use the string representation
		dataValue = fmt.Sprintf("%v", v)
	}

	// Create resume token with the simplified structure
	resumeToken := ResumeToken{
		Data: dataValue,
	}

	// Print the token structure for debugging
	// fmt.Printf("Final resumeToken structure: %+v\n", resumeToken)

	// Marshal to JSON
	data, err := json.MarshalIndent(resumeToken, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal resume token: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write resume token file: %w", err)
	}

	return nil
}

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	// Read source file
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}

	// Write to destination file
	return os.WriteFile(dst, data, 0644)
}
