package db

import (
	"testing"

	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestSetFullDocumentMode(t *testing.T) {
	tests := []struct {
		input    string
		expected options.FullDocument
	}{
		{"updateLookup", options.UpdateLookup},
		{"whenAvailable", options.WhenAvailable},
		{"required", options.Required},
		{"default", options.Default},
		{"", options.UpdateLookup}, // default fallback
	}

	for _, tt := range tests {
		m := &MongoDB{}
		m.SetFullDocumentMode(tt.input)
		if got := m.GetFullDocument(); got != tt.expected {
			t.Errorf("SetFullDocumentMode(%q) = %q, expected %q", tt.input, got, tt.expected)
		}
	}
}
