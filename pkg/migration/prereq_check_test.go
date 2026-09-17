package migration

import (
	"strings"
	"testing"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestValidatePrePostImageRequirements(t *testing.T) {
	log := logger.New()

	collections := []config.CollectionConfig{
		{SourceCollection: "users", TargetCollection: "users"},
		{SourceCollection: "orders", TargetCollection: "orders"},
	}

	testCases := []struct {
		name             string
		mode             string
		statusMap        map[string]bool
		expectAllEnabled bool
		expectErr        bool
		errSubstring     string
	}{
		{
			name:             "RequiredMode_AllEnabled_Passes",
			mode:             "required",
			statusMap:        map[string]bool{"users": true, "orders": true},
			expectAllEnabled: true,
			expectErr:        false,
		},
		{
			name:             "RequiredMode_MissingCollection_FailsFast",
			mode:             "required",
			statusMap:        map[string]bool{"users": true, "orders": false},
			expectAllEnabled: false,
			expectErr:        true,
			errSubstring:     "pre/post-images are NOT enabled on: [orders]",
		},
		{
			name:             "WhenAvailableMode_Missing_WarnsAndReturnsAllEnabledFalse",
			mode:             "whenAvailable",
			statusMap:        map[string]bool{"users": true, "orders": false},
			expectAllEnabled: false,
			expectErr:        false,
		},
		{
			name:             "WhenAvailableMode_AllEnabled_ReturnsAllEnabledTrue",
			mode:             "whenAvailable",
			statusMap:        map[string]bool{"users": true, "orders": true},
			expectAllEnabled: true,
			expectErr:        false,
		},
		{
			name:             "UpdateLookupMode_Missing_ReturnsAllEnabledFalse",
			mode:             "updateLookup",
			statusMap:        map[string]bool{"users": false, "orders": false},
			expectAllEnabled: false,
			expectErr:        false,
		},
		{
			name:             "UpdateLookupMode_AllEnabled_ReturnsAllEnabledTrue",
			mode:             "updateLookup",
			statusMap:        map[string]bool{"users": true, "orders": true},
			expectAllEnabled: true,
			expectErr:        false,
		},
		{
			name:             "DefaultMode_EmptyString_DefaultsToUpdateLookup",
			mode:             "",
			statusMap:        map[string]bool{"users": false, "orders": false},
			expectAllEnabled: false,
			expectErr:        false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mode := strings.ToLower(tc.mode)
			if mode == "" {
				mode = "updatelookup"
			}
			allEnabled, err := validatePrePostImageRequirements("testdb", collections, tc.statusMap, mode, log)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error, but got nil")
				}
				if !strings.Contains(err.Error(), tc.errSubstring) {
					t.Errorf("expected error containing %q, got %q", tc.errSubstring, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
			}
			if allEnabled != tc.expectAllEnabled {
				t.Errorf("expected allEnabled=%v, got %v", tc.expectAllEnabled, allEnabled)
			}
		})
	}
}

func TestParsePostImageExpiration(t *testing.T) {
	testCases := []struct {
		name        string
		res         bson.M
		expectSec   float64
		expectIsSet bool
	}{
		{
			name: "StandardClusterParametersPrimitiveA",
			res: bson.M{
				"clusterParameters": primitive.A{
					bson.M{
						"_id": "changeStreamOptions",
						"preAndPostImages": bson.M{
							"expireAfterSeconds": int32(1800),
						},
					},
				},
				"ok": 1,
			},
			expectSec:   1800,
			expectIsSet: true,
		},
		{
			name: "StandardClusterParametersSliceInterface",
			res: bson.M{
				"clusterParameters": []interface{}{
					bson.M{
						"_id": "changeStreamOptions",
						"preAndPostImages": bson.M{
							"expireAfterSeconds": float64(3600),
						},
					},
				},
				"ok": 1,
			},
			expectSec:   3600,
			expectIsSet: true,
		},
		{
			name: "FallbackDirectChangeStreamOptionsMap",
			res: bson.M{
				"changeStreamOptions": bson.M{
					"preAndPostImages": bson.M{
						"expireAfterSeconds": int64(900),
					},
				},
				"ok": 1,
			},
			expectSec:   900,
			expectIsSet: true,
		},
		{
			name: "ExpireAfterSecondsOff",
			res: bson.M{
				"clusterParameters": primitive.A{
					bson.M{
						"_id": "changeStreamOptions",
						"preAndPostImages": bson.M{
							"expireAfterSeconds": "off",
						},
					},
				},
				"ok": 1,
			},
			expectSec:   0,
			expectIsSet: false,
		},
		{
			name: "NoPreAndPostImages",
			res: bson.M{
				"clusterParameters": primitive.A{
					bson.M{
						"_id": "otherParameter",
					},
				},
				"ok": 1,
			},
			expectSec:   0,
			expectIsSet: false,
		},
		{
			name:        "EmptyResponse",
			res:         bson.M{},
			expectSec:   0,
			expectIsSet: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sec, isSet := parsePostImageExpiration(tc.res)
			if isSet != tc.expectIsSet {
				t.Fatalf("expected isSet=%v, got %v", tc.expectIsSet, isSet)
			}
			if sec != tc.expectSec {
				t.Fatalf("expected sec=%v, got %v", tc.expectSec, sec)
			}
		})
	}
}
