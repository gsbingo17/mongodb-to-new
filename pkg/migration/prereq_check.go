package migration

import (
	"context"
	"fmt"
	"strings"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"github.com/gsbingo17/mongodb-migration/pkg/db"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CheckSourceChangeStreamPrerequisites inspects the source cluster and collections before
// starting data migration or CDC.
func CheckSourceChangeStreamPrerequisites(
	ctx context.Context,
	sourceDB *db.MongoDB,
	dbName string,
	collections []config.CollectionConfig,
	fullDocumentMode string,
	log *logger.Logger,
) error {
	mode := strings.ToLower(fullDocumentMode)
	if mode == "" {
		mode = "updatelookup"
	}

	// 1. Report oplog retention window for all modes
	reportOplogRetention(ctx, sourceDB, log)

	// 2. Inspect collection options for pre/post-images
	statusMap, err := getCollectionPrePostImageStatus(ctx, sourceDB, dbName)
	if err != nil {
		log.Debugf("[%s] Could not inspect collection options for pre/post-images: %v", dbName, err)
		return nil
	}

	allEnabled, err := validatePrePostImageRequirements(dbName, collections, statusMap, mode, log)
	if err != nil {
		return err
	}

	// 3. Report post-image expiration only when using post-image mode and all collections have it enabled
	if (mode == "required" || mode == "whenavailable") && allEnabled {
		reportPostImageExpiration(ctx, sourceDB, log)
	}

	return nil
}

// validatePrePostImageRequirements evaluates pre/post-image status for the configured mode.
func validatePrePostImageRequirements(
	dbName string,
	collections []config.CollectionConfig,
	statusMap map[string]bool,
	mode string,
	log *logger.Logger,
) (allEnabled bool, err error) {
	var missing, enabled []string
	for _, coll := range collections {
		if statusMap[coll.SourceCollection] {
			enabled = append(enabled, coll.SourceCollection)
		} else {
			missing = append(missing, coll.SourceCollection)
		}
	}

	switch mode {
	case "updatelookup", "default":
		if len(missing) == 0 && len(enabled) > 0 {
			log.Infof("[%s] Tip: Pre/post-images are enabled on all collections. You can use '-full-document-mode=required' for better CDC throughput.", dbName)
		} else if len(missing) > 0 {
			log.Infof("[%s] Tip: If you enable changeStreamPreAndPostImages on your source collections and specify '-full-document-mode=required', you will be able to reach better CDC replication throughput without point-lookup queries.", dbName)
		}

	case "required":
		if len(missing) > 0 {
			return false, fmt.Errorf("fullDocumentMode is 'required', but pre/post-images are NOT enabled on: %v. Enable with: db.runCommand({collMod: \"<coll>\", changeStreamPreAndPostImages: {enabled: true}})", missing)
		}
		log.Infof("[%s] Verified pre/post-images enabled on all %d collections", dbName, len(enabled))

	case "whenavailable":
		if len(missing) > 0 {
			log.Warnf("[%s] fullDocumentMode is 'whenAvailable', but pre/post-images not enabled on: %v (updates will have null fullDocument)", dbName, missing)
		}
	}

	return len(missing) == 0 && len(enabled) > 0, nil
}

// getCollectionPrePostImageStatus queries collection options for the given database
func getCollectionPrePostImageStatus(ctx context.Context, sourceDB *db.MongoDB, dbName string) (map[string]bool, error) {
	cursor, err := sourceDB.GetClient().Database(dbName).ListCollections(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	status := make(map[string]bool)
	for cursor.Next(ctx) {
		var doc struct {
			Name    string `bson:"name"`
			Options struct {
				ChangeStreamPreAndPostImages struct {
					Enabled bool `bson:"enabled"`
				} `bson:"changeStreamPreAndPostImages"`
			} `bson:"options"`
		}
		if err := cursor.Decode(&doc); err == nil {
			status[doc.Name] = doc.Options.ChangeStreamPreAndPostImages.Enabled
		}
	}
	return status, cursor.Err()
}

// reportPostImageExpiration checks cluster-level expireAfterSeconds for pre/post-images
func reportPostImageExpiration(ctx context.Context, sourceDB *db.MongoDB, log *logger.Logger) {
	var res bson.M
	if err := sourceDB.GetClient().Database("admin").RunCommand(ctx, bson.D{{Key: "getClusterParameter", Value: "changeStreamOptions"}}).Decode(&res); err != nil {
		log.Debugf("Could not query changeStreamOptions: %v", err)
		return
	}
	opts, _ := res["changeStreamOptions"].(bson.M)
	prePost, _ := opts["preAndPostImages"].(bson.M)
	val := prePost["expireAfterSeconds"]
	if val == nil || val == "off" {
		log.Info("Post-image retention (expireAfterSeconds): not set (retained until oplog roll-off)")
		return
	}

	var sec float64
	switch v := val.(type) {
	case int32:
		sec = float64(v)
	case int64:
		sec = float64(v)
	case float64:
		sec = v
	}
	if sec > 0 {
		log.Infof("Post-image retention (expireAfterSeconds): %.0fs (~%.1fm)", sec, sec/60.0)
	}
}

// reportOplogRetention queries local.oplog.rs to determine the available oplog time window
func reportOplogRetention(ctx context.Context, sourceDB *db.MongoDB, log *logger.Logger) {
	coll := sourceDB.GetClient().Database("local").Collection("oplog.rs")
	var first, last struct {
		Ts primitive.Timestamp `bson:"ts"`
	}
	if err := coll.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: 1}})).Decode(&first); err != nil {
		log.Debugf("Could not query earliest oplog entry: %v", err)
		return
	}
	if err := coll.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}})).Decode(&last); err != nil {
		log.Debugf("Could not query latest oplog entry: %v", err)
		return
	}
	if last.Ts.T > first.Ts.T {
		sec := last.Ts.T - first.Ts.T
		log.Infof("Source oplog retention window: ~%dh %dm (%ds)", sec/3600, (sec%3600)/60, sec)
	}
}
