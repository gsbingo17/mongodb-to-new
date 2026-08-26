package db

import (
	"context"
	"fmt"
	"time"

	"github.com/globalsign/mgo"
	mgobson "github.com/globalsign/mgo/bson"
	"github.com/gsbingo17/mongodb-migration/pkg/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// SourceServerInfo describes a detected source MongoDB server.
type SourceServerInfo struct {
	Version      string // raw version string, e.g. "3.2.22"
	VersionArray []int  // [major, minor, patch]
	IsReplicaSet bool   // true if the server reports a replica set name
	SetName      string // replica set name, if any
	ModernDriver bool   // true if reachable by the modern official driver
}

// DetectSourceServer connects to the source MongoDB and reports its version and
// topology. It tries the modern official driver first; if that fails (typical
// for very old servers the modern driver refuses to talk to), it falls back to
// the legacy mgo driver. Callers use the result to auto-select a replication
// method.
func DetectSourceServer(connectionString, database string) (*SourceServerInfo, error) {
	info, modernErr := detectModern(connectionString)
	if modernErr == nil {
		return info, nil
	}
	info, legacyErr := detectLegacy(connectionString)
	if legacyErr == nil {
		return info, nil
	}
	return nil, fmt.Errorf("failed to detect source server (modern driver: %v; legacy mgo driver: %v)", modernErr, legacyErr)
}

// detectModern uses the official go driver to read buildInfo and topology.
func detectModern(uri string) (*SourceServerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(context.Background())

	admin := client.Database("admin")

	var buildInfo bson.M
	if err := admin.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&buildInfo); err != nil {
		return nil, err
	}
	version, _ := buildInfo["version"].(string)
	verArr, err := util.ParseServerVersion(version)
	if err != nil {
		return nil, err
	}

	info := &SourceServerInfo{
		Version:      version,
		VersionArray: verArr,
		ModernDriver: true,
	}

	// "hello" (4.4+) with fallback to "isMaster" for older servers.
	var hello bson.M
	if err := admin.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		_ = admin.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}}).Decode(&hello)
	}
	if setName, ok := hello["setName"].(string); ok && setName != "" {
		info.IsReplicaSet = true
		info.SetName = setName
	}
	return info, nil
}

// detectLegacy uses the mgo driver (which supports very old servers) to read
// build info and topology.
func detectLegacy(uri string) (*SourceServerInfo, error) {
	dialInfo, err := mgo.ParseURL(uri)
	if err != nil {
		return nil, err
	}
	dialInfo.Timeout = 10 * time.Second

	session, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	bi, err := session.BuildInfo()
	if err != nil {
		return nil, err
	}
	verArr := bi.VersionArray
	if len(verArr) == 0 {
		verArr, err = util.ParseServerVersion(bi.Version)
		if err != nil {
			return nil, err
		}
	}

	info := &SourceServerInfo{
		Version:      bi.Version,
		VersionArray: verArr,
		ModernDriver: false,
	}

	var isMaster mgobson.M
	if err := session.Run("isMaster", &isMaster); err == nil {
		if setName, ok := isMaster["setName"].(string); ok && setName != "" {
			info.IsReplicaSet = true
			info.SetName = setName
		}
	}
	return info, nil
}
