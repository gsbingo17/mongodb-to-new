package migration

import (
	"github.com/gsbingo17/mongodb-migration/pkg/db"
	"github.com/gsbingo17/mongodb-migration/pkg/util"
)

// ReplicationDecision is the outcome of auto-selecting a replication method
// from a detected source server.
type ReplicationDecision struct {
	Method  string // "changestream" or "oplog-legacy"
	Warning string // non-empty when there is a caveat the operator should see
}

// ResolveReplicationMethod maps a detected source server to the best live
// replication method:
//
//   - MongoDB 3.6+        -> "changestream" (change streams; simplest, resumable)
//   - MongoDB < 3.6       -> "oplog-legacy" (mgo driver; the modern driver and
//     change streams are unavailable on these servers)
//
// The plain "oplog" (GTM) method is never auto-selected — change streams are
// preferred wherever they are available, and "oplog" remains a manual override.
//
// Both change streams and oplog tailing require a replica set. If the source is
// a standalone, the returned Warning explains that only full migration works.
func ResolveReplicationMethod(info *db.SourceServerInfo) ReplicationDecision {
	d := ReplicationDecision{}

	if util.VersionAtLeast(info.VersionArray, 3, 6) {
		d.Method = "changestream"
	} else {
		d.Method = "oplog-legacy"
	}

	if !info.IsReplicaSet {
		d.Warning = "source is not a replica set; live replication (change streams / oplog tailing) requires a replica set. Only full migration (-mode=migrate) is supported for a standalone source."
	}
	return d
}
