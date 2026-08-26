package assess

import (
	"context"
	"fmt"
	"sort"
	"time"

	mgobson "github.com/globalsign/mgo/bson"
	"github.com/gsbingo17/mongodb-migration/pkg/db"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// sourceReader abstracts reading a source database for assessment so the same
// document-level rules run against both a modern-driver source (MongoDB ≥ 3.6)
// and a legacy mgo source (MongoDB 3.0–3.4, which the modern Go driver refuses
// to talk to — it rejects wire version < 6). Sampled documents are always
// returned as mongo-driver bson.M so the shared rule walker sees uniform types.
type sourceReader interface {
	// listCollections returns the user collections in the database.
	listCollections(ctx context.Context) ([]string, error)
	// indexCount returns the number of indexes on a collection.
	indexCount(ctx context.Context, coll string) (int, error)
	// countDocs returns the true document count of a collection.
	countDocs(ctx context.Context, coll string) (int64, error)
	// sample returns up to want documents (all of them when the collection is at
	// or below want), normalized to mongo-driver bson.M.
	sample(ctx context.Context, coll string, total, want int64) ([]bson.M, error)
	close(ctx context.Context)
}

// newSourceReader picks the driver based on the source's replication method:
// "oplog-legacy" sources are pre-3.6 and must use the mgo driver.
func newSourceReader(connectionString, database, replicationMethod string, log *logger.Logger) (sourceReader, error) {
	if replicationMethod == "oplog-legacy" {
		return newLegacyReader(connectionString, database)
	}
	return newModernReader(connectionString, database, log)
}

/* ---------- modern (mongo-driver) reader ---------- */

type modernReader struct{ m *db.MongoDB }

func newModernReader(connectionString, database string, log *logger.Logger) (sourceReader, error) {
	m, err := db.NewMongoDB(connectionString, database, 0, 4, 30*time.Second, nil, log)
	if err != nil {
		return nil, err
	}
	return &modernReader{m: m}, nil
}

func (r *modernReader) close(ctx context.Context) { r.m.Close(ctx) }

func (r *modernReader) listCollections(ctx context.Context) ([]string, error) {
	names, err := r.m.GetClient().Database(r.m.GetDatabaseName()).ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}
	sort.Strings(names)
	return names, nil
}

func (r *modernReader) indexCount(ctx context.Context, coll string) (int, error) {
	idxs, err := r.m.ListIndexes(ctx, coll)
	if err != nil {
		return 0, err
	}
	return len(idxs), nil
}

func (r *modernReader) countDocs(ctx context.Context, coll string) (int64, error) {
	return r.m.GetCollection(coll).CountDocuments(ctx, bson.D{})
}

func (r *modernReader) sample(ctx context.Context, coll string, total, want int64) ([]bson.M, error) {
	c := r.m.GetCollection(coll)
	cctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	var cursor interface {
		Next(context.Context) bool
		Decode(interface{}) error
		Close(context.Context) error
	}
	if total > want {
		cur, err := c.Aggregate(cctx, bson.A{bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: want}}}}})
		if err != nil {
			return nil, err
		}
		cursor = cur
	} else {
		cur, err := c.Find(cctx, bson.D{}, options.Find())
		if err != nil {
			return nil, err
		}
		cursor = cur
	}
	defer cursor.Close(cctx)

	var docs []bson.M
	for cursor.Next(cctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

/* ---------- legacy (mgo) reader ---------- */

type legacyReader struct{ m *db.MongoDBLegacy }

func newLegacyReader(connectionString, database string) (sourceReader, error) {
	m, err := db.NewMongoDBLegacy(connectionString, database)
	if err != nil {
		return nil, err
	}
	return &legacyReader{m: m}, nil
}

func (r *legacyReader) close(ctx context.Context) { r.m.Close() }

func (r *legacyReader) listCollections(ctx context.Context) ([]string, error) {
	names, err := r.m.ListCollections()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func (r *legacyReader) indexCount(ctx context.Context, coll string) (int, error) {
	idxs, err := r.m.ListIndexes(coll)
	if err != nil {
		return 0, err
	}
	return len(idxs), nil
}

func (r *legacyReader) countDocs(ctx context.Context, coll string) (int64, error) {
	n, err := r.m.GetCollection(coll).Count()
	return int64(n), err
}

// sample returns up to want documents. MongoDB 3.0/3.4 predates the $sample
// aggregation stage, and large-offset skip is O(skip) on MMAPv1, so neither
// random sampling nor random access is viable. For collections larger than want
// we therefore stream the whole collection and keep every step-th document —
// systematic sampling that spans the entire collection (front to back), unlike
// the old "first N docs" which never inspected the tail where sparse issues
// often hide. Each kept mgo document is round-tripped through standard BSON so
// nested subdocuments and types arrive as mongo-driver values the rule walker
// understands.
func (r *legacyReader) sample(ctx context.Context, coll string, total, want int64) ([]bson.M, error) {
	if want < 1 {
		want = 1
	}
	if total <= want {
		return r.scanLimited(coll, int(total))
	}

	step := total / want
	if step < 1 {
		step = 1
	}
	iter := r.m.GetCollection(coll).Find(nil).Iter()

	var docs []bson.M
	var raw mgobson.M
	var i int64
	for iter.Next(&raw) {
		if i%step == 0 {
			if data, err := mgobson.Marshal(raw); err == nil {
				var doc bson.M
				if bson.Unmarshal(data, &doc) == nil {
					docs = append(docs, doc)
				}
			}
			if int64(len(docs)) >= want {
				break
			}
		}
		i++
		raw = mgobson.M{}
	}
	iter.Close()
	return docs, nil
}

// scanLimited reads up to limit documents in natural order, used when a
// collection is small enough to inspect in full (limit <= 0 means all).
func (r *legacyReader) scanLimited(coll string, limit int) ([]bson.M, error) {
	q := r.m.GetCollection(coll).Find(nil)
	if limit > 0 {
		q = q.Limit(limit)
	}
	iter := q.Iter()

	var docs []bson.M
	var raw mgobson.M
	for iter.Next(&raw) {
		if data, err := mgobson.Marshal(raw); err == nil {
			var doc bson.M
			if bson.Unmarshal(data, &doc) == nil {
				docs = append(docs, doc)
			}
		}
		raw = mgobson.M{}
	}
	return docs, iter.Close()
}
