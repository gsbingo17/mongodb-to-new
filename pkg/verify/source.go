package verify

import (
	"context"
	"sort"
	"time"

	mgobson "github.com/globalsign/mgo/bson"
	"github.com/gsbingo17/mongodb-migration/pkg/db"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
)

// srcReader abstracts the verification's read of the source database so it works
// against both a modern-driver source (MongoDB ≥ 3.6) and a legacy mgo source
// (MongoDB 3.0–3.4, which the modern Go driver refuses — wire version < 6).
// Documents are always surfaced as mongo-driver bson.M so hashing and the
// migrator's transform see the same types the target went through.
type srcReader interface {
	listCollections(ctx context.Context) ([]string, error)
	count(ctx context.Context, coll string) (int64, error)
	// streamDocs invokes fn for every document in the collection. Returning an
	// error from fn aborts the stream and is propagated.
	streamDocs(ctx context.Context, coll string, fn func(bson.M) error) error
	close(ctx context.Context)
}

// newSrcReader picks the driver by replication method: "oplog-legacy" sources
// are pre-3.6 and must use mgo.
func newSrcReader(connectionString, database, replicationMethod string, log *logger.Logger) (srcReader, error) {
	if replicationMethod == "oplog-legacy" {
		m, err := db.NewMongoDBLegacy(connectionString, database)
		if err != nil {
			return nil, err
		}
		return &legacySrc{m: m}, nil
	}
	m, err := db.NewMongoDB(connectionString, database, 0, 8, 30*time.Second, nil, log)
	if err != nil {
		return nil, err
	}
	return &modernSrc{m: m}, nil
}

/* ---------- modern (mongo-driver) source ---------- */

type modernSrc struct{ m *db.MongoDB }

func (s *modernSrc) close(ctx context.Context) { s.m.Close(ctx) }

func (s *modernSrc) listCollections(ctx context.Context) ([]string, error) {
	names, err := s.m.ListCollections(ctx)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func (s *modernSrc) count(ctx context.Context, coll string) (int64, error) {
	return s.m.GetCollection(coll).CountDocuments(ctx, bson.D{})
}

func (s *modernSrc) streamDocs(ctx context.Context, coll string, fn func(bson.M) error) error {
	cursor, err := s.m.GetCollection(coll).Find(ctx, bson.D{})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return err
		}
		if err := fn(doc); err != nil {
			return err
		}
	}
	return cursor.Err()
}

/* ---------- legacy (mgo) source ---------- */

type legacySrc struct{ m *db.MongoDBLegacy }

func (s *legacySrc) close(ctx context.Context) { s.m.Close() }

func (s *legacySrc) listCollections(ctx context.Context) ([]string, error) {
	names, err := s.m.ListCollections()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func (s *legacySrc) count(ctx context.Context, coll string) (int64, error) {
	n, err := s.m.GetCollection(coll).Count()
	return int64(n), err
}

// streamDocs iterates the whole collection, round-tripping each mgo document
// through standard BSON so nested subdocuments and types arrive as mongo-driver
// values (matching what the migrator wrote to the target).
func (s *legacySrc) streamDocs(ctx context.Context, coll string, fn func(bson.M) error) error {
	iter := s.m.GetCollection(coll).Find(nil).Iter()
	var raw mgobson.M
	for iter.Next(&raw) {
		data, err := mgobson.Marshal(raw)
		if err != nil {
			raw = mgobson.M{}
			continue
		}
		var doc bson.M
		if err := bson.Unmarshal(data, &doc); err != nil {
			raw = mgobson.M{}
			continue
		}
		if err := fn(doc); err != nil {
			iter.Close()
			return err
		}
		raw = mgobson.M{}
	}
	return iter.Close()
}
