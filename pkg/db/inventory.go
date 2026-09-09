package db

import (
	"context"
	"sort"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
)

// inventorySystemDBs are databases never offered for migration by the console.
var inventorySystemDBs = map[string]bool{"admin": true, "local": true, "config": true}

// ListInventory returns the user databases and their collections for a source
// server, choosing the driver by capability: the modern official driver for
// MongoDB ≥ 3.6, and the legacy mgo driver for older servers (3.0.x) that the
// modern driver cannot speak to. This lets the web console present a
// pick-and-click list of collections even against the oldest supported source.
func ListInventory(connectionString string, modern bool) (map[string][]string, error) {
	if modern {
		return listInventoryModern(connectionString)
	}
	return listInventoryLegacy(connectionString)
}

func listInventoryModern(connectionString string) (map[string][]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Read-only inventory listing: a tiny pool, no pool monitor, short idle.
	m, err := NewMongoDB(connectionString, "admin", 0, 4, 30*time.Second, nil, logger.New())
	if err != nil {
		return nil, err
	}
	defer m.Close(ctx)

	client := m.GetClient()
	dbNames, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}

	inv := make(map[string][]string)
	for _, dbn := range dbNames {
		if inventorySystemDBs[dbn] {
			continue
		}
		colls, err := client.Database(dbn).ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return nil, err
		}
		inv[dbn] = filterAndSort(colls)
	}
	return inv, nil
}

func listInventoryLegacy(connectionString string) (map[string][]string, error) {
	m, err := NewMongoDBLegacy(connectionString, "admin")
	if err != nil {
		return nil, err
	}
	defer m.Close()

	session := m.GetSession()
	dbNames, err := session.DatabaseNames()
	if err != nil {
		return nil, err
	}

	inv := make(map[string][]string)
	for _, dbn := range dbNames {
		if inventorySystemDBs[dbn] {
			continue
		}
		colls, err := session.DB(dbn).CollectionNames()
		if err != nil {
			return nil, err
		}
		inv[dbn] = filterAndSort(colls)
	}
	return inv, nil
}

// filterAndSort drops the legacy system.indexes/system.* catalog collections
// and returns the remainder sorted for stable UI rendering.
func filterAndSort(colls []string) []string {
	out := make([]string, 0, len(colls))
	for _, c := range colls {
		if len(c) >= 7 && c[:7] == "system." {
			continue
		}
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}
