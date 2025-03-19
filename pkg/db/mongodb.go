package db

import (
	"context"
	"fmt"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB represents a MongoDB connection
type MongoDB struct {
	client   *mongo.Client
	database *mongo.Database
	log      *logger.Logger
}

// NewMongoDB creates a new MongoDB connection
func NewMongoDB(connectionString, databaseName string, log *logger.Logger) (*MongoDB, error) {
	// Set client options
	clientOptions := options.Client().
		ApplyURI(connectionString).
		SetMaxPoolSize(30).
		SetMinPoolSize(10).
		SetConnectTimeout(30 * time.Second).
		SetSocketTimeout(45 * time.Second)

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping the database to verify connection
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	// Get database
	database := client.Database(databaseName)

	return &MongoDB{
		client:   client,
		database: database,
		log:      log,
	}, nil
}

// Close closes the MongoDB connection
func (m *MongoDB) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

// GetCollection returns a MongoDB collection
func (m *MongoDB) GetCollection(collectionName string) *mongo.Collection {
	return m.database.Collection(collectionName)
}

// ListCollections returns a list of all collection names in the database
func (m *MongoDB) ListCollections(ctx context.Context) ([]string, error) {
	collections, err := m.database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}
	return collections, nil
}

// GetDatabaseName returns the database name
func (m *MongoDB) GetDatabaseName() string {
	return m.database.Name()
}

// GetClient returns the MongoDB client
func (m *MongoDB) GetClient() *mongo.Client {
	return m.client
}

// CreateChangeStream creates a change stream for a collection
func (m *MongoDB) CreateChangeStream(ctx context.Context, collectionName string, resumeToken interface{}) (*mongo.ChangeStream, error) {
	collection := m.GetCollection(collectionName)

	// Set pipeline for full document lookup on updates
	pipeline := mongo.Pipeline{}

	// Set options
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if resumeToken != nil {
		opts.SetResumeAfter(resumeToken)
	}

	// Create change stream
	changeStream, err := collection.Watch(ctx, pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create change stream for collection %s: %w", collectionName, err)
	}

	return changeStream, nil
}

// CreateClientLevelChangeStream creates a change stream at the client level
// This watches for changes across all collections in all databases
func (m *MongoDB) CreateClientLevelChangeStream(ctx context.Context, resumeToken interface{}, batchSize int) (*mongo.ChangeStream, error) {
	// Set pipeline for full document lookup on updates
	pipeline := mongo.Pipeline{}

	// Set options
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if resumeToken != nil {
		opts.SetResumeAfter(resumeToken)
	}

	// Set batch size if provided
	if batchSize > 0 {
		opts.SetBatchSize(int32(batchSize))
		m.log.Infof("Setting change stream batch size to %d", batchSize)
	}

	// Create client-level change stream
	changeStream, err := m.client.Watch(ctx, pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create client-level change stream: %w", err)
	}

	m.log.Info("Created client-level change stream watching all databases and collections")
	return changeStream, nil
}
