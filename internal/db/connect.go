// internal/db/connect.go
package db

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Connect establishes a connection to MongoDB
func Connect(ctx context.Context, uri, host string, port int) (*mongo.Client, error) {
	var clientOptions *options.ClientOptions

	if uri != "" {
		clientOptions = options.Client().ApplyURI(uri)
	} else {
		mongoURI := fmt.Sprintf("mongodb://%s:%d", host, port)
		clientOptions = options.Client().ApplyURI(mongoURI)
	}

	// Set some reasonable defaults
	clientOptions.SetMaxPoolSize(10)
	clientOptions.SetMinPoolSize(1)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	// Ping the server to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	return client, nil
}

// DropCollection drops a collection if it exists
func DropCollection(ctx context.Context, client *mongo.Client, database, collection string) error {
	return client.Database(database).Collection(collection).Drop(ctx)
}
