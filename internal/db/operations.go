// internal/db/operations.go
package db

import (
	"context"
	"fmt"
	"runtime"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/sfi2k7/mc/internal/storage"
	"github.com/sfi2k7/mc/internal/utils"
)

// ExportCollection exports documents from a collection to a file
func ExportCollection(
	ctx context.Context,
	client *mongo.Client,
	database, collection, queryStr string,
	batchSize int,
	writer *storage.FileWriter,
	progress *utils.ProgressBar,
) (int64, error) {
	// Parse query
	var filter bson.M
	if err := bson.UnmarshalExtJSON([]byte(queryStr), true, &filter); err != nil {
		return 0, fmt.Errorf("invalid query: %w", err)
	}

	coll := client.Database(database).Collection(collection)

	// Get total count for progress bar
	count, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("failed to count documents: %w", err)
	}
	progress.SetTotal(count)

	// Find documents
	findOptions := options.Find().SetBatchSize(int32(batchSize))
	cursor, err := coll.Find(ctx, filter, findOptions)
	if err != nil {
		return 0, fmt.Errorf("failed to execute find: %w", err)
	}
	defer cursor.Close(ctx)

	var totalExported int64 = 0
	batch := make([]bson.D, 0, batchSize)

	// Process batches
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return totalExported, fmt.Errorf("failed to decode document: %w", err)
		}

		batch = append(batch, doc)

		if len(batch) >= batchSize {
			if err := processBatch(batch, writer, progress); err != nil {
				return totalExported, err
			}
			totalExported += int64(len(batch))
			batch = make([]bson.D, 0, batchSize)

			// Hint garbage collector after processing large batch
			runtime.GC()
		}
	}

	// Process remaining documents
	if len(batch) > 0 {
		if err := processBatch(batch, writer, progress); err != nil {
			return totalExported, err
		}
		totalExported += int64(len(batch))
	}

	if err := cursor.Err(); err != nil {
		return totalExported, fmt.Errorf("cursor error: %w", err)
	}

	return totalExported, nil
}

// processBatch processes a batch of documents for export
func processBatch(batch []bson.D, writer *storage.FileWriter, progress *utils.ProgressBar) error {
	if err := writer.WriteBatch(batch); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}
	progress.Add(int64(len(batch)))
	return nil
}

// ImportCollection imports documents from a file to a collection
func ImportCollection(
	ctx context.Context,
	client *mongo.Client,
	database, collection string,
	batchSize int,
	reader *storage.FileReader,
	progress *utils.ProgressBar,
) (int64, error) {
	coll := client.Database(database).Collection(collection)

	var totalImported int64 = 0

	for {
		// Read a batch of documents
		batch, err := reader.ReadBatch(batchSize)
		if err != nil {
			return totalImported, fmt.Errorf("failed to read batch: %w", err)
		}

		// Stop when no more documents
		if len(batch) == 0 {
			break
		}

		// Convert to interface slice for MongoDB
		docs := make([]interface{}, len(batch))
		for i, doc := range batch {
			docs[i] = doc
		}

		// Insert documents
		_, err = coll.InsertMany(ctx, docs)
		if err != nil {
			return totalImported, fmt.Errorf("failed to insert batch: %w", err)
		}

		totalImported += int64(len(batch))
		progress.Add(int64(len(batch)))

		// Memory optimization
		batch = nil
		docs = nil
		runtime.GC()
	}

	return totalImported, nil
}
