// cmd/export.go
package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/sfi2k7/mc/internal/db"
	"github.com/sfi2k7/mc/internal/storage"
	"github.com/sfi2k7/mc/internal/utils"
	"github.com/spf13/cobra"
)

func newExportCmd() *cobra.Command {
	var (
		database   string
		collection string
		query      string
	)

	exportCmd := &cobra.Command{
		Use:   "export -d DATABASE -c COLLECTION [flags] OUTPUT_FILE",
		Short: "Export a MongoDB collection to a file",
		Long:  `Export a MongoDB collection to a compressed BSON file.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			outputFile := args[0]
			return runExport(database, collection, query, outputFile)
		},
	}

	exportCmd.Flags().StringVarP(&database, "database", "d", "", "MongoDB database name")
	exportCmd.Flags().StringVarP(&collection, "collection", "c", "", "MongoDB collection name")
	exportCmd.Flags().StringVar(&query, "query", "{}", "Query filter in JSON format")

	exportCmd.MarkFlagRequired("database")
	exportCmd.MarkFlagRequired("collection")

	return exportCmd
}

// In cmd/export.go, modify runExport:

func runExport(database, collection, queryStr, outputFile string) error {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Connect to MongoDB
	client, err := db.Connect(ctx, uri, host, port)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer client.Disconnect(ctx)

	// Create file writer
	fileWriter, err := storage.NewFileWriter(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer fileWriter.Close()

	// Prepare metadata
	metadata := storage.Metadata{
		Database:   database,
		Collection: collection,
		Timestamp:  time.Now().Unix(),
		Source:     fmt.Sprintf("%s:%d", host, port),
	}

	// Write header
	if err := fileWriter.WriteHeader(metadata); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Initialize progress bar
	progress := utils.NewProgressBar("Exporting")

	// Export collection
	docCount, err := db.ExportCollection(
		ctx,
		client,
		database,
		collection,
		queryStr,
		batchSize,
		fileWriter,
		progress,
	)
	if err != nil {
		return fmt.Errorf("export failed: %w", err)
	}

	// Update metadata with doc count and finalize
	metadata.DocumentCount = docCount
	if err := fileWriter.WriteFooter(metadata); err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}

	logger.Info("Export completed",
		"docs", docCount,
		"file", outputFile,
		// "size", fileWriter.metadata.TotalSize
	)
	return nil
}
