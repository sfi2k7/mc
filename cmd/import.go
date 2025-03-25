// cmd/import.go
package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sfi2k7/mc/internal/db"
	"github.com/sfi2k7/mc/internal/storage"
	"github.com/sfi2k7/mc/internal/utils"
	"github.com/spf13/cobra"
)

func newImportCmd() *cobra.Command {
	var (
		database   string
		collection string
		drop       bool
	)

	importCmd := &cobra.Command{
		Use:   "import -d DATABASE -c COLLECTION [flags] INPUT_FILE",
		Short: "Import a MongoDB collection from a file",
		Long:  `Import a MongoDB collection from a compressed BSON file.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			inputFile := args[0]
			return runImport(database, collection, drop, inputFile)
		},
	}

	importCmd.Flags().StringVarP(&database, "database", "d", "", "MongoDB database name")
	importCmd.Flags().StringVarP(&collection, "collection", "c", "", "MongoDB collection name")
	importCmd.Flags().BoolVar(&drop, "drop", false, "Drop collection before import if exists")

	importCmd.MarkFlagRequired("database")
	importCmd.MarkFlagRequired("collection")

	return importCmd
}

func runImport(database, collection string, drop bool, inputFile string) error {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Create file reader
	fileReader, err := storage.NewFileReader(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer fileReader.Close()

	// Read header
	metadata, err := fileReader.ReadHeader()
	if err != nil {
		if strings.Contains(err.Error(), "invalid file format") ||
			strings.Contains(err.Error(), "magic number mismatch") {
			return fmt.Errorf("invalid file format: the file may be corrupted or not an MCBZ file")
		}
		if strings.Contains(err.Error(), "unsupported file version") {
			return fmt.Errorf("unsupported file version: this file was created with a newer version of mc")
		}
		return fmt.Errorf("failed to read header: %w", err)
	}

	logger.Info("Importing collection",
		"source_db", metadata.Database,
		"source_coll", metadata.Collection,
		"target_db", database,
		"target_coll", collection)

	// Connect to MongoDB
	client, err := db.Connect(ctx, uri, host, port)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer client.Disconnect(ctx)

	// Initialize progress bar
	progress := utils.NewProgressBar("Importing")
	progress.SetTotal(metadata.DocumentCount)

	// Drop collection if requested
	if drop {
		if err := db.DropCollection(ctx, client, database, collection); err != nil {
			return fmt.Errorf("failed to drop collection: %w", err)
		}
		logger.Info("Dropped existing collection", "database", database, "collection", collection)
	}

	// Import collection
	importedCount, err := db.ImportCollection(
		ctx,
		client,
		database,
		collection,
		batchSize,
		fileReader,
		progress,
	)
	if err != nil {
		return fmt.Errorf("import failed: %w", err)
	}

	logger.Info("Import completed",
		"docs", importedCount,
		"file", inputFile,
		"database", database,
		"collection", collection)
	return nil
}
