// cmd/inspect.go
package cmd

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/sfi2k7/mc/internal/storage"
	"github.com/sfi2k7/mc/internal/utils"
	"github.com/spf13/cobra"
)

func newInspectCmd() *cobra.Command {
	var validate bool
	inspectCmd := &cobra.Command{
		Use:   "inspect FILE",
		Short: "Display metadata information about an MCBZ file",
		Long:  `Inspect shows internal metadata and file information for an MCBZ file.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath := args[0]
			return runInspect(filePath, validate)
		},
	}

	inspectCmd.Flags().BoolVar(&validate, "validate", false, "Perform additional validation of file contents")

	return inspectCmd
}

// In the runInspect function in cmd/inspect.go:

func runInspect(filePath string, validate bool) error {
	// Get file stat info
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Create file reader
	fileReader, err := storage.NewFileReader(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer fileReader.Close()

	// Read header
	metadata, err := fileReader.ReadHeader()
	if err != nil {
		fmt.Println("=== File Header Error ===")
		fmt.Printf("Error: %s\n", err)
		return nil
	}

	// Format human-readable sizes
	fileSizeHuman := utils.FormatByteSize(fileInfo.Size())
	totalSizeHuman := utils.FormatByteSize(metadata.TotalSize)

	// Format creation times
	fileCreationTime := fileInfo.ModTime().Format(time.RFC1123)
	exportTime := time.Unix(metadata.Timestamp, 0).Format(time.RFC1123)

	// Print file information
	fmt.Println("=== MCBF File Information ===")
	fmt.Println("File path:", filePath)
	fmt.Println("File size:", fileSizeHuman, fmt.Sprintf("(%d bytes)", fileInfo.Size()))
	fmt.Println("File created:", fileCreationTime)
	fmt.Println("")

	// Print internal metadata
	fmt.Println("=== Collection Information ===")
	fmt.Println("Database:", metadata.Database)
	fmt.Println("Collection:", metadata.Collection)
	fmt.Println("Document count:", metadata.DocumentCount)
	fmt.Println("Source:", metadata.Source)
	fmt.Println("Export time:", exportTime)
	fmt.Println("")

	// Print platform information
	fmt.Println("=== Platform Information ===")
	fmt.Println("Source platform:", metadata.Platform)
	fmt.Println("Current platform:", runtime.GOARCH+"-"+runtime.GOOS)
	fmt.Println("Data size:", totalSizeHuman, fmt.Sprintf("(%d bytes)", metadata.TotalSize))

	// Add validation section if requested
	if validate {
		fmt.Println("\n=== Validation Results ===")

		// Validate file integrity by reading first batch
		batchSize := 10
		batch, err := fileReader.ReadBatch(batchSize)
		if err != nil {
			fmt.Println("Status: FAILED")
			fmt.Printf("Error: %s\n", err)
			return nil
		}

		docCount := len(batch)
		fmt.Printf("Status: OK (Read %d sample documents successfully)\n", docCount)

		if docCount > 0 {
			fmt.Println("Sample document keys:")
			// Display up to 5 keys from first document
			doc := batch[0]
			keyCount := 0
			for _, elem := range doc {
				if keyCount < 5 {
					fmt.Printf("  - %s (%T)\n", elem.Key, elem.Value)
					keyCount++
				} else {
					fmt.Printf("  - ... and %d more\n", len(doc)-5)
					break
				}
			}
		}
	}

	return nil
}
