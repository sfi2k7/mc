// cmd/uncompress.go
package cmd

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/sfi2k7/mc/internal/utils"
	"github.com/spf13/cobra"
)

func newUncompressCmd() *cobra.Command {
	var outputFile string

	uncompressCmd := &cobra.Command{
		Use:     "uncompress [flags] INPUT_FILE",
		Aliases: []string{"extract", "decompress"},
		Short:   "Uncompress a compressed MCBF file",
		Long:    `Uncompress a gzip compressed MCBF file.`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			inputFile := args[0]
			if outputFile == "" {
				// Auto-remove .gz extension if present
				if strings.HasSuffix(inputFile, ".gz") {
					outputFile = strings.TrimSuffix(inputFile, ".gz")
				} else {
					outputFile = inputFile + ".uncompressed"
				}
			}
			return runUncompress(inputFile, outputFile)
		},
	}

	uncompressCmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output file path")

	return uncompressCmd
}

func runUncompress(inputFile, outputFile string) error {
	// Check if input file exists
	inputStat, err := os.Stat(inputFile)
	if err != nil {
		return fmt.Errorf("failed to access input file: %w", err)
	}

	// Check if output file already exists
	if _, err := os.Stat(outputFile); err == nil {
		return fmt.Errorf("output file already exists: %s", outputFile)
	}

	// Create output directory if it doesn't exist
	outputDir := filepath.Dir(outputFile)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Open input file
	input, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer input.Close()

	// Create gzip reader
	gzipReader, err := gzip.NewReader(input)
	if err != nil {
		if strings.Contains(err.Error(), "not in gzip format") {
			return fmt.Errorf("input file is not in gzip format: %s", inputFile)
		}
		return fmt.Errorf("failed to read compressed file: %w", err)
	}
	defer gzipReader.Close()

	// Create output file
	output, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer output.Close()

	// Set up progress bar
	progress := utils.NewProgressBar("Uncompressing")
	// We don't know the final size in advance, so we'll update as we go

	// Create a buffer for reading
	buffer := make([]byte, 4*1024*1024) // 4MB buffer

	// Copy data with progress reporting
	var totalBytes int64
	for {
		n, err := gzipReader.Read(buffer)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read compressed data: %w", err)
		}

		if n == 0 {
			break
		}

		if _, err := output.Write(buffer[:n]); err != nil {
			return fmt.Errorf("failed to write uncompressed data: %w", err)
		}

		totalBytes += int64(n)
		progress.SetCurrent(totalBytes)
	}

	// Get output file stats
	outputStat, err := os.Stat(outputFile)
	if err != nil {
		return fmt.Errorf("failed to get output file info: %w", err)
	}

	// Calculate expansion ratio
	ratio := float64(outputStat.Size()) / float64(inputStat.Size())

	logger.Info("Uncompression completed",
		"input", inputFile,
		"output", outputFile,
		"input_size", utils.FormatByteSize(inputStat.Size()),
		"output_size", utils.FormatByteSize(outputStat.Size()),
		"expansion_ratio", fmt.Sprintf("%.2fx", ratio))

	return nil
}
