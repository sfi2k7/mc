// cmd/compress.go
package cmd

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/sfi2k7/mc/internal/utils"
	"github.com/spf13/cobra"
)

func newCompressCmd() *cobra.Command {
	var (
		outputFile string
		level      int
	)

	compressCmd := &cobra.Command{
		Use:   "compress [flags] INPUT_FILE",
		Short: "Compress an MCBF file",
		Long:  `Compress an MCBF file using gzip compression.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			inputFile := args[0]
			if outputFile == "" {
				outputFile = inputFile + ".gz"
			}
			return runCompress(inputFile, outputFile, level)
		},
	}

	compressCmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output file path (default: INPUT_FILE.gz)")
	compressCmd.Flags().IntVarP(&level, "level", "l", gzip.DefaultCompression,
		"Compression level (1-9, where 1 is fastest, 9 is best compression)")

	return compressCmd
}

func runCompress(inputFile, outputFile string, level int) error {
	// Validate compression level
	if level < gzip.BestSpeed || level > gzip.BestCompression {
		return fmt.Errorf("invalid compression level: %d (must be between 1-9)", level)
	}

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

	// Create output file
	output, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer output.Close()

	// Create gzip writer
	gzipWriter, err := gzip.NewWriterLevel(output, level)
	if err != nil {
		return fmt.Errorf("failed to create gzip writer: %w", err)
	}
	defer gzipWriter.Close()

	// Set up progress bar
	progress := utils.NewProgressBar("Compressing")
	progress.SetTotal(inputStat.Size())

	// Create a buffer for reading
	buffer := make([]byte, 4*1024*1024) // 4MB buffer

	// Copy data with progress reporting
	var totalBytes int64
	for {
		n, err := input.Read(buffer)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read input file: %w", err)
		}

		if n == 0 {
			break
		}

		if _, err := gzipWriter.Write(buffer[:n]); err != nil {
			return fmt.Errorf("failed to write compressed data: %w", err)
		}

		totalBytes += int64(n)
		progress.SetCurrent(totalBytes)
	}

	// Ensure all data is flushed
	if err := gzipWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush compressed data: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return fmt.Errorf("failed to finalize compressed data: %w", err)
	}

	// Get output file stats
	outputStat, err := os.Stat(outputFile)
	if err != nil {
		return fmt.Errorf("failed to get output file info: %w", err)
	}

	// Calculate compression ratio
	ratio := float64(inputStat.Size()) / float64(outputStat.Size())
	reduction := (1 - float64(outputStat.Size())/float64(inputStat.Size())) * 100

	logger.Info("Compression completed",
		"input", inputFile,
		"output", outputFile,
		"input_size", utils.FormatByteSize(inputStat.Size()),
		"output_size", utils.FormatByteSize(outputStat.Size()),
		"ratio", fmt.Sprintf("%.2f:1 (%.1f%% reduction)", ratio, reduction))

	return nil
}
