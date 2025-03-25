// cmd/root.go
package cmd

import (
	"github.com/sfi2k7/mc/internal/utils"
	"github.com/spf13/cobra"
)

var (
	host      string
	port      int
	uri       string
	batchSize int
	logger    *utils.Logger
	rootCmd   *cobra.Command
)

func init() {
	rootCmd = &cobra.Command{
		Use:   "mc",
		Short: "MongoDB Collection Transfer Utility",
		Long: `A utility for transferring MongoDB collections between servers.
Supports exporting and importing collections while preserving BSON types.`,
	}

	// Global flags
	rootCmd.PersistentFlags().StringVar(&host, "host", "localhost", "MongoDB host")
	rootCmd.PersistentFlags().IntVar(&port, "port", 27017, "MongoDB port")
	rootCmd.PersistentFlags().StringVar(&uri, "uri", "", "MongoDB URI (overrides host/port if specified)")
	rootCmd.PersistentFlags().IntVar(&batchSize, "batch-size", 1000, "Number of documents per batch")

	// Add subcommands
	rootCmd.AddCommand(newExportCmd())
	rootCmd.AddCommand(newImportCmd())
	rootCmd.AddCommand(newInspectCmd())
}

// Execute runs the root command
func Execute(log *utils.Logger) error {
	logger = log
	return rootCmd.Execute()
}
