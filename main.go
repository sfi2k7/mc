// main.go
package main

import (
	"os"

	"github.com/sfi2k7/mc/cmd"
	"github.com/sfi2k7/mc/internal/utils"
)

func main() {
	logger := utils.NewLogger()

	if err := cmd.Execute(logger); err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}
