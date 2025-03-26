// internal/utils/progress.go
package utils

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	progressBarWidth = 50
)

// ProgressBar provides a simple progress bar
type ProgressBar struct {
	mu         sync.Mutex
	operation  string
	total      int64
	current    int64
	startTime  time.Time
	lastUpdate time.Time
}

// NewProgressBar creates a new progress bar
func NewProgressBar(operation string) *ProgressBar {
	return &ProgressBar{
		operation:  operation,
		startTime:  time.Now(),
		lastUpdate: time.Now(),
	}
}

// SetTotal sets the total number of items to process
func (p *ProgressBar) SetTotal(total int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.total = total
	p.render()
}

// Add adds n to the current progress
func (p *ProgressBar) Add(n int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current += n

	// Only update visually every 100ms to avoid terminal flicker
	if time.Since(p.lastUpdate) > 100*time.Millisecond {
		p.render()
		p.lastUpdate = time.Now()
	}
}

// render displays the progress bar
func (p *ProgressBar) render() {
	if p.total <= 0 {
		fmt.Printf("\r%s: %d items... ", p.operation, p.current)
		return
	}

	percent := float64(p.current) / float64(p.total)
	if percent > 1.0 {
		percent = 1.0
	}

	// Calculate width
	width := int(percent * progressBarWidth)

	// Calculate ETA
	var eta string
	if p.current > 0 {
		elapsed := time.Since(p.startTime)
		estimatedTotal := float64(elapsed) * float64(p.total) / float64(p.current)
		remaining := time.Duration(estimatedTotal) - elapsed
		eta = fmt.Sprintf("ETA: %s", formatDuration(remaining))
	} else {
		eta = "ETA: --"
	}

	// Build progress bar
	bar := strings.Repeat("=", width) + strings.Repeat(" ", progressBarWidth-width)

	fmt.Printf("\r%s: [%s] %.2f%% (%d/%d) %s",
		p.operation, bar, percent*100, p.current, p.total, eta)
}

// SetCurrent sets the current progress value
func (p *ProgressBar) SetCurrent(current int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current = current

	// Only update visually every 100ms to avoid terminal flicker
	if time.Since(p.lastUpdate) > 100*time.Millisecond {
		p.render()
		p.lastUpdate = time.Now()
	}
}

// formatDuration formats a duration for display
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	} else if d < time.Hour {
		return fmt.Sprintf("%.0fm %.0fs", d.Minutes(), d.Seconds()-float64(int(d.Minutes()))*60)
	} else {
		return fmt.Sprintf("%.0fh %.0fm", d.Hours(), d.Minutes()-float64(int(d.Hours()))*60)
	}
}
