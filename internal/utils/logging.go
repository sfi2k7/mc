// internal/utils/logging.go
package utils

import (
	"fmt"
	"log"
	"os"
)

// LogLevel represents the severity of a log message
type LogLevel int

const (
	// DEBUG level for detailed information
	DEBUG LogLevel = iota
	// INFO level for general operational information
	INFO
	// WARN level for warning messages
	WARN
	// ERROR level for error conditions
	ERROR
)

// Logger provides structured logging capabilities
type Logger struct {
	debugLog *log.Logger
	infoLog  *log.Logger
	warnLog  *log.Logger
	errorLog *log.Logger
}

// NewLogger creates a new logger instance
func NewLogger() *Logger {
	return &Logger{
		debugLog: log.New(os.Stdout, "[DEBUG] ", log.Ldate|log.Ltime),
		infoLog:  log.New(os.Stdout, "[INFO] ", log.Ldate|log.Ltime),
		warnLog:  log.New(os.Stderr, "[WARN] ", log.Ldate|log.Ltime),
		errorLog: log.New(os.Stderr, "[ERROR] ", log.Ldate|log.Ltime),
	}
}

// formatAttrs formats key-value pairs for logging
func formatAttrs(attrs ...interface{}) string {
	if len(attrs) == 0 {
		return ""
	}

	result := " | "
	for i := 0; i < len(attrs); i += 2 {
		if i > 0 {
			result += ", "
		}

		// Handle the key
		key := fmt.Sprintf("%v", attrs[i])

		// Handle the value (which might be missing)
		var val interface{} = "<missing>"
		if i+1 < len(attrs) {
			val = attrs[i+1]
		}

		result += fmt.Sprintf("%s=%v", key, val)
	}
	return result
}

// Debug logs a debug message
func (l *Logger) Debug(msg string, attrs ...interface{}) {
	l.debugLog.Println(msg + formatAttrs(attrs...))
}

// Info logs an info message
func (l *Logger) Info(msg string, attrs ...interface{}) {
	l.infoLog.Println(msg + formatAttrs(attrs...))
}

// Warn logs a warning message
func (l *Logger) Warn(msg string, attrs ...interface{}) {
	l.warnLog.Println(msg + formatAttrs(attrs...))
}

// Error logs an error message
func (l *Logger) Error(msg string, attrs ...interface{}) {
	l.errorLog.Println(msg + formatAttrs(attrs...))
}
