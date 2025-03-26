// internal/storage/file.go
package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"

	"go.mongodb.org/mongo-driver/bson"
)

const (
	// Magic bytes for file format identification (as a string for readability)
	magicBytes = "MCBF" // MongoDB Collection Binary Format
	// Version of the file format
	fileVersion = uint8(1)
)

// Must use consistent byte order across architectures
var byteOrder = binary.LittleEndian

// Metadata holds information about the exported collection
type Metadata struct {
	Database      string
	Collection    string
	DocumentCount int64
	Timestamp     int64
	Source        string
	TotalSize     int64
	Platform      string // For cross-platform identification
}

// FileWriter handles writing data to the export file
type FileWriter struct {
	file         *os.File
	metadata     Metadata
	headerOffset int64
}

// FileReader handles reading data from the export file
type FileReader struct {
	file     *os.File
	metadata Metadata
}

// NewFileWriter creates a new file writer
func NewFileWriter(path string) (*FileWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	// Record current position so we know where header ends
	headerOffset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Write placeholder header (will be updated on close)
	// 4 bytes magic + 1 byte version + 8 bytes metadata length
	placeholder := make([]byte, 13)
	if _, err := file.Write(placeholder); err != nil {
		file.Close()
		return nil, err
	}

	return &FileWriter{
		file:         file,
		headerOffset: headerOffset,
		metadata: Metadata{
			Platform: runtime.GOARCH + "-" + runtime.GOOS,
		},
	}, nil
}

// WriteHeader writes initial metadata to the file
func (w *FileWriter) WriteHeader(metadata Metadata) error {
	w.metadata = metadata
	w.metadata.Platform = runtime.GOARCH + "-" + runtime.GOOS

	// The actual header will be written on close
	return nil
}

// WriteBatch writes a batch of BSON documents to the file
func (w *FileWriter) WriteBatch(batch []bson.D) error {
	// Write batch length as a 32-bit integer
	batchLengthBytes := make([]byte, 4)
	byteOrder.PutUint32(batchLengthBytes, uint32(len(batch)))
	if _, err := w.file.Write(batchLengthBytes); err != nil {
		return fmt.Errorf("failed to write batch length: %w", err)
	}

	// Write each document
	for _, doc := range batch {
		data, err := bson.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal document: %w", err)
		}

		// Write document length
		docLengthBytes := make([]byte, 4)
		byteOrder.PutUint32(docLengthBytes, uint32(len(data)))
		if _, err := w.file.Write(docLengthBytes); err != nil {
			return fmt.Errorf("failed to write document length: %w", err)
		}

		// Write document data
		if _, err := w.file.Write(data); err != nil {
			return fmt.Errorf("failed to write document data: %w", err)
		}

		// Update total size
		w.metadata.TotalSize += int64(len(data) + 4)
	}

	return nil
}

// WriteFooter finalizes the file by updating the header with metadata
func (w *FileWriter) WriteFooter(metadata Metadata) error {
	// Update document count from metadata parameter
	w.metadata.DocumentCount = metadata.DocumentCount

	// Get current position
	currentPos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}

	// Now go back and write the proper header
	if _, err := w.file.Seek(w.headerOffset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to header: %w", err)
	}

	// Write magic bytes (4 bytes)
	if _, err := w.file.Write([]byte(magicBytes)); err != nil {
		return fmt.Errorf("failed to write magic bytes: %w", err)
	}

	// Write version (1 byte)
	if _, err := w.file.Write([]byte{fileVersion}); err != nil {
		return fmt.Errorf("failed to write version byte: %w", err)
	}

	// Prepare metadata document
	metadataDoc := bson.D{
		{Key: "database", Value: w.metadata.Database},
		{Key: "collection", Value: w.metadata.Collection},
		{Key: "documentCount", Value: w.metadata.DocumentCount},
		{Key: "timestamp", Value: w.metadata.Timestamp},
		{Key: "source", Value: w.metadata.Source},
		{Key: "totalSize", Value: w.metadata.TotalSize},
		{Key: "platform", Value: w.metadata.Platform},
	}

	// Marshal metadata to BSON (which is architecture-independent)
	metadataBytes, err := bson.Marshal(metadataDoc)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Write metadata length (8 bytes)
	metadataLengthBytes := make([]byte, 8)
	byteOrder.PutUint64(metadataLengthBytes, uint64(len(metadataBytes)))
	if _, err := w.file.Write(metadataLengthBytes); err != nil {
		return fmt.Errorf("failed to write metadata length: %w", err)
	}

	// Write metadata
	if _, err := w.file.Write(metadataBytes); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Go back to where we were
	if _, err := w.file.Seek(currentPos, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to end of data: %w", err)
	}

	return nil
}

// Close closes the file writer
func (w *FileWriter) Close() error {
	var err error

	if w.file != nil {
		err = w.file.Close()
		w.file = nil
	}

	return err
}

// NewFileReader creates a new file reader
func NewFileReader(path string) (*FileReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	reader := &FileReader{
		file: file,
	}

	// Header will be read by ReadHeader method
	return reader, nil
}

// ReadHeader reads the file header with metadata
func (r *FileReader) ReadHeader() (Metadata, error) {
	// Read magic bytes (4 bytes)
	magicBytesRead := make([]byte, 4)
	if _, err := io.ReadFull(r.file, magicBytesRead); err != nil {
		return Metadata{}, fmt.Errorf("failed to read magic bytes: %w", err)
	}

	if string(magicBytesRead) != magicBytes {
		return Metadata{}, fmt.Errorf("invalid file format: expected %s, got %s",
			magicBytes, string(magicBytesRead))
	}

	// Read version (1 byte)
	versionByte := make([]byte, 1)
	if _, err := io.ReadFull(r.file, versionByte); err != nil {
		return Metadata{}, fmt.Errorf("failed to read version: %w", err)
	}

	if versionByte[0] != fileVersion {
		return Metadata{}, fmt.Errorf("unsupported file version: %d", versionByte[0])
	}

	// Read metadata length (8 bytes)
	metadataLengthBytes := make([]byte, 8)
	if _, err := io.ReadFull(r.file, metadataLengthBytes); err != nil {
		return Metadata{}, fmt.Errorf("failed to read metadata length: %w", err)
	}

	metadataLength := byteOrder.Uint64(metadataLengthBytes)
	if metadataLength > 10*1024*1024 { // Sanity check - metadata shouldn't be over 10MB
		return Metadata{}, fmt.Errorf("metadata too large: %d bytes", metadataLength)
	}

	// Read metadata
	metadataBytes := make([]byte, metadataLength)
	if _, err := io.ReadFull(r.file, metadataBytes); err != nil {
		return Metadata{}, fmt.Errorf("failed to read metadata: %w", err)
	}

	// Unmarshal metadata
	var metadataDoc bson.M
	if err := bson.Unmarshal(metadataBytes, &metadataDoc); err != nil {
		return Metadata{}, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// Extract metadata fields with type assertions and safety checks
	r.metadata = Metadata{}

	if db, ok := metadataDoc["database"].(string); ok {
		r.metadata.Database = db
	}

	if coll, ok := metadataDoc["collection"].(string); ok {
		r.metadata.Collection = coll
	}

	if count, ok := metadataDoc["documentCount"].(int64); ok {
		r.metadata.DocumentCount = count
	}

	if ts, ok := metadataDoc["timestamp"].(int64); ok {
		r.metadata.Timestamp = ts
	}

	if src, ok := metadataDoc["source"].(string); ok {
		r.metadata.Source = src
	}

	if totalSize, ok := metadataDoc["totalSize"].(int64); ok {
		r.metadata.TotalSize = totalSize
	}

	if platform, ok := metadataDoc["platform"].(string); ok {
		r.metadata.Platform = platform
	}

	return r.metadata, nil
}

// ReadBatch reads a batch of BSON documents from the file
func (r *FileReader) ReadBatch(maxBatchSize int) ([]bson.D, error) {
	// Read batch length (4 bytes)
	batchLengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(r.file, batchLengthBytes); err != nil {
		if err == io.EOF {
			return []bson.D{}, nil // End of file, return empty batch
		}
		return nil, fmt.Errorf("failed to read batch length: %w", err)
	}

	batchLength := byteOrder.Uint32(batchLengthBytes)
	if batchLength > 1000000 { // Sanity check
		return nil, fmt.Errorf("batch size too large: %d", batchLength)
	}

	// Limit batch size to what was requested
	actualBatchSize := int(batchLength)
	if actualBatchSize > maxBatchSize {
		actualBatchSize = maxBatchSize
	}

	batch := make([]bson.D, 0, actualBatchSize)

	// Read documents
	for i := 0; i < actualBatchSize; i++ {
		// Read document length (4 bytes)
		docLengthBytes := make([]byte, 4)
		if _, err := io.ReadFull(r.file, docLengthBytes); err != nil {
			if err == io.EOF && i > 0 {
				// Partial batch is ok
				return batch, nil
			}
			return batch, fmt.Errorf("failed to read document length: %w", err)
		}

		docLength := byteOrder.Uint32(docLengthBytes)
		if docLength > 16*1024*1024 { // Sanity check - docs over 16MB are not valid for MongoDB
			return batch, fmt.Errorf("document too large: %d bytes", docLength)
		}

		// Read document data
		docBytes := make([]byte, docLength)
		if _, err := io.ReadFull(r.file, docBytes); err != nil {
			return batch, fmt.Errorf("failed to read document data: %w", err)
		}

		// Unmarshal document
		var doc bson.D
		if err := bson.Unmarshal(docBytes, &doc); err != nil {
			return batch, fmt.Errorf("failed to unmarshal document: %w", err)
		}

		batch = append(batch, doc)
	}

	return batch, nil
}

// Close closes the file reader
func (r *FileReader) Close() error {
	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		return err
	}

	return nil
}
