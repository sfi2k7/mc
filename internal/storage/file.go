// internal/storage/file.go

// Replace the existing implementation with this version that explicitly handles endianness
package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"go.mongodb.org/mongo-driver/bson"
)

const (
	// Magic number for file format identification
	magicNumber = "MCBZ"
	// Version of the file format
	fileVersion = 1
)

// Use a consistent byte order across all architectures
var byteOrder = binary.LittleEndian

// Metadata holds information about the exported collection
type Metadata struct {
	Database       string
	Collection     string
	DocumentCount  int64
	Timestamp      int64
	Source         string
	OriginalSize   int64
	CompressedSize int64
}

// FileWriter handles writing data to the export file
type FileWriter struct {
	file       *os.File
	compressor *Compressor
	metadata   Metadata
}

// FileReader handles reading data from the export file
type FileReader struct {
	file         *os.File
	decompressor *Decompressor
	metadata     Metadata
}

// NewFileWriter creates a new file writer
func NewFileWriter(path string) (*FileWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	// Reserve space for the header (will be written later)
	headerSize := 4 + 1 + 4 + 100 // magic + version + metadata length + estimated metadata
	if _, err := file.Seek(int64(headerSize), io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}

	compressor, err := NewCompressor(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	return &FileWriter{
		file:       file,
		compressor: compressor,
	}, nil
}

// WriteHeader writes the file header with metadata
func (w *FileWriter) WriteHeader(metadata Metadata) error {
	w.metadata = metadata

	// We'll actually write the full header when closing the file
	// because we need final document count and size information
	return nil
}

// WriteBatch writes a batch of BSON documents to the file
func (w *FileWriter) WriteBatch(batch []bson.D) error {
	// Write batch length
	batchLengthBytes := make([]byte, 4)
	byteOrder.PutUint32(batchLengthBytes, uint32(len(batch)))
	if _, err := w.compressor.Write(batchLengthBytes); err != nil {
		return err
	}

	// Write each document
	for _, doc := range batch {
		data, err := bson.Marshal(doc)
		if err != nil {
			return err
		}

		// Write document length and data
		docLengthBytes := make([]byte, 4)
		byteOrder.PutUint32(docLengthBytes, uint32(len(data)))

		if _, err := w.compressor.Write(docLengthBytes); err != nil {
			return err
		}

		if _, err := w.compressor.Write(data); err != nil {
			return err
		}

		// Update original size
		w.metadata.OriginalSize += int64(len(data) + 4)
	}

	return nil
}

// WriteFooter finalizes the file by writing the footer
func (w *FileWriter) WriteFooter(metadata Metadata) error {
	// Update metadata
	w.metadata.DocumentCount = metadata.DocumentCount

	// Flush and close the compressor
	originalPosition, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	if err := w.compressor.Close(); err != nil {
		return err
	}

	// Calculate compressed size
	endPosition, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	w.metadata.CompressedSize = endPosition - int64(4+1+4+100) // Subtract header size

	// Go back to the beginning to write the header
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// Write magic number
	if _, err := w.file.Write([]byte(magicNumber)); err != nil {
		return err
	}

	// Write version
	if _, err := w.file.Write([]byte{fileVersion}); err != nil {
		return err
	}

	// Marshal metadata
	metadataDoc := bson.D{
		{Key: "database", Value: w.metadata.Database},
		{Key: "collection", Value: w.metadata.Collection},
		{Key: "documentCount", Value: w.metadata.DocumentCount},
		{Key: "timestamp", Value: w.metadata.Timestamp},
		{Key: "source", Value: w.metadata.Source},
		{Key: "originalSize", Value: w.metadata.OriginalSize},
		{Key: "compressedSize", Value: w.metadata.CompressedSize},
		{Key: "architecture", Value: "cross-platform"}, // Add this to indicate cross-platform compatibility
	}

	metadataBytes, err := bson.Marshal(metadataDoc)
	if err != nil {
		return err
	}

	// Write metadata length
	metadataLengthBytes := make([]byte, 4)
	byteOrder.PutUint32(metadataLengthBytes, uint32(len(metadataBytes)))
	if _, err := w.file.Write(metadataLengthBytes); err != nil {
		return err
	}

	// Write metadata
	if _, err := w.file.Write(metadataBytes); err != nil {
		return err
	}

	// Go back to where we were
	if _, err := w.file.Seek(originalPosition, io.SeekStart); err != nil {
		return err
	}

	return nil
}

// Close closes the file writer
func (w *FileWriter) Close() error {
	if w.compressor != nil {
		w.compressor.Close()
		w.compressor = nil
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
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

	// Read header in ReadHeader method
	return reader, nil
}

// ReadHeader reads the file header with metadata
func (r *FileReader) ReadHeader() (Metadata, error) {
	// Read magic number
	magicBytes := make([]byte, 4)
	if _, err := io.ReadFull(r.file, magicBytes); err != nil {
		return Metadata{}, err
	}
	if string(magicBytes) != magicNumber {
		return Metadata{}, fmt.Errorf("invalid file format: expected %s, got %s", magicNumber, string(magicBytes))
	}

	// Read version
	versionByte := make([]byte, 1)
	if _, err := io.ReadFull(r.file, versionByte); err != nil {
		return Metadata{}, err
	}
	if versionByte[0] != fileVersion {
		return Metadata{}, fmt.Errorf("unsupported file version: %d", versionByte[0])
	}

	// Read metadata length
	metadataLengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(r.file, metadataLengthBytes); err != nil {
		return Metadata{}, err
	}
	metadataLength := byteOrder.Uint32(metadataLengthBytes)

	// Read metadata
	metadataBytes := make([]byte, metadataLength)
	if _, err := io.ReadFull(r.file, metadataBytes); err != nil {
		return Metadata{}, err
	}

	// Unmarshal metadata
	var metadataDoc bson.M
	if err := bson.Unmarshal(metadataBytes, &metadataDoc); err != nil {
		return Metadata{}, err
	}

	// Extract metadata fields
	r.metadata = Metadata{
		Database:       metadataDoc["database"].(string),
		Collection:     metadataDoc["collection"].(string),
		DocumentCount:  metadataDoc["documentCount"].(int64),
		Timestamp:      metadataDoc["timestamp"].(int64),
		Source:         metadataDoc["source"].(string),
		OriginalSize:   metadataDoc["originalSize"].(int64),
		CompressedSize: metadataDoc["compressedSize"].(int64),
	}

	// Initialize decompressor
	decompressor, err := NewDecompressor(r.file)
	if err != nil {
		return Metadata{}, err
	}
	r.decompressor = decompressor

	return r.metadata, nil
}

// ReadBatch reads a batch of BSON documents from the file
func (r *FileReader) ReadBatch(maxBatchSize int) ([]bson.D, error) {
	// Read batch length
	batchLengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(r.decompressor, batchLengthBytes); err != nil {
		if err == io.EOF {
			return []bson.D{}, nil
		}
		return nil, err
	}
	batchLength := byteOrder.Uint32(batchLengthBytes)

	// Limit batch size
	actualBatchSize := int(batchLength)
	if actualBatchSize > maxBatchSize {
		actualBatchSize = maxBatchSize
	}

	batch := make([]bson.D, 0, actualBatchSize)

	// Read documents
	for i := 0; i < actualBatchSize; i++ {
		// Read document length
		docLengthBytes := make([]byte, 4)
		if _, err := io.ReadFull(r.decompressor, docLengthBytes); err != nil {
			return batch, err
		}
		docLength := byteOrder.Uint32(docLengthBytes)

		// Read document data
		docBytes := make([]byte, docLength)
		if _, err := io.ReadFull(r.decompressor, docBytes); err != nil {
			return batch, err
		}

		// Unmarshal document
		var doc bson.D
		if err := bson.Unmarshal(docBytes, &doc); err != nil {
			return batch, err
		}

		batch = append(batch, doc)
	}

	return batch, nil
}

// Close closes the file reader
func (r *FileReader) Close() error {
	if r.decompressor != nil {
		r.decompressor.Close()
		r.decompressor = nil
	}
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
