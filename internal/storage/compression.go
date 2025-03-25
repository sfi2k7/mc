// internal/storage/compression.go
package storage

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

// Compressor wraps a zstd encoder for writing compressed data
type Compressor struct {
	writer *zstd.Encoder
}

// Decompressor wraps a zstd decoder for reading compressed data
type Decompressor struct {
	reader *zstd.Decoder
}

// NewCompressor creates a new compressor
func NewCompressor(w io.Writer) (*Compressor, error) {
	encoder, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	return &Compressor{writer: encoder}, nil
}

// Write compresses and writes data
func (c *Compressor) Write(p []byte) (n int, err error) {
	return c.writer.Write(p)
}

// Close finalizes the compressed data
func (c *Compressor) Close() error {
	return c.writer.Close()
}

// NewDecompressor creates a new decompressor
func NewDecompressor(r io.Reader) (*Decompressor, error) {
	decoder, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	return &Decompressor{reader: decoder}, nil
}

// Read decompresses and reads data
func (d *Decompressor) Read(p []byte) (n int, err error) {
	return d.reader.Read(p)
}

// Close releases resources
func (d *Decompressor) Close() error {
	d.reader.Close()
	return nil
}
