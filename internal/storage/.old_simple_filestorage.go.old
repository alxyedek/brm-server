package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"brm/pkg/models"
)

// SimpleFileStorage implements ArtifactStorage using a file-based storage system
// with git-like object storage pattern (hash-based directory structure).
// It is optimized for streaming I/O with minimal memory footprint.
type SimpleFileStorage struct {
	basePath string
}

// NewSimpleFileStorage creates a new SimpleFileStorage instance with the given base path.
// The base directory will be created if it doesn't exist.
func NewSimpleFileStorage(basePath string) (*SimpleFileStorage, error) {
	// Create base directory if it doesn't exist
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	return &SimpleFileStorage{
		basePath: basePath,
	}, nil
}

// getDataPath returns the full path to the data file for the given hash.
// Uses git-like storage: {basePath}/{first2chars}/{remainingchars}
func (s *SimpleFileStorage) getDataPath(hash string) string {
	if len(hash) < 2 {
		// Fallback for invalid hash (shouldn't happen in practice)
		return filepath.Join(s.basePath, hash)
	}
	subdir := hash[:2]
	filename := hash[2:]
	return filepath.Join(s.basePath, subdir, filename)
}

// getMetaPath returns the full path to the metadata file for the given hash.
func (s *SimpleFileStorage) getMetaPath(hash string) string {
	return s.getDataPath(hash) + ".meta"
}

// ensureSubdirectory creates the subdirectory for the given hash if it doesn't exist.
func (s *SimpleFileStorage) ensureSubdirectory(hash string) error {
	if len(hash) < 2 {
		return fmt.Errorf("invalid hash: too short")
	}
	subdir := filepath.Join(s.basePath, hash[:2])
	return os.MkdirAll(subdir, 0755)
}

// Create streams data from 'r' to storage.
// We include 'size' because many storage backends (allocators/S3) need a size hint.
// If size is unknown, pass -1 (though this may disable some optimizations).
// The 'meta' parameter is optional (can be nil). If provided, metadata is stored atomically with the data.
func (s *SimpleFileStorage) Create(ctx context.Context, hash string, r io.Reader, size int64, meta *models.ArtifactMeta) error {
	// Ensure subdirectory exists
	if err := s.ensureSubdirectory(hash); err != nil {
		return err
	}

	// Calculate data file path
	dataPath := s.getDataPath(hash)

	// Open file for writing (create/truncate)
	file, err := os.OpenFile(dataPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}
	defer file.Close()

	// Stream data directly from reader to file using io.Copy
	// Go's runtime optimizes this with copy() builtin (can leverage DMA, zero-copy)
	_, err = io.Copy(file, r)
	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	// Write metadata if provided (streams directly using json.Encoder)
	if meta != nil {
		meta.Hash = hash // Ensure hash matches
		metaPath := s.getMetaPath(hash)
		metaFile, err := os.OpenFile(metaPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return fmt.Errorf("failed to create metadata file: %w", err)
		}
		defer metaFile.Close()

		// Use json.Encoder directly on file for streaming (no intermediate buffers)
		if err := json.NewEncoder(metaFile).Encode(meta); err != nil {
			return fmt.Errorf("failed to write metadata: %w", err)
		}
	}

	return nil
}

// rangeReader wraps a file with a limited range reader for streaming reads
type rangeReader struct {
	file   *os.File
	length int64
	read   int64
}

func (rr *rangeReader) Read(p []byte) (n int, err error) {
	if rr.length != -1 && rr.read >= rr.length {
		return 0, io.EOF
	}

	// Limit read size to remaining bytes
	maxRead := len(p)
	if rr.length != -1 {
		remaining := rr.length - rr.read
		if remaining < int64(maxRead) {
			maxRead = int(remaining)
		}
	}

	n, err = rr.file.Read(p[:maxRead])
	rr.read += int64(n)

	if err == io.EOF && rr.length == -1 {
		// Natural EOF when reading until end
		return n, io.EOF
	}

	if rr.length != -1 && rr.read >= rr.length {
		// Reached the requested length
		if err == nil {
			err = io.EOF
		}
	}

	return n, err
}

func (rr *rangeReader) Close() error {
	return rr.file.Close()
}

// Read returns a stream (rc) for the requested data.
// It returns 'actual' containing the actual range being returned (calculated).
// This is useful if the requested Length was -1 or exceeded the file size.
// IMPORTANT: The caller MUST close rc.
func (s *SimpleFileStorage) Read(ctx context.Context, req models.ArtifactRange) (rc io.ReadCloser, actual models.ArtifactRange, err error) {
	// Calculate data file path
	dataPath := s.getDataPath(req.Hash)

	// Open file for reading
	file, err := os.Open(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, models.ArtifactRange{}, fmt.Errorf("artifact data not found: %s", req.Hash)
		}
		return nil, models.ArtifactRange{}, fmt.Errorf("failed to open data file: %w", err)
	}

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, models.ArtifactRange{}, fmt.Errorf("failed to get file info: %w", err)
	}
	fileSize := fileInfo.Size()

	// Calculate actual range
	actualOffset := req.Range.Offset
	if actualOffset < 0 {
		actualOffset = 0
	}
	if actualOffset > fileSize {
		actualOffset = fileSize
	}

	actualLength := req.Range.Length
	if req.Range.Length == -1 {
		// Read until end
		actualLength = fileSize - actualOffset
	} else {
		// Limit to available bytes
		available := fileSize - actualOffset
		if actualLength > available {
			actualLength = available
		}
	}

	if actualLength < 0 {
		actualLength = 0
	}

	// Seek to offset
	if _, err := file.Seek(actualOffset, io.SeekStart); err != nil {
		file.Close()
		return nil, models.ArtifactRange{}, fmt.Errorf("failed to seek: %w", err)
	}

	// Create range reader (streams directly from file, no buffering)
	rr := &rangeReader{
		file:   file,
		length: actualLength,
		read:   0,
	}

	actual = models.ArtifactRange{
		Hash: req.Hash,
		Range: models.ByteRange{
			Offset: actualOffset,
			Length: actualLength,
		},
	}

	return rr, actual, nil
}

// zeroReader is an io.Reader that produces zeros for efficient padding
type zeroReader struct{}

var zeroReaderInstance = &zeroReader{}

func (z *zeroReader) Read(p []byte) (n int, err error) {
	// Fill the buffer with zeros
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// Update modifies a specific range by streaming data from 'r'.
func (s *SimpleFileStorage) Update(ctx context.Context, req models.ArtifactRange, r io.Reader) error {
	hash := req.Hash
	offset := req.Range.Offset

	// Ensure subdirectory exists
	if err := s.ensureSubdirectory(hash); err != nil {
		return err
	}

	// Calculate data file path
	dataPath := s.getDataPath(hash)

	// Open file for read/write (create if doesn't exist)
	file, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}
	defer file.Close()

	// Get current file size
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	currentSize := fileInfo.Size()

	// If offset exceeds current file size, pad with zeros using streaming approach
	if offset > currentSize {
		// Seek to end of file
		if _, err := file.Seek(currentSize, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek to end: %w", err)
		}
		// Stream zeros to pad up to offset using io.Copy (efficient, no large buffer allocation)
		paddingSize := offset - currentSize
		limitedZeroReader := io.LimitReader(zeroReaderInstance, paddingSize)
		if _, err := io.Copy(file, limitedZeroReader); err != nil {
			return fmt.Errorf("failed to write padding: %w", err)
		}
	}

	// Seek to the offset position
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to position: %w", err)
	}

	// Stream data directly from reader to file using io.Copy
	// If length is specified and not -1, limit the copy
	if req.Range.Length != -1 {
		limitedReader := io.LimitReader(r, req.Range.Length)
		_, err = io.Copy(file, limitedReader)
	} else {
		_, err = io.Copy(file, r)
	}

	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	return nil
}

// Delete removes an artifact.
func (s *SimpleFileStorage) Delete(ctx context.Context, hash string) error {
	// Calculate paths
	dataPath := s.getDataPath(hash)
	metaPath := s.getMetaPath(hash)

	// Delete data file
	if err := os.Remove(dataPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete data file: %w", err)
	}

	// Delete metadata file
	if err := os.Remove(metaPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete metadata file: %w", err)
	}

	// Best-effort cleanup of empty subdirectory (ignore errors)
	if len(hash) >= 2 {
		subdir := filepath.Join(s.basePath, hash[:2])
		os.Remove(subdir) // Ignore error - directory might not be empty or might not exist
	}

	return nil
}

// GetMeta retrieves the metadata for an artifact identified by the hash.
func (s *SimpleFileStorage) GetMeta(ctx context.Context, hash string) (*models.ArtifactMeta, error) {
	metaPath := s.getMetaPath(hash)

	// Open metadata file
	file, err := os.Open(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("artifact metadata not found: %s", hash)
		}
		return nil, fmt.Errorf("failed to open metadata file: %w", err)
	}
	defer file.Close()

	// Use json.Decoder directly on file for streaming JSON decode (no intermediate buffers)
	var meta models.ArtifactMeta
	if err := json.NewDecoder(file).Decode(&meta); err != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}

	return &meta, nil
}

// UpdateMeta updates the metadata for an artifact identified by the hash in ArtifactMeta.
// If metadata doesn't exist, it will be created.
func (s *SimpleFileStorage) UpdateMeta(ctx context.Context, meta models.ArtifactMeta) (*models.ArtifactMeta, error) {
	// Try to read existing metadata
	existingMeta, err := s.GetMeta(ctx, meta.Hash)

	var updatedMeta models.ArtifactMeta
	if err != nil {
		// Metadata doesn't exist, create new one
		updatedMeta = meta
		// Ensure hash is set
		if updatedMeta.Hash == "" {
			updatedMeta.Hash = meta.Hash
		}
	} else {
		// Merge with existing metadata, keeping existing values if new ones are empty/zero
		updatedMeta = *existingMeta
		if meta.Name != "" {
			updatedMeta.Name = meta.Name
		}
		if meta.CreatedTimestamp != 0 {
			updatedMeta.CreatedTimestamp = meta.CreatedTimestamp
		}
		if meta.Repo != "" {
			updatedMeta.Repo = meta.Repo
		}
		if meta.Length != 0 {
			updatedMeta.Length = meta.Length
		}
		// Hash should not be updated, keep existing
	}

	// Ensure subdirectory exists
	if err := s.ensureSubdirectory(updatedMeta.Hash); err != nil {
		return nil, err
	}

	// Write metadata using json.Encoder directly on file (streams, no intermediate buffers)
	metaPath := s.getMetaPath(updatedMeta.Hash)
	file, err := os.OpenFile(metaPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata file: %w", err)
	}
	defer file.Close()

	if err := json.NewEncoder(file).Encode(updatedMeta); err != nil {
		return nil, fmt.Errorf("failed to encode metadata: %w", err)
	}

	return &updatedMeta, nil
}
