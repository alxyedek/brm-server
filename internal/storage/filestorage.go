package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"brm/pkg/models"
)

// FileStorage implements ArtifactStorage using a file-based storage system
// with git-like object storage pattern (hash-based directory structure)
type FileStorage struct {
	basePath string
}

// NewFileStorage creates a new FileStorage instance with the given base path.
// The base directory will be created if it doesn't exist.
func NewFileStorage(basePath string) (*FileStorage, error) {
	// Create base directory if it doesn't exist
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	return &FileStorage{
		basePath: basePath,
	}, nil
}

// getDataPath returns the full path to the data file for the given hash.
// Uses git-like storage: {basePath}/{first2chars}/{remaining62chars}
func (fs *FileStorage) getDataPath(hash string) string {
	if len(hash) < 2 {
		// Fallback for invalid hash (shouldn't happen in practice)
		return filepath.Join(fs.basePath, hash)
	}
	subdir := hash[:2]
	filename := hash[2:]
	return filepath.Join(fs.basePath, subdir, filename)
}

// getMetaPath returns the full path to the metadata file for the given hash.
// Uses git-like storage: {basePath}/{first2chars}/{remaining62chars}.meta
func (fs *FileStorage) getMetaPath(hash string) string {
	return fs.getDataPath(hash) + ".meta"
}

// ensureSubdirectory creates the subdirectory for the given hash if it doesn't exist.
func (fs *FileStorage) ensureSubdirectory(hash string) error {
	if len(hash) < 2 {
		return fmt.Errorf("invalid hash: too short")
	}
	subdir := filepath.Join(fs.basePath, hash[:2])
	return os.MkdirAll(subdir, 0755)
}

// readMeta reads and deserializes the metadata file for the given hash.
func (fs *FileStorage) readMeta(hash string) (*models.ArtifactMeta, error) {
	metaPath := fs.getMetaPath(hash)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("artifact not found: %s", hash)
		}
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var meta models.ArtifactMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &meta, nil
}

// writeMeta serializes and writes the metadata file for the given artifact.
func (fs *FileStorage) writeMeta(meta models.ArtifactMeta) error {
	if err := fs.ensureSubdirectory(meta.Hash); err != nil {
		return err
	}

	metaPath := fs.getMetaPath(meta.Hash)
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(metaPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

// readDataRange reads data from the artifact file within the specified range.
// Returns the data, the actual range read, and an error.
func (fs *FileStorage) readDataRange(hash string, start, end int64) ([]byte, models.ByteRange, error) {
	dataPath := fs.getDataPath(hash)
	file, err := os.Open(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, models.ByteRange{}, fmt.Errorf("artifact data not found: %s", hash)
		}
		return nil, models.ByteRange{}, fmt.Errorf("failed to open data file: %w", err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, models.ByteRange{}, fmt.Errorf("failed to get file info: %w", err)
	}
	fileSize := fileInfo.Size()

	// Calculate actual range
	actualStart := start
	if actualStart < 0 {
		actualStart = 0
	}
	if actualStart > fileSize {
		actualStart = fileSize
	}

	actualEnd := end
	if end == -1 {
		actualEnd = fileSize
	} else if end > fileSize {
		actualEnd = fileSize
	}

	if actualStart >= actualEnd {
		// Empty range
		return []byte{}, models.ByteRange{Start: actualStart, End: actualEnd}, nil
	}

	// Seek to start position
	if _, err := file.Seek(actualStart, io.SeekStart); err != nil {
		return nil, models.ByteRange{}, fmt.Errorf("failed to seek: %w", err)
	}

	// Read the range
	length := actualEnd - actualStart
	data := make([]byte, length)
	n, err := io.ReadFull(file, data)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, models.ByteRange{}, fmt.Errorf("failed to read data: %w", err)
	}

	// Return only the bytes actually read
	return data[:n], models.ByteRange{Start: actualStart, End: actualStart + int64(n)}, nil
}

// writeDataRange writes data to the artifact file at the specified offset.
// If the file doesn't exist, it will be created. For partial writes, the file
// will be extended if necessary.
func (fs *FileStorage) writeDataRange(hash string, data []byte, offset int64) error {
	if err := fs.ensureSubdirectory(hash); err != nil {
		return err
	}

	dataPath := fs.getDataPath(hash)
	file, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}
	defer file.Close()

	// Seek to offset
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}

	// Write data
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	return nil
}

// Create stores a new artifact data file. Only creates the data, not metadata.
// Returns an error if creation fails.
func (fs *FileStorage) Create(hash string, data []byte) error {
	// Ensure subdirectory exists
	if err := fs.ensureSubdirectory(hash); err != nil {
		return err
	}

	// Write data file (write at offset 0, which creates/overwrites the file)
	if err := fs.writeDataRange(hash, data, 0); err != nil {
		return err
	}

	return nil
}

// Read retrieves an artifact based on the request. The Range field in ArtifactRequest
// specifies which byte range to retrieve. Returns ArtifactResponse with the actual range returned.
func (fs *FileStorage) Read(request models.ArtifactRequest) (*models.ArtifactResponse, error) {
	// Read data range
	data, actualRange, err := fs.readDataRange(request.Hash, request.Range.Start, request.Range.End)
	if err != nil {
		return nil, err
	}

	// Create response
	response := &models.ArtifactResponse{
		Request: request,
		Data:    data,
		Range:   actualRange,
	}

	return response, nil
}

// Update modifies an existing artifact by replacing the specified byte range.
// The range may extend beyond the current file length (append behavior).
// Only updates data, not metadata. Returns an error if update fails.
func (fs *FileStorage) Update(update models.ArtifactRangeUpdate) error {
	hash := update.Hash
	rangeStart := update.Range.Start
	data := update.Data

	// Ensure subdirectory exists
	if err := fs.ensureSubdirectory(hash); err != nil {
		return err
	}

	dataPath := fs.getDataPath(hash)
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

	// If range.Start exceeds current file size, we need to pad with zeros
	if rangeStart > currentSize {
		// Seek to end of file
		if _, err := file.Seek(currentSize, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek to end: %w", err)
		}
		// Write zeros to pad up to rangeStart
		paddingSize := rangeStart - currentSize
		zeros := make([]byte, paddingSize)
		if _, err := file.Write(zeros); err != nil {
			return fmt.Errorf("failed to write padding: %w", err)
		}
	}

	// Seek to the start position
	if _, err := file.Seek(rangeStart, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to position: %w", err)
	}

	// Write the data
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	return nil
}

// Delete removes an artifact identified by the hash.
// Returns an error if the artifact doesn't exist or deletion fails.
func (fs *FileStorage) Delete(hash string) error {
	dataPath := fs.getDataPath(hash)
	metaPath := fs.getMetaPath(hash)

	// Delete data file
	if err := os.Remove(dataPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete data file: %w", err)
	}

	// Delete metadata file
	if err := os.Remove(metaPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete metadata file: %w", err)
	}

	// Optionally remove subdirectory if empty (best effort, ignore errors)
	if len(hash) >= 2 {
		subdir := filepath.Join(fs.basePath, hash[:2])
		os.Remove(subdir) // Ignore error - directory might not be empty or might not exist
	}

	return nil
}

// GetMeta retrieves the metadata for an artifact identified by the hash.
// Returns the ArtifactMeta or an error if the artifact doesn't exist.
func (fs *FileStorage) GetMeta(hash string) (*models.ArtifactMeta, error) {
	return fs.readMeta(hash)
}

// UpdateMeta updates the metadata for an artifact identified by the hash in ArtifactMeta.
// If metadata doesn't exist, it will be created. Returns the updated ArtifactMeta or an error.
func (fs *FileStorage) UpdateMeta(meta models.ArtifactMeta) (*models.ArtifactMeta, error) {
	// Try to read existing metadata
	existingMeta, err := fs.readMeta(meta.Hash)

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

	// Write metadata (create or update)
	if err := fs.writeMeta(updatedMeta); err != nil {
		return nil, err
	}

	return &updatedMeta, nil
}
