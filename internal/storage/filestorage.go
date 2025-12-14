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
// with git-like object storage pattern (shasum-based directory structure)
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

// getDataPath returns the full path to the data file for the given shasum.
// Uses git-like storage: {basePath}/{first2chars}/{remaining62chars}
func (fs *FileStorage) getDataPath(shasum string) string {
	if len(shasum) < 2 {
		// Fallback for invalid shasum (shouldn't happen in practice)
		return filepath.Join(fs.basePath, shasum)
	}
	subdir := shasum[:2]
	filename := shasum[2:]
	return filepath.Join(fs.basePath, subdir, filename)
}

// getMetaPath returns the full path to the metadata file for the given shasum.
// Uses git-like storage: {basePath}/{first2chars}/{remaining62chars}.meta
func (fs *FileStorage) getMetaPath(shasum string) string {
	return fs.getDataPath(shasum) + ".meta"
}

// ensureSubdirectory creates the subdirectory for the given shasum if it doesn't exist.
func (fs *FileStorage) ensureSubdirectory(shasum string) error {
	if len(shasum) < 2 {
		return fmt.Errorf("invalid shasum: too short")
	}
	subdir := filepath.Join(fs.basePath, shasum[:2])
	return os.MkdirAll(subdir, 0755)
}

// readMeta reads and deserializes the metadata file for the given shasum.
func (fs *FileStorage) readMeta(shasum string) (*models.ArtifactMeta, error) {
	metaPath := fs.getMetaPath(shasum)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("artifact not found: %s", shasum)
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
	if err := fs.ensureSubdirectory(meta.Shasum); err != nil {
		return err
	}

	metaPath := fs.getMetaPath(meta.Shasum)
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
func (fs *FileStorage) readDataRange(shasum string, start, end int64) ([]byte, models.ByteRange, error) {
	dataPath := fs.getDataPath(shasum)
	file, err := os.Open(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, models.ByteRange{}, fmt.Errorf("artifact data not found: %s", shasum)
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
func (fs *FileStorage) writeDataRange(shasum string, data []byte, offset int64) error {
	if err := fs.ensureSubdirectory(shasum); err != nil {
		return err
	}

	dataPath := fs.getDataPath(shasum)
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

// Create stores a new artifact. The Artifact's Data field may contain full data or a byte range.
// Returns the created ArtifactMeta or an error.
func (fs *FileStorage) Create(artifact models.Artifact) (*models.ArtifactMeta, error) {
	// Ensure subdirectory exists
	if err := fs.ensureSubdirectory(artifact.Meta.Shasum); err != nil {
		return nil, err
	}

	// Write metadata
	if err := fs.writeMeta(artifact.Meta); err != nil {
		return nil, err
	}

	// Write data file (write at offset 0, which creates/overwrites the file)
	if err := fs.writeDataRange(artifact.Meta.Shasum, artifact.Data, 0); err != nil {
		return nil, err
	}

	// Return a copy of the metadata
	result := artifact.Meta
	return &result, nil
}

// Read retrieves an artifact based on the request. The Range field in ArtifactRequest
// specifies which byte range to retrieve. Returns ArtifactResponse with the actual range returned.
func (fs *FileStorage) Read(request models.ArtifactRequest) (*models.ArtifactResponse, error) {
	// Read metadata
	meta, err := fs.readMeta(request.Meta.Shasum)
	if err != nil {
		return nil, err
	}

	// Read data range
	data, actualRange, err := fs.readDataRange(request.Meta.Shasum, request.Range.Start, request.Range.End)
	if err != nil {
		return nil, err
	}

	// Create response
	response := &models.ArtifactResponse{
		Meta: *meta,
		Artifact: models.Artifact{
			Meta: *meta,
			Data: data,
		},
		Range: actualRange,
	}

	return response, nil
}

// Update modifies an existing artifact. The Artifact's Data field may contain full data
// or a byte range for partial updates. Returns the updated ArtifactMeta or an error.
func (fs *FileStorage) Update(artifact models.Artifact) (*models.ArtifactMeta, error) {
	// Verify artifact exists
	existingMeta, err := fs.readMeta(artifact.Meta.Shasum)
	if err != nil {
		return nil, err
	}

	// Determine if this is a full update or partial update
	// If Data length matches the existing length, treat as full update (overwrite from start)
	// Otherwise, write at offset 0 (which will overwrite from the beginning)
	// For true partial updates with offset, the caller would need to provide offset info
	// For now, we'll write the data starting at offset 0
	offset := int64(0)

	// If the data length is less than the existing length, we might want to preserve the rest
	// But per plan: "Direct write for partial updates (no merging with existing data)"
	// So we'll just write the data at the specified offset
	if err := fs.writeDataRange(artifact.Meta.Shasum, artifact.Data, offset); err != nil {
		return nil, err
	}

	// Update metadata if provided
	// Merge with existing metadata, keeping existing values if new ones are empty/zero
	updatedMeta := *existingMeta
	if artifact.Meta.Name != "" {
		updatedMeta.Name = artifact.Meta.Name
	}
	if artifact.Meta.CreatedTimestamp != 0 {
		updatedMeta.CreatedTimestamp = artifact.Meta.CreatedTimestamp
	}
	if artifact.Meta.Repo != "" {
		updatedMeta.Repo = artifact.Meta.Repo
	}
	// Update length based on actual data written
	// If we wrote at offset 0, the new length is max(offset + len(data), existing length)
	// But for simplicity, if offset is 0 and we have data, update length to data length
	if offset == 0 && len(artifact.Data) > 0 {
		updatedMeta.Length = int64(len(artifact.Data))
	}

	// Write updated metadata
	if err := fs.writeMeta(updatedMeta); err != nil {
		return nil, err
	}

	return &updatedMeta, nil
}

// Delete removes an artifact identified by the shasum in ArtifactMeta.
// Returns an error if the artifact doesn't exist or deletion fails.
func (fs *FileStorage) Delete(meta models.ArtifactMeta) error {
	dataPath := fs.getDataPath(meta.Shasum)
	metaPath := fs.getMetaPath(meta.Shasum)

	// Delete data file
	if err := os.Remove(dataPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete data file: %w", err)
	}

	// Delete metadata file
	if err := os.Remove(metaPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete metadata file: %w", err)
	}

	// Optionally remove subdirectory if empty (best effort, ignore errors)
	if len(meta.Shasum) >= 2 {
		subdir := filepath.Join(fs.basePath, meta.Shasum[:2])
		os.Remove(subdir) // Ignore error - directory might not be empty or might not exist
	}

	return nil
}

// GetMeta retrieves the metadata for an artifact identified by the shasum in ArtifactMeta.
// Returns the ArtifactMeta or an error if the artifact doesn't exist.
func (fs *FileStorage) GetMeta(meta models.ArtifactMeta) (*models.ArtifactMeta, error) {
	return fs.readMeta(meta.Shasum)
}

// UpdateMeta updates the metadata for an artifact identified by the shasum in ArtifactMeta.
// Returns the updated ArtifactMeta or an error if the artifact doesn't exist or update fails.
func (fs *FileStorage) UpdateMeta(meta models.ArtifactMeta) (*models.ArtifactMeta, error) {
	// Verify artifact exists
	existingMeta, err := fs.readMeta(meta.Shasum)
	if err != nil {
		return nil, err
	}

	// Merge with existing metadata, keeping existing values if new ones are empty/zero
	updatedMeta := *existingMeta
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
	// Shasum should not be updated, keep existing

	// Write updated metadata
	if err := fs.writeMeta(updatedMeta); err != nil {
		return nil, err
	}

	return &updatedMeta, nil
}
