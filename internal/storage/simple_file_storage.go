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

// SimpleFileStorage implements models.ArtifactStorage
type SimpleFileStorage struct {
	baseDir string
}

// NewSimpleFileStorage creates a new storage instance and ensures the base directory exists.
func NewSimpleFileStorage(baseDir string) (*SimpleFileStorage, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}
	return &SimpleFileStorage{baseDir: baseDir}, nil
}

// getPaths returns the directory, artifact path, and metadata path for a given hash.
func (s *SimpleFileStorage) getPaths(hash string) (dir, artifactPath, metaPath string) {
	if len(hash) < 2 {
		dir = s.baseDir
		artifactPath = filepath.Join(s.baseDir, hash)
	} else {
		subDir := hash[:2]
		fileName := hash[2:]
		dir = filepath.Join(s.baseDir, subDir)
		artifactPath = filepath.Join(dir, fileName)
	}
	metaPath = artifactPath + ".meta.json"
	return
}

// Create stores the artifact and optional metadata.
func (s *SimpleFileStorage) Create(ctx context.Context, hash string, r io.Reader, size int64, meta *models.ArtifactMeta) error {
	dir, artifactPath, metaPath := s.getPaths(hash)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create subdirectory: %w", err)
	}

	// 1. Write Artifact Data
	f, err := os.Create(artifactPath)
	if err != nil {
		return fmt.Errorf("failed to create artifact file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("failed to write artifact data: %w", err)
	}

	// 2. Write Metadata (if provided)
	if meta != nil {
		metaFile, err := os.Create(metaPath)
		if err != nil {
			return fmt.Errorf("failed to create metadata file: %w", err)
		}
		defer metaFile.Close()

		if err := json.NewEncoder(metaFile).Encode(meta); err != nil {
			return fmt.Errorf("failed to encode metadata: %w", err)
		}
	}

	return nil
}

// Read retrieves the artifact data using standard library SectionReader.
func (s *SimpleFileStorage) Read(ctx context.Context, req models.ArtifactRange) (io.ReadCloser, models.ArtifactRange, error) {
	_, artifactPath, _ := s.getPaths(req.Hash)

	// Open the file.
	f, err := os.Open(artifactPath)
	if err != nil {
		return nil, models.ArtifactRange{}, err
	}

	// Get file size to handle "read until end" (-1)
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, models.ArtifactRange{}, err
	}
	fileSize := stat.Size()

	offset := req.Range.Offset
	if offset < 0 {
		offset = 0
	}

	length := req.Range.Length
	// If length is -1 or extends past EOF, limit it to available bytes
	if length == -1 || offset+length > fileSize {
		length = fileSize - offset
	}
	if length < 0 {
		length = 0
	}

	actualRange := models.ArtifactRange{
		Hash: req.Hash,
		Range: models.ByteRange{
			Offset: offset,
			Length: length,
		},
	}

	// Use standard io.NewSectionReader.
	sectionReader := io.NewSectionReader(f, offset, length)

	// We wrap it to add the Close() method, which must close the underlying file.
	rc := &closingSectionReader{
		SectionReader: sectionReader,
		closer:        f,
	}

	return rc, actualRange, nil
}

// Update modifies a range of the artifact.
func (s *SimpleFileStorage) Update(ctx context.Context, req models.ArtifactRange, r io.Reader) error {
	_, artifactPath, _ := s.getPaths(req.Hash)

	f, err := os.OpenFile(artifactPath, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Seek(req.Range.Offset, io.SeekStart); err != nil {
		return err
	}

	if req.Range.Length > 0 {
		_, err = io.CopyN(f, r, req.Range.Length)
	} else {
		_, err = io.Copy(f, r)
	}

	return err
}

// Delete removes the artifact and its metadata.
func (s *SimpleFileStorage) Delete(ctx context.Context, hash string) error {
	_, artifactPath, metaPath := s.getPaths(hash)
	if err := os.Remove(artifactPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	_ = os.Remove(metaPath)
	return nil
}

// GetMeta reads the metadata JSON file.
func (s *SimpleFileStorage) GetMeta(ctx context.Context, hash string) (*models.ArtifactMeta, error) {
	_, _, metaPath := s.getPaths(hash)
	f, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var meta models.ArtifactMeta
	if err := json.NewDecoder(f).Decode(&meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// UpdateMeta overwrites the metadata JSON file.
func (s *SimpleFileStorage) UpdateMeta(ctx context.Context, meta models.ArtifactMeta) (*models.ArtifactMeta, error) {
	_, _, metaPath := s.getPaths(meta.Hash)
	f, err := os.Create(metaPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// Exists checks if the artifact data and metadata exist using lightweight stat calls.
// It does NOT read the content of the files.
func (s *SimpleFileStorage) Exists(ctx context.Context, hash string) (bool, bool, error) {
	_, artifactPath, metaPath := s.getPaths(hash)

	// 1. Check if Artifact binary exists
	artifactExists := false
	if _, err := os.Stat(artifactPath); err == nil {
		artifactExists = true
	} else if !os.IsNotExist(err) {
		// Return actual IO errors (permission, etc.)
		return false, false, err
	}

	// 2. Check if Metadata file exists
	metaExists := false
	if _, err := os.Stat(metaPath); err == nil {
		metaExists = true
	} else if !os.IsNotExist(err) {
		return artifactExists, false, err
	}

	return artifactExists, metaExists, nil
}

// Move renames an artifact and its metadata to a new hash location.
func (s *SimpleFileStorage) Move(ctx context.Context, srcHash, destHash string) error {
	srcDir, srcArt, srcMeta := s.getPaths(srcHash)
	destDir, destArt, destMeta := s.getPaths(destHash)

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create dest directory: %w", err)
	}

	// 1. Move Artifact
	if err := os.Rename(srcArt, destArt); err != nil {
		return fmt.Errorf("failed to move artifact from %s to %s: %w", srcArt, destArt, err)
	}

	// 2. Move Metadata (if it exists)
	if err := os.Rename(srcMeta, destMeta); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to move metadata: %w", err)
	}

	// 3. Cleanup source directory if it's empty
	if srcDir != destDir {
		_ = os.Remove(srcDir)
	}

	return nil
}

// --- Helper for Read ---

type closingSectionReader struct {
	*io.SectionReader
	closer io.Closer
}

func (r *closingSectionReader) Close() error {
	return r.closer.Close()
}
