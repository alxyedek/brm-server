package storage

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"brm/pkg/models"
)

// TestSimpleFileStorageFileSystemStructure tests the git-like directory structure
func TestSimpleFileStorageFileSystemStructure(t *testing.T) {
	baseDir := t.TempDir()
	storage, err := NewSimpleFileStorage(baseDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	ctx := context.Background()
	hash := "abc123def456" // First two chars: "ab"
	testData := []byte("test data")

	err = storage.Create(ctx, hash, bytes.NewReader(testData), int64(len(testData)), nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Verify directory structure
	expectedSubdir := filepath.Join(baseDir, "ab")
	if _, err := os.Stat(expectedSubdir); err != nil {
		t.Errorf("Expected subdirectory %s to exist", expectedSubdir)
	}

	expectedFile := filepath.Join(expectedSubdir, "c123def456")
	if _, err := os.Stat(expectedFile); err != nil {
		t.Errorf("Expected file %s to exist", expectedFile)
	}
}

// TestSimpleFileStorageZeroPadding tests zero padding behavior in Update
func TestSimpleFileStorageZeroPadding(t *testing.T) {
	baseDir := t.TempDir()
	storage, err := NewSimpleFileStorage(baseDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	ctx := context.Background()
	hash := "zeropad123"
	initialData := []byte("Hello")
	err = storage.Create(ctx, hash, bytes.NewReader(initialData), int64(len(initialData)), nil)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Update at offset beyond current size (should pad with zeros)
	updateData := []byte("World")
	req := models.ArtifactRange{
		Hash: hash,
		Range: models.ByteRange{
			Offset: 10, // Beyond current size of 5
			Length: int64(len(updateData)),
		},
	}
	err = storage.Update(ctx, req, bytes.NewReader(updateData))
	if err != nil {
		t.Fatalf("Update with padding failed: %v", err)
	}

	// Verify the result
	readReq := models.ArtifactRange{
		Hash: hash,
		Range: models.ByteRange{
			Offset: 0,
			Length: -1,
		},
	}
	rc, _, err := storage.Read(ctx, readReq)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	defer rc.Close()

	readData := readAllData(t, rc)
	expectedLength := 10 + len(updateData)
	if len(readData) != expectedLength {
		t.Errorf("Expected length %d, got %d", expectedLength, len(readData))
	}

	// Verify padding (bytes 5-9 should be zeros)
	for i := 5; i < 10; i++ {
		if readData[i] != 0 {
			t.Errorf("Expected zero padding at offset %d, got %d", i, readData[i])
		}
	}

	// Verify update data is at correct position
	expectedUpdate := readData[10:]
	verifyData(t, expectedUpdate, updateData)
}

// TestSimpleFileStorageMetadataFileExtension tests metadata file naming
func TestSimpleFileStorageMetadataFileExtension(t *testing.T) {
	baseDir := t.TempDir()
	storage, err := NewSimpleFileStorage(baseDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	ctx := context.Background()
	hash := "metafile123"
	testData := []byte("test")
	meta := &models.ArtifactMeta{
		Name:             "test",
		CreatedTimestamp: 1234567890,
		Hash:             hash,
		Repo:             "docker:test",
		Length:           int64(len(testData)),
	}

	err = storage.Create(ctx, hash, bytes.NewReader(testData), int64(len(testData)), meta)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Verify metadata file exists with .meta.json extension
	_, _, metaPath := storage.getPaths(hash)
	if _, err := os.Stat(metaPath); err != nil {
		t.Errorf("Expected metadata file %s to exist", metaPath)
	}
}
