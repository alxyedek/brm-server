package storage

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"brm/pkg/models"
)

// Helper function to create test data
func createTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

// Helper function to create test metadata
func createTestMeta(hash, name, repo string, length int64) *models.ArtifactMeta {
	return &models.ArtifactMeta{
		Name:             name,
		CreatedTimestamp: time.Now().Unix(),
		Hash:             hash,
		Repo:             repo,
		Length:           length,
	}
}

// Helper function to read all data from a ReadCloser
func readAllData(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}
	return data
}

// Helper function to verify data matches expected
func verifyData(t *testing.T, actual, expected []byte) {
	t.Helper()
	if !bytes.Equal(actual, expected) {
		t.Errorf("Data mismatch: expected %d bytes, got %d bytes", len(expected), len(actual))
		if len(actual) == len(expected) {
			for i := range actual {
				if actual[i] != expected[i] {
					t.Errorf("First mismatch at byte %d: expected %d, got %d", i, expected[i], actual[i])
					break
				}
			}
		}
	}
}

// testArtifactStorageCreate tests the Create method with various scenarios
func testArtifactStorageCreate(t *testing.T, storage models.ArtifactStorage) {
	ctx := context.Background()

	t.Run("create_with_data_only", func(t *testing.T) {
		hash := "abc123def456"
		testData := []byte("Hello, World!")
		r := bytes.NewReader(testData)

		err := storage.Create(ctx, hash, r, int64(len(testData)), nil)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		// Verify data can be read back
		req := models.ArtifactRange{
			Hash: hash,
			Range: models.ByteRange{
				Offset: 0,
				Length: -1,
			},
		}
		rc, actual, err := storage.Read(ctx, req)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		defer rc.Close()

		readData := readAllData(t, rc)
		verifyData(t, readData, testData)

		if actual.Range.Length != int64(len(testData)) {
			t.Errorf("Expected length %d, got %d", len(testData), actual.Range.Length)
		}
	})

	t.Run("create_with_data_and_metadata", func(t *testing.T) {
		hash := "xyz789uvw012"
		testData := []byte("Test artifact data")
		meta := createTestMeta(hash, "test-artifact", "docker:hub.docker.com", int64(len(testData)))
		r := bytes.NewReader(testData)

		err := storage.Create(ctx, hash, r, int64(len(testData)), meta)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		// Verify metadata
		retrievedMeta, err := storage.GetMeta(ctx, hash)
		if err != nil {
			t.Fatalf("GetMeta failed: %v", err)
		}

		if retrievedMeta.Name != meta.Name {
			t.Errorf("Expected name %s, got %s", meta.Name, retrievedMeta.Name)
		}
		if retrievedMeta.Hash != hash {
			t.Errorf("Expected hash %s, got %s", hash, retrievedMeta.Hash)
		}
		if retrievedMeta.Repo != meta.Repo {
			t.Errorf("Expected repo %s, got %s", meta.Repo, retrievedMeta.Repo)
		}
		if retrievedMeta.Length != int64(len(testData)) {
			t.Errorf("Expected length %d, got %d", len(testData), retrievedMeta.Length)
		}
	})

	t.Run("create_with_empty_data", func(t *testing.T) {
		hash := "empty123"
		testData := []byte{}
		r := bytes.NewReader(testData)

		err := storage.Create(ctx, hash, r, 0, nil)
		if err != nil {
			t.Fatalf("Create failed with empty data: %v", err)
		}

		// Verify empty data can be read
		req := models.ArtifactRange{
			Hash: hash,
			Range: models.ByteRange{
				Offset: 0,
				Length: -1,
			},
		}
		rc, actual, err := storage.Read(ctx, req)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		defer rc.Close()

		readData := readAllData(t, rc)
		if len(readData) != 0 {
			t.Errorf("Expected empty data, got %d bytes", len(readData))
		}
		if actual.Range.Length != 0 {
			t.Errorf("Expected length 0, got %d", actual.Range.Length)
		}
	})

	t.Run("create_with_large_data", func(t *testing.T) {
		hash := "large456"
		testData := createTestData(1024 * 1024) // 1MB
		r := bytes.NewReader(testData)

		err := storage.Create(ctx, hash, r, int64(len(testData)), nil)
		if err != nil {
			t.Fatalf("Create failed with large data: %v", err)
		}

		// Verify large data can be read back
		req := models.ArtifactRange{
			Hash: hash,
			Range: models.ByteRange{
				Offset: 0,
				Length: -1,
			},
		}
		rc, actual, err := storage.Read(ctx, req)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		defer rc.Close()

		readData := readAllData(t, rc)
		if len(readData) != len(testData) {
			t.Errorf("Expected %d bytes, got %d bytes", len(testData), len(readData))
		}
		if actual.Range.Length != int64(len(testData)) {
			t.Errorf("Expected length %d, got %d", len(testData), actual.Range.Length)
		}
	})
}

// testArtifactStorageRead tests the Read method with different range scenarios
func testArtifactStorageRead(t *testing.T, storage models.ArtifactStorage) {
	ctx := context.Background()

	// Setup: create test artifact
	hash := "readtest123"
	testData := []byte("0123456789ABCDEF")
	err := storage.Create(ctx, hash, bytes.NewReader(testData), int64(len(testData)), nil)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	t.Run("read_full_artifact", func(t *testing.T) {
		req := models.ArtifactRange{
			Hash: hash,
			Range: models.ByteRange{
				Offset: 0,
				Length: -1,
			},
		}
		rc, actual, err := storage.Read(ctx, req)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		defer rc.Close()

		readData := readAllData(t, rc)
		verifyData(t, readData, testData)

		if actual.Range.Offset != 0 {
			t.Errorf("Expected offset 0, got %d", actual.Range.Offset)
		}
		if actual.Range.Length != int64(len(testData)) {
			t.Errorf("Expected length %d, got %d", len(testData), actual.Range.Length)
		}
	})

	t.Run("read_partial_from_start", func(t *testing.T) {
		req := models.ArtifactRange{
			Hash: hash,
			Range: models.ByteRange{
				Offset: 0,
				Length: 5,
			},
		}
		rc, actual, err := storage.Read(ctx, req)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		defer rc.Close()

		readData := readAllData(t, rc)
		expected := testData[0:5]
		verifyData(t, readData, expected)

		if actual.Range.Offset != 0 {
			t.Errorf("Expected offset 0, got %d", actual.Range.Offset)
		}
		if actual.Range.Length != 5 {
			t.Errorf("Expected length 5, got %d", actual.Range.Length)
		}
	})

	t.Run("read_partial_from_middle", func(t *testing.T) {
		req := models.ArtifactRange{
			Hash: hash,
			Range: models.ByteRange{
				Offset: 5,
				Length: 5,
			},
		}
		rc, actual, err := storage.Read(ctx, req)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		defer rc.Close()

		readData := readAllData(t, rc)
		expected := testData[5:10]
		verifyData(t, readData, expected)

		if actual.Range.Offset != 5 {
			t.Errorf("Expected offset 5, got %d", actual.Range.Offset)
		}
		if actual.Range.Length != 5 {
			t.Errorf("Expected length 5, got %d", actual.Range.Length)
		}
	})

	t.Run("read_partial_from_end", func(t *testing.T) {
		req := models.ArtifactRange{
			Hash: hash,
			Range: models.ByteRange{
				Offset: 12,
				Length: -1,
			},
		}
		rc, actual, err := storage.Read(ctx, req)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		defer rc.Close()

		readData := readAllData(t, rc)
		expected := testData[12:]
		verifyData(t, readData, expected)

		if actual.Range.Offset != 12 {
			t.Errorf("Expected offset 12, got %d", actual.Range.Offset)
		}
		if actual.Range.Length != int64(len(expected)) {
			t.Errorf("Expected length %d, got %d", len(expected), actual.Range.Length)
		}
	})

	t.Run("read_range_exceeds_file_size", func(t *testing.T) {
		req := models.ArtifactRange{
			Hash: hash,
			Range: models.ByteRange{
				Offset: 0,
				Length: 1000, // Much larger than file size
			},
		}
		rc, actual, err := storage.Read(ctx, req)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		defer rc.Close()

		readData := readAllData(t, rc)
		verifyData(t, readData, testData) // Should return only available data

		if actual.Range.Length != int64(len(testData)) {
			t.Errorf("Expected length %d (file size), got %d", len(testData), actual.Range.Length)
		}
	})

	t.Run("read_offset_beyond_file_size", func(t *testing.T) {
		req := models.ArtifactRange{
			Hash: hash,
			Range: models.ByteRange{
				Offset: 1000,
				Length: 10,
			},
		}
		rc, actual, err := storage.Read(ctx, req)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		defer rc.Close()

		readData := readAllData(t, rc)
		if len(readData) != 0 {
			t.Errorf("Expected empty data, got %d bytes", len(readData))
		}
		if actual.Range.Length != 0 {
			t.Errorf("Expected length 0, got %d", actual.Range.Length)
		}
	})

	t.Run("read_nonexistent_artifact", func(t *testing.T) {
		req := models.ArtifactRange{
			Hash: "nonexistent",
			Range: models.ByteRange{
				Offset: 0,
				Length: -1,
			},
		}
		_, _, err := storage.Read(ctx, req)
		if err == nil {
			t.Error("Expected error for nonexistent artifact")
		}
	})
}

// testArtifactStorageUpdate tests the Update method with various range updates
func testArtifactStorageUpdate(t *testing.T, storage models.ArtifactStorage) {
	ctx := context.Background()

	t.Run("update_existing_range", func(t *testing.T) {
		hash := "updatetest1"
		initialData := []byte("0123456789")
		err := storage.Create(ctx, hash, bytes.NewReader(initialData), int64(len(initialData)), nil)
		if err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		// Update bytes 2-5
		updateData := []byte("ABC")
		req := models.ArtifactRange{
			Hash: hash,
			Range: models.ByteRange{
				Offset: 2,
				Length: 3,
			},
		}
		err = storage.Update(ctx, req, bytes.NewReader(updateData))
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		// Verify update
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
		expected := []byte("01ABC56789")
		verifyData(t, readData, expected)
	})

	t.Run("update_with_append", func(t *testing.T) {
		hash := "updatetest2"
		initialData := []byte("Hello")
		err := storage.Create(ctx, hash, bytes.NewReader(initialData), int64(len(initialData)), nil)
		if err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		// Append data
		appendData := []byte(", World!")
		req := models.ArtifactRange{
			Hash: hash,
			Range: models.ByteRange{
				Offset: int64(len(initialData)),
				Length: -1,
			},
		}
		err = storage.Update(ctx, req, bytes.NewReader(appendData))
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		// Verify append
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
		expected := []byte("Hello, World!")
		verifyData(t, readData, expected)
	})

	t.Run("update_with_specific_length", func(t *testing.T) {
		hash := "updatetest3"
		initialData := []byte("0123456789")
		err := storage.Create(ctx, hash, bytes.NewReader(initialData), int64(len(initialData)), nil)
		if err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		// Update with specific length (shorter than source)
		updateData := []byte("ABCDEFGHIJ")
		req := models.ArtifactRange{
			Hash: hash,
			Range: models.ByteRange{
				Offset: 2,
				Length: 4, // Only write 4 bytes
			},
		}
		err = storage.Update(ctx, req, bytes.NewReader(updateData))
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		// Verify update
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
		expected := []byte("01ABCD6789")
		verifyData(t, readData, expected)
	})
}

// testArtifactStorageDelete tests the Delete method
func testArtifactStorageDelete(t *testing.T, storage models.ArtifactStorage) {
	ctx := context.Background()

	t.Run("delete_existing_artifact", func(t *testing.T) {
		hash := "deletetest1"
		testData := []byte("test data")
		err := storage.Create(ctx, hash, bytes.NewReader(testData), int64(len(testData)), nil)
		if err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		err = storage.Delete(ctx, hash)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify deletion
		req := models.ArtifactRange{
			Hash: hash,
			Range: models.ByteRange{
				Offset: 0,
				Length: -1,
			},
		}
		_, _, err = storage.Read(ctx, req)
		if err == nil {
			t.Error("Expected error when reading deleted artifact")
		}
	})

	t.Run("delete_nonexistent_artifact", func(t *testing.T) {
		err := storage.Delete(ctx, "nonexistent")
		if err != nil {
			t.Errorf("Delete should not error for nonexistent artifact: %v", err)
		}
	})

	t.Run("delete_with_metadata", func(t *testing.T) {
		hash := "deletetest2"
		testData := []byte("test data")
		meta := createTestMeta(hash, "test", "docker:test", int64(len(testData)))
		err := storage.Create(ctx, hash, bytes.NewReader(testData), int64(len(testData)), meta)
		if err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		err = storage.Delete(ctx, hash)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify metadata is also deleted
		_, err = storage.GetMeta(ctx, hash)
		if err == nil {
			t.Error("Expected error when reading deleted metadata")
		}
	})
}

// testArtifactStorageGetMeta tests the GetMeta method
func testArtifactStorageGetMeta(t *testing.T, storage models.ArtifactStorage) {
	ctx := context.Background()

	t.Run("get_existing_metadata", func(t *testing.T) {
		hash := "metatest1"
		testData := []byte("test")
		meta := createTestMeta(hash, "test-artifact", "docker:hub.docker.com", int64(len(testData)))
		err := storage.Create(ctx, hash, bytes.NewReader(testData), int64(len(testData)), meta)
		if err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		retrievedMeta, err := storage.GetMeta(ctx, hash)
		if err != nil {
			t.Fatalf("GetMeta failed: %v", err)
		}

		if retrievedMeta.Name != meta.Name {
			t.Errorf("Expected name %s, got %s", meta.Name, retrievedMeta.Name)
		}
		if retrievedMeta.Hash != hash {
			t.Errorf("Expected hash %s, got %s", hash, retrievedMeta.Hash)
		}
		if retrievedMeta.Repo != meta.Repo {
			t.Errorf("Expected repo %s, got %s", meta.Repo, retrievedMeta.Repo)
		}
		if retrievedMeta.Length != meta.Length {
			t.Errorf("Expected length %d, got %d", meta.Length, retrievedMeta.Length)
		}
	})

	t.Run("get_nonexistent_metadata", func(t *testing.T) {
		_, err := storage.GetMeta(ctx, "nonexistent")
		if err == nil {
			t.Error("Expected error for nonexistent metadata")
		}
	})
}

// testArtifactStorageUpdateMeta tests the UpdateMeta method
func testArtifactStorageUpdateMeta(t *testing.T, storage models.ArtifactStorage) {
	ctx := context.Background()

	t.Run("update_existing_metadata", func(t *testing.T) {
		hash := "metatest2"
		testData := []byte("test")
		initialMeta := createTestMeta(hash, "initial", "docker:test", int64(len(testData)))
		err := storage.Create(ctx, hash, bytes.NewReader(testData), int64(len(testData)), initialMeta)
		if err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		updatedMeta := models.ArtifactMeta{
			Name:             "updated",
			CreatedTimestamp: time.Now().Unix(),
			Hash:             hash,
			Repo:             "docker:updated",
			Length:           int64(len(testData)),
		}

		result, err := storage.UpdateMeta(ctx, updatedMeta)
		if err != nil {
			t.Fatalf("UpdateMeta failed: %v", err)
		}

		if result.Name != updatedMeta.Name {
			t.Errorf("Expected name %s, got %s", updatedMeta.Name, result.Name)
		}
		if result.Repo != updatedMeta.Repo {
			t.Errorf("Expected repo %s, got %s", updatedMeta.Repo, result.Repo)
		}
	})

	t.Run("create_new_metadata", func(t *testing.T) {
		hash := "metatest3"
		testData := []byte("test")
		// Create artifact without metadata
		err := storage.Create(ctx, hash, bytes.NewReader(testData), int64(len(testData)), nil)
		if err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		newMeta := models.ArtifactMeta{
			Name:             "new",
			CreatedTimestamp: time.Now().Unix(),
			Hash:             hash,
			Repo:             "docker:new",
			Length:           int64(len(testData)),
		}

		result, err := storage.UpdateMeta(ctx, newMeta)
		if err != nil {
			t.Fatalf("UpdateMeta failed: %v", err)
		}

		if result.Name != newMeta.Name {
			t.Errorf("Expected name %s, got %s", newMeta.Name, result.Name)
		}
	})
}

// testArtifactStorageFullWorkflow tests a complete workflow combining multiple operations
func testArtifactStorageFullWorkflow(t *testing.T, storage models.ArtifactStorage) {
	ctx := context.Background()

	hash := "workflow123"
	testData := []byte("Initial data")
	meta := createTestMeta(hash, "workflow-artifact", "docker:test", int64(len(testData)))

	// 1. Create artifact with metadata
	err := storage.Create(ctx, hash, bytes.NewReader(testData), int64(len(testData)), meta)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// 2. Read full artifact
	req := models.ArtifactRange{
		Hash: hash,
		Range: models.ByteRange{
			Offset: 0,
			Length: -1,
		},
	}
	rc, _, err := storage.Read(ctx, req)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	readData := readAllData(t, rc)
	verifyData(t, readData, testData)

	// 3. Read partial range
	partialReq := models.ArtifactRange{
		Hash: hash,
		Range: models.ByteRange{
			Offset: 0,
			Length: 7,
		},
	}
	rc, _, err = storage.Read(ctx, partialReq)
	if err != nil {
		t.Fatalf("Partial read failed: %v", err)
	}
	partialData := readAllData(t, rc)
	expectedPartial := testData[0:7]
	verifyData(t, partialData, expectedPartial)

	// 4. Update artifact range (overwrite first part)
	updateData := []byte("Updated")
	updateReq := models.ArtifactRange{
		Hash: hash,
		Range: models.ByteRange{
			Offset: 0,
			Length: int64(len(updateData)),
		},
	}
	err = storage.Update(ctx, updateReq, bytes.NewReader(updateData))
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// 5. Update metadata
	updatedMeta := models.ArtifactMeta{
		Name:             "updated-workflow",
		CreatedTimestamp: time.Now().Unix(),
		Hash:             hash,
		Repo:             "docker:updated",
		Length:           int64(len(testData)), // Original length (Update doesn't truncate)
	}
	_, err = storage.UpdateMeta(ctx, updatedMeta)
	if err != nil {
		t.Fatalf("UpdateMeta failed: %v", err)
	}

	// 6. Read updated artifact
	rc, _, err = storage.Read(ctx, req)
	if err != nil {
		t.Fatalf("Read after update failed: %v", err)
	}
	updatedReadData := readAllData(t, rc)
	// Update overwrites the first part, rest remains
	expectedUpdated := make([]byte, len(testData))
	copy(expectedUpdated, updateData)
	copy(expectedUpdated[len(updateData):], testData[len(updateData):])
	verifyData(t, updatedReadData, expectedUpdated)

	// 7. Verify updated metadata
	retrievedMeta, err := storage.GetMeta(ctx, hash)
	if err != nil {
		t.Fatalf("GetMeta failed: %v", err)
	}
	if retrievedMeta.Name != updatedMeta.Name {
		t.Errorf("Expected name %s, got %s", updatedMeta.Name, retrievedMeta.Name)
	}

	// 8. Delete artifact
	err = storage.Delete(ctx, hash)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 9. Verify deletion
	_, _, err = storage.Read(ctx, req)
	if err == nil {
		t.Error("Expected error when reading deleted artifact")
	}
}
