package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"brm/pkg/models"
)

func TestStorageManagerCreate(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	manager := GetManager()

	t.Run("create_with_valid_dns_alias", func(t *testing.T) {
		storage, err := manager.Create("std.filestorage", "valid-storage", baseDir)
		if err != nil {
			t.Fatalf("Failed to create storage with valid alias: %v", err)
		}

		// Verify storage works
		hash := "test123"
		testData := []byte("test")
		_, err = storage.Create(ctx, hash, bytes.NewReader(testData), int64(len(testData)), nil)
		if err != nil {
			t.Fatalf("Storage operation failed: %v", err)
		}
	})

	t.Run("create_with_invalid_dns_alias", func(t *testing.T) {
		_, err := manager.Create("std.filestorage", "Invalid-Storage", baseDir)
		if err == nil {
			t.Error("Expected error for invalid DNS alias")
		}
	})

	t.Run("create_with_duplicate_alias", func(t *testing.T) {
		alias := "duplicate-test"
		_, err := manager.Create("std.filestorage", alias, baseDir)
		if err != nil {
			t.Fatalf("First create failed: %v", err)
		}

		_, err = manager.Create("std.filestorage", alias, baseDir)
		if err == nil {
			t.Error("Expected error for duplicate alias")
		}
	})

	t.Run("create_with_nonexistent_class", func(t *testing.T) {
		_, err := manager.Create("nonexistent.class", "test", baseDir)
		if err == nil {
			t.Error("Expected error for nonexistent storage class")
		}
	})
}

func TestStorageManagerGetManager(t *testing.T) {
	// Test that GetManager returns a singleton
	manager1 := GetManager()
	manager2 := GetManager()

	if manager1 != manager2 {
		t.Error("GetManager should return the same instance (singleton)")
	}
}

func TestStorageManagerRegisterFactory(t *testing.T) {
	manager := GetManager()
	baseDir := t.TempDir()

	// Create a test factory that returns a valid storage
	testFactory := func(params ...interface{}) (models.ArtifactStorage, error) {
		if len(params) == 0 {
			return nil, fmt.Errorf("test factory requires baseDir parameter")
		}
		basePath, ok := params[0].(string)
		if !ok {
			return nil, fmt.Errorf("test factory basePath must be a string")
		}
		return NewSimpleFileStorage(basePath)
	}

	manager.RegisterFactory("test.factory", testFactory)

	// Verify factory was registered by trying to create with it
	storage, err := manager.Create("test.factory", "test-alias", baseDir)
	if err != nil {
		t.Fatalf("Failed to create storage with registered factory: %v", err)
	}
	if storage == nil {
		t.Error("Expected non-nil storage instance")
	}
}

func TestStorageManagerConcurrentFileStorage(t *testing.T) {
	manager := GetManager()
	baseDir := t.TempDir()
	lockDir := t.TempDir()
	lockTimeout := 30 * time.Second

	// Create concurrent file storage via manager
	storage, err := manager.Create("concurrent.filestorage", "concurrent-test", baseDir, lockDir, lockTimeout)
	if err != nil {
		t.Fatalf("Failed to create concurrent storage: %v", err)
	}
	if storage == nil {
		t.Fatal("Expected non-nil storage instance")
	}

	// Verify it's actually a ConcurrentArtifactStorage by testing it works
	ctx := context.Background()
	hash := "test123"
	testData := []byte("test data")

	meta, err := storage.Create(ctx, hash, bytes.NewReader(testData), int64(len(testData)), nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if meta == nil {
		t.Fatal("Create returned nil metadata")
	}
	if meta.Hash != hash {
		t.Errorf("Expected hash %s, got %s", hash, meta.Hash)
	}
}
