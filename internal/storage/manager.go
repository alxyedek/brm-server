package storage

import (
	"fmt"
	"regexp"
	"sync"

	"brm/pkg/models"
)

var (
	defaultManager *StorageManager
	managerOnce    sync.Once
)

// StorageManager manages storage instances and their factory functions
type StorageManager struct {
	storages  map[string]models.ArtifactStorage
	factories map[string]func(...interface{}) (models.ArtifactStorage, error)
	mu        sync.RWMutex
}

// GetManager returns the singleton StorageManager instance
func GetManager() *StorageManager {
	managerOnce.Do(func() {
		defaultManager = &StorageManager{
			storages:  make(map[string]models.ArtifactStorage),
			factories: make(map[string]func(...interface{}) (models.ArtifactStorage, error)),
		}
		// Register built-in factories
		defaultManager.init()
	})
	return defaultManager
}

// init registers built-in storage factory functions
func (sm *StorageManager) init() {
	// Register SimpleFileStorage factory
	sm.RegisterFactory("std.filestorage", func(params ...interface{}) (models.ArtifactStorage, error) {
		if len(params) == 0 {
			return nil, fmt.Errorf("filestorage requires basePath parameter")
		}
		basePath, ok := params[0].(string)
		if !ok {
			return nil, fmt.Errorf("filestorage basePath must be a string")
		}
		return NewSimpleFileStorage(basePath)
	})
}

// isValidDNSName validates that a string is a valid DNS name
// Rules: lowercase only, alphanumeric + hyphens + dots, labels 1-63 chars,
// max 253 total, no leading/trailing hyphens, no consecutive dots
func isValidDNSName(name string) bool {
	// Check total length (max 253 characters)
	if len(name) > 253 {
		return false
	}

	// Check if empty
	if len(name) == 0 {
		return false
	}

	// Regex pattern for valid DNS name
	// ^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$
	// This ensures:
	// - Each label starts and ends with alphanumeric
	// - Labels can contain hyphens in the middle
	// - Labels are 1-63 characters
	// - Multiple labels separated by dots
	dnsPattern := regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$`)
	return dnsPattern.MatchString(name)
}

// RegisterFactory registers a storage factory function for the given class name
func (sm *StorageManager) RegisterFactory(className string, factory func(...interface{}) (models.ArtifactStorage, error)) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.factories[className] = factory
}

// Create creates a new storage instance with the given class name and alias
// The alias must be a valid DNS name (lowercase). The params are passed to the factory function.
func (sm *StorageManager) Create(className, alias string, params ...interface{}) (models.ArtifactStorage, error) {
	// Validate alias is valid DNS name
	if !isValidDNSName(alias) {
		return nil, fmt.Errorf("invalid DNS name for alias: %s", alias)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if alias already exists
	if _, exists := sm.storages[alias]; exists {
		return nil, fmt.Errorf("storage alias already exists: %s", alias)
	}

	// Look up factory
	factory, exists := sm.factories[className]
	if !exists {
		return nil, fmt.Errorf("storage class not found: %s", className)
	}

	// Create storage instance
	storage, err := factory(params...)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage instance: %w", err)
	}

	// Store instance
	sm.storages[alias] = storage

	return storage, nil
}
