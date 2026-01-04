package private

import (
	"fmt"

	"brm/internal/storage"
	"brm/pkg/models"
)

// DockerRegistryPrivate implements a private Docker registry that stores artifacts locally
type DockerRegistryPrivate struct {
	registryType       models.RegistryType
	implementationType string
	storageAlias       string
	config             *models.PrivateRegistryConfig
	service            *DockerRegistryPrivateService
}

// NewDockerRegistryPrivate creates a new private Docker registry instance
func NewDockerRegistryPrivate(
	storageAlias string,
	config *models.PrivateRegistryConfig,
) (*DockerRegistryPrivate, error) {
	if storageAlias == "" {
		return nil, fmt.Errorf("storageAlias cannot be empty")
	}

	// Resolve storage from StorageManager
	storageManager := storage.GetManager()
	storageInstance, err := storageManager.Get(storageAlias)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage by alias %s: %w", storageAlias, err)
	}

	// Create service
	service, err := NewDockerRegistryPrivateService(storageAlias, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create registry service: %w", err)
	}

	// Set storage in service
	service.SetStorage(storageInstance)

	registry := &DockerRegistryPrivate{
		registryType:       models.RegistryTypePrivate,
		implementationType: "docker.registry.private",
		storageAlias:       storageAlias,
		config:             config,
		service:            service,
	}

	return registry, nil
}

// Type returns the registry type
func (d *DockerRegistryPrivate) Type() models.RegistryType {
	return d.registryType
}

// ImplementationType returns the implementation type/class name
func (d *DockerRegistryPrivate) ImplementationType() string {
	return d.implementationType
}

// Service returns the Docker registry service instance
func (d *DockerRegistryPrivate) Service() *DockerRegistryPrivateService {
	return d.service
}
