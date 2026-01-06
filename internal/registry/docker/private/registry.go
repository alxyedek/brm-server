package private

import (
	"fmt"
	"net"

	"brm/internal/storage"
	"brm/pkg/models"
)

// DockerRegistryPrivate implements a private Docker registry that stores artifacts locally
type DockerRegistryPrivate struct {
	models.BaseRegistry
	registryType       models.RegistryType
	implementationType string
	storageAlias       string
	serviceBinding     net.Addr
	description        string
	service            *DockerRegistryPrivateService
}

// NewDockerRegistryPrivate creates a new private Docker registry instance
func NewDockerRegistryPrivate(
	alias string,
	storageAlias string,
	serviceBinding net.Addr,
	description string,
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
	service, err := NewDockerRegistryPrivateService(storageAlias, description)
	if err != nil {
		return nil, fmt.Errorf("failed to create registry service: %w", err)
	}

	// Set storage in service
	service.SetStorage(storageInstance)

	registry := &DockerRegistryPrivate{
		registryType:       models.RegistryTypePrivate,
		implementationType: "docker.registry.private",
		storageAlias:       storageAlias,
		serviceBinding:     serviceBinding,
		description:        description,
		service:            service,
	}
	registry.BaseRegistry.SetAlias(alias)

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
