package docker

import (
	"fmt"

	"brm/internal/storage"
	"brm/pkg/models"
)

// DockerRegistry implements a Docker registry proxy that caches artifacts from upstream registries
type DockerRegistry struct {
	registryType       models.RegistryType
	implementationType string
	storageAlias       string
	upstream           *models.UpstreamRegistry
	config             *models.ProxyRegistryConfig
	service            *DockerRegistryService
}

// NewDockerRegistry creates a new Docker registry proxy instance
func NewDockerRegistry(
	storageAlias string,
	upstream *models.UpstreamRegistry,
	config *models.ProxyRegistryConfig,
) (*DockerRegistry, error) {
	if storageAlias == "" {
		return nil, fmt.Errorf("storageAlias cannot be empty")
	}
	if upstream == nil {
		return nil, fmt.Errorf("upstream configuration cannot be nil")
	}
	if upstream.URL == "" {
		return nil, fmt.Errorf("upstream URL cannot be empty")
	}

	// Resolve storage from StorageManager
	storageManager := storage.GetManager()
	storageInstance, err := storageManager.Get(storageAlias)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage by alias %s: %w", storageAlias, err)
	}

	// Create service
	service, err := NewDockerRegistryService(storageAlias, upstream, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create registry service: %w", err)
	}

	// Set storage in service
	service.SetStorage(storageInstance)

	registry := &DockerRegistry{
		registryType:       models.RegistryTypeProxy,
		implementationType: "docker.registry",
		storageAlias:       storageAlias,
		upstream:           upstream,
		config:             config,
		service:            service,
	}

	return registry, nil
}

// Type returns the registry type
func (d *DockerRegistry) Type() models.RegistryType {
	return d.registryType
}

// ImplementationType returns the implementation type/class name
func (d *DockerRegistry) ImplementationType() string {
	return d.implementationType
}

// Service returns the Docker registry service instance
func (d *DockerRegistry) Service() *DockerRegistryService {
	return d.service
}
