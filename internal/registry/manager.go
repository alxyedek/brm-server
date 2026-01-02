package registry

import (
	"fmt"
	"regexp"
	"sync"

	"brm/internal/registry/docker"
	"brm/pkg/models"
)

var (
	defaultManager *RegistryManager
	managerOnce    sync.Once
)

// RegistryManager manages registry instances and their factory functions
type RegistryManager struct {
	registries map[string]models.Registry
	factories  map[string]func(...interface{}) (models.Registry, error)
	mu         sync.RWMutex
}

// GetManager returns the singleton RegistryManager instance
func GetManager() *RegistryManager {
	managerOnce.Do(func() {
		defaultManager = &RegistryManager{
			registries: make(map[string]models.Registry),
			factories:  make(map[string]func(...interface{}) (models.Registry, error)),
		}
		// Register built-in factories
		defaultManager.init()
	})
	return defaultManager
}

// init registers built-in registry factory functions
func (rm *RegistryManager) init() {
	// Register Docker registry factory
	// Parameters: [storageAlias string, upstream *models.UpstreamRegistry, config *models.ProxyRegistryConfig]
	rm.RegisterFactory("docker.registry", func(params ...interface{}) (models.Registry, error) {
		if len(params) < 2 {
			return nil, fmt.Errorf("docker.registry requires at least storageAlias and upstream parameters")
		}

		storageAlias, ok := params[0].(string)
		if !ok {
			return nil, fmt.Errorf("docker.registry storageAlias must be a string")
		}

		upstream, ok := params[1].(*models.UpstreamRegistry)
		if !ok {
			return nil, fmt.Errorf("docker.registry upstream must be *models.UpstreamRegistry")
		}

		var config *models.ProxyRegistryConfig
		if len(params) >= 3 {
			config, ok = params[2].(*models.ProxyRegistryConfig)
			if !ok {
				return nil, fmt.Errorf("docker.registry config must be *models.ProxyRegistryConfig")
			}
		}

		return docker.NewDockerRegistry(storageAlias, upstream, config)
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

// RegisterFactory registers a registry factory function for the given class name
func (rm *RegistryManager) RegisterFactory(className string, factory func(...interface{}) (models.Registry, error)) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.factories[className] = factory
}

// Create creates a new registry instance with the given class name and alias
// The alias must be a valid DNS name (lowercase). The params are passed to the factory function.
func (rm *RegistryManager) Create(className, alias string, params ...interface{}) (models.Registry, error) {
	// Validate alias is valid DNS name
	if !isValidDNSName(alias) {
		return nil, fmt.Errorf("invalid DNS name for alias: %s", alias)
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Check if alias already exists
	if _, exists := rm.registries[alias]; exists {
		return nil, fmt.Errorf("registry alias already exists: %s", alias)
	}

	// Look up factory
	factory, exists := rm.factories[className]
	if !exists {
		return nil, fmt.Errorf("registry class not found: %s", className)
	}

	// Create registry instance
	registry, err := factory(params...)
	if err != nil {
		return nil, fmt.Errorf("failed to create registry instance: %w", err)
	}

	// Store instance
	rm.registries[alias] = registry

	return registry, nil
}
