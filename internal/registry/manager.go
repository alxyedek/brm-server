package registry

import (
	"fmt"
	"net"
	"regexp"
	"sync"

	"brm/internal/registry/docker/private"
	"brm/internal/registry/docker/proxy"
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
	// Parameters: [alias, serviceBinding, storageAlias, upstream, cacheTTL]
	rm.RegisterFactory("docker.registry", func(params ...interface{}) (models.Registry, error) {
		if len(params) < 3 {
			return nil, fmt.Errorf("docker.registry requires at least alias, serviceBinding, storageAlias, and upstream parameters")
		}

		alias, ok := params[0].(string)
		if !ok {
			return nil, fmt.Errorf("docker.registry alias must be a string")
		}

		var serviceBinding net.Addr
		if params[1] != nil {
			serviceBinding, ok = params[1].(net.Addr)
			if !ok {
				return nil, fmt.Errorf("docker.registry serviceBinding must be net.Addr")
			}
		}

		storageAlias, ok := params[2].(string)
		if !ok {
			return nil, fmt.Errorf("docker.registry storageAlias must be a string")
		}

		upstream, ok := params[3].(*models.UpstreamRegistry)
		if !ok {
			return nil, fmt.Errorf("docker.registry upstream must be *models.UpstreamRegistry")
		}

		var cacheTTL int64
		if len(params) >= 5 {
			cacheTTL, ok = params[4].(int64)
			if !ok {
				return nil, fmt.Errorf("docker.registry cacheTTL must be int64")
			}
		}

		return proxy.NewDockerRegistryProxy(alias, storageAlias, upstream, serviceBinding, cacheTTL)
	})

	// Register Docker private registry factory
	// Parameters: [alias, serviceBinding, storageAlias, description]
	rm.RegisterFactory("docker.registry.private", func(params ...interface{}) (models.Registry, error) {
		if len(params) < 2 {
			return nil, fmt.Errorf("docker.registry.private requires at least alias and serviceBinding parameters")
		}

		alias, ok := params[0].(string)
		if !ok {
			return nil, fmt.Errorf("docker.registry.private alias must be a string")
		}

		var serviceBinding net.Addr
		if params[1] != nil {
			serviceBinding, ok = params[1].(net.Addr)
			if !ok {
				return nil, fmt.Errorf("docker.registry.private serviceBinding must be net.Addr")
			}
		}

		storageAlias, ok := params[2].(string)
		if !ok {
			return nil, fmt.Errorf("docker.registry.private storageAlias must be a string")
		}

		var description string
		if len(params) >= 4 {
			description, ok = params[3].(string)
			if !ok {
				return nil, fmt.Errorf("docker.registry.private description must be a string")
			}
		}

		return private.NewDockerRegistryPrivate(alias, storageAlias, serviceBinding, description)
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
// serviceBinding is optional and can be nil.
func (rm *RegistryManager) Create(className, alias string, serviceBinding net.Addr, params ...interface{}) (models.Registry, error) {
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

	// Create registry instance (pass alias and serviceBinding as first parameters)
	registry, err := factory(append([]interface{}{alias, serviceBinding}, params...)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create registry instance: %w", err)
	}

	// Store instance
	rm.registries[alias] = registry

	return registry, nil
}
