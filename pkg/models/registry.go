package models

// RegistryType represents the type of registry.
type RegistryType string

const (
	// RegistryTypePrivate represents a private registry that stores artifacts locally.
	RegistryTypePrivate RegistryType = "private"

	// RegistryTypeProxy represents a proxy registry that caches artifacts from upstream registries.
	RegistryTypeProxy RegistryType = "proxy"

	// RegistryTypeCompound represents a compound registry that combines private storage with proxy registries.
	RegistryTypeCompound RegistryType = "compound"
)

// Registry is the base interface for all registry types.
type Registry interface {
	// Type returns the registry type (private, proxy, or compound).
	Type() RegistryType

	// ImplementationType returns the implementation type/class name of the registry (e.g., "docker.registry", "raw.registry").
	// This is used by managers to identify the registry implementation type.
	ImplementationType() string
}

// UpstreamRegistry represents the configuration for an upstream registry used by proxy registries.
type UpstreamRegistry struct {
	// URL is the base URL of the upstream registry (e.g., "https://registry-1.docker.io").
	URL string `json:"url"`

	// Username is the optional authentication username for accessing the upstream registry.
	Username string `json:"username,omitempty"`

	// Password is the optional authentication password for accessing the upstream registry.
	// Note: In production, consider using secure credential storage instead of plain text.
	Password string `json:"password,omitempty"`

	// TTL is the cache time-to-live in seconds. After this period, cached artifacts may be refreshed.
	// If 0, uses default TTL (typically 168 hours / 604800 seconds).
	TTL int64 `json:"ttl,omitempty"`
}

// PrivateRegistryConfig holds configuration for a private registry.
type PrivateRegistryConfig struct {
	// URL is the registry URL/endpoint (e.g., "https://my-registry.example.com").
	URL string `json:"url,omitempty"`

	// Description is an optional human-readable description of the registry.
	Description string `json:"description,omitempty"`
}

// PrivateRegistry represents a private registry that stores artifacts locally.
// It implements the Registry interface.
type PrivateRegistry struct {
	// registryType is always RegistryTypePrivate for private registries.
	registryType RegistryType `json:"type"`

	// implementationType is the implementation class name (e.g., "docker.registry", "raw.registry").
	implementationType string `json:"implementationType"`

	// StorageAlias is the alias/name of the storage backend registered in StorageManager.
	// The actual ArtifactStorage instance is resolved by looking up this alias.
	StorageAlias string `json:"storageAlias"`

	// Config holds additional configuration for the private registry.
	Config *PrivateRegistryConfig `json:"config,omitempty"`
}

// Type returns the registry type.
func (p *PrivateRegistry) Type() RegistryType {
	return p.registryType
}

// ImplementationType returns the implementation type/class name.
func (p *PrivateRegistry) ImplementationType() string {
	return p.implementationType
}

// ProxyRegistryConfig holds configuration for a proxy registry.
type ProxyRegistryConfig struct {
	// CacheTTL is the cache expiration time in seconds.
	// After this period, cached artifacts may be refreshed from upstream.
	CacheTTL int64 `json:"cacheTTL,omitempty"`
}

// ProxyRegistry represents a proxy registry that caches artifacts from upstream registries.
// It implements the Registry interface.
type ProxyRegistry struct {
	// registryType is always RegistryTypeProxy for proxy registries.
	registryType RegistryType `json:"type"`

	// implementationType is the implementation class name (e.g., "docker.registry", "raw.registry").
	implementationType string `json:"implementationType"`

	// StorageAlias is the alias/name of the cache storage backend registered in StorageManager.
	// The actual ArtifactStorage instance is resolved by looking up this alias.
	StorageAlias string `json:"storageAlias"`

	// Upstream is the upstream registry configuration.
	Upstream *UpstreamRegistry `json:"upstream"`

	// Config holds additional configuration for the proxy registry.
	Config *ProxyRegistryConfig `json:"config,omitempty"`
}

// Type returns the registry type.
func (p *ProxyRegistry) Type() RegistryType {
	return p.registryType
}

// ImplementationType returns the implementation type/class name.
func (p *ProxyRegistry) ImplementationType() string {
	return p.implementationType
}

// CompoundRegistryConfig holds configuration for a compound registry.
type CompoundRegistryConfig struct {
	// ReadStrategy defines the strategy for reading artifacts.
	// Possible values: "local-first" (check local storage first, then proxies),
	// "proxy-first" (check proxies first, then local), or "local-only" (only local).
	ReadStrategy string `json:"readStrategy,omitempty"`
}

// CompoundRegistry represents a compound registry that combines private storage with proxy registries.
// It implements the Registry interface.
type CompoundRegistry struct {
	// registryType is always RegistryTypeCompound for compound registries.
	registryType RegistryType `json:"type"`

	// implementationType is the implementation class name (e.g., "docker.registry", "raw.registry").
	implementationType string `json:"implementationType"`

	// PrivateStorageAlias is the alias/name of the private storage backend registered in StorageManager.
	// The actual ArtifactStorage instance is resolved by looking up this alias.
	PrivateStorageAlias string `json:"privateStorageAlias"`

	// Proxies is an ordered list of proxy registries to check when artifacts are not found locally.
	// The order matters: artifacts are checked from proxies in the order they appear in this slice.
	Proxies []*ProxyRegistry `json:"proxies,omitempty"`

	// Config holds additional configuration for the compound registry.
	Config *CompoundRegistryConfig `json:"config,omitempty"`
}

// Type returns the registry type.
func (c *CompoundRegistry) Type() RegistryType {
	return c.registryType
}

// ImplementationType returns the implementation type/class name.
func (c *CompoundRegistry) ImplementationType() string {
	return c.implementationType
}
