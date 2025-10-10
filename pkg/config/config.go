package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// Config wraps koanf.Koanf to provide configuration access for the application.
// @see https://github.com/knadh/koanf .
// @see Load documentation for more information.
// Config.prefix is both the prefix for the configuration keys and the prefix for the environment variables.
// prefix is empty for the root config.
// subconfig system is configured by appeding new keys to the prefix. @see GetSubConfig
type Config struct {
	k      *koanf.Koanf
	prefix string
}

// Load loads configuration from YAML files and environment variables
// reads environment variables with the prefix "APPLICATION_CONFIGURATION_PREFIX" if set.
// reads environment variables without prefix if "APPLICATION_CONFIGURATION_PREFIX" is not set.
// reads configuration from the directory specified by the "APPLICATION_CONFIGURATION_DIR" environment variable.
// if "APPLICATION_CONFIGURATION_DIR" is not set, it defaults to "./configs".
// reads configuration from the "application.yaml" file in the configuration directory. It;s existance is mandatory.
// reads configuration from the "application-<profile>.yaml" files in the configuration directory, for all active profiles, in their defined order.
// profiles are defined by the "APPLICATION_PROFILES_ACTIVE" environment variable.
func Load() (*Config, error) {
	k := koanf.New(".")

	// Initialize temporary logger for initial loading
	tempLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Get configuration directory (defaults to "./configs")
	configDir := os.Getenv("APPLICATION_CONFIGURATION_DIR")
	if configDir == "" {
		configDir = "./configs"
	}
	tempLogger.Info("Loading configuration", "directory", configDir)

	// Check if config directory exists
	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		tempLogger.Error("Configuration directory does not exist", "directory", configDir)
		return nil, fmt.Errorf("configuration directory does not exist: %s", configDir)
	}

	// Load base application.yaml
	baseConfigPath := filepath.Join(configDir, "application.yaml")
	if _, err := os.Stat(baseConfigPath); os.IsNotExist(err) {
		tempLogger.Error("Base configuration file does not exist", "file", baseConfigPath)
		return nil, fmt.Errorf("base configuration file does not exist: %s", baseConfigPath)
	}

	tempLogger.Info("Loading base configuration", "file", baseConfigPath)
	if err := k.Load(file.Provider(baseConfigPath), yaml.Parser()); err != nil {
		tempLogger.Error("Failed to load base configuration", "file", baseConfigPath, "error", err)
		return nil, fmt.Errorf("failed to load base configuration: %w", err)
	}

	// Load profile-specific configurations
	profiles := os.Getenv("APPLICATION_PROFILES_ACTIVE")
	profileList := []string{}
	if profiles != "" {
		profileList = strings.Split(profiles, ",")
		// Trim spaces from profile names
		for i, profile := range profileList {
			profileList[i] = strings.TrimSpace(profile)
		}
	}
	tempLogger.Info("Active profiles", "profiles", profileList)

	for _, profile := range profileList {
		if profile == "" {
			continue
		}

		profileConfigPath := filepath.Join(configDir, fmt.Sprintf("application-%s.yaml", profile))
		if _, err := os.Stat(profileConfigPath); os.IsNotExist(err) {
			tempLogger.Warn("Profile configuration file not found", "profile", profile, "file", profileConfigPath)
			continue
		}

		tempLogger.Info("Loading profile configuration", "profile", profile, "file", profileConfigPath)
		if err := k.Load(file.Provider(profileConfigPath), yaml.Parser()); err != nil {
			tempLogger.Error("Failed to load profile configuration", "profile", profile, "file", profileConfigPath, "error", err)
			return nil, fmt.Errorf("failed to load profile configuration %s: %w", profile, err)
		}
	}

	// Load environment variables
	envPrefix := os.Getenv("APPLICATION_CONFIGURATION_PREFIX")
	tempLogger.Info("Environment variable prefix", "prefix", envPrefix)

	if envPrefix != "" {
		// Load environment variables with prefix
		if err := k.Load(env.Provider(envPrefix+"_", ".", func(s string) string {
			// Convert BRM_SERVER_PORT to server.port
			s = strings.TrimPrefix(s, envPrefix+"_")
			return strings.ToLower(strings.ReplaceAll(s, "_", "."))
		}), nil); err != nil {
			tempLogger.Error("Failed to load environment variables with prefix", "prefix", envPrefix, "error", err)
			return nil, fmt.Errorf("failed to load environment variables with prefix: %w", err)
		}
	} else {
		// Load environment variables without prefix
		if err := k.Load(env.Provider("", ".", func(s string) string {
			// Convert SERVER_PORT to server.port
			return strings.ToLower(strings.ReplaceAll(s, "_", "."))
		}), nil); err != nil {
			tempLogger.Error("Failed to load environment variables", "error", err)
			return nil, fmt.Errorf("failed to load environment variables: %w", err)
		}
	}

	tempLogger.Info("Configuration loaded successfully")
	return &Config{k: k, prefix: ""}, nil
}

// buildKey constructs the full key with current prefix
func (c *Config) buildKey(key string) string {
	if c.prefix == "" {
		return key
	}
	return c.prefix + "." + key
}

// getPropertiesWithPrefix returns a new Config instance with the specified prefix
func (c *Config) getPropertiesWithPrefix(prefix string) *Config {
	return &Config{
		k:      c.k,
		prefix: c.buildKey(prefix),
	}
}

// GetSubConfig returns a configuration instance for a specific sub-tree
// This is an alias for GetPropertiesWithPrefix for better readability
func (c *Config) GetSubConfig(prefix string) *Config {
	return c.getPropertiesWithPrefix(prefix)
}

// HasPrefix checks if the current configuration has any keys with the given prefix
func (c *Config) HasPrefix(prefix string) bool {
	keys := c.Keys()
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// GetString gets a string value by key
func (c *Config) GetString(key string) string {
	return c.k.String(c.buildKey(key))
}

// GetInt gets an integer value by key
func (c *Config) GetInt(key string) int {
	return c.k.Int(c.buildKey(key))
}

// GetBool gets a boolean value by key
func (c *Config) GetBool(key string) bool {
	return c.k.Bool(c.buildKey(key))
}

// GetFloat64 gets a float64 value by key
func (c *Config) GetFloat64(key string) float64 {
	return c.k.Float64(c.buildKey(key))
}

// GetDuration gets a duration value by key
func (c *Config) GetDuration(key string) int64 {
	return c.k.Int64(c.buildKey(key))
}

// Exists checks if a key exists
func (c *Config) Exists(key string) bool {
	return c.k.Exists(c.buildKey(key))
}

// GetStringWithDefault gets a string value with a default fallback
func (c *Config) GetStringWithDefault(key, defaultValue string) string {
	if c.Exists(key) {
		return c.GetString(key)
	}
	return defaultValue
}

// GetIntWithDefault gets an integer value with a default fallback
func (c *Config) GetIntWithDefault(key string, defaultValue int) int {
	if c.Exists(key) {
		return c.GetInt(key)
	}
	return defaultValue
}

// GetBoolWithDefault gets a boolean value with a default fallback
func (c *Config) GetBoolWithDefault(key string, defaultValue bool) bool {
	if c.Exists(key) {
		return c.GetBool(key)
	}
	return defaultValue
}

// GetLogLevel gets the log level from configuration with default fallback
func (c *Config) GetLogLevel(defaultLevel slog.Level) slog.Level {
	if c.Exists("logging.level") {
		levelStr := strings.ToLower(c.GetString("logging.level"))
		switch levelStr {
		case "debug":
			return slog.LevelDebug
		case "info":
			return slog.LevelInfo
		case "warn", "warning":
			return slog.LevelWarn
		case "error":
			return slog.LevelError
		default:
			return defaultLevel
		}
	}
	return defaultLevel
}

// Keys returns all keys at the current level
func (c *Config) Keys() []string {
	if c.prefix == "" {
		// Return only top-level configuration keys (those that are direct children)
		allKeys := c.k.Keys()
		var configKeys []string
		for _, key := range allKeys {
			// Only include keys that have exactly one dot (top-level keys)
			if strings.Count(key, ".") == 1 {
				// Extract the top-level key name
				topLevelKey := strings.Split(key, ".")[0]
				// Check if we already have this key
				found := false
				for _, existing := range configKeys {
					if existing == topLevelKey {
						found = true
						break
					}
				}
				if !found {
					configKeys = append(configKeys, topLevelKey)
				}
			}
		}
		return configKeys
	}

	// Filter keys that start with our prefix
	allKeys := c.k.Keys()
	var filteredKeys []string
	prefixWithDot := c.prefix + "."

	for _, key := range allKeys {
		if strings.HasPrefix(key, prefixWithDot) {
			// Remove the prefix and the dot
			relativeKey := strings.TrimPrefix(key, prefixWithDot)
			// Only include direct children (not nested grandchildren)
			if !strings.Contains(relativeKey, ".") {
				filteredKeys = append(filteredKeys, relativeKey)
			}
		}
	}

	return filteredKeys
}

// All returns all configuration as a map
func (c *Config) All() map[string]interface{} {
	if c.prefix == "" {
		// Filter out environment variables and return only config file data
		allKeys := c.k.Keys()
		result := make(map[string]interface{})
		for _, key := range allKeys {
			// Only include configuration keys (those with dots)
			if strings.Contains(key, ".") {
				result[key] = c.k.Get(key)
			}
		}
		return result
	}

	// Get all keys and filter by prefix
	allKeys := c.k.Keys()
	result := make(map[string]interface{})
	prefixWithDot := c.prefix + "."

	for _, key := range allKeys {
		if strings.HasPrefix(key, prefixWithDot) {
			relativeKey := strings.TrimPrefix(key, prefixWithDot)
			result[relativeKey] = c.k.Get(key)
		}
	}

	return result
}
