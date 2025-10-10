package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDefaultConfig(t *testing.T) {
	// Setup: create temp config dir with application.yaml
	tmpDir := t.TempDir()
	baseConfig := `server:
  port: 8080
database:
  host: "localhost"
  port: 5432`
	os.WriteFile(filepath.Join(tmpDir, "application.yaml"), []byte(baseConfig), 0644)

	// Set environment variable
	os.Setenv("APPLICATION_CONFIGURATION_DIR", tmpDir)
	defer os.Unsetenv("APPLICATION_CONFIGURATION_DIR")

	// Test
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Verify values
	if cfg.GetInt("server.port") != 8080 {
		t.Errorf("Expected server.port to be 8080, got %d", cfg.GetInt("server.port"))
	}
	if cfg.GetString("database.host") != "localhost" {
		t.Errorf("Expected database.host to be localhost, got %s", cfg.GetString("database.host"))
	}
	if cfg.GetInt("database.port") != 5432 {
		t.Errorf("Expected database.port to be 5432, got %d", cfg.GetInt("database.port"))
	}
}

func TestLoadWithProfile(t *testing.T) {
	// Setup: create temp config dir with base and profile configs
	tmpDir := t.TempDir()
	baseConfig := `server:
  port: 8080
database:
  host: "localhost"`
	profileConfig := `server:
  port: 8081
database:
  host: "dev-db"`

	os.WriteFile(filepath.Join(tmpDir, "application.yaml"), []byte(baseConfig), 0644)
	os.WriteFile(filepath.Join(tmpDir, "application-dev.yaml"), []byte(profileConfig), 0644)

	// Set environment variables
	os.Setenv("APPLICATION_CONFIGURATION_DIR", tmpDir)
	os.Setenv("APPLICATION_PROFILES_ACTIVE", "dev")
	defer func() {
		os.Unsetenv("APPLICATION_CONFIGURATION_DIR")
		os.Unsetenv("APPLICATION_PROFILES_ACTIVE")
	}()

	// Test
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Verify profile overrides base
	if cfg.GetInt("server.port") != 8081 {
		t.Errorf("Expected server.port to be 8081 (from profile), got %d", cfg.GetInt("server.port"))
	}
	if cfg.GetString("database.host") != "dev-db" {
		t.Errorf("Expected database.host to be dev-db (from profile), got %s", cfg.GetString("database.host"))
	}
}

func TestLoadWithMultipleProfiles(t *testing.T) {
	// Setup: create temp config dir with multiple profile configs
	tmpDir := t.TempDir()
	baseConfig := `server:
  port: 8080
app:
  name: "base"`
	devConfig := `server:
  port: 8081
app:
  debug: true`
	prodConfig := `server:
  port: 8082
app:
  debug: false
  metrics:
    enabled: true`

	os.WriteFile(filepath.Join(tmpDir, "application.yaml"), []byte(baseConfig), 0644)
	os.WriteFile(filepath.Join(tmpDir, "application-dev.yaml"), []byte(devConfig), 0644)
	os.WriteFile(filepath.Join(tmpDir, "application-prod.yaml"), []byte(prodConfig), 0644)

	// Set environment variables
	os.Setenv("APPLICATION_CONFIGURATION_DIR", tmpDir)
	os.Setenv("APPLICATION_PROFILES_ACTIVE", "dev,prod")
	defer func() {
		os.Unsetenv("APPLICATION_CONFIGURATION_DIR")
		os.Unsetenv("APPLICATION_PROFILES_ACTIVE")
	}()

	// Test
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Verify last profile wins
	if cfg.GetInt("server.port") != 8082 {
		t.Errorf("Expected server.port to be 8082 (from last profile), got %d", cfg.GetInt("server.port"))
	}
	if cfg.GetBool("app.debug") != false {
		t.Errorf("Expected app.debug to be false (from last profile), got %v", cfg.GetBool("app.debug"))
	}
	if cfg.GetBool("app.metrics.enabled") != true {
		t.Errorf("Expected app.metrics.enabled to be true (from last profile), got %v", cfg.GetBool("app.metrics.enabled"))
	}
}

func TestLoadWithEnvOverride(t *testing.T) {
	// Setup: create temp config dir
	tmpDir := t.TempDir()
	baseConfig := `server:
  port: 8080
database:
  host: "localhost"`
	os.WriteFile(filepath.Join(tmpDir, "application.yaml"), []byte(baseConfig), 0644)

	// Set environment variables
	os.Setenv("APPLICATION_CONFIGURATION_DIR", tmpDir)
	os.Setenv("SERVER_PORT", "9090")
	defer func() {
		os.Unsetenv("APPLICATION_CONFIGURATION_DIR")
		os.Unsetenv("SERVER_PORT")
	}()

	// Test
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Verify environment variable overrides config
	if cfg.GetInt("server.port") != 9090 {
		t.Errorf("Expected server.port to be 9090 (from env), got %d", cfg.GetInt("server.port"))
	}
}

func TestLoadWithEnvPrefix(t *testing.T) {
	// Setup: create temp config dir
	tmpDir := t.TempDir()
	baseConfig := `server:
  port: 8080
database:
  host: "localhost"`
	os.WriteFile(filepath.Join(tmpDir, "application.yaml"), []byte(baseConfig), 0644)

	// Set environment variables with prefix
	os.Setenv("APPLICATION_CONFIGURATION_DIR", tmpDir)
	os.Setenv("APPLICATION_CONFIGURATION_PREFIX", "BRM")
	os.Setenv("BRM_SERVER_PORT", "7070")
	defer func() {
		os.Unsetenv("APPLICATION_CONFIGURATION_DIR")
		os.Unsetenv("APPLICATION_CONFIGURATION_PREFIX")
		os.Unsetenv("BRM_SERVER_PORT")
	}()

	// Test
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Verify prefixed environment variable overrides config
	if cfg.GetInt("server.port") != 7070 {
		t.Errorf("Expected server.port to be 7070 (from prefixed env), got %d", cfg.GetInt("server.port"))
	}
}

func TestMissingConfigDir(t *testing.T) {
	// Set non-existent config directory
	os.Setenv("APPLICATION_CONFIGURATION_DIR", "/non/existent/dir")
	defer os.Unsetenv("APPLICATION_CONFIGURATION_DIR")

	// Test
	_, err := Load()
	if err == nil {
		t.Fatal("Expected error for missing config directory, got nil")
	}
	if !strings.Contains(err.Error(), "configuration directory does not exist") {
		t.Errorf("Expected error about missing directory, got: %v", err)
	}
}

func TestMissingBaseConfig(t *testing.T) {
	// Setup: create temp config dir without application.yaml
	tmpDir := t.TempDir()
	os.Setenv("APPLICATION_CONFIGURATION_DIR", tmpDir)
	defer os.Unsetenv("APPLICATION_CONFIGURATION_DIR")

	// Test
	_, err := Load()
	if err == nil {
		t.Fatal("Expected error for missing base config, got nil")
	}
	if !strings.Contains(err.Error(), "base configuration file does not exist") {
		t.Errorf("Expected error about missing base config, got: %v", err)
	}
}

func TestMissingProfileConfig(t *testing.T) {
	// Setup: create temp config dir with base config but missing profile
	tmpDir := t.TempDir()
	baseConfig := `server:
  port: 8080`
	os.WriteFile(filepath.Join(tmpDir, "application.yaml"), []byte(baseConfig), 0644)

	// Set environment variables
	os.Setenv("APPLICATION_CONFIGURATION_DIR", tmpDir)
	os.Setenv("APPLICATION_PROFILES_ACTIVE", "nonexistent")
	defer func() {
		os.Unsetenv("APPLICATION_CONFIGURATION_DIR")
		os.Unsetenv("APPLICATION_PROFILES_ACTIVE")
	}()

	// Test - should not error, just warn
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Expected no error for missing profile config, got: %v", err)
	}

	// Verify base config is still loaded
	if cfg.GetInt("server.port") != 8080 {
		t.Errorf("Expected server.port to be 8080 (from base), got %d", cfg.GetInt("server.port"))
	}
}

func TestGetPropertiesWithPrefix(t *testing.T) {
	// Setup: create temp config dir
	tmpDir := t.TempDir()
	baseConfig := `server:
  port: 8080
database:
  host: "localhost"
  port: 5432
  pool:
    maxSize: 10
    minSize: 2`
	os.WriteFile(filepath.Join(tmpDir, "application.yaml"), []byte(baseConfig), 0644)

	os.Setenv("APPLICATION_CONFIGURATION_DIR", tmpDir)
	defer os.Unsetenv("APPLICATION_CONFIGURATION_DIR")

	// Test
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Test hierarchical access
	dbCfg := cfg.getPropertiesWithPrefix("database")
	if dbCfg.GetString("host") != "localhost" {
		t.Errorf("Expected database.host to be localhost, got %s", dbCfg.GetString("host"))
	}
	if dbCfg.GetInt("port") != 5432 {
		t.Errorf("Expected database.port to be 5432, got %d", dbCfg.GetInt("port"))
	}

	// Test nested prefix
	poolCfg := dbCfg.getPropertiesWithPrefix("pool")
	if poolCfg.GetInt("maxSize") != 10 {
		t.Errorf("Expected database.pool.maxSize to be 10, got %d", poolCfg.GetInt("maxSize"))
	}
	if poolCfg.GetInt("minSize") != 2 {
		t.Errorf("Expected database.pool.minSize to be 2, got %d", poolCfg.GetInt("minSize"))
	}
}

func TestGetStringWithDefault(t *testing.T) {
	// Setup: create temp config dir
	tmpDir := t.TempDir()
	baseConfig := `server:
  port: 8080`
	os.WriteFile(filepath.Join(tmpDir, "application.yaml"), []byte(baseConfig), 0644)

	os.Setenv("APPLICATION_CONFIGURATION_DIR", tmpDir)
	defer os.Unsetenv("APPLICATION_CONFIGURATION_DIR")

	// Test
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Test existing value
	if cfg.GetIntWithDefault("server.port", 9999) != 8080 {
		t.Errorf("Expected existing value 8080, got %d", cfg.GetIntWithDefault("server.port", 9999))
	}

	// Test default value
	if cfg.GetStringWithDefault("server.host", "localhost") != "localhost" {
		t.Errorf("Expected default value localhost, got %s", cfg.GetStringWithDefault("server.host", "localhost"))
	}
}

func TestGetIntWithDefault(t *testing.T) {
	// Setup: create temp config dir
	tmpDir := t.TempDir()
	baseConfig := `server:
  port: 8080
database:
  port: 5432`
	os.WriteFile(filepath.Join(tmpDir, "application.yaml"), []byte(baseConfig), 0644)

	os.Setenv("APPLICATION_CONFIGURATION_DIR", tmpDir)
	defer os.Unsetenv("APPLICATION_CONFIGURATION_DIR")

	// Test
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Test existing value
	if cfg.GetIntWithDefault("database.port", 9999) != 5432 {
		t.Errorf("Expected existing value 5432, got %d", cfg.GetIntWithDefault("database.port", 9999))
	}

	// Test default value
	if cfg.GetIntWithDefault("database.maxConnections", 100) != 100 {
		t.Errorf("Expected default value 100, got %d", cfg.GetIntWithDefault("database.maxConnections", 100))
	}
}

func TestGetBoolWithDefault(t *testing.T) {
	// Setup: create temp config dir
	tmpDir := t.TempDir()
	baseConfig := `app:
  debug: true
  production: false`
	os.WriteFile(filepath.Join(tmpDir, "application.yaml"), []byte(baseConfig), 0644)

	os.Setenv("APPLICATION_CONFIGURATION_DIR", tmpDir)
	defer os.Unsetenv("APPLICATION_CONFIGURATION_DIR")

	// Test
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Test existing values
	if cfg.GetBoolWithDefault("app.debug", false) != true {
		t.Errorf("Expected existing value true, got %v", cfg.GetBoolWithDefault("app.debug", false))
	}
	if cfg.GetBoolWithDefault("app.production", true) != false {
		t.Errorf("Expected existing value false, got %v", cfg.GetBoolWithDefault("app.production", true))
	}

	// Test default value
	if cfg.GetBoolWithDefault("app.testing", true) != true {
		t.Errorf("Expected default value true, got %v", cfg.GetBoolWithDefault("app.testing", true))
	}
}

func TestNestedPrefix(t *testing.T) {
	// Setup: create temp config dir
	tmpDir := t.TempDir()
	baseConfig := `app:
  database:
    host: "localhost"
    port: 5432
    pool:
      maxSize: 10
      minSize: 2
  cache:
    enabled: true
    ttl: 3600`
	os.WriteFile(filepath.Join(tmpDir, "application.yaml"), []byte(baseConfig), 0644)

	os.Setenv("APPLICATION_CONFIGURATION_DIR", tmpDir)
	defer os.Unsetenv("APPLICATION_CONFIGURATION_DIR")

	// Test
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Test multiple levels of nesting
	appCfg := cfg.getPropertiesWithPrefix("app")
	dbCfg := appCfg.getPropertiesWithPrefix("database")
	poolCfg := dbCfg.getPropertiesWithPrefix("pool")

	if poolCfg.GetInt("maxSize") != 10 {
		t.Errorf("Expected app.database.pool.maxSize to be 10, got %d", poolCfg.GetInt("maxSize"))
	}

	// Test that we can still access cache from app level
	cacheCfg := appCfg.getPropertiesWithPrefix("cache")
	if cacheCfg.GetBool("enabled") != true {
		t.Errorf("Expected app.cache.enabled to be true, got %v", cacheCfg.GetBool("enabled"))
	}
}

func TestConfigKeys(t *testing.T) {
	// Setup: create temp config dir
	tmpDir := t.TempDir()
	baseConfig := `server:
  port: 8080
database:
  host: "localhost"
  port: 5432
app:
  name: "test"`
	os.WriteFile(filepath.Join(tmpDir, "application.yaml"), []byte(baseConfig), 0644)

	os.Setenv("APPLICATION_CONFIGURATION_DIR", tmpDir)
	defer os.Unsetenv("APPLICATION_CONFIGURATION_DIR")

	// Test
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Test root level keys
	keys := cfg.Keys()
	expectedKeys := []string{"server", "database", "app"}
	for _, expected := range expectedKeys {
		found := false
		for _, key := range keys {
			if key == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected key %s not found in root keys: %v", expected, keys)
		}
	}

	// Test sub-config keys
	dbCfg := cfg.getPropertiesWithPrefix("database")
	dbKeys := dbCfg.Keys()
	expectedDbKeys := []string{"host", "port"}
	for _, expected := range expectedDbKeys {
		found := false
		for _, key := range dbKeys {
			if key == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected key %s not found in database keys: %v", expected, dbKeys)
		}
	}
}

func TestConfigAll(t *testing.T) {
	// Setup: create temp config dir
	tmpDir := t.TempDir()
	baseConfig := `server:
  port: 8080
database:
  host: "localhost"
  port: 5432`
	os.WriteFile(filepath.Join(tmpDir, "application.yaml"), []byte(baseConfig), 0644)

	os.Setenv("APPLICATION_CONFIGURATION_DIR", tmpDir)
	defer os.Unsetenv("APPLICATION_CONFIGURATION_DIR")

	// Test
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Test root level all
	all := cfg.All()
	if all["server.port"] == nil {
		t.Error("Expected server.port in root config")
	}
	if all["database.host"] == nil {
		t.Error("Expected database.host in root config")
	}
	if all["database.port"] == nil {
		t.Error("Expected database.port in root config")
	}

	// Test sub-config all
	dbCfg := cfg.getPropertiesWithPrefix("database")
	dbAll := dbCfg.All()
	if dbAll["host"] != "localhost" {
		t.Errorf("Expected database.host to be localhost, got %v", dbAll["host"])
	}
	if dbAll["port"] != 5432 {
		t.Errorf("Expected database.port to be 5432, got %v", dbAll["port"])
	}
}
