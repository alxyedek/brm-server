package docker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"brm/pkg/models"
)

// DockerRegistryService handles core registry logic: cache management and upstream communication
type DockerRegistryService struct {
	storage        models.ArtifactStorage
	client         *DockerRegistryClient
	cacheTTL       time.Duration
	upstreamConfig *models.UpstreamRegistry
}

// NewDockerRegistryService creates a new Docker registry service
func NewDockerRegistryService(
	storageAlias string,
	upstream *models.UpstreamRegistry,
	config *models.ProxyRegistryConfig,
) (*DockerRegistryService, error) {
	// Create upstream client
	client := NewDockerRegistryClient(upstream)

	// Determine cache TTL
	cacheTTL := 168 * time.Hour // Default 7 days
	if config != nil && config.CacheTTL > 0 {
		cacheTTL = time.Duration(config.CacheTTL) * time.Second
	}

	return &DockerRegistryService{
		client:         client,
		cacheTTL:       cacheTTL,
		upstreamConfig: upstream,
	}, nil
}

// SetStorage sets the storage backend (called after storage is resolved)
func (s *DockerRegistryService) SetStorage(storage models.ArtifactStorage) {
	s.storage = storage
}

// getCacheKey generates a cache key for a manifest or blob
func (s *DockerRegistryService) getCacheKey(name, digest string) string {
	// Use digest as hash for content-addressable storage
	return digest
}

// isCacheExpired checks if cached artifact has expired based on TTL
func (s *DockerRegistryService) isCacheExpired(meta *models.ArtifactMeta) bool {
	if meta == nil {
		return true
	}
	if s.cacheTTL <= 0 {
		return false // No expiration
	}
	age := time.Since(time.Unix(meta.CreatedTimestamp, 0))
	return age > s.cacheTTL
}

// GetManifest retrieves a manifest, checking cache first, then upstream
func (s *DockerRegistryService) GetManifest(ctx context.Context, name, reference string) ([]byte, string, error) {
	// First, try to get from upstream to get the digest
	manifestData, mediaType, err := s.client.GetManifest(ctx, name, reference)
	if err != nil {
		return nil, "", fmt.Errorf("failed to fetch manifest from upstream: %w", err)
	}

	// Calculate digest from manifest data
	digest := s.calculateDigest(manifestData)
	cacheKey := s.getCacheKey(name, digest)

	// Check cache
	meta, err := s.storage.GetMeta(ctx, cacheKey)
	if err == nil && meta != nil && !s.isCacheExpired(meta) {
		// Cache hit - read from cache
		readReq := models.ArtifactRange{
			Hash: cacheKey,
			Range: models.ByteRange{
				Offset: 0,
				Length: -1,
			},
		}
		rc, _, err := s.storage.Read(ctx, readReq)
		if err == nil {
			defer rc.Close()
			cachedData, err := io.ReadAll(rc)
			if err == nil {
				return cachedData, mediaType, nil
			}
		}
	}

	// Cache miss or expired - store in cache
	ref := models.ArtifactReference{
		Name:                name,
		Repo:                "manifest",
		ReferencedTimestamp: time.Now().Unix(),
	}
	meta = &models.ArtifactMeta{
		Hash:             cacheKey,
		Length:           int64(len(manifestData)),
		CreatedTimestamp: time.Now().Unix(),
		References:       []models.ArtifactReference{ref},
	}

	_, err = s.storage.Create(ctx, cacheKey, bytes.NewReader(manifestData), int64(len(manifestData)), meta)
	if err != nil {
		// Log error but continue - cache write failure shouldn't break the request
	}

	return manifestData, mediaType, nil
}

// CheckManifestExists checks if a manifest exists
func (s *DockerRegistryService) CheckManifestExists(ctx context.Context, name, reference string) (bool, string, error) {
	exists, digest, err := s.client.CheckManifestExists(ctx, name, reference)
	if err != nil {
		return false, "", err
	}
	return exists, digest, nil
}

// GetBlob retrieves a blob, checking cache first, then upstream
func (s *DockerRegistryService) GetBlob(ctx context.Context, name, digest string) (io.ReadCloser, int64, error) {
	cacheKey := s.getCacheKey(name, digest)

	// Check cache
	meta, err := s.storage.GetMeta(ctx, cacheKey)
	if err == nil && meta != nil && !s.isCacheExpired(meta) {
		// Cache hit - read from cache
		readReq := models.ArtifactRange{
			Hash: cacheKey,
			Range: models.ByteRange{
				Offset: 0,
				Length: -1,
			},
		}
		rc, actualRange, err := s.storage.Read(ctx, readReq)
		if err == nil {
			return rc, actualRange.Range.Length, nil
		}
	}

	// Cache miss or expired - fetch from upstream
	blobReader, _, err := s.client.GetBlob(ctx, name, digest)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch blob from upstream: %w", err)
	}

	// For now, read the blob into memory and cache it
	// In production, this could be optimized with streaming and background caching
	blobData, err := io.ReadAll(blobReader)
	blobReader.Close()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read blob from upstream: %w", err)
	}

	// Cache the blob
	ref := models.ArtifactReference{
		Name:                name,
		Repo:                "blob",
		ReferencedTimestamp: time.Now().Unix(),
	}
	meta = &models.ArtifactMeta{
		Hash:             cacheKey,
		Length:           int64(len(blobData)),
		CreatedTimestamp: time.Now().Unix(),
		References:       []models.ArtifactReference{ref},
	}

	_, err = s.storage.Create(ctx, cacheKey, bytes.NewReader(blobData), int64(len(blobData)), meta)
	if err != nil {
		// Log error but continue - cache write failure shouldn't break the request
	}

	// Return the blob data
	return io.NopCloser(bytes.NewReader(blobData)), int64(len(blobData)), nil
}

// CheckBlobExists checks if a blob exists
func (s *DockerRegistryService) CheckBlobExists(ctx context.Context, name, digest string) (bool, int64, error) {
	cacheKey := s.getCacheKey(name, digest)

	// Check cache first
	meta, err := s.storage.GetMeta(ctx, cacheKey)
	if err == nil && meta != nil && !s.isCacheExpired(meta) {
		return true, meta.Length, nil
	}

	// Check upstream
	exists, size, err := s.client.CheckBlobExists(ctx, name, digest)
	if err != nil {
		return false, 0, err
	}
	return exists, size, nil
}

// CalculateDigest calculates SHA256 digest (exported for use in handlers)
func (s *DockerRegistryService) CalculateDigest(data []byte) string {
	hasher := sha256.New()
	hasher.Write(data)
	return "sha256:" + hex.EncodeToString(hasher.Sum(nil))
}

// calculateDigest calculates SHA256 digest (internal method)
func (s *DockerRegistryService) calculateDigest(data []byte) string {
	return s.CalculateDigest(data)
}
