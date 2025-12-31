package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"brm/pkg/models"

	"github.com/google/uuid"
)

// HashComputingArtifactStorage wraps an ArtifactStorage implementation to automatically
// compute SHA-256 hashes when the hash is unknown (empty, length<3, or "UNKNOWN").
type HashComputingArtifactStorage struct {
	storage models.ArtifactStorage
}

// NewHashComputingArtifactStorage creates a new HashComputingArtifactStorage wrapper.
func NewHashComputingArtifactStorage(storage models.ArtifactStorage) *HashComputingArtifactStorage {
	if storage == nil {
		panic("storage cannot be nil")
	}
	return &HashComputingArtifactStorage{
		storage: storage,
	}
}

// isUnknownHash checks if the hash should be treated as unknown.
// Returns true if hash is empty, length < 3, or equals "UNKNOWN" (case-insensitive).
func (h *HashComputingArtifactStorage) isUnknownHash(hash string) bool {
	if hash == "" {
		return true
	}
	if len(hash) < 3 {
		return true
	}
	if strings.EqualFold(hash, "UNKNOWN") {
		return true
	}
	return false
}

// generateTempHash generates a temporary UUID-based hash for initial storage.
func (h *HashComputingArtifactStorage) generateTempHash() string {
	id := uuid.New()
	return "temp-" + id.String()
}

// cleanupTempHash removes a temporary artifact by adding a cleanup reference and then deleting it.
func (h *HashComputingArtifactStorage) cleanupTempHash(ctx context.Context, tempHash string) error {
	// Check if temp artifact exists
	tempMeta, err := h.storage.GetMeta(ctx, tempHash)
	if err != nil {
		// Already doesn't exist or error reading - nothing to clean up
		return nil
	}

	// If no references exist, add a temporary one so we can delete it
	if len(tempMeta.References) == 0 {
		tempRef := models.ArtifactReference{
			Name:                "temp-cleanup",
			Repo:                "temp-cleanup",
			ReferencedTimestamp: time.Now().Unix(),
		}
		tempMeta.References = []models.ArtifactReference{tempRef}
		_, err = h.storage.UpdateMeta(ctx, *tempMeta)
		if err != nil {
			return fmt.Errorf("failed to add cleanup reference: %w", err)
		}
	}

	// Find and delete the cleanup reference
	cleanupRef := models.ArtifactReference{
		Name: "temp-cleanup",
		Repo: "temp-cleanup",
	}
	_, err = h.storage.Delete(ctx, tempHash, cleanupRef)
	return err
}

// Create streams data from 'r' to storage.
// If hash is unknown (empty, len<3, or "UNKNOWN"), computes SHA-256 hash automatically.
func (h *HashComputingArtifactStorage) Create(ctx context.Context, hash string, r io.Reader, size int64, meta *models.ArtifactMeta) (*models.ArtifactMeta, error) {
	// Check if hash is unknown
	if !h.isUnknownHash(hash) {
		// Known hash: delegate directly to underlying storage
		return h.storage.Create(ctx, hash, r, size, meta)
	}

	// Unknown hash: compute it using temp storage approach
	tempHash := h.generateTempHash()

	// Create SHA-256 hasher
	hasher := sha256.New()

	// Use TeeReader to compute hash while streaming to storage
	teeReader := io.TeeReader(r, hasher)

	// Create artifact with temp hash (this streams the data)
	tempMeta, err := h.storage.Create(ctx, tempHash, teeReader, size, meta)
	if err != nil {
		return nil, fmt.Errorf("failed to create with temp hash: %w", err)
	}

	// Compute final hash from hasher
	computedHash := hex.EncodeToString(hasher.Sum(nil))

	// Check if computed hash already exists (with retry for race conditions)
	var existingMeta *models.ArtifactMeta
	for attempt := 0; attempt < 3; attempt++ {
		var err error
		existingMeta, err = h.storage.GetMeta(ctx, computedHash)
		if err == nil && existingMeta != nil {
			// Hash already exists: cleanup temp file and merge references
			if cleanupErr := h.cleanupTempHash(ctx, tempHash); cleanupErr != nil {
				// Log cleanup error but continue with merge
				// The temp file will be cleaned up later or remain in trash
			}

			// Merge references if provided (follow normal Create behavior for existing artifacts)
			// Use tempMeta.Length (actual size we just wrote) for validation
			if meta != nil && len(meta.References) > 0 {
				mergeSize := existingMeta.Length
				if tempMeta != nil {
					mergeSize = tempMeta.Length
				}
				return h.storage.Create(ctx, computedHash, bytes.NewReader(nil), mergeSize, meta)
			}
			return existingMeta, nil
		}
		// Small delay before retry (for race conditions)
		if attempt < 2 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Hash doesn't exist: move from temp to final location
	// First, check if underlying storage supports Move
	if moveStorage, ok := h.storage.(interface {
		Move(ctx context.Context, srcHash, destHash string) error
	}); ok {
		if err := moveStorage.Move(ctx, tempHash, computedHash); err != nil {
			// Move failed - check if hash now exists (another goroutine might have created it)
			// Retry check a few times to handle race conditions
			var checkMeta *models.ArtifactMeta
			for retry := 0; retry < 3; retry++ {
				var checkErr error
				checkMeta, checkErr = h.storage.GetMeta(ctx, computedHash)
				if checkErr == nil && checkMeta != nil {
					// Hash exists now (race condition: another goroutine created it)
					// Cleanup temp and merge references
					h.cleanupTempHash(ctx, tempHash)
					if meta != nil && len(meta.References) > 0 {
						mergeSize := checkMeta.Length
						if tempMeta != nil {
							mergeSize = tempMeta.Length
						}
						return h.storage.Create(ctx, computedHash, bytes.NewReader(nil), mergeSize, meta)
					}
					return checkMeta, nil
				}
				if retry < 2 {
					time.Sleep(10 * time.Millisecond)
				}
			}
			// Move failed for other reason, cleanup and return error
			h.cleanupTempHash(ctx, tempHash)
			return nil, fmt.Errorf("failed to move from temp to final hash: %w", err)
		}

		// Update metadata with computed hash
		if tempMeta != nil {
			tempMeta.Hash = computedHash
			updatedMeta, err := h.storage.UpdateMeta(ctx, *tempMeta)
			if err != nil {
				// Metadata update failed, but file is already moved
				// Try to get the metadata
				return h.storage.GetMeta(ctx, computedHash)
			}
			return updatedMeta, nil
		}

		// Get final metadata
		return h.storage.GetMeta(ctx, computedHash)
	}

	// Storage doesn't support Move - need alternative approach
	// Read from temp, write to final location, then cleanup
	// This is less efficient but works for any storage backend
	readReq := models.ArtifactRange{
		Hash: tempHash,
		Range: models.ByteRange{
			Offset: 0,
			Length: -1,
		},
	}
	rc, actualRange, err := h.storage.Read(ctx, readReq)
	if err != nil {
		h.cleanupTempHash(ctx, tempHash)
		return nil, fmt.Errorf("failed to read temp artifact: %w", err)
	}
	defer rc.Close()

	// Create with computed hash (Create will handle existing hash case)
	finalMeta, err := h.storage.Create(ctx, computedHash, rc, actualRange.Range.Length, meta)
	rc.Close() // Close early

	if err != nil {
		// Check if hash now exists (another goroutine might have created it)
		existingMeta, checkErr := h.storage.GetMeta(ctx, computedHash)
		if checkErr == nil && existingMeta != nil {
			// Hash exists now - cleanup temp and return existing
			h.cleanupTempHash(ctx, tempHash)
			if meta != nil && len(meta.References) > 0 {
				// Merge references
				mergeSize := existingMeta.Length
				if tempMeta != nil {
					mergeSize = tempMeta.Length
				}
				return h.storage.Create(ctx, computedHash, bytes.NewReader(nil), mergeSize, meta)
			}
			return existingMeta, nil
		}
		// Real error
		h.cleanupTempHash(ctx, tempHash)
		return nil, fmt.Errorf("failed to create with computed hash: %w", err)
	}

	// Cleanup temp file
	if cleanupErr := h.cleanupTempHash(ctx, tempHash); cleanupErr != nil {
		// Log but don't fail - temp file will be cleaned up later
	}

	return finalMeta, nil
}

// Read returns a stream for the requested data.
func (h *HashComputingArtifactStorage) Read(ctx context.Context, req models.ArtifactRange) (io.ReadCloser, models.ArtifactRange, error) {
	return h.storage.Read(ctx, req)
}

// Update modifies a specific range by streaming data from 'r'.
func (h *HashComputingArtifactStorage) Update(ctx context.Context, req models.ArtifactRange, r io.Reader) error {
	return h.storage.Update(ctx, req, r)
}

// Delete removes a specific reference to an artifact.
func (h *HashComputingArtifactStorage) Delete(ctx context.Context, hash string, ref models.ArtifactReference) (*models.ArtifactMeta, error) {
	return h.storage.Delete(ctx, hash, ref)
}

// GetMeta reads the metadata JSON file.
func (h *HashComputingArtifactStorage) GetMeta(ctx context.Context, hash string) (*models.ArtifactMeta, error) {
	return h.storage.GetMeta(ctx, hash)
}

// UpdateMeta overwrites the metadata JSON file.
func (h *HashComputingArtifactStorage) UpdateMeta(ctx context.Context, meta models.ArtifactMeta) (*models.ArtifactMeta, error) {
	return h.storage.UpdateMeta(ctx, meta)
}
