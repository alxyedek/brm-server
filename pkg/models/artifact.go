package models

import (
	"context"
	"io"
)

// ArtifactMeta holds metadata about an artifact
type ArtifactMeta struct {
	Name             string `json:"name"`
	CreatedTimestamp int64  `json:"createdTimestamp"`
	Hash             string `json:"hash"`
	Repo             string `json:"repo"`
	Length           int64  `json:"length"`
}

// Artifact struct is REMOVED.
// We do not want a struct representing the binary data in memory.
// We use io.Reader and io.ReadCloser instead.

// ByteRange represents a byte range for partial content requests.
// Switching to Length is preferred over End for consistency with Go IO interfaces.
type ByteRange struct {
	Offset int64 `json:"offset"` // Starting byte position
	Length int64 `json:"length"` // Number of bytes to read/write. -1 means "until the end"
}

// ArtifactRange identifies a specific range within an artifact by hash.
// It replaces ArtifactRequest, ArtifactResponse, and ArtifactRangeUpdate.
type ArtifactRange struct {
	Hash  string    `json:"hash"`
	Range ByteRange `json:"range"`
}

// ArtifactStorage is the high-performance interface.
type ArtifactStorage interface {
	// Create streams data from 'r' to storage.
	// We include 'size' because many storage backends (allocators/S3) need a size hint.
	// If size is unknown, pass -1 (though this may disable some optimizations).
	// The 'meta' parameter is optional (can be nil). If provided, metadata is stored atomically with the data.
	Create(ctx context.Context, hash string, r io.Reader, size int64, meta *ArtifactMeta) error

	// Read returns a stream (rc) for the requested data.
	// It returns 'actual' containing the actual range being returned (calculated).
	// This is useful if the requested Length was -1 or exceeded the file size.
	// IMPORTANT: The caller MUST close rc.
	Read(ctx context.Context, req ArtifactRange) (rc io.ReadCloser, actual ArtifactRange, err error)

	// Update modifies a specific range by streaming data from 'r'.
	Update(ctx context.Context, req ArtifactRange, r io.Reader) error

	// Delete removes an artifact.
	Delete(ctx context.Context, hash string) error

	// Meta operations
	GetMeta(ctx context.Context, hash string) (*ArtifactMeta, error)
	UpdateMeta(ctx context.Context, meta ArtifactMeta) (*ArtifactMeta, error)
}
