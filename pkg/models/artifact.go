package models

// ArtifactMeta holds metadata about an artifact
type ArtifactMeta struct {
	Name             string `json:"name"`
	CreatedTimestamp int64  `json:"createdTimestamp"`
	Hash             string `json:"hash"`   // Hash used as identifier (e.g., SHA256)
	Repo             string `json:"repo"`   // Repository path in format "type:alias" (e.g., "docker:hub.docker.com")
	Length           int64  `json:"length"` // Length of the artifact in bytes
}

// Artifact represents the binary data of an artifact
type Artifact struct {
	Data []byte `json:"data"`
}

// ByteRange represents a byte range for partial content requests.
// Start is inclusive (0-based), End is exclusive (like Go slices).
// If End is -1, it represents "until the end of artifact data".
// For example, Start=0, End=1024 represents bytes 0-1023.
// Start=1024, End=-1 represents bytes 1024 until the end.
type ByteRange struct {
	Start int64 `json:"start"` // Starting byte position (inclusive)
	End   int64 `json:"end"`   // Ending byte position (exclusive). -1 means "until the end"
}

// ArtifactRequest represents a request for an artifact with optional byte range
type ArtifactRequest struct {
	Hash  string    `json:"hash"`  // Hash used to identify the artifact
	Range ByteRange `json:"range"` // Byte range to retrieve
}

// ArtifactResponse represents a response containing artifact data and the actual byte range returned.
// The Range field contains the actual returned range, which may differ from the requested range
// if the artifact is shorter than requested (e.g., if End exceeds artifact length, or Start exceeds it).
type ArtifactResponse struct {
	Request ArtifactRequest `json:"request"` // The original request
	Data    []byte          `json:"data"`    // The actual data returned
	Range   ByteRange       `json:"range"`   // Actual returned byte range
}

// ArtifactRangeUpdate represents an update operation for a specific byte range of an artifact.
// The range may extend beyond the current file length (append behavior).
type ArtifactRangeUpdate struct {
	Hash  string    `json:"hash"`  // Hash used to identify the artifact
	Range ByteRange `json:"range"` // Byte range to update (can be partial or full append)
	Data  []byte    `json:"data"`  // Data to write at the specified range
}

// ArtifactStorage is an interface for artifact storage operations.
// Artifact (data) and ArtifactMeta are independent objects matched by hash.
type ArtifactStorage interface {
	// Create stores a new artifact data file. Only creates the data, not metadata.
	// Returns an error if creation fails.
	Create(hash string, data []byte) error

	// Read retrieves an artifact based on the request. The Range field in ArtifactRequest
	// specifies which byte range to retrieve. Returns ArtifactResponse with the actual range returned.
	Read(request ArtifactRequest) (*ArtifactResponse, error)

	// Update modifies an existing artifact by replacing the specified byte range.
	// The range may extend beyond the current file length (append behavior).
	// Only updates data, not metadata. Returns an error if update fails.
	Update(update ArtifactRangeUpdate) error

	// Delete removes an artifact identified by the hash.
	// Returns an error if the artifact doesn't exist or deletion fails.
	Delete(hash string) error

	// GetMeta retrieves the metadata for an artifact identified by the hash.
	// Returns the ArtifactMeta or an error if the artifact doesn't exist.
	GetMeta(hash string) (*ArtifactMeta, error)

	// UpdateMeta updates the metadata for an artifact identified by the hash in ArtifactMeta.
	// If metadata doesn't exist, it will be created. Returns the updated ArtifactMeta or an error.
	UpdateMeta(meta ArtifactMeta) (*ArtifactMeta, error)
}
