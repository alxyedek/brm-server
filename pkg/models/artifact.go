package models

// ArtifactMeta holds metadata about an artifact
type ArtifactMeta struct {
	Name             string `json:"name"`
	CreatedTimestamp int64  `json:"createdTimestamp"`
	Shasum           string `json:"shasum"` // SHA256 hash used as identifier
	Repo             string `json:"repo"`   // Repository path in format "type:alias" (e.g., "docker:hub.docker.com")
	Length           int64  `json:"length"` // Length of the artifact in bytes
}

// Artifact represents an artifact with its metadata and binary data
type Artifact struct {
	Meta ArtifactMeta `json:"meta"`
	Data []byte       `json:"data"`
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
	Meta  ArtifactMeta `json:"meta"`
	Range ByteRange    `json:"range"`
}

// ArtifactResponse represents a response containing an artifact and the actual byte range returned.
// The Range field contains the actual returned range, which may differ from the requested range
// if the artifact is shorter than requested (e.g., if End exceeds artifact length, or Start exceeds it).
type ArtifactResponse struct {
	Meta     ArtifactMeta `json:"meta"`
	Artifact Artifact     `json:"artifact"`
	Range    ByteRange    `json:"range"` // Actual returned byte range
}

// ArtifactStorage is an interface for artifact storage operations.
// The Data field in Artifact may represent a byte range for partial operations.
type ArtifactStorage interface {
	// Create stores a new artifact. The Artifact's Data field may contain full data or a byte range.
	// Returns the created ArtifactMeta or an error.
	Create(artifact Artifact) (*ArtifactMeta, error)

	// Read retrieves an artifact based on the request. The Range field in ArtifactRequest
	// specifies which byte range to retrieve. Returns ArtifactResponse with the actual range returned.
	Read(request ArtifactRequest) (*ArtifactResponse, error)

	// Update modifies an existing artifact. The Artifact's Data field may contain full data
	// or a byte range for partial updates. Returns the updated ArtifactMeta or an error.
	Update(artifact Artifact) (*ArtifactMeta, error)

	// Delete removes an artifact identified by the shasum in ArtifactMeta.
	// Returns an error if the artifact doesn't exist or deletion fails.
	Delete(meta ArtifactMeta) error

	// GetMeta retrieves the metadata for an artifact identified by the shasum in ArtifactMeta.
	// Returns the ArtifactMeta or an error if the artifact doesn't exist.
	GetMeta(meta ArtifactMeta) (*ArtifactMeta, error)

	// UpdateMeta updates the metadata for an artifact identified by the shasum in ArtifactMeta.
	// Returns the updated ArtifactMeta or an error if the artifact doesn't exist or update fails.
	UpdateMeta(meta ArtifactMeta) (*ArtifactMeta, error)
}
