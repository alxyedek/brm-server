package docker

import (
	"encoding/json"
	"fmt"
)

// Manifest represents a Docker/OCI manifest
type Manifest struct {
	SchemaVersion int               `json:"schemaVersion"`
	MediaType     string            `json:"mediaType"`
	Config        *Descriptor       `json:"config,omitempty"`
	Layers        []Descriptor      `json:"layers,omitempty"`
	Annotations   map[string]string `json:"annotations,omitempty"`
	Raw           json.RawMessage   `json:"-"` // Store raw JSON for exact preservation
}

// Descriptor represents a content descriptor (blob or config)
type Descriptor struct {
	MediaType   string            `json:"mediaType"`
	Size        int64             `json:"size"`
	Digest      string            `json:"digest"`
	URLs        []string          `json:"urls,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ParseManifest parses a JSON manifest
func ParseManifest(data []byte) (*Manifest, error) {
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	// Store raw JSON for exact preservation
	manifest.Raw = data

	return &manifest, nil
}

// ToJSON converts manifest back to JSON
func (m *Manifest) ToJSON() ([]byte, error) {
	if len(m.Raw) > 0 {
		// Return raw JSON if available (preserves exact format)
		return m.Raw, nil
	}
	// Otherwise marshal the struct
	return json.Marshal(m)
}

// Common media types
const (
	MediaTypeManifestV2       = "application/vnd.docker.distribution.manifest.v2+json"
	MediaTypeManifestList     = "application/vnd.docker.distribution.manifest.list.v2+json"
	MediaTypeImageConfig      = "application/vnd.docker.container.image.v1+json"
	MediaTypeLayer            = "application/vnd.docker.image.rootfs.diff.tar.gzip"
	MediaTypeOCIManifest      = "application/vnd.oci.image.manifest.v1+json"
	MediaTypeOCIManifestIndex = "application/vnd.oci.image.index.v1+json"
	MediaTypeOCIImageConfig   = "application/vnd.oci.image.config.v1+json"
	MediaTypeOCILayer         = "application/vnd.oci.image.layer.v1.tar+gzip"
)

// IsManifestMediaType checks if the media type is a manifest type
func IsManifestMediaType(mediaType string) bool {
	return mediaType == MediaTypeManifestV2 ||
		mediaType == MediaTypeManifestList ||
		mediaType == MediaTypeOCIManifest ||
		mediaType == MediaTypeOCIManifestIndex
}
