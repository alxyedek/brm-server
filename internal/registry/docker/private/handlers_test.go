package private

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// setupTestHandlers creates a test HTTP server with handlers
func setupTestHandlers(t *testing.T) (*http.ServeMux, *DockerRegistryPrivateService) {
	service, _ := setupTestService(t)
	mux := http.NewServeMux()
	SetupRoutes(mux, service)
	return mux, service
}

// TestHandleAPIVersion tests GET /v2/ endpoint
func TestHandleAPIVersion(t *testing.T) {
	mux, _ := setupTestHandlers(t)

	req := httptest.NewRequest("GET", "/v2/", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	apiVersion := w.Header().Get("Docker-Distribution-API-Version")
	if apiVersion != "registry/2.0" {
		t.Errorf("Expected API version 'registry/2.0', got %s", apiVersion)
	}
}

// TestHandleGetManifest tests GET /v2/{name}/manifests/{reference}
func TestHandleGetManifest(t *testing.T) {
	mux, service := setupTestHandlers(t)
	ctx := context.Background()

	// Store a manifest first
	manifestData := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json"}`)
	err := service.PutManifest(ctx, "test-repo", "latest", manifestData, "application/vnd.docker.distribution.manifest.v2+json")
	if err != nil {
		t.Fatalf("Failed to store manifest: %v", err)
	}

	req := httptest.NewRequest("GET", "/v2/test-repo/manifests/latest", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	body := w.Body.Bytes()
	if !bytes.Equal(body, manifestData) {
		t.Errorf("Manifest data mismatch")
	}

	contentType := w.Header().Get("Content-Type")
	if contentType == "" {
		t.Error("Content-Type header should be set")
	}

	digest := w.Header().Get("Docker-Content-Digest")
	if digest == "" {
		t.Error("Docker-Content-Digest header should be set")
	}
}

// TestHandleGetManifestNotFound tests GET /v2/{name}/manifests/{reference} for non-existent manifest
func TestHandleGetManifestNotFound(t *testing.T) {
	mux, _ := setupTestHandlers(t)

	req := httptest.NewRequest("GET", "/v2/test-repo/manifests/nonexistent", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

// TestHandleHeadManifest tests HEAD /v2/{name}/manifests/{reference}
func TestHandleHeadManifest(t *testing.T) {
	mux, service := setupTestHandlers(t)
	ctx := context.Background()

	// Store a manifest first
	manifestData := []byte(`{"schemaVersion":2}`)
	err := service.PutManifest(ctx, "test-repo", "v1.0.0", manifestData, "application/vnd.docker.distribution.manifest.v2+json")
	if err != nil {
		t.Fatalf("Failed to store manifest: %v", err)
	}

	req := httptest.NewRequest("HEAD", "/v2/test-repo/manifests/v1.0.0", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	digest := w.Header().Get("Docker-Content-Digest")
	if digest == "" {
		t.Error("Docker-Content-Digest header should be set")
	}
}

// TestHandlePutManifest tests PUT /v2/{name}/manifests/{reference}
func TestHandlePutManifest(t *testing.T) {
	mux, service := setupTestHandlers(t)

	manifestData := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json"}`)
	req := httptest.NewRequest("PUT", "/v2/test-repo/manifests/latest", bytes.NewReader(manifestData))
	req.Header.Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status %d, got %d", http.StatusCreated, w.Code)
	}

	digest := w.Header().Get("Docker-Content-Digest")
	if digest == "" {
		t.Error("Docker-Content-Digest header should be set")
	}

	location := w.Header().Get("Location")
	if location == "" {
		t.Error("Location header should be set")
	}

	// Verify manifest can be retrieved
	ctx := context.Background()
	retrievedData, _, err := service.GetManifest(ctx, "test-repo", "latest")
	if err != nil {
		t.Fatalf("Failed to retrieve manifest: %v", err)
	}

	if !bytes.Equal(retrievedData, manifestData) {
		t.Error("Retrieved manifest data mismatch")
	}
}

// TestHandleGetBlob tests GET /v2/{name}/blobs/{digest}
func TestHandleGetBlob(t *testing.T) {
	mux, service := setupTestHandlers(t)
	ctx := context.Background()

	// Store a blob first
	blobData := []byte("test blob content")
	digest := service.CalculateDigest(blobData)
	err := service.PutBlob(ctx, "test-repo", digest, bytes.NewReader(blobData), int64(len(blobData)))
	if err != nil {
		t.Fatalf("Failed to store blob: %v", err)
	}

	req := httptest.NewRequest("GET", "/v2/test-repo/blobs/"+digest, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	body := w.Body.Bytes()
	if !bytes.Equal(body, blobData) {
		t.Error("Blob data mismatch")
	}

	contentLength := w.Header().Get("Content-Length")
	if contentLength != "17" {
		t.Errorf("Expected Content-Length '17', got %s", contentLength)
	}
}

// TestHandleHeadBlob tests HEAD /v2/{name}/blobs/{digest}
func TestHandleHeadBlob(t *testing.T) {
	mux, service := setupTestHandlers(t)
	ctx := context.Background()

	// Store a blob first
	blobData := []byte("test blob")
	digest := service.CalculateDigest(blobData)
	err := service.PutBlob(ctx, "test-repo", digest, bytes.NewReader(blobData), int64(len(blobData)))
	if err != nil {
		t.Fatalf("Failed to store blob: %v", err)
	}

	req := httptest.NewRequest("HEAD", "/v2/test-repo/blobs/"+digest, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	contentLength := w.Header().Get("Content-Length")
	if contentLength != "9" {
		t.Errorf("Expected Content-Length '9', got %s", contentLength)
	}
}

// TestHandleStartBlobUpload tests POST /v2/{name}/blobs/uploads/
func TestHandleStartBlobUpload(t *testing.T) {
	mux, _ := setupTestHandlers(t)

	req := httptest.NewRequest("POST", "/v2/test-repo/blobs/uploads/", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("Expected status %d, got %d", http.StatusAccepted, w.Code)
	}

	location := w.Header().Get("Location")
	if location == "" {
		t.Error("Location header should be set")
	}

	uuid := w.Header().Get("Docker-Upload-UUID")
	if uuid == "" {
		t.Error("Docker-Upload-UUID header should be set")
	}
}

// TestHandleSingleRequestBlobUpload tests POST /v2/{name}/blobs/uploads/?digest={digest}
func TestHandleSingleRequestBlobUpload(t *testing.T) {
	mux, service := setupTestHandlers(t)

	blobData := []byte("single request blob")
	digest := service.CalculateDigest(blobData)

	req := httptest.NewRequest("POST", "/v2/test-repo/blobs/uploads/?digest="+digest, bytes.NewReader(blobData))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status %d, got %d", http.StatusCreated, w.Code)
	}

	location := w.Header().Get("Location")
	if location == "" {
		t.Error("Location header should be set")
	}

	// Verify blob was stored
	ctx := context.Background()
	exists, _, err := service.CheckBlobExists(ctx, "test-repo", digest)
	if err != nil {
		t.Fatalf("CheckBlobExists failed: %v", err)
	}
	if !exists {
		t.Error("Blob should exist after upload")
	}
}

// TestHandleCompleteBlobUpload tests PUT /v2/{name}/blobs/uploads/{uuid}?digest={digest}
func TestHandleCompleteBlobUpload(t *testing.T) {
	mux, service := setupTestHandlers(t)
	ctx := context.Background()

	// Start upload session
	uuid, err := service.StartBlobUpload(ctx, "test-repo")
	if err != nil {
		t.Fatalf("StartBlobUpload failed: %v", err)
	}

	// Upload chunk
	chunkData := []byte("chunk data")
	_, err = service.UploadBlobChunk(ctx, "test-repo", uuid, bytes.NewReader(chunkData), 0)
	if err != nil {
		t.Fatalf("UploadBlobChunk failed: %v", err)
	}

	// Complete upload
	finalChunk := []byte("final")
	combinedData := append(chunkData, finalChunk...)
	digest := service.CalculateDigest(combinedData)

	req := httptest.NewRequest("PUT", "/v2/test-repo/blobs/uploads/"+uuid+"?digest="+digest, bytes.NewReader(finalChunk))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status %d, got %d", http.StatusCreated, w.Code)
	}

	// Verify blob was stored
	exists, _, err := service.CheckBlobExists(ctx, "test-repo", digest)
	if err != nil {
		t.Fatalf("CheckBlobExists failed: %v", err)
	}
	if !exists {
		t.Error("Blob should exist after upload")
	}
}

// TestHandleUploadBlobChunk tests PATCH /v2/{name}/blobs/uploads/{uuid}
func TestHandleUploadBlobChunk(t *testing.T) {
	mux, service := setupTestHandlers(t)
	ctx := context.Background()

	// Start upload session
	uuid, err := service.StartBlobUpload(ctx, "test-repo")
	if err != nil {
		t.Fatalf("StartBlobUpload failed: %v", err)
	}

	chunkData := []byte("chunk data")
	req := httptest.NewRequest("PATCH", "/v2/test-repo/blobs/uploads/"+uuid, bytes.NewReader(chunkData))
	req.Header.Set("Content-Range", "bytes 0-9/*")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("Expected status %d, got %d", http.StatusNoContent, w.Code)
	}

	rangeHeader := w.Header().Get("Range")
	if rangeHeader == "" {
		t.Error("Range header should be set")
	}
}

// TestHandlePutManifestInvalidJSON tests PUT with invalid manifest data
func TestHandlePutManifestInvalidJSON(t *testing.T) {
	mux, _ := setupTestHandlers(t)

	invalidData := []byte("not valid json")
	req := httptest.NewRequest("PUT", "/v2/test-repo/manifests/latest", bytes.NewReader(invalidData))
	req.Header.Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	// Should still succeed (we don't validate JSON structure in handler)
	// But the service might handle it differently
	if w.Code != http.StatusCreated && w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d or %d, got %d", http.StatusCreated, http.StatusBadRequest, w.Code)
	}
}

// TestHandleGetBlobNotFound tests GET /v2/{name}/blobs/{digest} for non-existent blob
func TestHandleGetBlobNotFound(t *testing.T) {
	mux, _ := setupTestHandlers(t)

	req := httptest.NewRequest("GET", "/v2/test-repo/blobs/sha256:0000000000000000000000000000000000000000000000000000000000000000", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

// TestHandleCompleteBlobUploadInvalidDigest tests PUT with invalid digest
func TestHandleCompleteBlobUploadInvalidDigest(t *testing.T) {
	mux, service := setupTestHandlers(t)
	ctx := context.Background()

	// Start upload session
	uuid, err := service.StartBlobUpload(ctx, "test-repo")
	if err != nil {
		t.Fatalf("StartBlobUpload failed: %v", err)
	}

	// Complete with wrong digest
	wrongDigest := "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	req := httptest.NewRequest("PUT", "/v2/test-repo/blobs/uploads/"+uuid+"?digest="+wrongDigest, bytes.NewReader([]byte("data")))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	// Should return error for digest mismatch
	if w.Code == http.StatusCreated {
		t.Error("Expected error for digest mismatch, got success")
	}
}
