package docker

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// RegistryError represents an error response following OCI Distribution Spec
type RegistryError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Detail  string `json:"detail,omitempty"`
}

// Error implements the error interface
func (e *RegistryError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("%s: %s (%s)", e.Code, e.Message, e.Detail)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// HTTPStatus returns the HTTP status code for the error
func (e *RegistryError) HTTPStatus() int {
	switch e.Code {
	case "UNAUTHORIZED":
		return http.StatusUnauthorized
	case "DENIED":
		return http.StatusForbidden
	case "NAME_UNKNOWN", "BLOB_UNKNOWN", "MANIFEST_UNKNOWN":
		return http.StatusNotFound
	case "BLOB_UPLOAD_UNKNOWN":
		return http.StatusNotFound
	case "BLOB_UPLOAD_INVALID", "MANIFEST_INVALID":
		return http.StatusBadRequest
	case "RANGE_INVALID":
		return http.StatusRequestedRangeNotSatisfiable
	case "UNSUPPORTED":
		return http.StatusMethodNotAllowed
	default:
		return http.StatusInternalServerError
	}
}

// WriteError writes an error response in OCI Distribution Spec format
func WriteError(w http.ResponseWriter, err error) {
	var regErr *RegistryError
	if registryError, ok := err.(*RegistryError); ok {
		regErr = registryError
	} else {
		// Convert generic error to internal server error
		regErr = &RegistryError{
			Code:    "INTERNAL_ERROR",
			Message: "Internal server error",
			Detail:  err.Error(),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(regErr.HTTPStatus())
	json.NewEncoder(w).Encode(regErr)
}

// Error constructors following OCI Distribution Spec

// ErrNameUnknown returns a NAME_UNKNOWN error (404)
func ErrNameUnknown(name string) *RegistryError {
	return &RegistryError{
		Code:    "NAME_UNKNOWN",
		Message: "repository name not known to registry",
		Detail:  fmt.Sprintf("name: %s", name),
	}
}

// ErrBlobUnknown returns a BLOB_UNKNOWN error (404)
func ErrBlobUnknown(digest string) *RegistryError {
	return &RegistryError{
		Code:    "BLOB_UNKNOWN",
		Message: "blob unknown to registry",
		Detail:  fmt.Sprintf("digest: %s", digest),
	}
}

// ErrManifestUnknown returns a MANIFEST_UNKNOWN error (404)
func ErrManifestUnknown(reference string) *RegistryError {
	return &RegistryError{
		Code:    "MANIFEST_UNKNOWN",
		Message: "manifest unknown",
		Detail:  fmt.Sprintf("reference: %s", reference),
	}
}

// ErrUnauthorized returns an UNAUTHORIZED error (401)
func ErrUnauthorized(message string) *RegistryError {
	return &RegistryError{
		Code:    "UNAUTHORIZED",
		Message: "authentication required",
		Detail:  message,
	}
}

// ErrDenied returns a DENIED error (403)
func ErrDenied(message string) *RegistryError {
	return &RegistryError{
		Code:    "DENIED",
		Message: "requested access to the resource is denied",
		Detail:  message,
	}
}

// ErrUnsupported returns an UNSUPPORTED error (405)
func ErrUnsupported(message string) *RegistryError {
	return &RegistryError{
		Code:    "UNSUPPORTED",
		Message: "the operation is unsupported",
		Detail:  message,
	}
}

// ErrBlobUploadUnknown returns a BLOB_UPLOAD_UNKNOWN error (404)
func ErrBlobUploadUnknown(message string) *RegistryError {
	return &RegistryError{
		Code:    "BLOB_UPLOAD_UNKNOWN",
		Message: "blob upload unknown to registry",
		Detail:  message,
	}
}

// ErrBlobUploadInvalid returns a BLOB_UPLOAD_INVALID error (400)
func ErrBlobUploadInvalid(message string) *RegistryError {
	return &RegistryError{
		Code:    "BLOB_UPLOAD_INVALID",
		Message: "blob upload invalid",
		Detail:  message,
	}
}

// ErrManifestInvalid returns a MANIFEST_INVALID error (400)
func ErrManifestInvalid(message string) *RegistryError {
	return &RegistryError{
		Code:    "MANIFEST_INVALID",
		Message: "manifest invalid",
		Detail:  message,
	}
}
