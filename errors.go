package main

import (
	"fmt"
	"net/http"
	"time"
)

// ============== Error Types ==============

// ErrorCode represents a specific error condition
// Using string constants makes errors self-documenting
type ErrorCode string

const (
	// Client errors (4xx)
	ErrKeyNotFound      ErrorCode = "KEY_NOT_FOUND"      // 404
	ErrInvalidKey       ErrorCode = "INVALID_KEY"        // 400
	ErrInvalidValue     ErrorCode = "INVALID_VALUE"      // 400
	ErrMissingBody      ErrorCode = "MISSING_BODY"       // 400
	ErrMethodNotAllowed ErrorCode = "METHOD_NOT_ALLOWED" // 405
	ErrRequestTimeout   ErrorCode = "REQUEST_TIMEOUT"    // 408

	// Server errors (5xx)
	ErrInternalServer     ErrorCode = "INTERNAL_SERVER"     // 500
	ErrWALWriteFailed     ErrorCode = "WAL_WRITE_FAILED"    // 500
	ErrDiskWriteFailed    ErrorCode = "DISK_WRITE_FAILED"   // 500
	ErrServiceUnavailable ErrorCode = "SERVICE_UNAVAILABLE" // 503

	// Raft-specific errors (for later use)
	ErrNotLeader ErrorCode = "NOT_LEADER" // 503
	ErrNoQuorum  ErrorCode = "NO_QUORUM"  // 503
	ErrStaleRead ErrorCode = "STALE_READ" // 409
)

// ============== Enhanced Response Structure ==============

// ErrorResponse is a structured error response with rich context
// This replaces the generic Response struct for errors
type ErrorResponse struct {
	Success   bool      `json:"success"`              // Always false for errors
	ErrorCode ErrorCode `json:"error_code"`           // Machine-readable error code
	Message   string    `json:"message"`              // Human-readable error message
	Details   string    `json:"details,omitempty"`    // Additional context (optional)
	Key       string    `json:"key,omitempty"`        // Affected key (if applicable)
	Timestamp string    `json:"timestamp"`            // When error occurred
	RequestID string    `json:"request_id,omitempty"` // For tracing (future)
}

// SuccessResponse is for successful operations
type SuccessResponse struct {
	Success bool   `json:"success"`         // Always true
	Message string `json:"message"`         // Human-readable message
	Key     string `json:"key,omitempty"`   // Affected key
	Value   string `json:"value,omitempty"` // Returned value (for GET)
}

// ============== Error Constructor ==============

// NewErrorResponse creates a structured error response
func NewErrorResponse(code ErrorCode, message string, key string, details string) ErrorResponse {
	return ErrorResponse{
		Success:   false,
		ErrorCode: code,
		Message:   message,
		Details:   details,
		Key:       key,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
}

// ============== HTTP Status Code Mapping ==============

// HTTPStatusForError maps an ErrorCode to appropriate HTTP status code
// This ensures consistent status codes across the application
func HTTPStatusForError(code ErrorCode) int {
	switch code {
	// 4xx Client Errors
	case ErrKeyNotFound:
		return http.StatusNotFound // 404
	case ErrInvalidKey, ErrInvalidValue, ErrMissingBody:
		return http.StatusBadRequest // 400
	case ErrMethodNotAllowed:
		return http.StatusMethodNotAllowed // 405
	case ErrRequestTimeout:
		return http.StatusRequestTimeout // 408
	case ErrStaleRead:
		return http.StatusConflict // 409

	// 5xx Server Errors
	case ErrWALWriteFailed, ErrDiskWriteFailed, ErrInternalServer:
		return http.StatusInternalServerError // 500
	case ErrServiceUnavailable, ErrNotLeader, ErrNoQuorum:
		return http.StatusServiceUnavailable // 503

	default:
		return http.StatusInternalServerError // 500 (safe default)
	}
}

// ============== Error Response Helpers ==============

// SendError sends a structured error response
// This is the main function you'll use in handlers
func SendError(w http.ResponseWriter, code ErrorCode, message string, key string, details string) {
	statusCode := HTTPStatusForError(code)
	errResp := NewErrorResponse(code, message, key, details)
	SendJSONError(w, statusCode, errResp)
}

// SendSuccess sends a structured success response
func SendSuccess(w http.ResponseWriter, message string, key string, value string) {
	resp := SuccessResponse{
		Success: true,
		Message: message,
		Key:     key,
		Value:   value,
	}
	SendJSONSuccess(w, http.StatusOK, resp)
}

// ============== Error Logging ==============

// LogError logs an error with context for debugging
// In production, this would integrate with your logging system
func LogError(code ErrorCode, message string, details string, err error) {
	if err != nil {
		fmt.Printf("[ERROR] %s: %s (details: %s, underlying: %v)\n",
			code, message, details, err)
	} else {
		fmt.Printf("[ERROR] %s: %s (details: %s)\n",
			code, message, details)
	}
}

// ============== Validation Helpers ==============

// ValidateKey checks if a key is valid
// Returns error code and message if invalid, empty strings if valid
func ValidateKey(key string) (ErrorCode, string) {
	if key == "" {
		return ErrInvalidKey, "key cannot be empty"
	}
	if len(key) > 256 {
		return ErrInvalidKey, "key too long (max 256 characters)"
	}
	// Add more validation as needed (e.g., allowed characters)
	return "", ""
}

// ValidateValue checks if a value is valid
func ValidateValue(value string) (ErrorCode, string) {
	if len(value) > 1024*1024 { // 1MB limit
		return ErrInvalidValue, "value too large (max 1MB)"
	}
	return "", ""
}

// ============== Error Wrapping (Go 1.13+) ==============

// WrapError wraps an error with additional context
// This preserves the error chain for debugging
func WrapError(err error, context string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", context, err)
}
