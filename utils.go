package main

import (
	"encoding/json"
	"net/http"
)

// SendJSON sends the old Response format (backward compatibility)
func SendJSON(w http.ResponseWriter, statusCode int, data Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// SendJSONError sends a structured error response
func SendJSONError(w http.ResponseWriter, statusCode int, data ErrorResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// SendJSONSuccess sends a structured success response
func SendJSONSuccess(w http.ResponseWriter, statusCode int, data SuccessResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}
