package main

import (
	"encoding/json"
	"net/http"
)

func SendJSON(w http.ResponseWriter, statusCode int, data Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}
