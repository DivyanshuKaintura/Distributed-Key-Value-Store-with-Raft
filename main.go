package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
)

// ============== In-Memory Store ==============

var store = make(map[string]string)
var mu sync.RWMutex

// ============== Response Helpers ==============

type Response struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Key     string `json:"key,omitempty"`
	Value   string `json:"value,omitempty"`
}

func sendJSON(w http.ResponseWriter, statusCode int, data Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// ============== KV Handler ==============

// kvHandler handles all /kv/{key} requests
// GET    /kv/{key}         - retrieve value
// PUT    /kv/{key}         - store value (body = value)
// DELETE /kv/{key}         - delete key
func kvHandler(w http.ResponseWriter, r *http.Request) {
	// Extract key from URL path: /kv/{key}
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	log.Println("Received", r.Method, "request for key:", key)

	if key == "" {
		sendJSON(w, http.StatusBadRequest, Response{
			Success: false,
			Message: "key is required in path: /kv/{key}",
		})
		return
	}

	switch r.Method {
	case http.MethodGet:
		handleGet(w, key)
	case http.MethodPut:
		handlePut(w, r, key)
	case http.MethodDelete:
		handleDelete(w, key)
	default:
		sendJSON(w, http.StatusMethodNotAllowed, Response{
			Success: false,
			Message: "method not allowed, use GET, PUT, or DELETE",
		})
	}
}

// GET - retrieve a value
func handleGet(w http.ResponseWriter, key string) {
	mu.RLock()
	value, exists := store[key]
	mu.RUnlock()

	if !exists {
		sendJSON(w, http.StatusNotFound, Response{
			Success: false,
			Message: "key not found",
			Key:     key,
		})
		return
	}

	sendJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "value retrieved",
		Key:     key,
		Value:   value,
	})
}

// PUT - store a value
func handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil || len(body) == 0 {
		sendJSON(w, http.StatusBadRequest, Response{
			Success: false,
			Message: "value is required in request body",
		})
		return
	}
	value := string(body)

	mu.Lock()
	store[key] = value
	mu.Unlock()

	sendJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "value stored",
		Key:     key,
		Value:   value,
	})
}

// DELETE - delete a key
func handleDelete(w http.ResponseWriter, key string) {
	mu.Lock()
	_, exists := store[key]
	if !exists {
		mu.Unlock()
		sendJSON(w, http.StatusNotFound, Response{
			Success: false,
			Message: "key not found",
			Key:     key,
		})
		return
	}
	delete(store, key)
	mu.Unlock()

	sendJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "key deleted",
		Key:     key,
	})
}

// ============== Main ==============

func main() {
	http.HandleFunc("/kv/", kvHandler)

	log.Println("KV Store running on http://localhost:8080")
	log.Println("Usage:")
	log.Println("  PUT    curl -X PUT -d 'value' http://localhost:8080/kv/mykey")
	log.Println("  GET    curl http://localhost:8080/kv/mykey")
	log.Println("  DELETE curl -X DELETE http://localhost:8080/kv/mykey")
	http.ListenAndServe(":8080", nil)
}
