package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
)

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

var store = make(map[string]string)
var mu sync.RWMutex

// GET /get?key=mykey - Retrieve a value
func getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")

	if key == "" {
		sendJSON(w, http.StatusBadRequest, Response{
			Success: false,
			Message: "key is required",
		})
		return
	}

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

// GET /put?key=mykey&value=myvalue - Store a value
func putHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	if key == "" || value == "" {
		sendJSON(w, http.StatusBadRequest, Response{
			Success: false,
			Message: "key and value are required",
		})
		return
	}

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

// GET /delete?key=mykey - Delete a value
func deleteHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")

	if key == "" {
		sendJSON(w, http.StatusBadRequest, Response{
			Success: false,
			Message: "key is required",
		})
		return
	}

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

func mainHandler(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Welcome to the KV store",
	})
}

func main() {
	http.HandleFunc("/", mainHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/delete", deleteHandler)

	log.Println("KV store Running on http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}
