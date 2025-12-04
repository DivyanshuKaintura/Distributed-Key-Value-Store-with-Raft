package main

import (
	"io"
	"net/http"
)

// GET - retrieve a value
func HandleGet(w http.ResponseWriter, key string) {
	mu.RLock()
	value, exists := store[key]
	mu.RUnlock()

	if !exists {
		SendJSON(w, http.StatusNotFound, Response{
			Success: false,
			Message: "key not found",
			Key:     key,
		})
		return
	}

	SendJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "value retrieved",
		Key:     key,
		Value:   value,
	})
}

// PUT - store a value
func HandlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil || len(body) == 0 {
		SendJSON(w, http.StatusBadRequest, Response{
			Success: false,
			Message: "value is required in request body",
		})
		return
	}
	value := string(body)

	// write to write ahead log
	err = WriteAheadLog("PUT", key, value)
	if err != nil {
		SendJSON(w, http.StatusInternalServerError, Response{
			Success: false,
			Message: "failed to write to log",
		})
		return
	}

	mu.Lock()
	store[key] = value
	mu.Unlock()

	// Save to disk after storing
	SaveToDisk()

	SendJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "value stored",
		Key:     key,
		Value:   value,
	})
}

// DELETE - delete a key
func HandleDelete(w http.ResponseWriter, key string) {
	mu.Lock()
	_, exists := store[key]
	if !exists {
		mu.Unlock()
		SendJSON(w, http.StatusNotFound, Response{
			Success: false,
			Message: "key not found",
			Key:     key,
		})
		return
	}

	// write to write ahead log
	err := WriteAheadLog("DELETE", key, "")
	if err != nil {
		SendJSON(w, http.StatusInternalServerError, Response{
			Success: false,
			Message: "failed to write to log",
		})
		return
	}

	delete(store, key)
	mu.Unlock()

	// Save to disk after deleting
	SaveToDisk()

	SendJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "key deleted",
		Key:     key,
	})
}
