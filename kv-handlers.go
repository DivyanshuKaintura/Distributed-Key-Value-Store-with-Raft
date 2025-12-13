package main

import (
	"io"
	"net/http"
)

// GET - retrieve a value
func HandleGet(w http.ResponseWriter, key string) {
	RecordRequest()

	// Validate key
	if errCode, errMsg := ValidateKey(key); errCode != "" {
		RecordError()
		SendError(w, errCode, errMsg, key, "")
		return
	}

	mu.RLock()
	value, exists := store[key]
	mu.RUnlock()

	if !exists {
		RecordError()
		SendError(w, ErrKeyNotFound, "key does not exist", key, "")
		return
	}

	RecordSuccess()
	SendSuccess(w, "value retrieved successfully", key, value)
}

// PUT - store a value
func HandlePut(w http.ResponseWriter, r *http.Request, key string) {
	RecordRequest()

	// Validate key
	if errCode, errMsg := ValidateKey(key); errCode != "" {
		RecordError()
		SendError(w, errCode, errMsg, key, "")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		RecordError()
		LogError(ErrInternalServer, "failed to read request body", key, err)
		SendError(w, ErrInternalServer, "failed to read request body", key, err.Error())
		return
	}
	if len(body) == 0 {
		RecordError()
		SendError(w, ErrMissingBody, "value is required in request body", key, "")
		return
	}
	value := string(body)

	// Validate value
	if errCode, errMsg := ValidateValue(value); errCode != "" {
		RecordError()
		SendError(w, errCode, errMsg, key, "")
		return
	}

	// write to write ahead log
	err = WriteAheadLog("PUT", key, value)
	if err != nil {
		RecordError()
		LogError(ErrWALWriteFailed, "WAL write failed", key, err)
		SendError(w, ErrWALWriteFailed, "failed to write to write-ahead log", key, err.Error())
		return
	}

	mu.Lock()
	store[key] = value
	mu.Unlock()

	// Save to disk after storing
	SaveToDisk()

	RecordSuccess()
	SendSuccess(w, "value stored successfully", key, value)
}

// DELETE - delete a key
func HandleDelete(w http.ResponseWriter, key string) {
	RecordRequest()

	// Validate key
	if errCode, errMsg := ValidateKey(key); errCode != "" {
		RecordError()
		SendError(w, errCode, errMsg, key, "")
		return
	}

	mu.Lock()
	_, exists := store[key]
	if !exists {
		mu.Unlock()
		RecordError()
		SendError(w, ErrKeyNotFound, "key does not exist", key, "")
		return
	}

	// write to write ahead log
	err := WriteAheadLog("DELETE", key, "")
	if err != nil {
		mu.Unlock()
		RecordError()
		LogError(ErrWALWriteFailed, "WAL write failed", key, err)
		SendError(w, ErrWALWriteFailed, "failed to write to write-ahead log", key, err.Error())
		return
	}

	delete(store, key)
	mu.Unlock()

	// Save to disk after deleting
	SaveToDisk()

	RecordSuccess()
	SendSuccess(w, "key deleted successfully", key, "")
}
