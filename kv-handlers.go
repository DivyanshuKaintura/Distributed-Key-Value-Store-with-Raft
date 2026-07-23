package main

import (
	"io"
	"net/http"

	"github.com/DivyanshuKaintura/Distributed-Key-Value-Store-with-Raft/raft"
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

	// Ensure we're the leader; if not, instruct client to retry at leader
	if raftNode == nil || !raftNode.IsLeader() {
		leaderID := ""
		if raftNode != nil {
			leaderID = raftNode.GetLeaderID()
		}
		if leaderID == "" {
			RecordError()
			SendError(w, ErrServiceUnavailable, "no leader elected", key, "")
			return
		}
		RecordError()
		SendError(w, ErrNotLeader, "not leader", key, leaderID)
		return
	}

	// Propose the command to Raft (will be persisted and replicated)
	cmd := "PUT|" + key + "|" + value
	_, err = raftNode.Propose(cmd)
	if err != nil {
		RecordError()
		switch err {
		case raft.ErrNotLeader:
			leaderID := raftNode.GetLeaderID()
			SendError(w, ErrNotLeader, "not leader", key, leaderID)
		case raft.ErrTimeout:
			SendError(w, ErrRequestTimeout, "replication timeout", key, err.Error())
		default:
			LogError(ErrInternalServer, "proposal failed", key, err)
			SendError(w, ErrInternalServer, "proposal failed", key, err.Error())
		}
		return
	}

	// Success — the entry was committed and will be applied by applier
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

	// Ensure we're the leader; if not, instruct client to retry at leader
	if raftNode == nil || !raftNode.IsLeader() {
		mu.Unlock()
		leaderID := ""
		if raftNode != nil {
			leaderID = raftNode.GetLeaderID()
		}
		if leaderID == "" {
			RecordError()
			SendError(w, ErrServiceUnavailable, "no leader elected", key, "")
			return
		}
		RecordError()
		SendError(w, ErrNotLeader, "not leader", key, leaderID)
		return
	}

	// Propose delete command to Raft
	cmd := "DELETE|" + key
	_, err := raftNode.Propose(cmd)
	if err != nil {
		mu.Unlock()
		RecordError()
		switch err {
		case raft.ErrNotLeader:
			leaderID := raftNode.GetLeaderID()
			SendError(w, ErrNotLeader, "not leader", key, leaderID)
		case raft.ErrTimeout:
			SendError(w, ErrRequestTimeout, "replication timeout", key, err.Error())
		default:
			LogError(ErrInternalServer, "proposal failed", key, err)
			SendError(w, ErrInternalServer, "proposal failed", key, err.Error())
		}
		return
	}

	mu.Unlock()
	RecordSuccess()
	SendSuccess(w, "key deleted successfully", key, "")
}
