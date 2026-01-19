package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// PersistentState represents the Raft state that must survive restarts
// According to Raft paper, these must be persisted before responding to RPCs:
// - currentTerm: latest term server has seen
// - votedFor: candidateId that received vote in current term (or empty)
// - log[]: log entries (handled separately in log.go)
type PersistentState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

// RaftPersistence handles saving and loading Raft state to/from disk
type RaftPersistence struct {
	dataDir   string
	stateFile string
	logFile   string
}

// NewRaftPersistence creates a new persistence handler
func NewRaftPersistence(dataDir string) (*RaftPersistence, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	return &RaftPersistence{
		dataDir:   dataDir,
		stateFile: filepath.Join(dataDir, "raft_state.json"),
		logFile:   filepath.Join(dataDir, "raft_log.json"),
	}, nil
}

// SaveState persists the current term and votedFor to disk
// This must be called BEFORE responding to any RPC that changes these values
func (p *RaftPersistence) SaveState(currentTerm uint64, votedFor string) error {
	state := PersistentState{
		CurrentTerm: currentTerm,
		VotedFor:    votedFor,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	// Write to a temp file first, then rename for atomic write
	tempFile := p.stateFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	if err := os.Rename(tempFile, p.stateFile); err != nil {
		return fmt.Errorf("failed to rename state file: %w", err)
	}

	log.Printf("[Persistence] State saved: term=%d, votedFor=%s", currentTerm, votedFor)
	return nil
}

// LoadState loads the persisted state from disk
// Returns default values if file doesn't exist
func (p *RaftPersistence) LoadState() (currentTerm uint64, votedFor string, err error) {
	data, err := os.ReadFile(p.stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[Persistence] No existing state file, starting fresh")
			return 0, "", nil
		}
		return 0, "", fmt.Errorf("failed to read state file: %w", err)
	}

	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return 0, "", fmt.Errorf("failed to parse state file: %w", err)
	}

	log.Printf("[Persistence] State loaded: term=%d, votedFor=%s", state.CurrentTerm, state.VotedFor)
	return state.CurrentTerm, state.VotedFor, nil
}

// SaveLog persists the log entries to disk
func (p *RaftPersistence) SaveLog(entries []LogEntry) error {
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal log: %w", err)
	}

	// Atomic write
	tempFile := p.logFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write log file: %w", err)
	}

	if err := os.Rename(tempFile, p.logFile); err != nil {
		return fmt.Errorf("failed to rename log file: %w", err)
	}

	log.Printf("[Persistence] Log saved: %d entries", len(entries))
	return nil
}

// LoadLog loads persisted log entries from disk
func (p *RaftPersistence) LoadLog() ([]LogEntry, error) {
	data, err := os.ReadFile(p.logFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[Persistence] No existing log file, starting with empty log")
			return []LogEntry{}, nil
		}
		return nil, fmt.Errorf("failed to read log file: %w", err)
	}

	var entries []LogEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("failed to parse log file: %w", err)
	}

	log.Printf("[Persistence] Log loaded: %d entries", len(entries))
	return entries, nil
}

// SaveSnapshot saves a snapshot of the state machine
func (p *RaftPersistence) SaveSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error {
	snapshot := struct {
		LastIncludedIndex uint64 `json:"last_included_index"`
		LastIncludedTerm  uint64 `json:"last_included_term"`
		Data              []byte `json:"data"`
	}{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}

	snapshotData, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	snapshotFile := filepath.Join(p.dataDir, "snapshot.json")
	tempFile := snapshotFile + ".tmp"

	if err := os.WriteFile(tempFile, snapshotData, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot file: %w", err)
	}

	if err := os.Rename(tempFile, snapshotFile); err != nil {
		return fmt.Errorf("failed to rename snapshot file: %w", err)
	}

	log.Printf("[Persistence] Snapshot saved: lastIndex=%d, lastTerm=%d, size=%d",
		lastIncludedIndex, lastIncludedTerm, len(data))
	return nil
}

// LoadSnapshot loads a snapshot from disk
func (p *RaftPersistence) LoadSnapshot() (lastIncludedIndex, lastIncludedTerm uint64, data []byte, err error) {
	snapshotFile := filepath.Join(p.dataDir, "snapshot.json")
	fileData, err := os.ReadFile(snapshotFile)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil, nil
		}
		return 0, 0, nil, fmt.Errorf("failed to read snapshot file: %w", err)
	}

	var snapshot struct {
		LastIncludedIndex uint64 `json:"last_included_index"`
		LastIncludedTerm  uint64 `json:"last_included_term"`
		Data              []byte `json:"data"`
	}

	if err := json.Unmarshal(fileData, &snapshot); err != nil {
		return 0, 0, nil, fmt.Errorf("failed to parse snapshot file: %w", err)
	}

	log.Printf("[Persistence] Snapshot loaded: lastIndex=%d, lastTerm=%d, size=%d",
		snapshot.LastIncludedIndex, snapshot.LastIncludedTerm, len(snapshot.Data))
	return snapshot.LastIncludedIndex, snapshot.LastIncludedTerm, snapshot.Data, nil
}
