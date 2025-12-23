package raft

import (
	"log"
	"sync"
	"time"

	pb "github.com/DivyanshuKaintura/Distributed-Key-Value-Store-with-Raft/proto"
)

// NodeState represents the current state of a Raft node
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command string
}

// RaftNode represents a node in the Raft cluster
// This is a stub implementation - full implementation will be in Step 4
type RaftNode struct {
	mu sync.RWMutex

	// Node identification
	nodeID   string
	leaderID string

	// Persistent state (saved to disk)
	currentTerm uint64
	votedFor    string
	log         []LogEntry

	// Volatile state (all servers)
	commitIndex uint64
	lastApplied uint64
	state       NodeState

	// Volatile state (leaders only)
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Timers
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	// Channels
	stopCh chan struct{}
}

// NewRaftNode creates a new Raft node
func NewRaftNode(nodeID string) *RaftNode {
	return &RaftNode{
		nodeID:      nodeID,
		state:       Follower,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		stopCh:      make(chan struct{}),
	}
}

// GetCurrentTerm returns the current term
func (rn *RaftNode) GetCurrentTerm() uint64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.currentTerm
}

// SetCurrentTerm sets the current term
func (rn *RaftNode) SetCurrentTerm(term uint64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.currentTerm = term
	// TODO: Persist to disk
}

// GetVotedFor returns who this node voted for in the current term
func (rn *RaftNode) GetVotedFor() string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.votedFor
}

// SetVotedFor sets who this node voted for
func (rn *RaftNode) SetVotedFor(candidateID string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.votedFor = candidateID
	// TODO: Persist to disk
}

// GetState returns the current state of the node
func (rn *RaftNode) GetState() NodeState {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state
}

// GetLeaderID returns the current leader ID
func (rn *RaftNode) GetLeaderID() string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.leaderID
}

// SetLeaderID sets the current leader ID
func (rn *RaftNode) SetLeaderID(leaderID string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.leaderID = leaderID
}

// GetCommitIndex returns the commit index
func (rn *RaftNode) GetCommitIndex() uint64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.commitIndex
}

// SetCommitIndex sets the commit index
func (rn *RaftNode) SetCommitIndex(index uint64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if index > rn.commitIndex {
		rn.commitIndex = index
		log.Printf("[Raft] Commit index updated to %d", index)
	}
}

// GetLastLogInfo returns the index and term of the last log entry
func (rn *RaftNode) GetLastLogInfo() (uint64, uint64) {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if len(rn.log) == 0 {
		return 0, 0
	}

	lastEntry := rn.log[len(rn.log)-1]
	return lastEntry.Index, lastEntry.Term
}

// GetLogEntry returns the log entry at the given index (1-based)
func (rn *RaftNode) GetLogEntry(index uint64) *LogEntry {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if index == 0 || index > uint64(len(rn.log)) {
		return nil
	}

	// Log indices are 1-based, slice indices are 0-based
	return &rn.log[index-1]
}

// TruncateLogFrom removes all log entries from the given index onwards
func (rn *RaftNode) TruncateLogFrom(index uint64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if index == 0 || index > uint64(len(rn.log)) {
		return
	}

	log.Printf("[Raft] Truncating log from index %d", index)
	rn.log = rn.log[:index-1]
	// TODO: Persist to disk
}

// AppendLogEntries appends new entries to the log
func (rn *RaftNode) AppendLogEntries(entries []*pb.LogEntry) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for _, entry := range entries {
		rn.log = append(rn.log, LogEntry{
			Term:    entry.Term,
			Index:   entry.Index,
			Command: entry.Command,
		})
		log.Printf("[Raft] Appended log entry: index=%d, term=%d, command=%s",
			entry.Index, entry.Term, entry.Command)
	}
	// TODO: Persist to disk
}

// BecomeFollower transitions the node to follower state
func (rn *RaftNode) BecomeFollower(term uint64, leaderID string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	log.Printf("[Raft] Becoming follower for term %d", term)

	rn.state = Follower
	rn.currentTerm = term
	rn.votedFor = "" // Reset vote for new term
	rn.leaderID = leaderID

	// TODO: Stop heartbeat ticker if we were leader
	// TODO: Persist currentTerm and votedFor to disk
}

// BecomeCandidate transitions the node to candidate state
func (rn *RaftNode) BecomeCandidate() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.currentTerm++
	rn.state = Candidate
	rn.votedFor = rn.nodeID // Vote for self

	log.Printf("[Raft] Becoming candidate for term %d", rn.currentTerm)
	// TODO: Persist currentTerm and votedFor to disk
}

// BecomeLeader transitions the node to leader state
func (rn *RaftNode) BecomeLeader() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	log.Printf("[Raft] Becoming leader for term %d", rn.currentTerm)

	rn.state = Leader
	rn.leaderID = rn.nodeID

	// Initialize leader volatile state
	// nextIndex for each server = last log index + 1
	// matchIndex for each server = 0
	lastIndex, _ := rn.getLastLogInfoLocked()
	for peerID := range rn.nextIndex {
		rn.nextIndex[peerID] = lastIndex + 1
		rn.matchIndex[peerID] = 0
	}

	// TODO: Start heartbeat ticker
	// TODO: Send initial empty AppendEntries to all peers
}

// getLastLogInfoLocked returns last log info (must be called with lock held)
func (rn *RaftNode) getLastLogInfoLocked() (uint64, uint64) {
	if len(rn.log) == 0 {
		return 0, 0
	}
	lastEntry := rn.log[len(rn.log)-1]
	return lastEntry.Index, lastEntry.Term
}

// ResetElectionTimer resets the election timer
func (rn *RaftNode) ResetElectionTimer() {
	// This will be fully implemented in Step 4
	// For now, just log that we would reset the timer
	log.Printf("[Raft] Election timer reset")
}

// ApplySnapshot applies a snapshot received from the leader
func (rn *RaftNode) ApplySnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	log.Printf("[Raft] Applying snapshot: lastIncludedIndex=%d, lastIncludedTerm=%d, dataSize=%d",
		lastIncludedIndex, lastIncludedTerm, len(data))

	// If existing log entry has same index and term as snapshot's last included entry,
	// retain log entries following it
	if lastIncludedIndex <= uint64(len(rn.log)) {
		entry := rn.log[lastIncludedIndex-1]
		if entry.Term == lastIncludedTerm {
			rn.log = rn.log[lastIncludedIndex:]
		} else {
			rn.log = make([]LogEntry, 0)
		}
	} else {
		// Discard the entire log
		rn.log = make([]LogEntry, 0)
	}

	// Update commit and applied indices
	if lastIncludedIndex > rn.commitIndex {
		rn.commitIndex = lastIncludedIndex
	}
	if lastIncludedIndex > rn.lastApplied {
		rn.lastApplied = lastIncludedIndex
	}

	// TODO: Apply snapshot data to state machine (KV store)
	// This will involve deserializing the data and loading it into the store

	return nil
}

// IsLeader returns true if this node is the leader
func (rn *RaftNode) IsLeader() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state == Leader
}

// GetNodeID returns the node's ID
func (rn *RaftNode) GetNodeID() string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.nodeID
}
