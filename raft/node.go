package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/DivyanshuKaintura/Distributed-Key-Value-Store-with-Raft/config"
	pb "github.com/DivyanshuKaintura/Distributed-Key-Value-Store-with-Raft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	Term    uint64 `json:"term"`
	Index   uint64 `json:"index"`
	Command string `json:"command"`
}

// ApplyMsg is sent to the state machine when an entry is committed
type ApplyMsg struct {
	CommandValid bool
	Command      string
	CommandIndex uint64
	CommandTerm  uint64
}

// PeerConnection represents a connection to a peer node
type PeerConnection struct {
	ID      string
	Address string
	Client  pb.RaftServiceClient
	Conn    *grpc.ClientConn
}

// RaftNode represents a node in the Raft cluster
// Implements the full Raft consensus algorithm
type RaftNode struct {
	mu sync.RWMutex

	// Node identification
	nodeID   string
	leaderID string

	// Cluster configuration
	peers       map[string]*PeerConnection
	peerAddrs   map[string]string // ID -> address
	clusterSize int

	// Persistent state (saved to disk before responding to RPCs)
	currentTerm uint64
	votedFor    string
	raftLog     *RaftLog

	// Volatile state (all servers)
	commitIndex uint64
	lastApplied uint64
	state       NodeState

	// Volatile state (leaders only, reinitialized after election)
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Timing configuration
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration

	// Timers
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	// Channels
	stopCh    chan struct{}
	applyCh   chan ApplyMsg // Channel to send committed entries to state machine
	proposeCh chan proposeRequest

	// Persistence
	persistence *RaftPersistence

	// State for tracking ongoing operations
	pendingProposals map[uint64]chan proposeResult
}

// proposeRequest represents a client request to append to the log
type proposeRequest struct {
	command  string
	resultCh chan proposeResult
}

// proposeResult is the result of a propose operation
type proposeResult struct {
	Success bool
	Index   uint64
	Term    uint64
	Error   string
}

// RaftConfig holds configuration for creating a RaftNode
type RaftConfig struct {
	NodeID             string
	Peers              map[string]string // ID -> gRPC address
	DataDir            string
	ElectionTimeoutMin int // milliseconds
	ElectionTimeoutMax int // milliseconds
	HeartbeatInterval  int // milliseconds
	ApplyCh            chan ApplyMsg
}

// NewRaftNodeWithConfig creates a new Raft node with the given configuration
func NewRaftNodeWithConfig(cfg *RaftConfig) (*RaftNode, error) {
	// Initialize persistence
	persistence, err := NewRaftPersistence(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	// Create raft log
	raftLog := NewRaftLog(persistence)

	// Load persisted state
	currentTerm, votedFor, err := persistence.LoadState()
	if err != nil {
		log.Printf("[Raft] Warning: failed to load persisted state: %v", err)
	}

	// Load persisted log
	if err := raftLog.LoadFromPersistence(); err != nil {
		log.Printf("[Raft] Warning: failed to load persisted log: %v", err)
	}

	rn := &RaftNode{
		nodeID:             cfg.NodeID,
		state:              Follower,
		currentTerm:        currentTerm,
		votedFor:           votedFor,
		raftLog:            raftLog,
		commitIndex:        0,
		lastApplied:        0,
		peers:              make(map[string]*PeerConnection),
		peerAddrs:          cfg.Peers,
		clusterSize:        len(cfg.Peers) + 1, // peers + self
		nextIndex:          make(map[string]uint64),
		matchIndex:         make(map[string]uint64),
		electionTimeoutMin: time.Duration(cfg.ElectionTimeoutMin) * time.Millisecond,
		electionTimeoutMax: time.Duration(cfg.ElectionTimeoutMax) * time.Millisecond,
		heartbeatInterval:  time.Duration(cfg.HeartbeatInterval) * time.Millisecond,
		stopCh:             make(chan struct{}),
		applyCh:            cfg.ApplyCh,
		proposeCh:          make(chan proposeRequest, 100),
		persistence:        persistence,
		pendingProposals:   make(map[uint64]chan proposeResult),
	}

	// Initialize peer connections (will connect lazily)
	for peerID, addr := range cfg.Peers {
		rn.peers[peerID] = &PeerConnection{
			ID:      peerID,
			Address: addr,
		}
		rn.nextIndex[peerID] = 1
		rn.matchIndex[peerID] = 0
	}

	log.Printf("[Raft] Node %s initialized: term=%d, votedFor=%s, logLen=%d, peers=%d",
		cfg.NodeID, currentTerm, votedFor, raftLog.Len(), len(cfg.Peers))

	return rn, nil
}

// NewRaftNode creates a new Raft node with default settings (for backward compatibility)
func NewRaftNode(nodeID string) *RaftNode {
	return &RaftNode{
		nodeID:             nodeID,
		state:              Follower,
		currentTerm:        0,
		votedFor:           "",
		raftLog:            NewRaftLog(nil),
		commitIndex:        0,
		lastApplied:        0,
		peers:              make(map[string]*PeerConnection),
		peerAddrs:          make(map[string]string),
		clusterSize:        1,
		nextIndex:          make(map[string]uint64),
		matchIndex:         make(map[string]uint64),
		electionTimeoutMin: 150 * time.Millisecond,
		electionTimeoutMax: 300 * time.Millisecond,
		heartbeatInterval:  50 * time.Millisecond,
		stopCh:             make(chan struct{}),
		proposeCh:          make(chan proposeRequest, 100),
		pendingProposals:   make(map[uint64]chan proposeResult),
	}
}

// NewRaftNodeFromConfig creates a RaftNode from config.Config
func NewRaftNodeFromConfig(cfg *config.Config, applyCh chan ApplyMsg) (*RaftNode, error) {
	peers := make(map[string]string)
	for _, peer := range cfg.Peers {
		peers[peer.ID] = peer.Address
	}

	raftCfg := &RaftConfig{
		NodeID:             cfg.NodeID,
		Peers:              peers,
		DataDir:            cfg.DataDir,
		ElectionTimeoutMin: cfg.ElectionTimeoutMin,
		ElectionTimeoutMax: cfg.ElectionTimeoutMax,
		HeartbeatInterval:  cfg.HeartbeatInterval,
		ApplyCh:            applyCh,
	}

	return NewRaftNodeWithConfig(raftCfg)
}

// Start begins the Raft main loop
func (rn *RaftNode) Start() {
	log.Printf("[Raft] Starting node %s", rn.nodeID)

	// Connect to peers
	rn.connectToPeers()

	// Start with a random election timeout
	rn.resetElectionTimer()

	// Start the main loop
	go rn.run()
}

// Stop gracefully stops the Raft node
func (rn *RaftNode) Stop() {
	log.Printf("[Raft] Stopping node %s", rn.nodeID)
	close(rn.stopCh)

	// Stop timers
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	if rn.heartbeatTicker != nil {
		rn.heartbeatTicker.Stop()
	}

	// Close peer connections
	rn.mu.Lock()
	for _, peer := range rn.peers {
		if peer.Conn != nil {
			peer.Conn.Close()
		}
	}
	rn.mu.Unlock()

	// Persist final state
	if rn.persistence != nil {
		rn.mu.RLock()
		term := rn.currentTerm
		votedFor := rn.votedFor
		rn.mu.RUnlock()
		rn.persistence.SaveState(term, votedFor)
		rn.raftLog.Persist()
	}
}

// run is the main Raft loop
func (rn *RaftNode) run() {
	for {
		select {
		case <-rn.stopCh:
			log.Printf("[Raft] Node %s stopped", rn.nodeID)
			return

		case <-rn.electionTimer.C:
			rn.handleElectionTimeout()

		case req := <-rn.proposeCh:
			rn.handlePropose(req)
		}
	}
}

// connectToPeers establishes gRPC connections to all peers
func (rn *RaftNode) connectToPeers() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for peerID, peer := range rn.peers {
		if peer.Conn != nil {
			continue // Already connected
		}

		conn, err := grpc.Dial(peer.Address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(2*time.Second),
		)
		if err != nil {
			log.Printf("[Raft] Failed to connect to peer %s at %s: %v", peerID, peer.Address, err)
			continue
		}

		peer.Conn = conn
		peer.Client = pb.NewRaftServiceClient(conn)
		log.Printf("[Raft] Connected to peer %s at %s", peerID, peer.Address)
	}
}

// randomElectionTimeout returns a random timeout between min and max
func (rn *RaftNode) randomElectionTimeout() time.Duration {
	diff := rn.electionTimeoutMax - rn.electionTimeoutMin
	return rn.electionTimeoutMin + time.Duration(rand.Int63n(int64(diff)))
}

// resetElectionTimer resets the election timer with a new random timeout
func (rn *RaftNode) resetElectionTimer() {
	timeout := rn.randomElectionTimeout()

	if rn.electionTimer == nil {
		rn.electionTimer = time.NewTimer(timeout)
	} else {
		// Stop and drain the timer before resetting
		if !rn.electionTimer.Stop() {
			select {
			case <-rn.electionTimer.C:
			default:
			}
		}
		rn.electionTimer.Reset(timeout)
	}

	log.Printf("[Raft] Election timer reset to %v", timeout)
}

// ResetElectionTimer is the public method to reset the election timer
func (rn *RaftNode) ResetElectionTimer() {
	rn.resetElectionTimer()
}

// handleElectionTimeout handles when the election timer fires
func (rn *RaftNode) handleElectionTimeout() {
	rn.mu.Lock()
	state := rn.state
	rn.mu.Unlock()

	if state == Leader {
		// Leaders don't start elections
		rn.resetElectionTimer()
		return
	}

	log.Printf("[Raft] Election timeout, starting election")
	rn.startElection()
}

// startElection begins a new leader election
func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	// Increment term and become candidate
	rn.currentTerm++
	rn.state = Candidate
	rn.votedFor = rn.nodeID
	currentTerm := rn.currentTerm
	lastLogIndex, lastLogTerm := rn.raftLog.GetLastLogInfo()

	// Persist state
	if rn.persistence != nil {
		rn.persistence.SaveState(rn.currentTerm, rn.votedFor)
	}
	rn.mu.Unlock()

	log.Printf("[Raft] Starting election for term %d", currentTerm)

	// Reset election timer
	rn.resetElectionTimer()

	// Count votes (start with 1 for self)
	votes := 1
	votesNeeded := (rn.clusterSize / 2) + 1
	voteMu := sync.Mutex{}

	// Request votes from all peers in parallel
	var wg sync.WaitGroup
	for peerID, peer := range rn.peers {
		if peer.Client == nil {
			// Try to reconnect
			conn, err := grpc.Dial(peer.Address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithTimeout(2*time.Second),
			)
			if err != nil {
				log.Printf("[Raft] Failed to connect to peer %s: %v", peerID, err)
				continue
			}
			peer.Conn = conn
			peer.Client = pb.NewRaftServiceClient(conn)
		}

		wg.Add(1)
		go func(peerID string, client pb.RaftServiceClient) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			req := &pb.RequestVoteRequest{
				Term:         currentTerm,
				CandidateId:  rn.nodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			resp, err := client.RequestVote(ctx, req)
			if err != nil {
				log.Printf("[Raft] RequestVote to %s failed: %v", peerID, err)
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			// Check if we're still a candidate for this term
			if rn.state != Candidate || rn.currentTerm != currentTerm {
				return
			}

			// If response contains higher term, become follower
			if resp.Term > rn.currentTerm {
				log.Printf("[Raft] Discovered higher term %d from %s, becoming follower", resp.Term, peerID)
				rn.becomeFollowerLocked(resp.Term, "")
				return
			}

			// Count vote
			if resp.VoteGranted {
				voteMu.Lock()
				votes++
				currentVotes := votes
				voteMu.Unlock()

				log.Printf("[Raft] Received vote from %s, total votes: %d/%d", peerID, currentVotes, votesNeeded)

				// Check if we have majority
				if currentVotes >= votesNeeded && rn.state == Candidate {
					log.Printf("[Raft] Won election with %d votes", currentVotes)
					rn.becomeLeaderLocked()
				}
			}
		}(peerID, peer.Client)
	}

	// Wait for all vote requests to complete (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(rn.electionTimeoutMin):
		log.Printf("[Raft] Election timed out")
	}

	// Check if we won (might have already transitioned)
	rn.mu.Lock()
	if rn.state == Candidate && votes >= votesNeeded {
		log.Printf("[Raft] Won election with %d votes (checked after wait)", votes)
		rn.becomeLeaderLocked()
	}
	rn.mu.Unlock()
}

// becomeFollowerLocked transitions to follower state (must hold lock)
func (rn *RaftNode) becomeFollowerLocked(term uint64, leaderID string) {
	log.Printf("[Raft] Becoming follower for term %d, leader=%s", term, leaderID)

	prevState := rn.state
	rn.state = Follower
	rn.currentTerm = term
	rn.votedFor = ""
	if leaderID != "" {
		rn.leaderID = leaderID
	}

	// Stop heartbeat ticker if we were leader
	if prevState == Leader && rn.heartbeatTicker != nil {
		rn.heartbeatTicker.Stop()
		rn.heartbeatTicker = nil
	}

	// Persist state
	if rn.persistence != nil {
		go rn.persistence.SaveState(term, "")
	}
}

// BecomeFollower transitions to follower state (public method)
func (rn *RaftNode) BecomeFollower(term uint64, leaderID string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.becomeFollowerLocked(term, leaderID)
}

// becomeLeaderLocked transitions to leader state (must hold lock)
func (rn *RaftNode) becomeLeaderLocked() {
	if rn.state == Leader {
		return // Already leader
	}

	log.Printf("[Raft] Becoming leader for term %d", rn.currentTerm)

	rn.state = Leader
	rn.leaderID = rn.nodeID

	// Initialize leader volatile state
	lastIndex := rn.raftLog.GetLastIndex()
	for peerID := range rn.peers {
		rn.nextIndex[peerID] = lastIndex + 1
		rn.matchIndex[peerID] = 0
	}

	// Stop election timer
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}

	// Start heartbeat ticker
	rn.heartbeatTicker = time.NewTicker(rn.heartbeatInterval)
	go rn.runHeartbeats()

	// Send initial heartbeat immediately
	go rn.sendHeartbeats()
}

// BecomeLeader transitions to leader state (public method)
func (rn *RaftNode) BecomeLeader() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.becomeLeaderLocked()
}

// BecomeCandidate transitions to candidate state
func (rn *RaftNode) BecomeCandidate() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.currentTerm++
	rn.state = Candidate
	rn.votedFor = rn.nodeID

	log.Printf("[Raft] Becoming candidate for term %d", rn.currentTerm)

	if rn.persistence != nil {
		go rn.persistence.SaveState(rn.currentTerm, rn.votedFor)
	}
}

// runHeartbeats sends periodic heartbeats while leader
func (rn *RaftNode) runHeartbeats() {
	for {
		select {
		case <-rn.stopCh:
			return
		case <-rn.heartbeatTicker.C:
			rn.mu.RLock()
			isLeader := rn.state == Leader
			rn.mu.RUnlock()

			if !isLeader {
				return
			}

			rn.sendHeartbeats()
		}
	}
}

// sendHeartbeats sends AppendEntries RPCs to all peers
func (rn *RaftNode) sendHeartbeats() {
	rn.mu.RLock()
	if rn.state != Leader {
		rn.mu.RUnlock()
		return
	}

	currentTerm := rn.currentTerm
	leaderCommit := rn.commitIndex
	rn.mu.RUnlock()

	for peerID, peer := range rn.peers {
		go rn.sendAppendEntries(peerID, peer, currentTerm, leaderCommit)
	}
}

// sendAppendEntries sends AppendEntries RPC to a single peer
func (rn *RaftNode) sendAppendEntries(peerID string, peer *PeerConnection, term uint64, leaderCommit uint64) {
	if peer.Client == nil {
		// Try to reconnect
		conn, err := grpc.Dial(peer.Address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithTimeout(2*time.Second),
		)
		if err != nil {
			return
		}
		peer.Conn = conn
		peer.Client = pb.NewRaftServiceClient(conn)
	}

	rn.mu.RLock()
	nextIdx := rn.nextIndex[peerID]
	prevLogIndex := nextIdx - 1
	prevLogTerm := rn.raftLog.GetTerm(prevLogIndex)
	entries := rn.raftLog.GetEntriesFrom(nextIdx)
	rn.mu.RUnlock()

	req := &pb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     rn.nodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	resp, err := peer.Client.AppendEntries(ctx, req)
	if err != nil {
		log.Printf("[Raft] AppendEntries to %s failed: %v", peerID, err)
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Check if we're still leader for this term
	if rn.state != Leader || rn.currentTerm != term {
		return
	}

	// If response contains higher term, become follower
	if resp.Term > rn.currentTerm {
		log.Printf("[Raft] Discovered higher term %d from %s, becoming follower", resp.Term, peerID)
		rn.becomeFollowerLocked(resp.Term, "")
		return
	}

	if resp.Success {
		// Update nextIndex and matchIndex for this peer
		if len(entries) > 0 {
			newMatchIndex := entries[len(entries)-1].Index
			rn.matchIndex[peerID] = newMatchIndex
			rn.nextIndex[peerID] = newMatchIndex + 1
			log.Printf("[Raft] AppendEntries to %s succeeded, matchIndex=%d", peerID, newMatchIndex)

			// Try to advance commit index
			rn.advanceCommitIndexLocked()
		}
	} else {
		// Decrement nextIndex and retry
		if rn.nextIndex[peerID] > 1 {
			rn.nextIndex[peerID]--
			log.Printf("[Raft] AppendEntries to %s failed, decremented nextIndex to %d", peerID, rn.nextIndex[peerID])
		}
	}
}

// advanceCommitIndexLocked tries to advance the commit index (must hold lock)
func (rn *RaftNode) advanceCommitIndexLocked() {
	// Find the highest N such that:
	// 1. N > commitIndex
	// 2. A majority of matchIndex[i] >= N
	// 3. log[N].term == currentTerm (leader only commits from current term)

	lastIndex := rn.raftLog.GetLastIndex()

	for n := lastIndex; n > rn.commitIndex; n-- {
		if rn.raftLog.GetTerm(n) != rn.currentTerm {
			continue // Can only commit entries from current term
		}

		// Count replicas (including self)
		replicaCount := 1 // self
		for peerID := range rn.peers {
			if rn.matchIndex[peerID] >= n {
				replicaCount++
			}
		}

		// Check if majority
		if replicaCount > rn.clusterSize/2 {
			log.Printf("[Raft] Advancing commit index from %d to %d (replicaCount=%d/%d)",
				rn.commitIndex, n, replicaCount, rn.clusterSize)
			rn.commitIndex = n
			rn.applyCommittedEntries()
			break
		}
	}
}

// applyCommittedEntries applies all committed but not yet applied entries
func (rn *RaftNode) applyCommittedEntries() {
	// This should be called with lock held or in a safe context
	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry := rn.raftLog.GetEntry(rn.lastApplied)
		if entry == nil {
			log.Printf("[Raft] Warning: entry at index %d not found", rn.lastApplied)
			continue
		}

		log.Printf("[Raft] Applying entry: index=%d, term=%d, command=%s",
			entry.Index, entry.Term, entry.Command)

		// Send to state machine if channel is available
		if rn.applyCh != nil {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}

			// Non-blocking send
			select {
			case rn.applyCh <- msg:
			default:
				log.Printf("[Raft] Warning: apply channel full, dropping entry %d", entry.Index)
			}
		}

		// Complete any pending proposals
		if ch, ok := rn.pendingProposals[entry.Index]; ok {
			select {
			case ch <- proposeResult{Success: true, Index: entry.Index, Term: entry.Term}:
			default:
			}
			delete(rn.pendingProposals, entry.Index)
		}
	}
}

// handlePropose handles a client proposal to append to the log
func (rn *RaftNode) handlePropose(req proposeRequest) {
	rn.mu.Lock()

	if rn.state != Leader {
		rn.mu.Unlock()
		req.resultCh <- proposeResult{
			Success: false,
			Error:   "not leader",
		}
		return
	}

	// Append to local log
	entry := rn.raftLog.Append(rn.currentTerm, req.command)

	// Track pending proposal
	rn.pendingProposals[entry.Index] = req.resultCh

	log.Printf("[Raft] Proposed entry: index=%d, term=%d, command=%s",
		entry.Index, entry.Term, req.command)

	// Persist log
	if rn.persistence != nil {
		go rn.raftLog.Persist()
	}

	currentTerm := rn.currentTerm
	leaderCommit := rn.commitIndex
	rn.mu.Unlock()

	// Send to all followers
	for peerID, peer := range rn.peers {
		go rn.sendAppendEntries(peerID, peer, currentTerm, leaderCommit)
	}
}

// Propose proposes a command to be replicated (blocks until committed or error)
func (rn *RaftNode) Propose(command string) (uint64, error) {
	resultCh := make(chan proposeResult, 1)

	select {
	case rn.proposeCh <- proposeRequest{command: command, resultCh: resultCh}:
	case <-time.After(5 * time.Second):
		return 0, ErrTimeout
	}

	select {
	case result := <-resultCh:
		if result.Success {
			return result.Index, nil
		}
		if result.Error == "not leader" {
			return 0, ErrNotLeader
		}
		return 0, ErrProposalFailed
	case <-time.After(5 * time.Second):
		return 0, ErrTimeout
	}
}

// === Getter methods ===

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
	if rn.persistence != nil {
		go rn.persistence.SaveState(term, rn.votedFor)
	}
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
	if rn.persistence != nil {
		go rn.persistence.SaveState(rn.currentTerm, candidateID)
	}
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
		rn.applyCommittedEntries()
	}
}

// GetLastLogInfo returns the index and term of the last log entry
func (rn *RaftNode) GetLastLogInfo() (uint64, uint64) {
	return rn.raftLog.GetLastLogInfo()
}

// GetLogEntry returns the log entry at the given index (1-based)
func (rn *RaftNode) GetLogEntry(index uint64) *LogEntry {
	return rn.raftLog.GetEntry(index)
}

// TruncateLogFrom removes all log entries from the given index onwards
func (rn *RaftNode) TruncateLogFrom(index uint64) {
	rn.raftLog.TruncateFrom(index)
	if rn.persistence != nil {
		go rn.raftLog.Persist()
	}
}

// AppendLogEntries appends new entries to the log (from leader)
func (rn *RaftNode) AppendLogEntries(entries []*pb.LogEntry) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for _, entry := range entries {
		rn.raftLog.Append(entry.Term, entry.Command)
	}

	if rn.persistence != nil {
		go rn.raftLog.Persist()
	}
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

// ApplySnapshot applies a snapshot received from the leader
func (rn *RaftNode) ApplySnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	log.Printf("[Raft] Applying snapshot: lastIncludedIndex=%d, lastIncludedTerm=%d, dataSize=%d",
		lastIncludedIndex, lastIncludedTerm, len(data))

	// Compact the log
	rn.raftLog.CompactUntil(lastIncludedIndex, lastIncludedTerm)

	// Update commit and applied indices
	if lastIncludedIndex > rn.commitIndex {
		rn.commitIndex = lastIncludedIndex
	}
	if lastIncludedIndex > rn.lastApplied {
		rn.lastApplied = lastIncludedIndex
	}

	// Save snapshot to disk
	if rn.persistence != nil {
		if err := rn.persistence.SaveSnapshot(lastIncludedIndex, lastIncludedTerm, data); err != nil {
			log.Printf("[Raft] Failed to save snapshot: %v", err)
		}
	}

	// TODO: Apply snapshot data to state machine (KV store)
	// This will involve deserializing the data and loading it into the store

	return nil
}

// === Log compatibility methods (for grpc_server.go) ===

// These provide compatibility with the existing grpc_server.go implementation
// which uses slightly different method signatures

// GetLog returns the raft log (for internal use)
func (rn *RaftNode) GetLog() *RaftLog {
	return rn.raftLog
}
