package raft

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	pb "github.com/DivyanshuKaintura/Distributed-Key-Value-Store-with-Raft/proto"
	"google.golang.org/grpc"
)

// RaftGRPCServer implements the gRPC server for Raft inter-node communication
type RaftGRPCServer struct {
	pb.UnimplementedRaftServiceServer

	mu         sync.RWMutex
	raftNode   *RaftNode
	grpcServer *grpc.Server
	listener   net.Listener
	port       int
}

// NewRaftGRPCServer creates a new gRPC server for Raft communication
func NewRaftGRPCServer(raftNode *RaftNode, port int) *RaftGRPCServer {
	return &RaftGRPCServer{
		raftNode: raftNode,
		port:     port,
	}
}

// Start starts the gRPC server on the specified port
func (s *RaftGRPCServer) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.port, err)
	}
	s.listener = listener

	s.grpcServer = grpc.NewServer()
	pb.RegisterRaftServiceServer(s.grpcServer, s)

	log.Printf("[gRPC] Server starting on port %d", s.port)

	go func() {
		if err := s.grpcServer.Serve(listener); err != nil {
			log.Printf("[gRPC] Server error: %v", err)
		}
	}()

	return nil
}

// Stop gracefully stops the gRPC server
func (s *RaftGRPCServer) Stop() {
	if s.grpcServer != nil {
		log.Printf("[gRPC] Server stopping...")
		s.grpcServer.GracefulStop()
	}
	if s.listener != nil {
		s.listener.Close()
	}
}

// AppendEntries handles the AppendEntries RPC from the leader
// Used for log replication and heartbeats
func (s *RaftGRPCServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[gRPC] AppendEntries received from leader %s, term=%d, entries=%d",
		req.LeaderId, req.Term, len(req.Entries))

	// Get current state from raft node
	currentTerm := s.raftNode.GetCurrentTerm()

	// Rule 1: Reply false if term < currentTerm (§5.1)
	if req.Term < currentTerm {
		log.Printf("[gRPC] AppendEntries rejected: request term %d < current term %d", req.Term, currentTerm)
		return &pb.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}, nil
	}

	// Rule 2: If RPC request contains term > currentTerm, update currentTerm and convert to follower (§5.1)
	if req.Term > currentTerm {
		log.Printf("[gRPC] Discovered higher term %d, converting to follower", req.Term)
		s.raftNode.BecomeFollower(req.Term, req.LeaderId)
		currentTerm = req.Term
	}

	// Reset election timer - we heard from a valid leader
	s.raftNode.ResetElectionTimer()

	// Update leader ID
	s.raftNode.SetLeaderID(req.LeaderId)

	// Rule 3: Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if req.PrevLogIndex > 0 {
		prevEntry := s.raftNode.GetLogEntry(req.PrevLogIndex)
		if prevEntry == nil || prevEntry.Term != req.PrevLogTerm {
			log.Printf("[gRPC] AppendEntries rejected: log inconsistency at index %d", req.PrevLogIndex)
			return &pb.AppendEntriesResponse{
				Term:    currentTerm,
				Success: false,
			}, nil
		}
	}

	// Rule 4: If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	for i, entry := range req.Entries {
		index := req.PrevLogIndex + uint64(i) + 1
		existingEntry := s.raftNode.GetLogEntry(index)

		if existingEntry != nil {
			if existingEntry.Term != entry.Term {
				// Conflict: delete this entry and all following
				log.Printf("[gRPC] Log conflict at index %d, truncating", index)
				s.raftNode.TruncateLogFrom(index)
				// Append remaining entries
				s.raftNode.AppendLogEntries(req.Entries[i:])
				break
			}
			// Entry already exists and matches, skip
		} else {
			// Rule 5: Append any new entries not already in the log
			s.raftNode.AppendLogEntries(req.Entries[i:])
			break
		}
	}

	// Rule 6: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if req.LeaderCommit > s.raftNode.GetCommitIndex() {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		newCommitIndex := min(req.LeaderCommit, lastNewIndex)
		s.raftNode.SetCommitIndex(newCommitIndex)
		log.Printf("[gRPC] Updated commit index to %d", newCommitIndex)
	}

	log.Printf("[gRPC] AppendEntries successful")
	return &pb.AppendEntriesResponse{
		Term:    currentTerm,
		Success: true,
	}, nil
}

// RequestVote handles the RequestVote RPC from candidates
// Used for leader election
func (s *RaftGRPCServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[gRPC] RequestVote received from candidate %s, term=%d, lastLogIndex=%d, lastLogTerm=%d",
		req.CandidateId, req.Term, req.LastLogIndex, req.LastLogTerm)

	currentTerm := s.raftNode.GetCurrentTerm()
	votedFor := s.raftNode.GetVotedFor()

	// Rule 1: Reply false if term < currentTerm (§5.1)
	if req.Term < currentTerm {
		log.Printf("[gRPC] RequestVote rejected: request term %d < current term %d", req.Term, currentTerm)
		return &pb.RequestVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}, nil
	}

	// Rule 2: If RPC request contains term > currentTerm, update currentTerm and convert to follower (§5.1)
	if req.Term > currentTerm {
		log.Printf("[gRPC] Discovered higher term %d, converting to follower", req.Term)
		s.raftNode.BecomeFollower(req.Term, "")
		currentTerm = req.Term
		votedFor = "" // Reset votedFor for new term
	}

	// Rule 3: If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (§5.2, §5.4)
	voteGranted := false

	if votedFor == "" || votedFor == req.CandidateId {
		// Check if candidate's log is at least as up-to-date as ours
		if s.isCandidateLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
			voteGranted = true
			s.raftNode.SetVotedFor(req.CandidateId)
			s.raftNode.ResetElectionTimer() // Reset election timer when granting vote
			log.Printf("[gRPC] Vote granted to candidate %s", req.CandidateId)
		} else {
			log.Printf("[gRPC] RequestVote rejected: candidate's log is not up-to-date")
		}
	} else {
		log.Printf("[gRPC] RequestVote rejected: already voted for %s in term %d", votedFor, currentTerm)
	}

	return &pb.RequestVoteResponse{
		Term:        currentTerm,
		VoteGranted: voteGranted,
	}, nil
}

// isCandidateLogUpToDate checks if the candidate's log is at least as up-to-date as ours
// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries
// If the logs have last entries with different terms, then the log with the later term is more up-to-date
// If the logs end with the same term, then whichever log is longer is more up-to-date
func (s *RaftGRPCServer) isCandidateLogUpToDate(candidateLastIndex, candidateLastTerm uint64) bool {
	lastIndex, lastTerm := s.raftNode.GetLastLogInfo()

	// If candidate's last term is greater, their log is more up-to-date
	if candidateLastTerm > lastTerm {
		return true
	}

	// If terms are equal, check if candidate's log is at least as long
	if candidateLastTerm == lastTerm && candidateLastIndex >= lastIndex {
		return true
	}

	return false
}

// InstallSnapshot handles the InstallSnapshot RPC from the leader
// Used to send a snapshot to followers that are far behind
func (s *RaftGRPCServer) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[gRPC] InstallSnapshot received from leader %s, term=%d, lastIncludedIndex=%d",
		req.LeaderId, req.Term, req.LastIncludedIndex)

	currentTerm := s.raftNode.GetCurrentTerm()

	// Rule 1: Reply immediately if term < currentTerm
	if req.Term < currentTerm {
		log.Printf("[gRPC] InstallSnapshot rejected: request term %d < current term %d", req.Term, currentTerm)
		return &pb.InstallSnapshotResponse{
			Term: currentTerm,
		}, nil
	}

	// If RPC request contains term > currentTerm, update currentTerm and convert to follower
	if req.Term > currentTerm {
		log.Printf("[gRPC] Discovered higher term %d, converting to follower", req.Term)
		s.raftNode.BecomeFollower(req.Term, req.LeaderId)
		currentTerm = req.Term
	}

	// Reset election timer
	s.raftNode.ResetElectionTimer()
	s.raftNode.SetLeaderID(req.LeaderId)

	// Apply the snapshot
	err := s.raftNode.ApplySnapshot(req.LastIncludedIndex, req.LastIncludedTerm, req.Data)
	if err != nil {
		log.Printf("[gRPC] Failed to apply snapshot: %v", err)
		// Still return success to avoid leader retrying indefinitely
	}

	log.Printf("[gRPC] InstallSnapshot successful")
	return &pb.InstallSnapshotResponse{
		Term: currentTerm,
	}, nil
}
