package raft

import "errors"

// Raft errors
var (
	// ErrNotLeader is returned when a write operation is attempted on a non-leader node
	ErrNotLeader = errors.New("not leader")

	// ErrNoLeader is returned when there is no known leader in the cluster
	ErrNoLeader = errors.New("no leader elected")

	// ErrTimeout is returned when an operation times out
	ErrTimeout = errors.New("operation timed out")

	// ErrProposalFailed is returned when a proposal fails to be committed
	ErrProposalFailed = errors.New("proposal failed")

	// ErrStopped is returned when the node has been stopped
	ErrStopped = errors.New("raft node stopped")

	// ErrInvalidLogIndex is returned when an invalid log index is accessed
	ErrInvalidLogIndex = errors.New("invalid log index")
)
