package raft

import (
	"fmt"
	"log"
	"sync"

	pb "github.com/DivyanshuKaintura/Distributed-Key-Value-Store-with-Raft/proto"
)

// RaftLog manages the Raft log entries
// The log is 1-indexed (first entry is at index 1)
type RaftLog struct {
	mu      sync.RWMutex
	entries []LogEntry

	// Snapshot state
	snapshotLastIndex uint64
	snapshotLastTerm  uint64

	// Persistence
	persistence *RaftPersistence
}

// NewRaftLog creates a new Raft log
func NewRaftLog(persistence *RaftPersistence) *RaftLog {
	return &RaftLog{
		entries:     make([]LogEntry, 0),
		persistence: persistence,
	}
}

// LoadFromPersistence loads log entries from disk
func (l *RaftLog) LoadFromPersistence() error {
	if l.persistence == nil {
		return nil
	}

	entries, err := l.persistence.LoadLog()
	if err != nil {
		return err
	}

	l.mu.Lock()
	l.entries = entries
	l.mu.Unlock()

	return nil
}

// Persist saves the log to disk
func (l *RaftLog) Persist() error {
	if l.persistence == nil {
		return nil
	}

	l.mu.RLock()
	entries := make([]LogEntry, len(l.entries))
	copy(entries, l.entries)
	l.mu.RUnlock()

	return l.persistence.SaveLog(entries)
}

// GetLastIndex returns the index of the last log entry
// Returns 0 if log is empty
func (l *RaftLog) GetLastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.entries) == 0 {
		return l.snapshotLastIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// GetLastTerm returns the term of the last log entry
// Returns 0 if log is empty
func (l *RaftLog) GetLastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.entries) == 0 {
		return l.snapshotLastTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// GetLastLogInfo returns both index and term of the last entry
func (l *RaftLog) GetLastLogInfo() (index uint64, term uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.entries) == 0 {
		return l.snapshotLastIndex, l.snapshotLastTerm
	}
	lastEntry := l.entries[len(l.entries)-1]
	return lastEntry.Index, lastEntry.Term
}

// GetEntry returns the log entry at the given index (1-based)
// Returns nil if index is out of range
func (l *RaftLog) GetEntry(index uint64) *LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.getEntryLocked(index)
}

// getEntryLocked returns entry without acquiring lock (caller must hold lock)
func (l *RaftLog) getEntryLocked(index uint64) *LogEntry {
	if index == 0 {
		return nil
	}

	// Account for snapshot
	if index <= l.snapshotLastIndex {
		return nil // Entry is in snapshot
	}

	// Adjust index for snapshot
	adjustedIndex := index - l.snapshotLastIndex - 1

	if adjustedIndex >= uint64(len(l.entries)) {
		return nil
	}

	return &l.entries[adjustedIndex]
}

// GetTerm returns the term of the entry at the given index
// Returns 0 if index is 0 or out of range
func (l *RaftLog) GetTerm(index uint64) uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == 0 {
		return 0
	}

	if index == l.snapshotLastIndex {
		return l.snapshotLastTerm
	}

	entry := l.getEntryLocked(index)
	if entry == nil {
		return 0
	}
	return entry.Term
}

// Append adds a new entry to the log
func (l *RaftLog) Append(term uint64, command string) LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	index := l.snapshotLastIndex + uint64(len(l.entries)) + 1
	entry := LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}

	l.entries = append(l.entries, entry)
	log.Printf("[RaftLog] Appended entry: index=%d, term=%d, command=%s", index, term, command)

	return entry
}

// AppendEntries appends multiple entries from a leader
// Returns true if successful
func (l *RaftLog) AppendEntries(prevLogIndex, prevLogTerm uint64, entries []*pb.LogEntry) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if log matches at prevLogIndex
	if prevLogIndex > 0 {
		if prevLogIndex == l.snapshotLastIndex {
			if prevLogTerm != l.snapshotLastTerm {
				log.Printf("[RaftLog] Log mismatch at prevLogIndex %d (in snapshot)", prevLogIndex)
				return false
			}
		} else {
			prevEntry := l.getEntryLocked(prevLogIndex)
			if prevEntry == nil || prevEntry.Term != prevLogTerm {
				log.Printf("[RaftLog] Log mismatch at prevLogIndex %d", prevLogIndex)
				return false
			}
		}
	}

	// Process each entry
	for i, entry := range entries {
		index := prevLogIndex + uint64(i) + 1
		existingEntry := l.getEntryLocked(index)

		if existingEntry != nil {
			if existingEntry.Term != entry.Term {
				// Conflict: delete this entry and all following
				log.Printf("[RaftLog] Conflict at index %d, truncating log", index)
				l.truncateFromLocked(index)
				// Append this and remaining entries
				l.appendEntriesLocked(entries[i:])
				break
			}
			// Entry matches, skip it
		} else {
			// New entry, append it and all following
			l.appendEntriesLocked(entries[i:])
			break
		}
	}

	return true
}

// appendEntriesLocked appends entries without acquiring lock
func (l *RaftLog) appendEntriesLocked(entries []*pb.LogEntry) {
	for _, entry := range entries {
		l.entries = append(l.entries, LogEntry{
			Term:    entry.Term,
			Index:   entry.Index,
			Command: entry.Command,
		})
		log.Printf("[RaftLog] Appended entry from leader: index=%d, term=%d", entry.Index, entry.Term)
	}
}

// TruncateFrom removes all entries from the given index onwards
func (l *RaftLog) TruncateFrom(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.truncateFromLocked(index)
}

// truncateFromLocked truncates without acquiring lock
func (l *RaftLog) truncateFromLocked(index uint64) {
	if index <= l.snapshotLastIndex {
		l.entries = make([]LogEntry, 0)
		return
	}

	adjustedIndex := index - l.snapshotLastIndex - 1
	if adjustedIndex < uint64(len(l.entries)) {
		l.entries = l.entries[:adjustedIndex]
		log.Printf("[RaftLog] Truncated log from index %d, new length=%d", index, len(l.entries))
	}
}

// GetEntriesFrom returns all entries starting from the given index
func (l *RaftLog) GetEntriesFrom(startIndex uint64) []*pb.LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if startIndex <= l.snapshotLastIndex {
		// Need to send snapshot instead
		return nil
	}

	var result []*pb.LogEntry
	adjustedStart := startIndex - l.snapshotLastIndex - 1

	for i := int(adjustedStart); i < len(l.entries); i++ {
		entry := l.entries[i]
		result = append(result, &pb.LogEntry{
			Term:    entry.Term,
			Index:   entry.Index,
			Command: entry.Command,
		})
	}

	return result
}

// GetEntryRange returns entries in the given range [start, end]
func (l *RaftLog) GetEntryRange(startIndex, endIndex uint64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var result []LogEntry
	for i := startIndex; i <= endIndex; i++ {
		entry := l.getEntryLocked(i)
		if entry != nil {
			result = append(result, *entry)
		}
	}
	return result
}

// Len returns the number of entries in the log (excluding snapshot)
func (l *RaftLog) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

// String returns a string representation of the log for debugging
func (l *RaftLog) String() string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	lastIndex, lastTerm := uint64(0), uint64(0)
	if len(l.entries) > 0 {
		lastIndex = l.entries[len(l.entries)-1].Index
		lastTerm = l.entries[len(l.entries)-1].Term
	}

	return fmt.Sprintf("RaftLog{entries=%d, lastIndex=%d, lastTerm=%d, snapshotLastIndex=%d}",
		len(l.entries), lastIndex, lastTerm, l.snapshotLastIndex)
}

// CompactUntil removes log entries up to and including the given index
// Called after a snapshot is created
func (l *RaftLog) CompactUntil(lastIncludedIndex, lastIncludedTerm uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if lastIncludedIndex <= l.snapshotLastIndex {
		return // Already compacted
	}

	// Find entries to keep (after lastIncludedIndex)
	var newEntries []LogEntry
	for _, entry := range l.entries {
		if entry.Index > lastIncludedIndex {
			newEntries = append(newEntries, entry)
		}
	}

	l.entries = newEntries
	l.snapshotLastIndex = lastIncludedIndex
	l.snapshotLastTerm = lastIncludedTerm

	log.Printf("[RaftLog] Compacted log until index %d, remaining entries=%d",
		lastIncludedIndex, len(l.entries))
}

// SetSnapshotState sets the snapshot metadata (used when loading from disk)
func (l *RaftLog) SetSnapshotState(lastIndex, lastTerm uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.snapshotLastIndex = lastIndex
	l.snapshotLastTerm = lastTerm
}
