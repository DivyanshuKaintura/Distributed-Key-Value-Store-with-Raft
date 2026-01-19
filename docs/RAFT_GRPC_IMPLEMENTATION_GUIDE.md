# Raft Consensus with gRPC Implementation Guide

A comprehensive guide for implementing Raft consensus protocol with gRPC for inter-node communication in the Distributed Key-Value Store.

---

## Table of Contents

1. [Introduction](#introduction)
2. [Architecture Overview](#architecture-overview)
3. [Understanding Raft Consensus](#understanding-raft-consensus)
4. [Implementation Steps](#implementation-steps)
   - [Step 1: Define Raft Protocol Buffer Schema](#step-1-define-raft-protocol-buffer-schema)
   - [Step 2: Generate and Implement gRPC Server](#step-2-generate-and-implement-grpc-server)
   - [Step 3: Add Cluster Configuration](#step-3-add-cluster-configuration)
   - [Step 4: Implement Raft State Machine Core](#step-4-implement-raft-state-machine-core)
   - [Step 5: Dual-Server Startup](#step-5-dual-server-startup)
   - [Step 6: Write Forwarding](#step-6-write-forwarding)
5. [Testing the Cluster](#testing-the-cluster)
6. [References](#references)

---

## Introduction

### What We're Building

We're transforming a single-node key-value store into a **fault-tolerant distributed system**. Instead of one server holding all data (single point of failure), we'll have multiple nodes that stay synchronized. If one crashes, others continue working.

### Why Raft?

Raft is a **consensus algorithm** designed to be understandable. It ensures that:
- All nodes agree on the same sequence of operations
- Data survives node failures
- The system remains available as long as majority of nodes are up

### Why Two Communication Protocols?

| Protocol | Used For | Why |
|----------|----------|-----|
| **HTTP REST** | Client ↔ Server | Simple, debuggable with curl/browser, widely compatible |
| **gRPC** | Server ↔ Server | Fast binary protocol, strongly typed, efficient for high-frequency RPCs |

Clients send GET/PUT/DELETE via HTTP. Nodes coordinate internally via gRPC.

---

## Architecture Overview

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                     CLUSTER                             │
                    │                                                         │
    ┌────────┐      │   ┌─────────────┐    gRPC     ┌─────────────┐          │
    │        │ HTTP │   │   Node 1    │◄───────────►│   Node 2    │          │
    │ Client │◄────►│   │  (Leader)   │             │ (Follower)  │          │
    │        │      │   └─────────────┘             └─────────────┘          │
    └────────┘      │          ▲                           ▲                  │
                    │          │          gRPC             │                  │
                    │          │    ┌─────────────┐        │                  │
                    │          └───►│   Node 3    │◄───────┘                  │
                    │               │ (Follower)  │                           │
                    │               └─────────────┘                           │
                    └─────────────────────────────────────────────────────────┘
```

### Port Assignments (Example 3-Node Cluster)

| Node | HTTP Port (Clients) | gRPC Port (Peers) |
|------|---------------------|-------------------|
| Node 1 | 8080 | 50051 |
| Node 2 | 8081 | 50052 |
| Node 3 | 8082 | 50053 |

---

## Understanding Raft Consensus

### Key Concepts

#### 1. Leader Election

- **Only one leader** exists at any time
- Leader handles all client writes
- If leader fails, remaining nodes elect a new one
- Elections use **terms** (logical time periods)

#### 2. Log Replication

- Leader receives client requests (PUT key=value)
- Leader appends to its **log** and sends to followers
- Once **majority** confirms, entry is **committed**
- Committed entries are applied to the key-value store

#### 3. Safety Guarantees

- **Election Safety**: At most one leader per term
- **Leader Append-Only**: Leader never overwrites its log
- **Log Matching**: If two logs have same index/term, all preceding entries are identical
- **State Machine Safety**: All nodes apply same operations in same order

### Node States

```
                    ┌──────────────────────────────────────┐
                    │                                      │
                    ▼                                      │
            ┌───────────────┐                              │
   Start───►│   FOLLOWER    │                              │
            └───────────────┘                              │
                    │                                      │
                    │ Election timeout                     │
                    │ (no heartbeat from leader)           │
                    ▼                                      │
            ┌───────────────┐                              │
            │   CANDIDATE   │──────────────────────────────┘
            └───────────────┘   Discovers higher term
                    │           or current leader
                    │
                    │ Receives majority votes
                    ▼
            ┌───────────────┐
            │    LEADER     │
            └───────────────┘
```

### Timeouts

| Timeout | Value | Purpose |
|---------|-------|---------|
| Election Timeout | 150-300ms (random) | Follower waits this long before starting election |
| Heartbeat Interval | 50-100ms | Leader sends heartbeats to prevent elections |

**Why random election timeout?** Prevents split votes where multiple nodes become candidates simultaneously.

---

## Implementation Steps

---

### Step 1: Define Raft Protocol Buffer Schema

#### What We're Doing
Creating a `.proto` file that defines the **message formats** and **RPC services** for node-to-node communication.

#### Why We Need It
- **Type Safety**: Protobuf generates Go structs with proper types
- **Efficiency**: Binary serialization is faster and smaller than JSON
- **Contract**: Clear API definition that all nodes follow
- **Code Generation**: Automatically generates Go code for serialization/deserialization

#### Files to Create
- `proto/raft.proto` - Protocol Buffer definitions

#### Protocol Buffer Basics

```protobuf
// Example syntax (for learning, actual implementation may vary)

// Define a message (like a struct)
message LogEntry {
  uint64 term = 1;      // Field number 1
  uint64 index = 2;     // Field number 2
  string command = 3;   // Field number 3
}

// Define a service (collection of RPCs)
service RaftService {
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
}
```

#### RPCs to Define

##### 1. AppendEntries RPC

**Purpose**: Log replication AND heartbeats (leader → followers)

**Request Fields**:
| Field | Type | Description |
|-------|------|-------------|
| term | uint64 | Leader's current term |
| leader_id | string | So followers can redirect clients |
| prev_log_index | uint64 | Index of log entry immediately before new ones |
| prev_log_term | uint64 | Term of prev_log_index entry |
| entries | repeated LogEntry | Log entries to store (empty for heartbeat) |
| leader_commit | uint64 | Leader's commit index |

**Response Fields**:
| Field | Type | Description |
|-------|------|-------------|
| term | uint64 | Current term, for leader to update itself |
| success | bool | True if follower contained matching prev_log entry |

**Heartbeat**: Same RPC with empty `entries` array. Sent every 50-100ms to maintain leadership.

##### 2. RequestVote RPC

**Purpose**: Leader election (candidate → all nodes)

**Request Fields**:
| Field | Type | Description |
|-------|------|-------------|
| term | uint64 | Candidate's term |
| candidate_id | string | Candidate requesting vote |
| last_log_index | uint64 | Index of candidate's last log entry |
| last_log_term | uint64 | Term of candidate's last log entry |

**Response Fields**:
| Field | Type | Description |
|-------|------|-------------|
| term | uint64 | Current term, for candidate to update itself |
| vote_granted | bool | True means candidate received vote |

**Why last_log_index/term?** Voters reject candidates with outdated logs. This ensures the elected leader has all committed entries.

##### 3. InstallSnapshot RPC (Optional, for later)

**Purpose**: Send complete state to far-behind followers instead of thousands of log entries.

#### Log Entry Structure

Each log entry contains:
```
┌──────────┬──────────┬─────────────────────────┐
│  Term    │  Index   │        Command          │
│  (uint64)│ (uint64) │       (string)          │
├──────────┼──────────┼─────────────────────────┤
│    1     │    1     │  "PUT|key1|value1"      │
│    1     │    2     │  "DELETE|key2"          │
│    2     │    3     │  "PUT|key3|value3"      │
└──────────┴──────────┴─────────────────────────┘
```

#### Tasks Checklist
- [ ] Install Protocol Buffer compiler (`protoc`)
- [ ] Install Go plugins for protoc
- [ ] Create `proto/raft.proto` with message definitions
- [ ] Define `RaftService` with `AppendEntries` and `RequestVote` RPCs
- [ ] Define `LogEntry` message
- [ ] Generate Go code using `protoc`

#### Commands to Run
```bash
# Install Go protobuf plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Generate Go code from proto file
protoc --go_out=. --go-grpc_out=. proto/raft.proto
```

---

### Step 2: Generate and Implement gRPC Server

#### What We're Doing
Writing Go code that implements the RPC handlers defined in Step 1.

#### Why We Need It
- Protoc generates **interfaces**, we implement the **logic**
- Handlers contain Raft algorithm rules
- Connects RPCs to actual log and state machine

#### Files to Create
- `raft/grpc_server.go` - gRPC server implementation

#### Implementation Details

##### gRPC Server Structure

```
┌──────────────────────────────────────────────────┐
│              RaftGRPCServer                      │
├──────────────────────────────────────────────────┤
│  - raftNode *RaftNode    (state machine)         │
│  - grpcServer *grpc.Server                       │
├──────────────────────────────────────────────────┤
│  + AppendEntries(ctx, req) → (resp, error)       │
│  + RequestVote(ctx, req) → (resp, error)         │
│  + Start(port)                                   │
│  + Stop()                                        │
└──────────────────────────────────────────────────┘
```

##### AppendEntries Handler Logic

```
1. If request.term < currentTerm:
   → Return (currentTerm, false)
   
2. If request.term > currentTerm:
   → Update currentTerm, become follower
   
3. Reset election timer (leader is alive)

4. If log doesn't contain entry at prevLogIndex with prevLogTerm:
   → Return (currentTerm, false)
   
5. If existing entry conflicts with new one (same index, different term):
   → Delete existing entry and all following
   
6. Append any new entries not already in log

7. If leaderCommit > commitIndex:
   → commitIndex = min(leaderCommit, index of last new entry)
   
8. Return (currentTerm, true)
```

##### RequestVote Handler Logic

```
1. If request.term < currentTerm:
   → Return (currentTerm, false)
   
2. If request.term > currentTerm:
   → Update currentTerm, reset votedFor
   
3. If votedFor is null OR votedFor == candidateId:
   AND candidate's log is at least as up-to-date as ours:
   → Grant vote, return (currentTerm, true)
   
4. Else:
   → Return (currentTerm, false)
```

##### Log Up-to-Date Check

Candidate's log is "at least as up-to-date" if:
1. Candidate's last log term > our last log term, OR
2. Terms equal AND candidate's last log index >= our last log index

#### Tasks Checklist
- [ ] Create struct implementing generated gRPC service interface
- [ ] Implement `AppendEntries` handler with Raft rules
- [ ] Implement `RequestVote` handler with Raft rules
- [ ] Add mutex protection for concurrent RPC handling
- [ ] Add logging for debugging
- [ ] Test handlers with unit tests

---

### Step 3: Add Cluster Configuration

#### What We're Doing
Enabling nodes to know their identity and discover peers.

#### Why We Need It
- Nodes must find each other to send RPCs
- Each node needs a unique ID for voting
- Different ports for multi-node testing on one machine

#### Files to Create/Modify
- `config/config.go` - Configuration structures and loading
- Modify `main.go` - Use configuration

#### Configuration Fields

| Field | Example | Description |
|-------|---------|-------------|
| NodeID | "node1" | Unique identifier for this node |
| HTTPPort | 8080 | Port for client REST API |
| GRPCPort | 50051 | Port for peer gRPC communication |
| Peers | ["node2:50052", "node3:50053"] | Other nodes' gRPC addresses |
| DataDir | "./data/node1" | Directory for persistent storage |
| ElectionTimeoutMin | 150 | Minimum election timeout (ms) |
| ElectionTimeoutMax | 300 | Maximum election timeout (ms) |
| HeartbeatInterval | 50 | Heartbeat interval (ms) |

#### Configuration Sources (Priority Order)

1. **Command-line flags**: `--node-id=node1 --http-port=8080`
2. **Environment variables**: `NODE_ID=node1 HTTP_PORT=8080`
3. **Config file**: `config.json` or `config.yaml`
4. **Defaults**: Fallback values

#### Example Configurations

##### Node 1 (config-node1.json)
```json
{
  "node_id": "node1",
  "http_port": 8080,
  "grpc_port": 50051,
  "peers": [
    {"id": "node2", "address": "localhost:50052"},
    {"id": "node3", "address": "localhost:50053"}
  ],
  "data_dir": "./data/node1"
}
```

##### Node 2 (config-node2.json)
```json
{
  "node_id": "node2",
  "http_port": 8081,
  "grpc_port": 50052,
  "peers": [
    {"id": "node1", "address": "localhost:50051"},
    {"id": "node3", "address": "localhost:50053"}
  ],
  "data_dir": "./data/node2"
}
```

##### Node 3 (config-node3.json)
```json
{
  "node_id": "node3",
  "http_port": 8082,
  "grpc_port": 50053,
  "peers": [
    {"id": "node1", "address": "localhost:50051"},
    {"id": "node2", "address": "localhost:50052"}
  ],
  "data_dir": "./data/node3"
}
```

#### Tasks Checklist
- [ ] Create config struct with all fields
- [ ] Implement config file loading (JSON)
- [ ] Add command-line flag parsing
- [ ] Add environment variable support
- [ ] Validate configuration (unique node ID, valid ports)
- [ ] Update main.go to load config on startup
- [ ] Create sample config files for 3-node cluster

---

### Step 4: Implement Raft State Machine Core

#### What We're Doing
Building the "brain" of Raft - the state machine that manages elections, heartbeats, and log replication.

#### Why We Need It
- Coordinates all Raft behavior
- Manages state transitions (follower → candidate → leader)
- Triggers elections when leader fails
- Sends heartbeats to maintain leadership

#### Files to Create
- `raft/node.go` - Core Raft state machine
- `raft/log.go` - Log management
- `raft/persistence.go` - State persistence

#### Raft Node Structure

```
┌─────────────────────────────────────────────────────────────┐
│                         RaftNode                            │
├─────────────────────────────────────────────────────────────┤
│  PERSISTENT STATE (saved to disk, survives restart)         │
│  ─────────────────────────────────────────────────────────  │
│  - currentTerm uint64    Current term number                │
│  - votedFor    string    CandidateId voted for in term      │
│  - log         []Entry   Log entries                        │
├─────────────────────────────────────────────────────────────┤
│  VOLATILE STATE (all servers)                               │
│  ─────────────────────────────────────────────────────────  │
│  - commitIndex uint64    Highest log entry known committed  │
│  - lastApplied uint64    Highest log entry applied to state │
│  - state       Role      FOLLOWER, CANDIDATE, or LEADER     │
│  - leaderId    string    Current leader (for redirects)     │
├─────────────────────────────────────────────────────────────┤
│  VOLATILE STATE (leaders only, reinitialized after election)│
│  ─────────────────────────────────────────────────────────  │
│  - nextIndex   map[string]uint64   Next entry to send each  │
│  - matchIndex  map[string]uint64   Highest replicated entry │
├─────────────────────────────────────────────────────────────┤
│  TIMERS                                                     │
│  ─────────────────────────────────────────────────────────  │
│  - electionTimer   *time.Timer    Election timeout          │
│  - heartbeatTicker *time.Ticker   Heartbeat interval        │
└─────────────────────────────────────────────────────────────┘
```

#### State Machine Behaviors

##### As Follower

```
┌─────────────────────────────────────────────────────┐
│                    FOLLOWER                         │
├─────────────────────────────────────────────────────┤
│  DO:                                                │
│  • Wait for RPCs from leader or candidates          │
│  • Reset election timer on valid AppendEntries      │
│  • Grant votes to candidates (if eligible)          │
│  • Apply committed entries to KV store              │
│                                                     │
│  TRANSITION TO CANDIDATE:                           │
│  • When election timer fires (no heartbeat)         │
└─────────────────────────────────────────────────────┘
```

##### As Candidate

```
┌─────────────────────────────────────────────────────┐
│                   CANDIDATE                         │
├─────────────────────────────────────────────────────┤
│  ON ENTRY:                                          │
│  • Increment currentTerm                            │
│  • Vote for self                                    │
│  • Reset election timer (random 150-300ms)          │
│  • Send RequestVote to all peers (in parallel)      │
│                                                     │
│  TRANSITION TO LEADER:                              │
│  • When receiving votes from majority               │
│                                                     │
│  TRANSITION TO FOLLOWER:                            │
│  • When receiving AppendEntries from valid leader   │
│  • When discovering higher term in any RPC          │
│                                                     │
│  RESTART ELECTION:                                  │
│  • When election timer fires again                  │
└─────────────────────────────────────────────────────┘
```

##### As Leader

```
┌─────────────────────────────────────────────────────┐
│                     LEADER                          │
├─────────────────────────────────────────────────────┤
│  ON ENTRY:                                          │
│  • Initialize nextIndex[] to last log index + 1     │
│  • Initialize matchIndex[] to 0                     │
│  • Send initial empty AppendEntries (heartbeat)     │
│                                                     │
│  PERIODICALLY (every 50-100ms):                     │
│  • Send heartbeat AppendEntries to all followers    │
│                                                     │
│  ON CLIENT REQUEST (PUT/DELETE):                    │
│  • Append entry to local log                        │
│  • Respond after entry committed (replicated)       │
│                                                     │
│  ON APPENDENTRIES RESPONSE:                         │
│  • If successful: update nextIndex, matchIndex      │
│  • If failed: decrement nextIndex, retry            │
│  • Check if entry can be committed (majority)       │
│                                                     │
│  TRANSITION TO FOLLOWER:                            │
│  • When discovering higher term                     │
└─────────────────────────────────────────────────────┘
```

#### Commit Logic (Leader Only)

```
For each N > commitIndex:
  If log[N].term == currentTerm AND
     majority of matchIndex[i] >= N:
    → Set commitIndex = N
```

**Important**: Leader only commits entries from its current term. Previous term entries are committed indirectly when a current-term entry is committed.

#### Applying Committed Entries (All Servers)

```
While lastApplied < commitIndex:
  lastApplied++
  Apply log[lastApplied] to KV state machine
  (Execute PUT or DELETE on the store)
```

#### Integration with Existing Code

| Existing File | Integration Point |
|---------------|-------------------|
| `wal.go` | Adapt for Raft log with term/index |
| `kv-handlers.go` | Apply committed entries to store |
| `persistant-store.go` | Save Raft state (term, votedFor) |
| `health.go` | Report real Raft state |

#### Tasks Checklist
- [ ] Create RaftNode struct with all state fields
- [ ] Implement state transitions (follower/candidate/leader)
- [ ] Implement election timer with random timeout
- [ ] Implement heartbeat ticker for leader
- [ ] Implement vote counting logic
- [ ] Implement log replication logic
- [ ] Implement commit index advancement
- [ ] Implement log application to state machine
- [ ] Persist currentTerm and votedFor to disk
- [ ] Add comprehensive logging for debugging
- [ ] Write unit tests for state transitions

---

### Step 5: Dual-Server Startup

#### What We're Doing
Modifying `main.go` to run HTTP server, gRPC server, and Raft state machine concurrently.

#### Why We Need It
- Clients need HTTP endpoints
- Peers need gRPC endpoints
- Raft needs background goroutines for timers
- Graceful shutdown prevents data loss

#### Startup Sequence

```
┌─────────────────────────────────────────────────────────────┐
│                     STARTUP SEQUENCE                        │
├─────────────────────────────────────────────────────────────┤
│  1. Load configuration                                      │
│     └── Parse flags, env vars, config file                  │
│                                                             │
│  2. Initialize persistent storage                           │
│     ├── Create data directory if needed                     │
│     ├── Load saved Raft state (term, votedFor)              │
│     └── Replay WAL to recover log entries                   │
│                                                             │
│  3. Create RaftNode                                         │
│     ├── Initialize state machine                            │
│     ├── Start election timer                                │
│     └── Connect to peer gRPC endpoints                      │
│                                                             │
│  4. Start gRPC server (goroutine)                           │
│     └── Listen on grpc_port for peer RPCs                   │
│                                                             │
│  5. Start HTTP server (goroutine)                           │
│     └── Listen on http_port for client requests             │
│                                                             │
│  6. Start Raft main loop (goroutine)                        │
│     └── Process timer events, apply commits                 │
│                                                             │
│  7. Wait for shutdown signal                                │
│     └── Block on SIGINT/SIGTERM                             │
└─────────────────────────────────────────────────────────────┘
```

#### Shutdown Sequence

```
┌─────────────────────────────────────────────────────────────┐
│                    SHUTDOWN SEQUENCE                        │
├─────────────────────────────────────────────────────────────┤
│  1. Receive shutdown signal (Ctrl+C, SIGTERM)               │
│                                                             │
│  2. Stop accepting new requests                             │
│     ├── Stop HTTP server gracefully                         │
│     └── Stop gRPC server gracefully                         │
│                                                             │
│  3. Stop Raft timers                                        │
│     ├── Cancel election timer                               │
│     └── Stop heartbeat ticker                               │
│                                                             │
│  4. Persist final state                                     │
│     ├── Flush WAL to disk                                   │
│     ├── Save Raft state (term, votedFor)                    │
│     └── Optionally create snapshot                          │
│                                                             │
│  5. Close peer connections                                  │
│                                                             │
│  6. Exit cleanly                                            │
└─────────────────────────────────────────────────────────────┘
```

#### Goroutine Structure

```
main()
  │
  ├──► goroutine: HTTP Server
  │      └── http.ListenAndServe(:8080)
  │
  ├──► goroutine: gRPC Server  
  │      └── grpcServer.Serve(:50051)
  │
  ├──► goroutine: Raft Main Loop
  │      └── select {
  │            case <-electionTimer.C:  // Start election
  │            case <-heartbeatTicker.C: // Send heartbeats (if leader)
  │            case entry := <-commitCh: // Apply committed entry
  │            case <-stopCh: // Shutdown
  │          }
  │
  └──► main: Wait for signals
         └── signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
```

#### Files to Modify
- `main.go` - Add dual-server startup and shutdown

#### Tasks Checklist
- [ ] Add signal handling for graceful shutdown
- [ ] Start gRPC server in goroutine
- [ ] Start HTTP server in goroutine  
- [ ] Start Raft main loop in goroutine
- [ ] Implement graceful shutdown sequence
- [ ] Add context with cancellation for cleanup
- [ ] Test startup with multiple nodes
- [ ] Test shutdown doesn't lose data

---

### Step 6: Write Forwarding

#### What We're Doing
Ensuring only the leader handles writes, with followers redirecting clients.

#### Why We Need It
- **Raft Safety**: All writes must go through one leader to maintain log order
- **Consistency**: If followers accepted writes, nodes would diverge
- **Client Experience**: Clients get redirected automatically (or receive leader address)

#### Write Flow Diagram

```
Client sends PUT key=value
          │
          ▼
    ┌───────────┐
    │  Node 2   │  (Follower)
    │           │
    │ Am I      │
    │ Leader?   │──── NO ────► Return error with leader address
    └───────────┘              {"error": "not_leader", "leader": "node1:8080"}
          │                              │
         YES                             │
          │                              ▼
          ▼                        Client retries
    ┌───────────┐                  to Node 1
    │  Node 1   │  (Leader)              │
    │           │◄───────────────────────┘
    │ 1. Append │
    │    to log │
    └───────────┘
          │
          ▼
    ┌─────────────────────────────────────────┐
    │  Send AppendEntries to all followers    │
    │  (Node 2, Node 3) in parallel           │
    └─────────────────────────────────────────┘
          │
          ▼
    ┌───────────┐
    │  Wait for │
    │  majority │  (2 of 3 nodes)
    │  ACKs     │
    └───────────┘
          │
          ▼
    ┌───────────┐
    │  Commit   │  (Apply to KV store)
    │  Entry    │
    └───────────┘
          │
          ▼
    Return success to client
    {"status": "ok", "key": "key", "value": "value"}
```

#### HTTP Handler Modification

```
func handlePut(w, r) {
    // Check if this node is leader
    if raftNode.State != LEADER {
        // Return redirect response
        respondWithLeaderRedirect(w, raftNode.LeaderID)
        return
    }
    
    // Parse request
    key, value := parseRequest(r)
    
    // Propose to Raft (blocks until committed or timeout)
    result := raftNode.Propose("PUT|" + key + "|" + value)
    
    if result.Success {
        respondSuccess(w, key, value)
    } else {
        respondError(w, result.Error)
    }
}
```

#### Client Retry Behavior

```
1. Client sends PUT to any known node
2. If receives "not_leader" error:
   a. Extract leader address from response
   b. Retry request to leader
   c. Update cached leader address
3. If leader address unknown:
   a. Try nodes round-robin until finding leader
4. If request times out:
   a. Retry with exponential backoff
```

#### Error Responses

| Scenario | HTTP Status | Error Code | Response Body |
|----------|-------------|------------|---------------|
| Not leader | 307 or 503 | `not_leader` | `{"error": "not_leader", "leader": "node1:8080"}` |
| No leader elected | 503 | `no_leader` | `{"error": "no_leader", "message": "cluster electing"}` |
| Commit timeout | 504 | `timeout` | `{"error": "timeout", "message": "replication timeout"}` |
| No quorum | 503 | `no_quorum` | `{"error": "no_quorum", "message": "insufficient nodes"}` |

#### Read Handling Options

| Strategy | Consistency | Performance | Implementation |
|----------|-------------|-------------|----------------|
| **Serve from any node** | Eventual | Fast | Read directly from local store |
| **Read from leader only** | Linearizable | Slower | Forward reads to leader |
| **Read with lease** | Linearizable | Fast | Leader serves if lease valid |

**Recommendation**: Start with "serve from any node" for simplicity. Document that reads may be slightly stale.

#### Files to Modify
- `kv-handlers.go` - Add leader check before writes
- `errors.go` - Add `ErrNotLeader`, `ErrNoLeader` error types
- `retry-client.go` - Handle leader redirects

#### Tasks Checklist
- [ ] Add leader check to PUT handler
- [ ] Add leader check to DELETE handler
- [ ] Implement leader redirect response
- [ ] Update error types in errors.go
- [ ] Update retry-client to handle redirects
- [ ] Implement Propose() method on RaftNode
- [ ] Add timeout for replication
- [ ] Test write forwarding with 3-node cluster
- [ ] Document read consistency behavior

---

## Testing the Cluster

### Manual Testing Steps

#### 1. Start 3-Node Cluster

```bash
# Terminal 1
./kvstore --config=config-node1.json

# Terminal 2
./kvstore --config=config-node2.json

# Terminal 3
./kvstore --config=config-node3.json
```

#### 2. Verify Leader Election

```bash
# Check health on each node
curl http://localhost:8080/health
curl http://localhost:8081/health
curl http://localhost:8082/health

# One should show state: "leader", others: "follower"
```

#### 3. Test Write Operations

```bash
# Write to leader
curl -X PUT http://localhost:8080/kv/testkey -d "testvalue"

# Verify replication - read from follower
curl http://localhost:8081/kv/testkey
curl http://localhost:8082/kv/testkey
```

#### 4. Test Leader Failure

```bash
# Kill leader (Ctrl+C on Terminal 1)

# Wait for election (~300ms)

# Check new leader
curl http://localhost:8081/health
curl http://localhost:8082/health

# Write to new leader
curl -X PUT http://localhost:8081/kv/newkey -d "newvalue"
```

#### 5. Test Node Recovery

```bash
# Restart node1
./kvstore --config=config-node1.json

# Verify it catches up (becomes follower)
curl http://localhost:8080/health

# Verify data replicated
curl http://localhost:8080/kv/newkey
```

### Automated Tests to Write

- [ ] Election completes within timeout
- [ ] Only one leader elected per term
- [ ] Write succeeds when majority available
- [ ] Write fails when majority unavailable
- [ ] Follower redirects writes to leader
- [ ] Data survives single node failure
- [ ] Recovered node catches up correctly
- [ ] Split-brain prevention (network partition)

---

## References

### Raft Paper
- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) - Original Raft paper by Diego Ongaro and John Ousterhout

### Raft Visualization
- [Raft Visualization](https://raft.github.io/) - Interactive visualization of Raft consensus

### gRPC Documentation
- [gRPC Go Quick Start](https://grpc.io/docs/languages/go/quickstart/)
- [Protocol Buffers Go Tutorial](https://protobuf.dev/getting-started/gotutorial/)

### Go Packages
- `google.golang.org/grpc` - gRPC library
- `google.golang.org/protobuf` - Protocol Buffers

---

## Progress Tracking

| Step | Status | Notes |
|------|--------|-------|
| Step 1: Proto Definitions | ✅ Complete | proto/raft.proto created with AppendEntries, RequestVote, InstallSnapshot RPCs |
| Step 2: gRPC Server | ✅ Complete | raft/grpc_server.go and raft/node.go created with RPC handlers |
| Step 3: Cluster Config | ✅ Complete | config/config.go and sample config files for 3-node cluster |
| Step 4: Raft State Machine | ✅ Complete | raft/node.go, raft/log.go, raft/persistence.go with full election, heartbeat, and replication logic |
| Step 5: Dual-Server Startup | ⬜ Not Started | |
| Step 6: Write Forwarding | ⬜ Not Started | |

---

*Last Updated: January 19, 2026*
