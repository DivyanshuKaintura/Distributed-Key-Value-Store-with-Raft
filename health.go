package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

// ============== Health Check System ==============

// ServerStats tracks server metrics for health monitoring
type ServerStats struct {
	StartTime     time.Time     // When server started
	TotalRequests atomic.Uint64 // Total requests handled
	SuccessCount  atomic.Uint64 // Successful operations
	ErrorCount    atomic.Uint64 // Failed operations
}

var stats = &ServerStats{
	StartTime: time.Now(),
}

// HealthStatus represents the overall health of the service
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"   // Service is fully operational
	HealthStatusDegraded  HealthStatus = "degraded"  // Service working but with issues
	HealthStatusUnhealthy HealthStatus = "unhealthy" // Service not operational
)

// HealthResponse is the structure returned by health check endpoint
type HealthResponse struct {
	Status    HealthStatus     `json:"status"`               // Overall health status
	Timestamp string           `json:"timestamp"`            // Current time
	Uptime    string           `json:"uptime"`               // How long server has been running
	Version   string           `json:"version,omitempty"`    // Service version
	Checks    map[string]Check `json:"checks"`               // Individual component health
	Metrics   Metrics          `json:"metrics"`              // Service metrics
	RaftState *RaftState       `json:"raft_state,omitempty"` // Raft state (future)
}

// Check represents health status of an individual component
type Check struct {
	Status    HealthStatus `json:"status"`            // Component status
	Message   string       `json:"message,omitempty"` // Additional info
	LastCheck string       `json:"last_check"`        // When this was checked
}

// Metrics contains operational metrics
type Metrics struct {
	TotalRequests  uint64  `json:"total_requests"`  // Total requests served
	SuccessCount   uint64  `json:"success_count"`   // Successful operations
	ErrorCount     uint64  `json:"error_count"`     // Failed operations
	ErrorRate      float64 `json:"error_rate"`      // Error percentage
	StoreSize      int     `json:"store_size"`      // Number of keys in store
	MemoryUsageMB  uint64  `json:"memory_usage_mb"` // Memory usage in MB
	GoroutineCount int     `json:"goroutine_count"` // Number of goroutines
}

// RaftState contains Raft consensus state (for future use)
type RaftState struct {
	NodeID      string `json:"node_id"`             // This node's ID
	State       string `json:"state"`               // leader/follower/candidate
	Term        uint64 `json:"term"`                // Current term
	LeaderID    string `json:"leader_id,omitempty"` // Current leader
	CommitIndex uint64 `json:"commit_index"`        // Last committed log index
	LastApplied uint64 `json:"last_applied"`        // Last applied log index
}

// ============== Health Check Handler ==============

// HealthCheckHandler returns the health status of the service
// This is used by load balancers, k8s, and monitoring systems
func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	// Only allow GET requests
	if r.Method != http.MethodGet {
		SendError(w, ErrMethodNotAllowed, "only GET method allowed", "", "")
		return
	}

	// Perform health checks on all components
	checks := performHealthChecks()

	// Calculate overall status
	overallStatus := calculateOverallHealth(checks)

	// Gather metrics
	metrics := gatherMetrics()

	// Build response
	response := HealthResponse{
		Status:    overallStatus,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Uptime:    formatUptime(time.Since(stats.StartTime)),
		Version:   "1.0.0", // Update this as you version your service
		Checks:    checks,
		Metrics:   metrics,
		// RaftState will be populated when Raft is implemented
	}

	// Return appropriate HTTP status code
	statusCode := http.StatusOK
	if overallStatus == HealthStatusUnhealthy {
		statusCode = http.StatusServiceUnavailable // 503
	} else if overallStatus == HealthStatusDegraded {
		statusCode = http.StatusOK // 200 but with warnings
	}

	// Send response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response)
}

// ============== Component Health Checks ==============

// performHealthChecks runs health checks on all components
func performHealthChecks() map[string]Check {
	checks := make(map[string]Check)
	now := time.Now().UTC().Format(time.RFC3339)

	// Check 1: Store accessibility
	checks["store"] = checkStoreHealth(now)

	// Check 2: WAL health
	checks["wal"] = checkWALHealth(now)

	// Check 3: Disk space
	checks["disk"] = checkDiskHealth(now)

	// Check 4: Memory usage
	checks["memory"] = checkMemoryHealth(now)

	// Future checks (when implementing Raft):
	// checks["raft_consensus"] = checkRaftHealth(now)
	// checks["cluster_connectivity"] = checkClusterHealth(now)

	return checks
}

// checkStoreHealth verifies the KV store is accessible
func checkStoreHealth(timestamp string) Check {
	// Try to acquire lock to ensure store is not deadlocked
	mu.RLock()
	// size := len(store)
	mu.RUnlock()

	return Check{
		Status:    HealthStatusHealthy,
		Message:   "store accessible",
		LastCheck: timestamp,
	}
}

// checkWALHealth verifies write-ahead log is writable
func checkWALHealth(timestamp string) Check {
	// Check if WAL file exists and is writable
	walPath := "wal.log"
	if _, err := os.Stat(walPath); err != nil {
		if os.IsNotExist(err) {
			return Check{
				Status:    HealthStatusDegraded,
				Message:   "WAL file not found (will be created on first write)",
				LastCheck: timestamp,
			}
		}
		return Check{
			Status:    HealthStatusUnhealthy,
			Message:   "WAL file inaccessible",
			LastCheck: timestamp,
		}
	}

	return Check{
		Status:    HealthStatusHealthy,
		Message:   "WAL operational",
		LastCheck: timestamp,
	}
}

// checkDiskHealth verifies sufficient disk space
func checkDiskHealth(timestamp string) Check {
	// Simple check - in production, you'd check actual disk space
	dataPath := "data.json"
	info, err := os.Stat(dataPath)
	if err != nil && !os.IsNotExist(err) {
		return Check{
			Status:    HealthStatusDegraded,
			Message:   "cannot access data file",
			LastCheck: timestamp,
		}
	}

	// Check if data file is too large (example: warn if > 100MB)
	if info != nil && info.Size() > 100*1024*1024 {
		return Check{
			Status:    HealthStatusDegraded,
			Message:   "data file size exceeds 100MB",
			LastCheck: timestamp,
		}
	}

	return Check{
		Status:    HealthStatusHealthy,
		Message:   "disk space adequate",
		LastCheck: timestamp,
	}
}

// checkMemoryHealth verifies memory usage is within limits
func checkMemoryHealth(timestamp string) Check {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Get memory usage in MB
	allocMB := m.Alloc / 1024 / 1024

	// Warn if using more than 500MB (adjust based on your needs)
	if allocMB > 500 {
		return Check{
			Status:    HealthStatusDegraded,
			Message:   "high memory usage",
			LastCheck: timestamp,
		}
	}

	return Check{
		Status:    HealthStatusHealthy,
		Message:   "memory usage normal",
		LastCheck: timestamp,
	}
}

// ============== Overall Health Calculation ==============

// calculateOverallHealth determines overall status from component checks
func calculateOverallHealth(checks map[string]Check) HealthStatus {
	hasUnhealthy := false
	hasDegraded := false

	for _, check := range checks {
		if check.Status == HealthStatusUnhealthy {
			hasUnhealthy = true
		}
		if check.Status == HealthStatusDegraded {
			hasDegraded = true
		}
	}

	// If any component is unhealthy, overall is unhealthy
	if hasUnhealthy {
		return HealthStatusUnhealthy
	}

	// If any component is degraded, overall is degraded
	if hasDegraded {
		return HealthStatusDegraded
	}

	// All components healthy
	return HealthStatusHealthy
}

// ============== Metrics Collection ==============

// gatherMetrics collects current operational metrics
func gatherMetrics() Metrics {
	// Get current memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Get store size
	mu.RLock()
	storeSize := len(store)
	mu.RUnlock()

	// Calculate error rate
	totalReq := stats.TotalRequests.Load()
	errCount := stats.ErrorCount.Load()
	var errorRate float64
	if totalReq > 0 {
		errorRate = float64(errCount) / float64(totalReq) * 100
	}

	return Metrics{
		TotalRequests:  totalReq,
		SuccessCount:   stats.SuccessCount.Load(),
		ErrorCount:     errCount,
		ErrorRate:      errorRate,
		StoreSize:      storeSize,
		MemoryUsageMB:  m.Alloc / 1024 / 1024,
		GoroutineCount: runtime.NumGoroutine(),
	}
}

// ============== Helper Functions ==============

// formatUptime formats duration as human-readable uptime
func formatUptime(d time.Duration) string {
	days := int(d.Hours() / 24)
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm %ds", days, hours, minutes, seconds)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}

// ============== Stats Tracking ==============

// RecordRequest increments request counter (call this in your handlers)
func RecordRequest() {
	stats.TotalRequests.Add(1)
}

// RecordSuccess increments success counter
func RecordSuccess() {
	stats.SuccessCount.Add(1)
}

// RecordError increments error counter
func RecordError() {
	stats.ErrorCount.Add(1)
}
