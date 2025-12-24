package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// PeerConfig represents a peer node in the cluster
type PeerConfig struct {
	ID      string `json:"id"`
	Address string `json:"address"` // gRPC address (host:port)
}

// Config holds all configuration for a Raft node
type Config struct {
	// Node identification
	NodeID string `json:"node_id"`

	// Network ports
	HTTPPort int `json:"http_port"` // Port for client REST API
	GRPCPort int `json:"grpc_port"` // Port for peer gRPC communication

	// Cluster peers
	Peers []PeerConfig `json:"peers"`

	// Storage
	DataDir string `json:"data_dir"` // Directory for persistent storage

	// Raft timing (milliseconds)
	ElectionTimeoutMin int `json:"election_timeout_min"` // Minimum election timeout (ms)
	ElectionTimeoutMax int `json:"election_timeout_max"` // Maximum election timeout (ms)
	HeartbeatInterval  int `json:"heartbeat_interval"`   // Heartbeat interval (ms)
}

// DefaultConfig returns a configuration with default values
func DefaultConfig() *Config {
	return &Config{
		NodeID:             "node1",
		HTTPPort:           8080,
		GRPCPort:           50051,
		Peers:              []PeerConfig{},
		DataDir:            "./data",
		ElectionTimeoutMin: 150,
		ElectionTimeoutMax: 300,
		HeartbeatInterval:  50,
	}
}

// LoadConfig loads configuration from file, environment variables, and command-line flags
// Priority (highest to lowest): flags > env vars > config file > defaults
func LoadConfig() (*Config, error) {
	cfg := DefaultConfig()

	// Parse command-line flags first to get config file path
	configFile := flag.String("config", "", "Path to config file (JSON)")
	nodeID := flag.String("node-id", "", "Unique node identifier")
	httpPort := flag.Int("http-port", 0, "HTTP port for client API")
	grpcPort := flag.Int("grpc-port", 0, "gRPC port for peer communication")
	dataDir := flag.String("data-dir", "", "Directory for persistent storage")
	peers := flag.String("peers", "", "Comma-separated peer addresses (id1:addr1,id2:addr2)")
	electionTimeoutMin := flag.Int("election-timeout-min", 0, "Minimum election timeout (ms)")
	electionTimeoutMax := flag.Int("election-timeout-max", 0, "Maximum election timeout (ms)")
	heartbeatInterval := flag.Int("heartbeat-interval", 0, "Heartbeat interval (ms)")

	flag.Parse()

	// Load from config file if specified
	if *configFile != "" {
		if err := cfg.LoadFromFile(*configFile); err != nil {
			return nil, fmt.Errorf("failed to load config file: %w", err)
		}
	}

	// Override with environment variables
	cfg.LoadFromEnv()

	// Override with command-line flags (highest priority)
	if *nodeID != "" {
		cfg.NodeID = *nodeID
	}
	if *httpPort != 0 {
		cfg.HTTPPort = *httpPort
	}
	if *grpcPort != 0 {
		cfg.GRPCPort = *grpcPort
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if *peers != "" {
		cfg.Peers = parsePeers(*peers)
	}
	if *electionTimeoutMin != 0 {
		cfg.ElectionTimeoutMin = *electionTimeoutMin
	}
	if *electionTimeoutMax != 0 {
		cfg.ElectionTimeoutMax = *electionTimeoutMax
	}
	if *heartbeatInterval != 0 {
		cfg.HeartbeatInterval = *heartbeatInterval
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// LoadFromFile loads configuration from a JSON file
func (c *Config) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	if err := json.Unmarshal(data, c); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	return nil
}

// LoadFromEnv loads configuration from environment variables
func (c *Config) LoadFromEnv() {
	if v := os.Getenv("NODE_ID"); v != "" {
		c.NodeID = v
	}
	if v := os.Getenv("HTTP_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.HTTPPort = port
		}
	}
	if v := os.Getenv("GRPC_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.GRPCPort = port
		}
	}
	if v := os.Getenv("DATA_DIR"); v != "" {
		c.DataDir = v
	}
	if v := os.Getenv("PEERS"); v != "" {
		c.Peers = parsePeers(v)
	}
	if v := os.Getenv("ELECTION_TIMEOUT_MIN"); v != "" {
		if timeout, err := strconv.Atoi(v); err == nil {
			c.ElectionTimeoutMin = timeout
		}
	}
	if v := os.Getenv("ELECTION_TIMEOUT_MAX"); v != "" {
		if timeout, err := strconv.Atoi(v); err == nil {
			c.ElectionTimeoutMax = timeout
		}
	}
	if v := os.Getenv("HEARTBEAT_INTERVAL"); v != "" {
		if interval, err := strconv.Atoi(v); err == nil {
			c.HeartbeatInterval = interval
		}
	}
}

// parsePeers parses a comma-separated peer string (id1:addr1,id2:addr2)
func parsePeers(peersStr string) []PeerConfig {
	var peers []PeerConfig
	if peersStr == "" {
		return peers
	}

	parts := strings.Split(peersStr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Expected format: id:address or just address
		colonIdx := strings.Index(part, ":")
		lastColonIdx := strings.LastIndex(part, ":")

		if colonIdx != lastColonIdx {
			// Format: id:host:port
			id := part[:colonIdx]
			addr := part[colonIdx+1:]
			peers = append(peers, PeerConfig{ID: id, Address: addr})
		} else {
			// Format: host:port (use address as ID)
			peers = append(peers, PeerConfig{ID: part, Address: part})
		}
	}

	return peers
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node_id is required")
	}

	if c.HTTPPort <= 0 || c.HTTPPort > 65535 {
		return fmt.Errorf("http_port must be between 1 and 65535")
	}

	if c.GRPCPort <= 0 || c.GRPCPort > 65535 {
		return fmt.Errorf("grpc_port must be between 1 and 65535")
	}

	if c.HTTPPort == c.GRPCPort {
		return fmt.Errorf("http_port and grpc_port must be different")
	}

	if c.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}

	if c.ElectionTimeoutMin <= 0 {
		return fmt.Errorf("election_timeout_min must be positive")
	}

	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		return fmt.Errorf("election_timeout_max must be greater than election_timeout_min")
	}

	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat_interval must be positive")
	}

	// Heartbeat should be much less than election timeout to prevent unnecessary elections
	if c.HeartbeatInterval >= c.ElectionTimeoutMin {
		return fmt.Errorf("heartbeat_interval should be less than election_timeout_min")
	}

	// Validate peer addresses
	for i, peer := range c.Peers {
		if peer.ID == "" {
			return fmt.Errorf("peer %d: id is required", i)
		}
		if peer.Address == "" {
			return fmt.Errorf("peer %d: address is required", i)
		}
		if peer.ID == c.NodeID {
			return fmt.Errorf("peer %d: cannot list self as peer", i)
		}
	}

	return nil
}

// GetPeerAddresses returns a map of peer ID to address
func (c *Config) GetPeerAddresses() map[string]string {
	addrs := make(map[string]string)
	for _, peer := range c.Peers {
		addrs[peer.ID] = peer.Address
	}
	return addrs
}

// String returns a string representation of the config (for logging)
func (c *Config) String() string {
	peerStrs := make([]string, len(c.Peers))
	for i, p := range c.Peers {
		peerStrs[i] = fmt.Sprintf("%s@%s", p.ID, p.Address)
	}

	return fmt.Sprintf(
		"Config{NodeID: %s, HTTPPort: %d, GRPCPort: %d, Peers: [%s], DataDir: %s, "+
			"ElectionTimeout: %d-%dms, HeartbeatInterval: %dms}",
		c.NodeID, c.HTTPPort, c.GRPCPort, strings.Join(peerStrs, ", "), c.DataDir,
		c.ElectionTimeoutMin, c.ElectionTimeoutMax, c.HeartbeatInterval,
	)
}
