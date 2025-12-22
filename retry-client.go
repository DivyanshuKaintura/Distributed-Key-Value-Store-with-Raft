package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"time"
)

// ============== Retry Configuration ==============

// RetryConfig defines the retry behavior for HTTP requests
type RetryConfig struct {
	MaxRetries        int           // Maximum number of retry attempts
	InitialDelay      time.Duration // Initial delay before first retry
	MaxDelay          time.Duration // Maximum delay between retries
	Multiplier        float64       // Exponential backoff multiplier
	RetryableCodesMap map[int]bool  // HTTP status codes that should trigger retry
}

// DefaultRetryConfig returns a sensible default configuration
// for distributed systems
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:   3,                      // Try up to 3 times after initial attempt = 4 total
		InitialDelay: 100 * time.Millisecond, // Start with 100ms
		MaxDelay:     3 * time.Second,        // Cap at 3 seconds
		Multiplier:   2.0,                    // Double delay each time
		RetryableCodesMap: map[int]bool{
			408: true, // Request Timeout - server was slow
			429: true, // Too Many Requests - server overloaded
			500: true, // Internal Server Error - temporary server issue
			502: true, // Bad Gateway - proxy/load balancer issue
			503: true, // Service Unavailable - server temporarily down
			504: true, // Gateway Timeout - upstream timeout
		},
	}
}

// IsRetryable checks if an HTTP status code should trigger a retry
func (c *RetryConfig) IsRetryable(statusCode int) bool {
	return c.RetryableCodesMap[statusCode]
}

// CalculateDelay returns the delay duration for a given attempt number
// Uses exponential backoff: delay = min(InitialDelay * Multiplier^attempt, MaxDelay)
func (c *RetryConfig) CalculateDelay(attempt int) time.Duration {
	// Exponential backoff formula
	delay := float64(c.InitialDelay) * math.Pow(c.Multiplier, float64(attempt))

	// Cap at MaxDelay
	if delay > float64(c.MaxDelay) {
		delay = float64(c.MaxDelay)
	}

	return time.Duration(delay)
}

// ============== Retry Client ==============

// KVClient is an HTTP client with built-in retry logic
// for communicating with the distributed KV store
type KVClient struct {
	BaseURL     string
	HTTPClient  *http.Client
	RetryConfig RetryConfig
	EnableDebug bool // Enable debug logging
}

// NewKVClient creates a new client with default retry configuration
func NewKVClient(baseURL string) *KVClient {
	return &KVClient{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second, // Overall timeout for each attempt
		},
		RetryConfig: DefaultRetryConfig(),
		EnableDebug: false,
	}
}

// ============== Core Retry Logic ==============

// doRequestWithRetry executes an HTTP request with automatic retry logic
// This is the heart of our retry mechanism
func (c *KVClient) doRequestWithRetry(method, url string, body []byte) (*http.Response, error) {
	var lastErr error
	var resp *http.Response

	// Try MaxRetries + 1 times (initial attempt + retries)
	for attempt := 0; attempt <= c.RetryConfig.MaxRetries; attempt++ {
		// If this is a retry (not first attempt), wait before trying
		if attempt > 0 {
			delay := c.RetryConfig.CalculateDelay(attempt - 1)
			if c.EnableDebug {
				log.Printf("Retry attempt %d/%d after %v", attempt, c.RetryConfig.MaxRetries, delay)
			}
			time.Sleep(delay)
		}

		// Create new request for each attempt (can't reuse request body)
		var bodyReader io.Reader
		if body != nil {
			bodyReader = bytes.NewReader(body)
		}

		req, err := http.NewRequest(method, url, bodyReader)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		// Execute request
		resp, err = c.HTTPClient.Do(req)

		// Success case: No error and non-retryable status code
		if err == nil {
			// Check if status code indicates we should retry
			if !c.RetryConfig.IsRetryable(resp.StatusCode) {
				// Success! (could be 200 OK, 404 Not Found, etc.)
				// 404 is not retryable - it's a valid response
				return resp, nil
			}

			// Status code indicates we should retry
			if c.EnableDebug {
				log.Printf("Received retryable status %d, will retry", resp.StatusCode)
			}
			resp.Body.Close() // Close body before retry
			lastErr = fmt.Errorf("retryable status code: %d", resp.StatusCode)
			continue
		}

		// Network error occurred
		if c.EnableDebug {
			log.Printf("Request failed: %v", err)
		}
		lastErr = err
		// Continue to next retry attempt
	}

	// All retries exhausted
	return nil, fmt.Errorf("request failed after %d attempts: %w", c.RetryConfig.MaxRetries+1, lastErr)
}

// ============== KV Operations with Retry ==============

// Get retrieves a value from the KV store with automatic retry
func (c *KVClient) Get(key string) (string, error) {
	url := fmt.Sprintf("%s/kv/%s", c.BaseURL, key)

	resp, err := c.doRequestWithRetry(http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	// Parse response
	var result SuccessResponse
	if err := json.Unmarshal(body, &result); err != nil {
		// Try parsing as error response
		var errResp ErrorResponse
		if err2 := json.Unmarshal(body, &errResp); err2 == nil {
			return "", fmt.Errorf("[%s] %s: %s", errResp.ErrorCode, errResp.Message, errResp.Details)
		}
		return "", fmt.Errorf("failed to parse response: %w", err)
	}

	if !result.Success {
		return "", fmt.Errorf("server error: %s", result.Message)
	}

	return result.Value, nil
}

// Put stores a key-value pair with automatic retry
func (c *KVClient) Put(key, value string) error {
	url := fmt.Sprintf("%s/kv/%s", c.BaseURL, key)

	resp, err := c.doRequestWithRetry(http.MethodPut, url, []byte(value))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	var result SuccessResponse
	if err := json.Unmarshal(body, &result); err != nil {
		// Try parsing as error response
		var errResp ErrorResponse
		if err2 := json.Unmarshal(body, &errResp); err2 == nil {
			return fmt.Errorf("[%s] %s: %s", errResp.ErrorCode, errResp.Message, errResp.Details)
		}
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if !result.Success {
		return fmt.Errorf("server error: %s", result.Message)
	}

	return nil
}

// Delete removes a key from the KV store with automatic retry
func (c *KVClient) Delete(key string) error {
	url := fmt.Sprintf("%s/kv/%s", c.BaseURL, key)

	resp, err := c.doRequestWithRetry(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	var result SuccessResponse
	if err := json.Unmarshal(body, &result); err != nil {
		// Try parsing as error response
		var errResp ErrorResponse
		if err2 := json.Unmarshal(body, &errResp); err2 == nil {
			return fmt.Errorf("[%s] %s: %s", errResp.ErrorCode, errResp.Message, errResp.Details)
		}
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if !result.Success {
		return fmt.Errorf("server error: %s", result.Message)
	}

	return nil
}
