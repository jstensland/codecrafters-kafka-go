package main

import "time"

// GetConnectionReadTimeout returns the current connection read timeout
func GetConnectionReadTimeout() time.Duration {
	return currentConnectionReadTimeout
}

// SetConnectionReadTimeout sets the connection read timeout
// This is primarily used for testing
func SetConnectionReadTimeout(timeout time.Duration) {
	currentConnectionReadTimeout = timeout
}
