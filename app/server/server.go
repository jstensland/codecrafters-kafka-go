// Package server implements the Kafka broker server functionality.
package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/handlers"
)

// Server represents a Kafka broker server.
type Server struct {
	listener net.Listener
}

// NewServer creates a new Server instance with the specified address.
func NewServer(l net.Listener) *Server {
	return &Server{
		listener: l,
	}
}

// Serve starts the Kafka broker server and handles client connections.
func (s *Server) Serve() error {
	defer func() {
		if err := s.listener.Close(); err != nil {
			log.Printf("Error closing listener: %v", err)
		}
	}() // Ensure listener is closed when Serve exits
	log.Printf("Listening on: %s", s.listener.Addr())

	// TODO: add signaling with graceful shutdown
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)

			// Check if the error indicates the listener is closed
			if errors.Is(err, net.ErrClosed) {
				return nil // return no error for graceful shutdown
			}

			// Check for other permanent errors
			var netErr net.Error
			if errors.As(err, &netErr) {
				return fmt.Errorf("permanent listener error: %w", err)
			}

			continue // Continue listening for other connections
		}

		// Handle connection in a new goroutine to allow concurrent connections
		go handlers.HandleConnection(conn, handlers.ConnectionReadTimeout)
	}
}

// Run creates a server with the default address and starts it.
// If the server fails to start, it exits the program.
func Run() {
	// Listen on the specified address
	address := ":9092"
	l, err := net.Listen("tcp", address) //nolint:gosec // should bind local for this address
	if err != nil {
		log.Printf("could not listen on %v: %v", address, err)
		os.Exit(1)
	}
	server := NewServer(l)
	if err := server.Serve(); err != nil {
		log.Printf("Failed to bind to port 9092: %v", err)
		os.Exit(1)
	}
}
