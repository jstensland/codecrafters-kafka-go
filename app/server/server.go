// Package server implements the Kafka broker server functionality.
package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/codecrafters-io/kafka-starter-go/app/handlers"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// Constants
const (
	ConnectionReadTimeout = 10 * time.Second // Timeout for reading from a connection
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
		go s.HandleConnection(conn, ConnectionReadTimeout)
	}
}

// handleConnection processes multiple requests from a single client connection
func (s *Server) HandleConnection(conn net.Conn, readTimeout time.Duration) {
	defer s.closeConn(conn) // Ensure connection is closed when handler exits

	for {
		// Set a deadline for reading the next request
		// If no data is received within the timeout period, the connection will time out.
		err := conn.SetReadDeadline(time.Now().Add(readTimeout))
		if err != nil {
			log.Printf("Error setting read deadline: %v", err)
			return
		}

		// Parse the incoming request using the protocol package
		req, err := protocol.ParseRequest(conn)
		if err != nil {
			// Check for timeout error
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				log.Println("Connection timed out due to inactivity.")
				return
			}
			// Handle EOF separately, client might just disconnect gracefully
			if errors.Is(err, io.EOF) {
				log.Println("Client disconnected gracefully.")
				return
			}

			// Handle other parsing errors
			log.Printf("Error parsing request: %v", err)
			return
		}

		// Reset the deadline after a successful read to only apply the timeout
		// to periods of inactivity, not the entire connection duration.
		err = conn.SetReadDeadline(time.Time{}) // Zero value means no deadline
		if err != nil {
			log.Printf("Error resetting read deadline: %v", err)
			return
		}

		// Dispatch based on API Key
		var response interface{} // Use interface{} to hold different response types
		var writeErr error

		switch req.APIKey {
		case protocol.APIKeyAPIVersions:
			// Call the exported handler from the handlers package
			response = handlers.HandleAPIVersionsRequest(req)
		// TODO: Add case for protocol.APIKeyDescribeTopicPartitions here later
		default:
			// Handle other unknown API keys
			log.Printf("Received unsupported ApiKey: %d", req.APIKey)
			// Use the ApiVersions response structure for a generic error
			response = &protocol.Response{
				CorrelationID:  req.CorrelationID,
				ErrorCode:      protocol.ErrorUnsupportedVersion,
				APIKeys:        []protocol.APIKeyVersion{},
				ThrottleTimeMs: 0,
			}
		}

		// Ensure the response implements the APIResponse interface
		apiResponse, ok := response.(protocol.APIResponse)
		if !ok {
			// This should ideally not happen if all handlers return types implementing APIResponse
			log.Printf("Error: Response type %T does not implement protocol.APIResponse", response)
			// Handle this error appropriately, maybe close connection or send a generic error
			return // Close connection as we cannot serialize the response
		}

		// Write the appropriate response using the protocol package function
		writeErr = protocol.WriteResponse(conn, apiResponse) // Pass the APIResponse interface
		if writeErr != nil {
			log.Printf("Error writing response: %v", writeErr)
			return // Close connection if writing fails
		}
	}
}

// closeConn closes the connection and logs any errors.
func (s *Server) closeConn(conn net.Conn) {
	if err := conn.Close(); err != nil {
		log.Printf("Error closing connection: %v", err)
	}
	log.Println("Connection closed.")
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
