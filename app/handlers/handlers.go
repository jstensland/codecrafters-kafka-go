// Package handlers contains the logic for handling client connections and requests.
package handlers

import (
	"errors"
	"io"
	"log"
	"net"
	"time"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// Constants
const (
	apiVersionsV4         = 4                // Supported API version for ApiVersions
	ConnectionReadTimeout = 10 * time.Second // Timeout for reading from a connection
)

// handleAPIVersionsRequest processes an ApiVersions request and returns the appropriate response
func handleAPIVersionsRequest(req *protocol.Request) *protocol.Response {
	resp := &protocol.Response{
		CorrelationID:  req.CorrelationID,
		ThrottleTimeMs: 0, // No throttling implemented
	}

	// Currently, only ApiVersions v4 is "supported" for a successful response.
	// Other versions will result in an UNSUPPORTED_VERSION error.
	// Note: Kafka protocol allows brokers to support multiple versions.
	// A real broker would check req.ApiVersion against its supported range.
	if req.APIVersion != apiVersionsV4 {
		resp.ErrorCode = protocol.ErrorUnsupportedVersion
		resp.APIKeys = []protocol.APIKeyVersion{} // Must be empty on error
	} else {
		resp.ErrorCode = protocol.ErrorNone // Success
		// Define the APIs supported by this broker
		// Always include ApiVersions (18) and DescribeTopicPartitions (75) for successful responses
		resp.APIKeys = []protocol.APIKeyVersion{
			// Report support for versions 0 through 4 for ApiVersions
			{APIKey: protocol.APIKeyAPIVersions, MinVersion: 0, MaxVersion: apiVersionsV4},
			// Report support for version 0 for DescribeTopicPartitions
			{APIKey: protocol.APIKeyDescribeTopicPartitions, MinVersion: 0, MaxVersion: 0},
		}
	}

	return resp
}

// HandleConnection processes multiple requests from a single client connection
func HandleConnection(conn net.Conn, readTimeout time.Duration) {
	defer closeConn(conn) // Ensure connection is closed when handler exits

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

		// For now, we only handle ApiVersions requests (ApiKey 18).
		// A real broker would use req.ApiKey to dispatch to different handlers.
		var resp *protocol.Response
		if req.APIKey == protocol.APIKeyAPIVersions {
			resp = handleAPIVersionsRequest(req)
		} else {
			// Handle other API keys or return an error response if unsupported
			// For now, let's just create a basic error response for unknown keys
			resp = &protocol.Response{
				CorrelationID:  req.CorrelationID,
				ErrorCode:      protocol.ErrorUnsupportedVersion, // Or a more specific error
				APIKeys:        []protocol.APIKeyVersion{},
				ThrottleTimeMs: 0,
			}
			log.Printf("Received unsupported ApiKey: %d", req.APIKey)
		}

		// Write the response using the protocol package function
		err = protocol.WriteResponse(conn, resp)
		if err != nil {
			log.Printf("Error writing response: %v", err)
			return // Close connection if writing fails
		}
	}
}

func closeConn(conn net.Conn) {
	if err := conn.Close(); err != nil {
		log.Printf("Error closing connection: %v", err)
	}
	log.Println("Connection closed.")
}
