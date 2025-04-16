package main

import (
	"errors" // Add errors import for errors.Is
	"fmt"
	"io"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol" // Import the new protocol package
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Listen on both IPv4 and IPv6
	l, err := net.Listen("tcp", ":9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close() // Ensure listener is closed when main exits

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue // Continue listening for other connections
		}

		// Handle connection in a new goroutine to allow concurrent connections
		go HandleConnection(conn)
	}
}

// handleApiVersionsRequest processes an ApiVersions request and returns the appropriate response
func handleApiVersionsRequest(req *protocol.Request) *protocol.Response {
	resp := &protocol.Response{
		CorrelationID:  req.CorrelationID,
		ThrottleTimeMs: 0, // No throttling implemented
	}

	// Currently, only ApiVersions v4 is "supported" for a successful response.
	// Other versions will result in an UNSUPPORTED_VERSION error.
	// Note: Kafka protocol allows brokers to support multiple versions.
	// A real broker would check req.ApiVersion against its supported range.
	if req.ApiVersion != 4 {
		resp.ErrorCode = protocol.ERROR_UNSUPPORTED_VERSION
		resp.ApiKeys = []protocol.ApiKeyVersion{} // Must be empty on error
	} else {
		resp.ErrorCode = protocol.ERROR_NONE // Success
		// Define the APIs supported by this broker
		// Always include ApiVersions (18) for successful responses
		resp.ApiKeys = []protocol.ApiKeyVersion{
			// Report support for versions 0 through 4 for ApiVersions
			{ApiKey: protocol.API_KEY_API_VERSIONS, MinVersion: 0, MaxVersion: 4}, // ApiVersions itself
			// Add other supported APIs here later
		}
	}

	return resp
}

// HandleConnection processes a single client connection
func HandleConnection(conn net.Conn) {
	defer conn.Close() // Ensure connection is closed when handler exits

	// Parse the incoming request using the protocol package
	req, err := protocol.ParseRequest(conn)
	if err != nil {
		// Handle EOF separately, client might just disconnect
		if errors.Is(err, io.EOF) { // Use errors.Is for potentially wrapped EOF
			fmt.Println("Client disconnected gracefully.")
		} else {
			fmt.Println("Error parsing request:", err.Error())
		}
		return // Stop processing on error
	}

	// For now, we only handle ApiVersions requests (ApiKey 18).
	// A real broker would use req.ApiKey to dispatch to different handlers.
	var resp *protocol.Response
	if req.ApiKey == protocol.API_KEY_API_VERSIONS {
		resp = handleApiVersionsRequest(req)
	} else {
		// Handle other API keys or return an error response if unsupported
		// For now, let's just create a basic error response for unknown keys
		resp = &protocol.Response{
			CorrelationID:  req.CorrelationID,
			ErrorCode:      protocol.ERROR_UNSUPPORTED_VERSION, // Or a more specific error
			ApiKeys:        []protocol.ApiKeyVersion{},
			ThrottleTimeMs: 0,
		}
		fmt.Printf("Received unsupported ApiKey: %d\n", req.ApiKey)
	}

	// Write the response using the protocol package function
	err = protocol.WriteResponse(conn, resp)
	if err != nil {
		fmt.Println("Error writing response:", err.Error())
	}
}
