package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// Request represents the structure of an incoming Kafka request header
type Request struct {
	Size           uint32
	ApiKey         uint16 // Not used yet, but part of the header
	ApiVersion     uint16 // Not used yet, but part of the header
	CorrelationID  uint32
	RemainingBytes []byte // The rest of the payload after the header fields
}

// Response represents the structure of a Kafka ApiVersions response
// For now, it only includes fields necessary for an error response.
type Response struct {
	CorrelationID uint32
	ErrorCode     int16 // Error code (e.g., 35 for UNSUPPORTED_VERSION)
}

// ParseRequest reads from the reader and parses the Kafka request header
func ParseRequest(reader io.Reader) (*Request, error) {
	// 1. Read the message size (first 4 bytes)
	sizeBytes := make([]byte, 4)
	if _, err := io.ReadFull(reader, sizeBytes); err != nil {
		// Distinguish EOF from other errors for cleaner handling
		if err == io.EOF {
			return nil, io.EOF // Indicate clean connection closure
		}
		return nil, fmt.Errorf("reading message size: %w", err)
	}
	messageSize := binary.BigEndian.Uint32(sizeBytes)

	// Check for reasonable message size to prevent huge allocations
	// (Adjust limit as needed)
	if messageSize > 1024*1024 { // Example limit: 1MB
		return nil, fmt.Errorf("message size %d exceeds limit", messageSize)
	}
	if messageSize < 8 { // Minimum size for ApiKey, ApiVersion, CorrelationID
		return nil, fmt.Errorf("message size %d is too small for header", messageSize)
	}

	// 2. Read the rest of the message payload (messageSize bytes)
	// The payload contains ApiKey, ApiVersion, CorrelationID, etc.
	payload := make([]byte, messageSize)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, fmt.Errorf("reading message payload: %w", err)
	}

	// 3. Parse fields from the payload
	req := &Request{
		Size:           messageSize,
		ApiKey:         binary.BigEndian.Uint16(payload[0:2]),
		ApiVersion:     binary.BigEndian.Uint16(payload[2:4]),
		CorrelationID:  binary.BigEndian.Uint32(payload[4:8]),
		RemainingBytes: payload[8:], // Store the rest if needed later
	}

	return req, nil
}

// WriteResponse serializes and writes the ApiVersions response (including error code) to the given writer
func WriteResponse(writer io.Writer, resp *Response) error {
	// ApiVersions Response Body: ErrorCode (int16), ApiKeys Array Length (int32)
	// Since we return an error, ApiKeys array is empty (length 0).
	responseBodySize := 2 + 4                                  // ErrorCode + Array Length
	responseHeaderSize := 4                                    // CorrelationID
	totalSize := uint32(responseHeaderSize + responseBodySize) // Total size excluding the size field itself

	// Total buffer size = Size field (4) + CorrelationID (4) + ErrorCode (2) + Array Length (4) = 14
	responseBytes := make([]byte, 4+totalSize)

	// 1. Write Total Size (excluding itself)
	binary.BigEndian.PutUint32(responseBytes[0:4], totalSize)

	// 2. Write Header: Correlation ID
	binary.BigEndian.PutUint32(responseBytes[4:8], resp.CorrelationID)

	// 3. Write Body: ErrorCode + ApiKeys Array (empty)
	binary.BigEndian.PutUint16(responseBytes[8:10], uint16(resp.ErrorCode))
	binary.BigEndian.PutUint32(responseBytes[10:14], 0) // ApiKeys Array Length = 0

	// Send the complete response
	_, err := writer.Write(responseBytes)
	if err != nil {
		return fmt.Errorf("writing response: %w", err)
	}
	return nil
}

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

// HandleConnection processes a single client connection
func HandleConnection(conn net.Conn) {
	defer conn.Close() // Ensure connection is closed when handler exits

	// Parse the incoming request
	req, err := ParseRequest(conn)
	if err != nil {
		// Handle EOF separately, client might just disconnect
		if err == io.EOF {
			fmt.Println("Client disconnected gracefully.")
		} else {
			fmt.Println("Error parsing request:", err.Error())
		}
		return // Stop processing on error
	}

	// For now, we assume all requests are ApiVersions requests (ApiKey 18).
	// We don't support any specific ApiVersion yet, so return UNSUPPORTED_VERSION (35).

	// Create the response object with the error code
	resp := &Response{
		CorrelationID: req.CorrelationID,
		ErrorCode:     35, // UNSUPPORTED_VERSION
	}

	// Write the response using the dedicated function
	err = WriteResponse(conn, resp)
	if err != nil {
		fmt.Println("Error writing response:", err.Error())
	}
}
