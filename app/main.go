package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// Kafka protocol constants
const (
	// Field sizes in bytes
	SIZE_FIELD_LENGTH     = 4
	CORRELATION_ID_LENGTH = 4
	ERROR_CODE_LENGTH     = 2
	ARRAY_LENGTH_FIELD    = 4
	API_KEY_FIELD_LENGTH  = 2
	VERSION_FIELD_LENGTH  = 2
	// THROTTLE_TIME_LENGTH  = 4
	TAGGED_FIELDS_LENGTH = 1 // UVarint 0 for empty tagged fields

	// ApiKeyVersion is 6 bytes: ApiKey(2) + MinVersion(2) + MaxVersion(2)
	API_KEY_ENTRY_LENGTH = API_KEY_FIELD_LENGTH + VERSION_FIELD_LENGTH + VERSION_FIELD_LENGTH

	// Compact array overhead (length field)
	COMPACT_ARRAY_LENGTH_FIELD = 4

	// Error codes
	ERROR_NONE                = 0
	ERROR_UNSUPPORTED_VERSION = 35
)

// Request represents the structure of an incoming Kafka request header
type Request struct {
	Size           uint32
	ApiKey         uint16 // Not used yet, but part of the header
	ApiVersion     uint16 // Not used yet, but part of the header
	CorrelationID  uint32
	RemainingBytes []byte // The rest of the payload after the header fields
}

// ApiKeyVersion represents the supported version range for a single Kafka API key
type ApiKeyVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

// Serialize converts an ApiKeyVersion to its binary representation
func (akv *ApiKeyVersion) Serialize() []byte {
	bytes := make([]byte, API_KEY_ENTRY_LENGTH)

	// Write ApiKey (2 bytes)
	binary.BigEndian.PutUint16(bytes[0:2], uint16(akv.ApiKey))

	// Write MinVersion (2 bytes)
	binary.BigEndian.PutUint16(bytes[2:4], uint16(akv.MinVersion))

	// Write MaxVersion (2 bytes)
	binary.BigEndian.PutUint16(bytes[4:6], uint16(akv.MaxVersion))

	return bytes
}

// Response represents the structure of a Kafka ApiVersions response (Version >= 3)
type Response struct {
	CorrelationID uint32
	ErrorCode     int16
	ApiKeys       []ApiKeyVersion
	// ThrottleTimeMs int32
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

// WriteResponse serializes and writes the full ApiVersions response to the given writer
func WriteResponse(writer io.Writer, resp *Response) error {
	// Calculate sizes for API keys
	apiKeySizeBytes := len(resp.ApiKeys) * API_KEY_ENTRY_LENGTH

	// Calculate response sizes (including the single byte for empty tagged fields)
	// responseBodySize := ERROR_CODE_LENGTH + ARRAY_LENGTH_FIELD + apiKeySizeBytes + THROTTLE_TIME_LENGTH + TAGGED_FIELDS_LENGTH
	responseBodySize := ERROR_CODE_LENGTH + ARRAY_LENGTH_FIELD + apiKeySizeBytes + TAGGED_FIELDS_LENGTH
	responseHeaderSize := CORRELATION_ID_LENGTH
	totalSize := uint32(responseHeaderSize + responseBodySize) // Size *excluding* the initial size field itself

	// Allocate buffer - exactly the size we need (Size field + rest of the message)
	responseBytes := make([]byte, SIZE_FIELD_LENGTH+totalSize)
	offset := 0

	// 1. Write Total Size (excluding itself)
	binary.BigEndian.PutUint32(responseBytes[offset:offset+SIZE_FIELD_LENGTH], totalSize)
	offset += SIZE_FIELD_LENGTH

	// 2. Write Header: Correlation ID
	binary.BigEndian.PutUint32(responseBytes[offset:offset+CORRELATION_ID_LENGTH], resp.CorrelationID)
	offset += CORRELATION_ID_LENGTH

	// 3. Write Body: ErrorCode
	binary.BigEndian.PutUint16(responseBytes[offset:offset+ERROR_CODE_LENGTH], uint16(resp.ErrorCode))
	offset += ERROR_CODE_LENGTH

	// 4. Write Body: ApiKeys Array
	binary.BigEndian.PutUint32(responseBytes[offset:offset+ARRAY_LENGTH_FIELD], uint32(len(resp.ApiKeys)))
	offset += ARRAY_LENGTH_FIELD

	// Use the Serialize method for each ApiKeyVersion
	for _, apiKey := range resp.ApiKeys {
		serialized := apiKey.Serialize()
		copy(responseBytes[offset:offset+API_KEY_ENTRY_LENGTH], serialized)
		offset += API_KEY_ENTRY_LENGTH
	}

	// // 5. Write Body: ThrottleTimeMs
	// binary.BigEndian.PutUint32(responseBytes[offset:offset+THROTTLE_TIME_LENGTH], uint32(resp.ThrottleTimeMs))
	// offset += THROTTLE_TIME_LENGTH

	// 6. Write Body: Tagged Fields (UVarint 0 indicates none)
	responseBytes[offset] = 0 // The UVarint encoding for 0 is a single byte 0

	// Send the complete response (already correctly sized)
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

// handleApiVersionsRequest processes an ApiVersions request and returns the appropriate response
func handleApiVersionsRequest(req *Request) *Response {
	resp := &Response{
		CorrelationID: req.CorrelationID,
		// ThrottleTimeMs: 0, // No throttling implemented
	}

	if req.ApiVersion != 4 {
		resp.ErrorCode = ERROR_UNSUPPORTED_VERSION
		resp.ApiKeys = []ApiKeyVersion{} // Must be empty on error
	} else {
		resp.ErrorCode = ERROR_NONE // Success
		// Define the APIs supported by this broker
		// Always include ApiVersions (18) for successful responses
		resp.ApiKeys = []ApiKeyVersion{
			// Report support for versions 0 through 4 for ApiVersions
			{ApiKey: 18, MinVersion: 0, MaxVersion: 4}, // ApiVersions itself
			// Add other supported APIs here later
		}
	}

	return resp
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
	resp := handleApiVersionsRequest(req)

	// Write the response using the dedicated function
	err = WriteResponse(conn, resp)
	if err != nil {
		fmt.Println("Error writing response:", err.Error())
	}
}
