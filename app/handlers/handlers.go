// Package handlers contains the logic for handling client connections and requests specific to Kafka API keys.
package handlers

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// Constants
const (
	apiVersionsV4 = 4 // Supported API version for ApiVersions

	// ArrayLengthField    = 4 // No longer used directly for ApiVersions response
	APIKeyFieldLength  = 2
	VersionFieldLength = 2
	ThrottleTimeLength = 4
	TaggedFieldsLength = 1 // UVarint 0 for empty tagged fields (at the end of the struct/array)

	// APIKeyVersion is 7 bytes: APIKey(2) + MinVersion(2) + MaxVersion(2) + TaggedFields(1)
	APIKeyEntryLength = APIKeyFieldLength + VersionFieldLength + VersionFieldLength + TaggedFieldsLength

	// API Keys (Exported)
	APIKeyAPIVersions             = 18
	APIKeyDescribeTopicPartitions = 75
)

// APIVersionsRequest represents the structure of an incoming Kafka request header
type APIVersionsRequest struct {
	Size           uint32
	APIKey         uint16
	APIVersion     uint16
	CorrelationID  uint32
	RemainingBytes []byte // The rest of the payload after the header fields
}

// APIVersionsResponse represents the structure of a Kafka ApiVersions response (Version >= 3)
type APIVersionsResponse struct {
	CorrelationID  uint32
	ErrorCode      int16
	APIKeys        []APIKeyVersion
	ThrottleTimeMs int32
}

// APIKeyVersion represents the supported version range for a single Kafka API key
type APIKeyVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// Serialize converts an APIKeyVersion to its binary representation
func (akv *APIKeyVersion) Serialize() []byte {
	bytes := make([]byte, APIKeyEntryLength)

	// Write APIKey (2 bytes)
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint16(bytes[0:2], uint16(akv.APIKey))

	// Write MinVersion (2 bytes)
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint16(bytes[2:4], uint16(akv.MinVersion))

	// Write MaxVersion (2 bytes)
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint16(bytes[4:6], uint16(akv.MaxVersion))

	// Write Tagged Fields (1 byte, UVarint 0)
	bytes[6] = 0 // The UVarint encoding for 0 is a single byte 0

	return bytes
}

// ParseRequest reads from the reader and parses the Kafka request header
func ParseRequest(reader io.Reader) (*APIVersionsRequest, error) {
	// 1. Read the message size (first 4 bytes)
	sizeBytes := make([]byte, protocol.SizeFieldLength)
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
		return nil, fmt.Errorf("%w: %d", protocol.ErrMessageSizeExceedsLimit, messageSize)
	}
	// Minimum header size: APIKey(2) + APIVersion(2) + CorrelationID(4) = 8
	minHeaderSize := uint32(APIKeyFieldLength + VersionFieldLength + protocol.CorrelationIDLength)
	if messageSize < minHeaderSize {
		return nil, fmt.Errorf("%w: %d", protocol.ErrMessageSizeTooSmall, messageSize)
	}

	// 2. Read the rest of the message payload (messageSize bytes)
	// The payload contains APIKey, APIVersion, CorrelationID, etc.
	payload := make([]byte, messageSize)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, fmt.Errorf("reading message payload: %w", err)
	}

	// 3. Parse fields from the payload
	apiKeyEnd := APIKeyFieldLength
	versionEnd := apiKeyEnd + VersionFieldLength
	correlationEnd := versionEnd + protocol.CorrelationIDLength

	req := &APIVersionsRequest{
		Size:           messageSize,
		APIKey:         binary.BigEndian.Uint16(payload[0:apiKeyEnd]),
		APIVersion:     binary.BigEndian.Uint16(payload[apiKeyEnd:versionEnd]),
		CorrelationID:  binary.BigEndian.Uint32(payload[versionEnd:correlationEnd]),
		RemainingBytes: payload[correlationEnd:], // Store the rest if needed later
	}

	return req, nil
}

// Serialize converts the ApiVersions Response (v3+) into its byte representation.
// This method implements the APIResponse interface.
func (resp *APIVersionsResponse) Serialize() ([]byte, error) {
	// Calculate sizes for API keys payload
	apiKeyPayloadBytes := len(resp.APIKeys) * APIKeyEntryLength

	// Calculate size needed for the Uvarint length prefix (N+1) for the compact array
	// We need a temporary buffer to determine the varint size
	varintBuf := make([]byte, binary.MaxVarintLen64) // Max size for a uvarint
	// #nosec G115 -- Conversion is safe in this context
	arrayLengthVarintSize := protocol.WriteUvarint(varintBuf, uint64(len(resp.APIKeys)+1))

	// Calculate response sizes using COMPACT_ARRAY format for APIKeys
	responseBodySize := protocol.ErrorCodeLength + arrayLengthVarintSize + apiKeyPayloadBytes +
		ThrottleTimeLength + TaggedFieldsLength
	responseHeaderSize := protocol.CorrelationIDLength
	// #nosec G115 -- Conversion is safe in this context
	totalSize := uint32(responseHeaderSize + responseBodySize) // Size *excluding* the initial size field itself

	// Allocate buffer - exactly the size we need (Size field + rest of the message)
	responseBytes := make([]byte, protocol.SizeFieldLength+totalSize)
	offset := 0

	// 1. Write Total Size (excluding itself)
	binary.BigEndian.PutUint32(responseBytes[offset:offset+protocol.SizeFieldLength], totalSize)
	offset += protocol.SizeFieldLength

	// 2. Write Header: Correlation ID
	binary.BigEndian.PutUint32(responseBytes[offset:offset+protocol.CorrelationIDLength], resp.CorrelationID)
	offset += protocol.CorrelationIDLength

	// 3. Write Body: ErrorCode
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint16(responseBytes[offset:offset+protocol.ErrorCodeLength], uint16(resp.ErrorCode))
	offset += protocol.ErrorCodeLength

	// 4. Write Body: APIKeys Array (COMPACT_ARRAY format) using writeCompactArray
	// Convert []APIKeyVersion to []Serializable (needs pointers for Serialize method)
	serializableAPIKeys := make([]protocol.Serializable, len(resp.APIKeys))
	for i := range resp.APIKeys {
		// Create a pointer to the element because Serialize has a pointer receiver
		serializableAPIKeys[i] = &resp.APIKeys[i]
	}
	nBytes := protocol.WriteCompactArray(responseBytes[offset:], serializableAPIKeys)
	offset += nBytes

	// 5. Write Body: ThrottleTimeMs
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint32(responseBytes[offset:offset+ThrottleTimeLength], uint32(resp.ThrottleTimeMs))
	offset += ThrottleTimeLength

	// 6. Write Body: Tagged Fields (UVarint 0 indicates none for the overall response)
	responseBytes[offset] = 0    // The UVarint encoding for 0 is a single byte 0
	offset += TaggedFieldsLength // Increment offset even though it's the last field

	// Sanity check: ensure offset matches the calculated total size
	if offset != len(responseBytes) {
		return nil, fmt.Errorf("%w: calculated=%d, actual=%d",
			protocol.ErrSizeMismatch, len(responseBytes), offset)
	}

	return responseBytes, nil
}

// HandleAPIVersionsRequest processes an ApiVersions request and returns the appropriate response
func HandleAPIVersionsRequest(req *APIVersionsRequest) *APIVersionsResponse {
	resp := &APIVersionsResponse{
		CorrelationID:  req.CorrelationID,
		ThrottleTimeMs: 0, // No throttling implemented
	}

	// Currently, only ApiVersions v4 is "supported" for a successful response.
	// Other versions will result in an UNSUPPORTED_VERSION error.
	// Note: Kafka protocol allows brokers to support multiple versions.
	// A real broker would check req.ApiVersion against its supported range.
	if req.APIVersion != apiVersionsV4 {
		resp.ErrorCode = protocol.ErrorUnsupportedVersion
		resp.APIKeys = []APIKeyVersion{} // Must be empty on error
	} else {
		resp.ErrorCode = protocol.ErrorNone // Success
		// Define the APIs supported by this broker
		// Always include ApiVersions (18) and DescribeTopicPartitions (75) for successful responses
		resp.APIKeys = []APIKeyVersion{
			// Report support for versions 0 through 4 for ApiVersions
			{APIKey: APIKeyAPIVersions, MinVersion: 0, MaxVersion: apiVersionsV4},
			// Report support for version 0 for DescribeTopicPartitions
			{APIKey: APIKeyDescribeTopicPartitions, MinVersion: 0, MaxVersion: 0},
		}
	}

	return resp
}
