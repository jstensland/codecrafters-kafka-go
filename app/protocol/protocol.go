// Package protocol implements the Kafka wire protocol for request and response handling.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Bit mask for varint encoding
const varintContinueBit = 0x80

// Error types for protocol operations
var (
	ErrMessageSizeExceedsLimit = errors.New("message size exceeds limit")
	ErrMessageSizeTooSmall     = errors.New("message size too small for header")
	ErrSizeMismatch            = errors.New("calculated response size does not match actual written size")
)

// Serializable defines an interface for types that can be serialized into a byte slice.
type Serializable interface {
	Serialize() []byte
}

// writeUvarint encodes a uint64 as an unsigned varint into the provided byte slice
// and returns the number of bytes written.
// It panics if the buffer is too small.
func writeUvarint(buf []byte, x uint64) int {
	i := 0
	for x >= varintContinueBit {
		buf[i] = byte(x) | varintContinueBit
		x >>= 7
		i++
	}
	buf[i] = byte(x)
	return i + 1
}

// Kafka protocol constants
const (
	// Field sizes in bytes
	SizeFieldLength     = 4
	CorrelationIDLength = 4
	ErrorCodeLength     = 2
	// ArrayLengthField    = 4 // No longer used directly for ApiVersions response
	APIKeyFieldLength  = 2
	VersionFieldLength = 2
	ThrottleTimeLength = 4
	TaggedFieldsLength = 1 // UVarint 0 for empty tagged fields (at the end of the struct/array)

	// APIKeyVersion is 7 bytes: APIKey(2) + MinVersion(2) + MaxVersion(2) + TaggedFields(1)
	APIKeyEntryLength = APIKeyFieldLength + VersionFieldLength + VersionFieldLength + TaggedFieldsLength

	// Error codes (Exported)
	ErrorNone               = 0
	ErrorUnsupportedVersion = 35

	// API Keys (Exported)
	APIKeyAPIVersions             = 18
	APIKeyDescribeTopicPartitions = 75
)

// Request represents the structure of an incoming Kafka request header
type Request struct {
	Size           uint32
	APIKey         uint16
	APIVersion     uint16
	CorrelationID  uint32
	RemainingBytes []byte // The rest of the payload after the header fields
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

// Response represents the structure of a Kafka ApiVersions response (Version >= 3)
type Response struct {
	CorrelationID  uint32
	ErrorCode      int16
	APIKeys        []APIKeyVersion
	ThrottleTimeMs int32
}

// ParseRequest reads from the reader and parses the Kafka request header
func ParseRequest(reader io.Reader) (*Request, error) {
	// 1. Read the message size (first 4 bytes)
	sizeBytes := make([]byte, SizeFieldLength)
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
		return nil, fmt.Errorf("%w: %d", ErrMessageSizeExceedsLimit, messageSize)
	}
	// Minimum header size: APIKey(2) + APIVersion(2) + CorrelationID(4) = 8
	minHeaderSize := uint32(APIKeyFieldLength + VersionFieldLength + CorrelationIDLength)
	if messageSize < minHeaderSize {
		return nil, fmt.Errorf("%w: %d", ErrMessageSizeTooSmall, messageSize)
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
	correlationEnd := versionEnd + CorrelationIDLength

	req := &Request{
		Size:           messageSize,
		APIKey:         binary.BigEndian.Uint16(payload[0:apiKeyEnd]),
		APIVersion:     binary.BigEndian.Uint16(payload[apiKeyEnd:versionEnd]),
		CorrelationID:  binary.BigEndian.Uint32(payload[versionEnd:correlationEnd]),
		RemainingBytes: payload[correlationEnd:], // Store the rest if needed later
	}

	return req, nil
}

// writeCompactArray encodes a slice of Serializable items into the Kafka COMPACT_ARRAY format
// into the provided buffer. It returns the number of bytes written.
// Assumes the buffer `buf` is large enough.
func writeCompactArray(buf []byte, items []Serializable) int {
	offset := 0

	// Write Array Length (N+1) as Uvarint
	// #nosec G115 -- Conversion is safe in this context
	nBytes := writeUvarint(buf[offset:], uint64(len(items)+1))
	offset += nBytes

	// Write Array Elements
	for _, item := range items {
		serialized := item.Serialize()
		itemLen := len(serialized)
		copy(buf[offset:offset+itemLen], serialized)
		offset += itemLen
	}
	return offset
}

// WriteResponse serializes and writes the full ApiVersions response to the given writer
// Note: This currently only supports ApiVersions response format (v3+)
func WriteResponse(writer io.Writer, resp *Response) error {
	// Calculate sizes for API keys payload
	apiKeyPayloadBytes := len(resp.APIKeys) * APIKeyEntryLength

	// Calculate size needed for the Uvarint length prefix (N+1)
	// We need a temporary buffer to determine the varint size
	varintBuf := make([]byte, binary.MaxVarintLen64) // Max size for a uvarint
	// #nosec G115 -- Conversion is safe in this context
	arrayLengthVarintSize := writeUvarint(varintBuf, uint64(len(resp.APIKeys)+1))

	// Calculate response sizes using COMPACT_ARRAY format for APIKeys
	responseBodySize := ErrorCodeLength + arrayLengthVarintSize + apiKeyPayloadBytes +
		ThrottleTimeLength + TaggedFieldsLength
	responseHeaderSize := CorrelationIDLength
	// #nosec G115 -- Conversion is safe in this context
	totalSize := uint32(responseHeaderSize + responseBodySize) // Size *excluding* the initial size field itself

	// Allocate buffer - exactly the size we need (Size field + rest of the message)
	responseBytes := make([]byte, SizeFieldLength+totalSize)
	offset := 0

	// 1. Write Total Size (excluding itself)
	binary.BigEndian.PutUint32(responseBytes[offset:offset+SizeFieldLength], totalSize)
	offset += SizeFieldLength

	// 2. Write Header: Correlation ID
	binary.BigEndian.PutUint32(responseBytes[offset:offset+CorrelationIDLength], resp.CorrelationID)
	offset += CorrelationIDLength

	// 3. Write Body: ErrorCode
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint16(responseBytes[offset:offset+ErrorCodeLength], uint16(resp.ErrorCode))
	offset += ErrorCodeLength

	// 4. Write Body: APIKeys Array (COMPACT_ARRAY format) using writeCompactArray
	// Convert []APIKeyVersion to []Serializable (needs pointers for Serialize method)
	serializableAPIKeys := make([]Serializable, len(resp.APIKeys))
	for i := range resp.APIKeys {
		// Create a pointer to the element because Serialize has a pointer receiver
		serializableAPIKeys[i] = &resp.APIKeys[i]
	}
	nBytes := writeCompactArray(responseBytes[offset:], serializableAPIKeys)
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
		return fmt.Errorf("%w: calculated=%d, actual=%d",
			ErrSizeMismatch, len(responseBytes), offset)
	}

	// Send the complete response
	_, err := writer.Write(responseBytes)
	if err != nil {
		return fmt.Errorf("writing response: %w", err)
	}
	return nil
}
