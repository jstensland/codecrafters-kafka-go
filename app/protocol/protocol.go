package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// writeUvarint encodes a uint64 as an unsigned varint into the provided byte slice
// and returns the number of bytes written.
// It panics if the buffer is too small.
func writeUvarint(buf []byte, x uint64) int {
	i := 0
	for x >= 0x80 {
		buf[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	buf[i] = byte(x)
	return i + 1
}

// Kafka protocol constants
const (
	// Field sizes in bytes
	SIZE_FIELD_LENGTH     = 4
	CORRELATION_ID_LENGTH = 4
	ERROR_CODE_LENGTH     = 2
	// ARRAY_LENGTH_FIELD    = 4 // No longer used directly for ApiVersions response
	API_KEY_FIELD_LENGTH = 2
	VERSION_FIELD_LENGTH = 2
	THROTTLE_TIME_LENGTH = 4
	TAGGED_FIELDS_LENGTH = 1 // UVarint 0 for empty tagged fields (at the end of the struct/array)

	// ApiKeyVersion is 7 bytes: ApiKey(2) + MinVersion(2) + MaxVersion(2) + TaggedFields(1)
	API_KEY_ENTRY_LENGTH = API_KEY_FIELD_LENGTH + VERSION_FIELD_LENGTH + VERSION_FIELD_LENGTH + TAGGED_FIELDS_LENGTH

	// Error codes (Exported)
	ERROR_NONE                = 0
	ERROR_UNSUPPORTED_VERSION = 35

	// API Keys (Exported)
	API_KEY_API_VERSIONS = 18
)

// Request represents the structure of an incoming Kafka request header
type Request struct {
	Size           uint32
	ApiKey         uint16
	ApiVersion     uint16
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

	// Write Tagged Fields (1 byte, UVarint 0)
	bytes[6] = 0 // The UVarint encoding for 0 is a single byte 0

	return bytes
}

// Response represents the structure of a Kafka ApiVersions response (Version >= 3)
type Response struct {
	CorrelationID  uint32
	ErrorCode      int16
	ApiKeys        []ApiKeyVersion
	ThrottleTimeMs int32
}

// ParseRequest reads from the reader and parses the Kafka request header
func ParseRequest(reader io.Reader) (*Request, error) {
	// 1. Read the message size (first 4 bytes)
	sizeBytes := make([]byte, SIZE_FIELD_LENGTH)
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
	// Minimum header size: ApiKey(2) + ApiVersion(2) + CorrelationID(4) = 8
	minHeaderSize := uint32(API_KEY_FIELD_LENGTH + VERSION_FIELD_LENGTH + CORRELATION_ID_LENGTH)
	if messageSize < minHeaderSize {
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
		ApiKey:         binary.BigEndian.Uint16(payload[0:API_KEY_FIELD_LENGTH]),
		ApiVersion:     binary.BigEndian.Uint16(payload[API_KEY_FIELD_LENGTH : API_KEY_FIELD_LENGTH+VERSION_FIELD_LENGTH]),
		CorrelationID:  binary.BigEndian.Uint32(payload[API_KEY_FIELD_LENGTH+VERSION_FIELD_LENGTH : API_KEY_FIELD_LENGTH+VERSION_FIELD_LENGTH+CORRELATION_ID_LENGTH]),
		RemainingBytes: payload[API_KEY_FIELD_LENGTH+VERSION_FIELD_LENGTH+CORRELATION_ID_LENGTH:], // Store the rest if needed later
	}

	return req, nil
}

// WriteResponse serializes and writes the full ApiVersions response to the given writer
// Note: This currently only supports ApiVersions response format (v3+)
func WriteResponse(writer io.Writer, resp *Response) error {
	// Calculate sizes for API keys payload
	apiKeyPayloadBytes := len(resp.ApiKeys) * API_KEY_ENTRY_LENGTH

	// Calculate size needed for the Uvarint length prefix (N+1)
	// We need a temporary buffer to determine the varint size
	varintBuf := make([]byte, binary.MaxVarintLen64) // Max size for a uvarint
	arrayLengthVarintSize := writeUvarint(varintBuf, uint64(len(resp.ApiKeys)+1))

	// Calculate response sizes using COMPACT_ARRAY format for ApiKeys
	responseBodySize := ERROR_CODE_LENGTH + arrayLengthVarintSize + apiKeyPayloadBytes + THROTTLE_TIME_LENGTH + TAGGED_FIELDS_LENGTH
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

	// 4. Write Body: ApiKeys Array (COMPACT_ARRAY format)
	// 4a. Write Array Length (N+1) as Uvarint
	nBytes := writeUvarint(responseBytes[offset:], uint64(len(resp.ApiKeys)+1))
	offset += nBytes

	// 4b. Write Array Elements
	for _, apiKey := range resp.ApiKeys {
		serialized := apiKey.Serialize()
		copy(responseBytes[offset:offset+API_KEY_ENTRY_LENGTH], serialized)
		offset += API_KEY_ENTRY_LENGTH
	}

	// 5. Write Body: ThrottleTimeMs
	binary.BigEndian.PutUint32(responseBytes[offset:offset+THROTTLE_TIME_LENGTH], uint32(resp.ThrottleTimeMs))
	offset += THROTTLE_TIME_LENGTH

	// 6. Write Body: Tagged Fields (UVarint 0 indicates none for the overall response)
	responseBytes[offset] = 0 // The UVarint encoding for 0 is a single byte 0
	offset += TAGGED_FIELDS_LENGTH // Increment offset even though it's the last field

	// Sanity check: ensure offset matches the calculated total size
	if offset != len(responseBytes) {
		return fmt.Errorf("internal error: calculated response size %d does not match actual written size %d", len(responseBytes), offset)
	}

	// Send the complete response
	_, err := writer.Write(responseBytes)
	if err != nil {
		return fmt.Errorf("writing response: %w", err)
	}
	return nil
}
