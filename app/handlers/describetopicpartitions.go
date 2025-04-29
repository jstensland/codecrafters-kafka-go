// Package handlers implements the logic for handling specific Kafka API requests.
package handlers

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/google/uuid"
)

// Constants specific to DescribeTopicPartitions v0
const (
	// Field lengths for DescribeTopicPartitions v0 Response
	describeTopicPartitionsV0TopicNameLengthBytes = 2
	describeTopicPartitionsV0TopicIDBytes         = 16 // UUID
	describeTopicPartitionsV0PartitionsArrayBytes = 4  // Array length prefix
)

// ErrParseDescribeTopicRequest indicates an error during the parsing of a DescribeTopicPartitions request.
var ErrParseDescribeTopicRequest = errors.New("error parsing describe topic partitions request")

// DescribeTopicPartitionsRequestTopicV0 represents a topic in the DescribeTopicPartitions request v0.
type DescribeTopicPartitionsRequestTopicV0 struct {
	TopicName string
	// PartitionIndexes are ignored for now as per requirements
}

// DescribeTopicPartitionsRequestV0 represents the DescribeTopicPartitions request v0.
type DescribeTopicPartitionsRequestV0 struct {
	ClientID string // Added ClientID field
	Topics   []*DescribeTopicPartitionsRequestTopicV0
	// IncludeClusterAuthorizedOperations and IncludeTopicAuthorizedOperations are ignored for v0
}

// DescribeTopicPartitionsResponsePartitionV0 represents a partition in the DescribeTopicPartitions response v0.
// Note: This struct is defined but will be empty in the response for UNKNOWN_TOPIC_OR_PARTITION.
type DescribeTopicPartitionsResponsePartitionV0 struct {
	// Fields omitted as the array will be empty
}

// DescribeTopicPartitionsResponseTopicV0 represents a topic in the DescribeTopicPartitions response v0.
type DescribeTopicPartitionsResponseTopicV0 struct {
	ErrorCode    int16
	TopicName    string
	TopicID      uuid.UUID
	IsInternal   bool // Not used in v0, defaults to false
	Partitions   []*DescribeTopicPartitionsResponsePartitionV0
	TopicAuthOps int32 // Not used in v0, defaults to 0
}

// DescribeTopicPartitionsResponseV0 represents the DescribeTopicPartitions response v0.
type DescribeTopicPartitionsResponseV0 struct {
	CorrelationID  uint32
	ThrottleTimeMs int32 // Not used in v0, defaults to 0
	Topics         []*DescribeTopicPartitionsResponseTopicV0
}

// ParseDescribeTopicPartitionsRequest parses the raw byte payload for a DescribeTopicPartitions v0 request.
// For this stage, we only care about the first topic name.
func ParseDescribeTopicPartitionsRequest(payload []byte) (*DescribeTopicPartitionsRequestV0, error) {
	req := &DescribeTopicPartitionsRequestV0{}
	offset := 0

	// Parse Client ID Length (int16)
	if len(payload) < offset+2 {
		return nil, fmt.Errorf("payload too short for client id length: %w", ErrParseDescribeTopicRequest)
	}
	clientIDLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	offset += 2

	if clientIDLen < 0 {
		// Kafka protocol allows -1 for null strings, handle if necessary, but typically client ID is not null.
		// For now, treat negative length as an error.
		return nil, fmt.Errorf("invalid negative client id length %d: %w", clientIDLen, ErrParseDescribeTopicRequest)
	}
	if clientIDLen == 0 {
		req.ClientID = "" // Empty client ID
	} else {
		// Parse Client ID (string)
		if len(payload) < offset+clientIDLen {
			return nil, fmt.Errorf("payload too short for client id: %w", ErrParseDescribeTopicRequest)
		}
		req.ClientID = string(payload[offset : offset+clientIDLen])
		offset += clientIDLen
	}

	log.Printf("DEBUG: ClientID: %s\n", req.ClientID)

	// Parse Tagged Fields byte (UVarint count, expected to be 0 for v0)
	if len(payload) < offset+1 {
		return nil, fmt.Errorf("payload too short for tagged fields byte: %w", ErrParseDescribeTopicRequest)
	}
	taggedFieldsByte := payload[offset]
	offset++
	if taggedFieldsByte != 0 {
		// v0 of DescribeTopicPartitions does not support tagged fields.
		// Log a warning but continue parsing as per the structure.
		log.Printf(
			"WARN: Received non-zero tagged fields byte (0x%X) for DescribeTopicPartitionsRequestV0, expected 0",
			taggedFieldsByte)
		// In a stricter implementation, this might be an error.
		// If tagged fields were actually present, the offset calculation would need to skip them.
	}

	// 1. Parse Topic Array Length (Uvarint)
	// Kafka compact arrays use Uvarint for length, encoded as N+1.
	topicArrayLenPlusOne, bytesRead := binary.Uvarint(payload[offset:])
	if bytesRead <= 0 {
		// bytesRead == 0 means buffer too small
		// bytesRead < 0 means value overflowed uint64
		return nil, fmt.Errorf(
			"failed to read topic array length (uvarint) bytesRead=%d: %w",
			bytesRead, ErrParseDescribeTopicRequest)
	}
	offset += bytesRead

	log.Printf("DEBUG: bytes read: %d\n", bytesRead)
	log.Printf("DEBUG: Topic Array Length (N+1): %d\n", topicArrayLenPlusOne)

	if topicArrayLenPlusOne == 0 {
		return nil, fmt.Errorf(
			"invalid topic array length uvarint N+1 cannot be 0: %w",
			ErrParseDescribeTopicRequest)
	}
	topicArrayLen := int(topicArrayLenPlusOne - 1) // Actual length is N

	// We only need the first topic for this stage
	if topicArrayLen > 0 {
		// 2. Parse Topic Name Length (Uvarint)
		topicNameLenPlusOne, bytesReadNameLen := binary.Uvarint(payload[offset:])
		if bytesReadNameLen <= 0 {
			return nil, fmt.Errorf("failed to read topic name length (uvarint) bytesRead=%d: %w",
				bytesReadNameLen, ErrParseDescribeTopicRequest)
		}
		offset += bytesReadNameLen

		if topicNameLenPlusOne == 0 {
			// This represents a null string in Kafka compact format.
			// Handle appropriately if null topic names are possible/expected.
			// For now, treat as an error or empty string? Let's assume error for now.
			return nil, fmt.Errorf("invalid topic name length uvarint N+1 cannot be 0 for non-null string: %w",
				ErrParseDescribeTopicRequest)
			// Alternatively, handle as empty string: topicName = ""
		}
		topicNameLen := int(topicNameLenPlusOne - 1) // Actual length is N

		log.Printf("DEBUG: Topic Name Length (N+1): %d, Bytes Read: %d\n", topicNameLenPlusOne, bytesReadNameLen)
		log.Printf("DEBUG: Actual Topic Name Length: %d\n", topicNameLen)

		// 3. Parse Topic Name (string)
		if len(payload) < offset+topicNameLen {
			return nil, fmt.Errorf("payload too short for topic name (expected %d bytes, have %d): %w",
				topicNameLen, len(payload)-offset, ErrParseDescribeTopicRequest)
		}
		topicName := string(payload[offset : offset+topicNameLen])
		offset += topicNameLen

		req.Topics = append(req.Topics, &DescribeTopicPartitionsRequestTopicV0{
			TopicName: topicName,
		})

		// 4. Parse Partition Index Array Length (int32) - Skip for now
		if len(payload) < offset+4 {
			return nil, fmt.Errorf("payload too short for partition index array length: %w", ErrParseDescribeTopicRequest)
		}
		// partitionArrayLen := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		// offset += 4
		// TODO: Skip partition indices if needed later

		log.Printf("DEBUG: Topic Name: %s\n", topicName)

	} else {
		// Handle case with zero topics if necessary, though the test case implies one topic.
		req.Topics = []*DescribeTopicPartitionsRequestTopicV0{}
	}

	// Ignore remaining fields
	// - Response Partition Limit
	// - Cursor
	// - Tag Buffer

	return req, nil
}

// Serialize serializes the DescribeTopicPartitionsResponseTopicV0 struct into bytes.
func (t *DescribeTopicPartitionsResponseTopicV0) Serialize() []byte {
	// Calculate size: ErrorCode(2) + TopicNameLen(2) + TopicName + TopicID(16) + PartitionsArrayLen(4)
	topicNameBytes := []byte(t.TopicName)
	topicNameLen := len(topicNameBytes)
	// #nosec G115 -- Conversion is safe in this context
	totalSize := protocol.ErrorCodeLength + describeTopicPartitionsV0TopicNameLengthBytes + topicNameLen + describeTopicPartitionsV0TopicIDBytes + describeTopicPartitionsV0PartitionsArrayBytes

	buf := make([]byte, totalSize)
	offset := 0

	// Write ErrorCode (int16)
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint16(buf[offset:offset+protocol.ErrorCodeLength], uint16(t.ErrorCode))
	offset += protocol.ErrorCodeLength

	// Write TopicName Length (int16)
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint16(buf[offset:offset+describeTopicPartitionsV0TopicNameLengthBytes], uint16(topicNameLen))
	offset += describeTopicPartitionsV0TopicNameLengthBytes

	// Write TopicName (string)
	copy(buf[offset:offset+topicNameLen], topicNameBytes)
	offset += topicNameLen

	// Write TopicID (UUID - 16 bytes)
	topicIDBytes, _ := t.TopicID.MarshalBinary() // uuid.MarshalBinary never returns an error
	copy(buf[offset:offset+describeTopicPartitionsV0TopicIDBytes], topicIDBytes)
	offset += describeTopicPartitionsV0TopicIDBytes

	// Write Partitions Array Length (int32) - Always 0 for UNKNOWN_TOPIC_OR_PARTITION
	binary.BigEndian.PutUint32(buf[offset:offset+describeTopicPartitionsV0PartitionsArrayBytes], 0)
	// offset += describeTopicPartitionsV0PartitionsArrayBytes // No need to increment offset, it's the last field

	return buf
}

// Serialize serializes the DescribeTopicPartitionsResponseV0 struct into bytes.
// Implements the protocol.APIResponse interface.
func (r *DescribeTopicPartitionsResponseV0) Serialize() ([]byte, error) {
	// Calculate total size: Size(4) + CorrelationID(4) + ThrottleTime(4) + TopicsArrayLen(4) + TopicsData
	topicPayloads := make([][]byte, len(r.Topics))
	topicsDataSize := 0
	for i, topic := range r.Topics {
		topicBytes := topic.Serialize()
		topicPayloads[i] = topicBytes
		topicsDataSize += len(topicBytes)
	}

	// Calculate size needed for the Uvarint length prefix (N+1) for the compact array
	varintBuf := make([]byte, binary.MaxVarintLen64) // Max size for a uvarint
	// #nosec G115 -- Conversion is safe in this context
	arrayLengthVarintSize := protocol.WriteUvarint(varintBuf, uint64(len(r.Topics)+1))

	log.Printf("DEBUG: number of topics: %d\n", len(r.Topics)+1)

	// ThrottleTime(4) + TopicsArrayLen(Uvarint)
	totalSize := protocol.SizeFieldLength + protocol.CorrelationIDLength + TaggedFieldsLength +
		4 + arrayLengthVarintSize + topicsDataSize
	buf := make([]byte, totalSize)
	offset := 0

	// Write Size (uint32) - Total size minus the size field itself
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint32(buf[offset:offset+protocol.SizeFieldLength], uint32(totalSize-protocol.SizeFieldLength))
	offset += protocol.SizeFieldLength

	log.Printf("DEBUG: write payload size bytes: %x", uint32(totalSize-protocol.SizeFieldLength))

	// Write CorrelationID (uint32)
	binary.BigEndian.PutUint32(buf[offset:offset+protocol.CorrelationIDLength], r.CorrelationID)
	offset += protocol.CorrelationIDLength

	log.Printf("DEBUG: correlation id bytes: %x", r.CorrelationID)

	// Tagged buffer expeted after headers
	buf[offset] = 0              // The UVarint encoding for 0 is a single byte 0
	offset += TaggedFieldsLength // Increment offset

	// Write ThrottleTimeMs (int32)
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(r.ThrottleTimeMs))
	offset += 4

	log.Printf("DEBUG: ThrottleTimeMs bytes: %x", r.ThrottleTimeMs)

	// Write Topics Array Length (Uvarint N+1)
	// #nosec G115 -- Conversion is safe in this context
	nBytes := protocol.WriteUvarint(buf[offset:], uint64(len(r.Topics)+1))
	offset += nBytes

	log.Printf("DEBUG: array length bytes: %x", buf[offset-nBytes:offset])

	// Write Topic Payloads
	for _, topicBytes := range topicPayloads {
		copy(buf[offset:], topicBytes)
		offset += len(topicBytes)
	}

	if offset != totalSize {
		return nil, fmt.Errorf("describe topic partitions response serialize size mismatch: expected %d, got %d",
			totalSize, offset)
	}

	return buf, nil
}

// HandleDescribeTopicPartitionsRequest handles a DescribeTopicPartitions v0 request.
// For this stage, it always returns UNKNOWN_TOPIC_OR_PARTITION.
func HandleDescribeTopicPartitionsRequest(req *BaseRequest) *DescribeTopicPartitionsResponseV0 {
	log.Printf("DEBUG: Handling DescribeTopicPartitions request: %s\n", req)
	parsedReq, err := ParseDescribeTopicPartitionsRequest(req.RemainingBytes)
	if err != nil {
		// Handle parsing error - potentially return a generic error response
		// For now, log and return a basic error response matching the structure
		log.Printf("Error parsing DescribeTopicPartitions request: %v\n", err)
		// Returning a response with error code might be complex if parsing failed early.
		// For simplicity in this stage, we might assume parsing succeeds enough
		// to get the correlation ID, or return a default error structure.
		// Let's try to return the expected structure even on parse error, if possible.
		return &DescribeTopicPartitionsResponseV0{
			CorrelationID:  req.CorrelationID,
			ThrottleTimeMs: 0,
			Topics:         []*DescribeTopicPartitionsResponseTopicV0{}, // Empty topics on parse error
		}
	}

	// Assume only one topic in the request for this stage as per requirements
	topicName := ""
	if len(parsedReq.Topics) > 0 {
		topicName = parsedReq.Topics[0].TopicName
	}

	// Construct the response topic
	responseTopic := &DescribeTopicPartitionsResponseTopicV0{
		ErrorCode:    protocol.ErrorUnknownTopicOrPartition,
		TopicName:    topicName,
		TopicID:      uuid.Nil, // Zero UUID
		IsInternal:   false,
		Partitions:   []*DescribeTopicPartitionsResponsePartitionV0{}, // Empty partitions array
		TopicAuthOps: 0,
	}

	// Construct the full response
	resp := &DescribeTopicPartitionsResponseV0{
		CorrelationID:  req.CorrelationID,
		ThrottleTimeMs: 0,
		Topics:         []*DescribeTopicPartitionsResponseTopicV0{responseTopic},
	}

	return resp
}
