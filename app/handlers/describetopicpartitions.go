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

// Constants specific to DescribeTopicPartitions
const (
	// Field lengths for DescribeTopicPartitions Response
	describeTopicPartitionsTopicNameLengthBytes = 2
	describeTopicPartitionsTopicIDBytes         = 16 // UUID
	describeTopicPartitionsPartitionsArrayBytes = 4  // Array length prefix
	describeTopicAuthorizeOperationBytes        = 4
)

// ErrParseDescribeTopicRequest indicates an error during the parsing of a DescribeTopicPartitions request.
var ErrParseDescribeTopicRequest = errors.New("error parsing describe topic partitions request")

// DescribeTopicPartitionsRequestTopic represents a topic in the DescribeTopicPartitions request.
type DescribeTopicPartitionsRequestTopic struct {
	TopicName string
	// PartitionIndexes are ignored for now as per requirements
}

// DescribeTopicPartitionsRequest represents the DescribeTopicPartitions request
type DescribeTopicPartitionsRequest struct {
	ClientID string // Added ClientID field
	Topics   []*DescribeTopicPartitionsRequestTopic
	// IncludeClusterAuthorizedOperations and IncludeTopicAuthorizedOperations are ignored
}

// DescribeTopicPartitionsResponsePartition represents a partition in the DescribeTopicPartitions response.
// Note: This struct is defined but will be empty in the response for UNKNOWN_TOPIC_OR_PARTITION.
type DescribeTopicPartitionsResponsePartition struct {
	// Fields omitted as the array will be empty
}

// DescribeTopicPartitionsResponseTopic represents a topic in the DescribeTopicPartitions response.
type DescribeTopicPartitionsResponseTopic struct {
	ErrorCode    int16
	TopicName    string
	TopicID      uuid.UUID
	IsInternal   bool
	Partitions   []*DescribeTopicPartitionsResponsePartition
	TopicAuthOps int32
}

// DescribeTopicPartitionsResponse represents the DescribeTopicPartitions response.
type DescribeTopicPartitionsResponse struct {
	CorrelationID  uint32
	ThrottleTimeMs int32
	Topics         []*DescribeTopicPartitionsResponseTopic
}

// ParseDescribeTopicPartitionsRequest parses the raw byte payload for a DescribeTopicPartitions request.
// For this stage, we only care about the first topic name.
//
//nolint:cyclop,funlen // TODO: break parsing into meaningful units by type
func ParseDescribeTopicPartitionsRequest(payload []byte) (*DescribeTopicPartitionsRequest, error) {
	req := &DescribeTopicPartitionsRequest{}
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

	// Parse Tagged Fields byte (UVarint count, expected to be 0)
	if len(payload) < offset+1 {
		return nil, fmt.Errorf("payload too short for tagged fields byte: %w", ErrParseDescribeTopicRequest)
	}
	taggedFieldsByte := payload[offset]
	offset++
	if taggedFieldsByte != 0 {
		// DescribeTopicPartitions does not support tagged fields right now.
		// Log a warning but continue parsing as per the structure.
		log.Printf(
			"WARN: Received non-zero tagged fields byte (0x%X) for DescribeTopicPartitionsRequest, expected 0",
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

	if topicArrayLenPlusOne == 0 {
		return nil, fmt.Errorf(
			"invalid topic array length uvarint N+1 cannot be 0: %w",
			ErrParseDescribeTopicRequest)
	}
	topicArrayLen := topicArrayLenPlusOne - 1 // Actual length is N

	if topicArrayLen <= 0 {
		// Handle case with zero topics if necessary
		req.Topics = []*DescribeTopicPartitionsRequestTopic{}
		return req, nil
	}
	// We only need the first topic for this stage

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
	// #nosec G115 -- Conversion is safe in this context
	topicNameLen := int(topicNameLenPlusOne - 1)

	// 3. Parse Topic Name (string)
	if len(payload) < offset+topicNameLen {
		return nil, fmt.Errorf("payload too short for topic name (expected %d bytes, have %d): %w",
			topicNameLen, len(payload)-offset, ErrParseDescribeTopicRequest)
	}
	topicName := string(payload[offset : offset+topicNameLen])
	offset += topicNameLen

	req.Topics = append(req.Topics, &DescribeTopicPartitionsRequestTopic{
		TopicName: topicName,
	})

	// 4. Parse Partition Index Array Length (int32) - Skip for now
	if len(payload) < offset+4 {
		return nil, fmt.Errorf("payload too short for partition index array length: %w", ErrParseDescribeTopicRequest)
	}
	// partitionArrayLen := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
	// offset += 4
	// TODO: Skip partition indices if needed later

	// Ignore remaining fields
	// - Response Partition Limit
	// - Cursor
	// - Tag Buffer

	return req, nil
}

// Serialize serializes the DescribeTopicPartitionsResponseTopic struct into bytes.
func (t *DescribeTopicPartitionsResponseTopic) Serialize() []byte {
	// Calculate size: ErrorCode(2) + TopicNameLen(Uvarint) + TopicName + TopicID(16) + PartitionsArrayLen(4)
	topicNameBytes := []byte(t.TopicName)
	topicNameLen := len(topicNameBytes)

	// Calculate size needed for the Uvarint length prefix (N+1) for the compact string
	varintBuf := make([]byte, binary.MaxVarintLen64) // Max size for a uvarint
	// #nosec G115 -- Conversion is safe in this context
	topicNameVarintSize := protocol.WriteUvarint(varintBuf, uint64(topicNameLen+1))

	// Calculate size for partitions array length (Uvarint N+1, where N=0)
	partitionsArrayVarintSize := protocol.WriteUvarint(varintBuf, uint64(0+1)) // Always 1 byte for Uvarint(1)

	totalSize := protocol.ErrorCodeLength + topicNameVarintSize + topicNameLen + describeTopicPartitionsTopicIDBytes +
		IsInternalLength + partitionsArrayVarintSize + describeTopicAuthorizeOperationBytes + TaggedFieldsLength

	buf := make([]byte, totalSize)
	offset := 0

	// Write ErrorCode (int16)
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint16(buf[offset:offset+protocol.ErrorCodeLength], uint16(t.ErrorCode))
	offset += protocol.ErrorCodeLength

	// Write TopicName Length (Uvarint N+1)
	// #nosec G115 -- Conversion is safe in this context
	nBytes := protocol.WriteUvarint(buf[offset:], uint64(topicNameLen+1))
	offset += nBytes

	// Write TopicName (string)
	copy(buf[offset:offset+topicNameLen], topicNameBytes)
	offset += topicNameLen

	// Write TopicID (UUID - 16 bytes)
	topicIDBytes, err := t.TopicID.MarshalBinary() // uuid.MarshalBinary never returns an error
	if err != nil {
		panic(fmt.Errorf("failed to marshal uuid into binary: %w", err))
	}
	copy(buf[offset:offset+describeTopicPartitionsTopicIDBytes], topicIDBytes)
	offset += describeTopicPartitionsTopicIDBytes

	// Write IsInternal (byte) - Always false (0) for now
	buf[offset] = 0
	offset++

	// Write Partitions Array Length (Uvarint N+1) - Always 1 (0x01) for empty array
	// #nosec G115 -- conversion is safe enough here
	nBytesPartitions := protocol.WriteUvarint(buf[offset:], uint64(len(t.Partitions)+1))
	offset += nBytesPartitions

	// Write TopicAuthOps (int32) - Always 0 for now
	// #nosec G115 -- conversion is safe enough here
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(t.TopicAuthOps))
	offset += 4

	// Write Tag Buffer (byte) - Always 0 for empty tags
	buf[offset] = 0
	// offset++ // No need to increment offset, we are at the end

	return buf
}

// Serialize serializes the DescribeTopicPartitionsResponse struct into bytes.
// Implements the protocol.APIResponse interface.
func (r *DescribeTopicPartitionsResponse) Serialize() ([]byte, error) {
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

	// ThrottleTime(4) + TopicsArrayLen(Uvarint) + Cursor(1) + TaggedFields(1)
	totalSize := protocol.SizeFieldLength + protocol.CorrelationIDLength + TaggedFieldsLength +
		4 + arrayLengthVarintSize + topicsDataSize + 1 + 1 // +1 for Cursor, +1 for Tagged Fields
	buf := make([]byte, totalSize)
	offset := 0

	// Write Size (uint32) - Total size minus the size field itself
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint32(buf[offset:offset+protocol.SizeFieldLength], uint32(totalSize-protocol.SizeFieldLength))
	offset += protocol.SizeFieldLength

	// Write CorrelationID (uint32)
	binary.BigEndian.PutUint32(buf[offset:offset+protocol.CorrelationIDLength], r.CorrelationID)
	offset += protocol.CorrelationIDLength

	// Tagged buffer expeted after headers
	buf[offset] = 0              // The UVarint encoding for 0 is a single byte 0
	offset += TaggedFieldsLength // Increment offset

	// Write ThrottleTimeMs (int32)
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(r.ThrottleTimeMs))
	offset += 4

	// Write Topics Array Length (Uvarint N+1)
	// #nosec G115 -- Conversion is safe in this context
	nBytes := protocol.WriteUvarint(buf[offset:], uint64(len(r.Topics)+1))
	offset += nBytes

	// Write Topic Payloads
	for _, topicBytes := range topicPayloads {
		copy(buf[offset:], topicBytes)
		offset += len(topicBytes)
	}

	// Write 0xff byte for Cursor (placeholder, indicates no cursor)
	buf[offset] = 0xff
	offset++

	// Write zero byte for Tagged Fields (placeholder)
	buf[offset] = 0
	// offset++ // no more to write

	return buf, nil
}

// HandleDescribeTopicPartitionsRequest handles a DescribeTopicPartitions request.
// For this stage, it always returns UNKNOWN_TOPIC_OR_PARTITION.
func HandleDescribeTopicPartitionsRequest(req *BaseRequest) *DescribeTopicPartitionsResponse {
	parsedReq, err := ParseDescribeTopicPartitionsRequest(req.RemainingBytes)
	if err != nil {
		// Handle parsing error - return a generic error response
		log.Printf("Error parsing DescribeTopicPartitions request: %v\n", err)

		// Return a response with an error topic even on parse error
		errorTopic := &DescribeTopicPartitionsResponseTopic{
			ErrorCode:    protocol.ErrorUnknownTopicOrPartition,
			TopicName:    "", // Empty topic name since we couldn't parse it
			TopicID:      uuid.Nil,
			IsInternal:   false,
			Partitions:   []*DescribeTopicPartitionsResponsePartition{},
			TopicAuthOps: 0,
		}

		return &DescribeTopicPartitionsResponse{
			CorrelationID:  req.CorrelationID,
			ThrottleTimeMs: 0,
			Topics:         []*DescribeTopicPartitionsResponseTopic{errorTopic},
		}
	}

	// Assume only one topic in the request for this stage as per requirements
	topicName := ""
	if len(parsedReq.Topics) > 0 {
		topicName = parsedReq.Topics[0].TopicName
	}

	// Construct the response topic
	responseTopic := &DescribeTopicPartitionsResponseTopic{
		ErrorCode:    protocol.ErrorUnknownTopicOrPartition,
		TopicName:    topicName,
		TopicID:      uuid.Nil, // Zero UUID
		IsInternal:   false,
		Partitions:   []*DescribeTopicPartitionsResponsePartition{}, // Empty partitions array
		TopicAuthOps: 0,
	}

	// Construct the full response
	resp := &DescribeTopicPartitionsResponse{
		CorrelationID:  req.CorrelationID,
		ThrottleTimeMs: 0,
		Topics:         []*DescribeTopicPartitionsResponseTopic{responseTopic},
	}

	return resp
}
