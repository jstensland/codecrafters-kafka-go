// Package handlers implements the logic for handling specific Kafka API requests.
package handlers

import (
	"encoding/binary"
	"fmt"

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

// DescribeTopicPartitionsRequestTopicV0 represents a topic in the DescribeTopicPartitions request v0.
type DescribeTopicPartitionsRequestTopicV0 struct {
	TopicName string
	// PartitionIndexes are ignored for now as per requirements
}

// DescribeTopicPartitionsRequestV0 represents the DescribeTopicPartitions request v0.
type DescribeTopicPartitionsRequestV0 struct {
	Topics []*DescribeTopicPartitionsRequestTopicV0
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

	// 1. Parse Topic Array Length (int32)
	if len(payload) < offset+4 {
		return nil, fmt.Errorf("payload too short for topic array length")
	}
	topicArrayLen := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
	offset += 4

	if topicArrayLen < 0 {
		return nil, fmt.Errorf("invalid negative topic array length: %d", topicArrayLen)
	}

	// We only need the first topic for this stage
	if topicArrayLen > 0 {
		// 2. Parse Topic Name Length (int16)
		if len(payload) < offset+2 {
			return nil, fmt.Errorf("payload too short for topic name length")
		}
		topicNameLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2

		if topicNameLen < 0 {
			return nil, fmt.Errorf("invalid negative topic name length: %d", topicNameLen)
		}

		// 3. Parse Topic Name (string)
		if len(payload) < offset+topicNameLen {
			return nil, fmt.Errorf("payload too short for topic name")
		}
		topicName := string(payload[offset : offset+topicNameLen])
		offset += topicNameLen

		req.Topics = append(req.Topics, &DescribeTopicPartitionsRequestTopicV0{
			TopicName: topicName,
		})

		// 4. Parse Partition Index Array Length (int32) - Skip for now
		if len(payload) < offset+4 {
			return nil, fmt.Errorf("payload too short for partition index array length")
		}
		// partitionArrayLen := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		// offset += 4
		// TODO: Skip partition indices if needed later

	} else {
		// Handle case with zero topics if necessary, though the test case implies one topic.
		req.Topics = []*DescribeTopicPartitionsRequestTopicV0{}
	}

	// Ignore remaining fields for v0 (IncludeClusterAuthorizedOperations, IncludeTopicAuthorizedOperations)

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

	// #nosec G115 -- Conversion is safe in this context
	totalSize := protocol.SizeFieldLength + protocol.CorrelationIDLength + 4 + 4 + topicsDataSize // ThrottleTime(4) + TopicsArrayLen(4)
	buf := make([]byte, totalSize)
	offset := 0

	// Write Size (uint32) - Total size minus the size field itself
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint32(buf[offset:offset+protocol.SizeFieldLength], uint32(totalSize-protocol.SizeFieldLength))
	offset += protocol.SizeFieldLength

	// Write CorrelationID (uint32)
	binary.BigEndian.PutUint32(buf[offset:offset+protocol.CorrelationIDLength], r.CorrelationID)
	offset += protocol.CorrelationIDLength

	// Write ThrottleTimeMs (int32)
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(r.ThrottleTimeMs))
	offset += 4

	// Write Topics Array Length (int32)
	// #nosec G115 -- Conversion is safe in this context
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(r.Topics)))
	offset += 4

	// Write Topic Payloads
	for _, topicBytes := range topicPayloads {
		copy(buf[offset:], topicBytes)
		offset += len(topicBytes)
	}

	if offset != totalSize {
		return nil, fmt.Errorf("describe topic partitions response serialize size mismatch: expected %d, got %d", totalSize, offset)
	}

	return buf, nil
}

// HandleDescribeTopicPartitionsRequest handles a DescribeTopicPartitions v0 request.
// For this stage, it always returns UNKNOWN_TOPIC_OR_PARTITION.
func HandleDescribeTopicPartitionsRequest(req *BaseRequest) *DescribeTopicPartitionsResponseV0 {
	parsedReq, err := ParseDescribeTopicPartitionsRequest(req.RemainingBytes)
	if err != nil {
		// Handle parsing error - potentially return a generic error response
		// For now, log and return a basic error response matching the structure
		fmt.Printf("Error parsing DescribeTopicPartitions request: %v\n", err)
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
