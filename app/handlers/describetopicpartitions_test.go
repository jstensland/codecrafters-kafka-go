//nolint:err113 // dynamic errors are useful in test cases
package handlers_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/app/handlers"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDescribeTopicPartitionsRequest(t *testing.T) {
	testCases := []struct {
		name        string
		payload     []byte
		expectedReq *handlers.DescribeTopicPartitionsRequest
		expectedErr error
	}{
		{
			name: "Valid request with one topic",
			// Payload structure:
			// Topics Array Length (int32) = 1
			//   Topic Name Length (int16) = 4
			//   Topic Name (string) = "test"
			//   Partition Index Array Length (int32) = 0 (ignored)
			payload: []byte{
				0x00, 0x00, 0x00, 0x01, // Topics Array Length = 1
				0x00, 0x04, // Topic Name Length = 4
				't', 'e', 's', 't', // Topic Name = "test"
				0x00, 0x00, 0x00, 0x00, // Partition Index Array Length = 0
			},
			expectedReq: &handlers.DescribeTopicPartitionsRequest{
				Topics: []*handlers.DescribeTopicPartitionsRequestTopic{
					{TopicName: "test"},
				},
			},
			expectedErr: nil,
		},
		{
			name: "Request with zero topics",
			payload: []byte{
				0x00, 0x00, 0x00, 0x00, // Topics Array Length = 0
			},
			expectedReq: &handlers.DescribeTopicPartitionsRequest{
				Topics: []*handlers.DescribeTopicPartitionsRequestTopic{},
			},
			expectedErr: nil,
		},
		{
			name:        "Payload too short for topic array length",
			payload:     []byte{0x00, 0x00, 0x00},
			expectedReq: nil,
			expectedErr: errors.New("payload too short for topic array length"),
		},
		{
			name: "Payload too short for topic name length",
			payload: []byte{
				0x00, 0x00, 0x00, 0x01, // Topics Array Length = 1
				0x00, // Missing byte for topic name length
			},
			expectedReq: nil,
			expectedErr: errors.New("payload too short for topic name length"),
		},
		{
			name: "Payload too short for topic name",
			payload: []byte{
				0x00, 0x00, 0x00, 0x01, // Topics Array Length = 1
				0x00, 0x04, // Topic Name Length = 4
				't', 'e', 's', // Missing 't'
			},
			expectedReq: nil,
			expectedErr: errors.New("payload too short for topic name"),
		},
		{
			name: "Payload too short for partition index array length",
			payload: []byte{
				0x00, 0x00, 0x00, 0x01, // Topics Array Length = 1
				0x00, 0x04, // Topic Name Length = 4
				't', 'e', 's', 't', // Topic Name = "test"
				0x00, 0x00, 0x00, // Missing byte
			},
			expectedReq: nil,
			expectedErr: errors.New("payload too short for partition index array length"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := handlers.ParseDescribeTopicPartitionsRequest(tc.payload)

			if tc.expectedErr != nil {
				require.Error(t, err)
				require.EqualError(t, err, tc.expectedErr.Error())
				assert.Nil(t, req)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedReq, req)
			}
		})
	}
}

func TestDescribeTopicPartitionsResponseTopic_Serialize(t *testing.T) {
	topic := &handlers.DescribeTopicPartitionsResponseTopic{
		ErrorCode:    protocol.ErrorUnknownTopicOrPartition,
		TopicName:    "my-topic",
		TopicID:      uuid.Nil,
		IsInternal:   false,
		Partitions:   []*handlers.DescribeTopicPartitionsResponsePartition{},
		TopicAuthOps: 0,
	}

	expectedBytes := []byte{
		0x00, 0x03, // ErrorCode = 3 (UNKNOWN_TOPIC_OR_PARTITION)
		0x00, 0x08, // TopicName Length = 8
		'm', 'y', '-', 't', 'o', 'p', 'i', 'c', // TopicName = "my-topic"
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // TopicID (Nil UUID)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, // Partitions Array Length = 0
	}

	actualBytes := topic.Serialize()
	assert.Equal(t, expectedBytes, actualBytes)
}

func TestDescribeTopicPartitionsResponse_Serialize(t *testing.T) {
	correlationID := uint32(12345)
	topicName := "unknown-topic"

	resp := &handlers.DescribeTopicPartitionsResponse{
		CorrelationID:  correlationID,
		ThrottleTimeMs: 0,
		Topics: []*handlers.DescribeTopicPartitionsResponseTopic{
			{
				ErrorCode:    protocol.ErrorUnknownTopicOrPartition,
				TopicName:    topicName,
				TopicID:      uuid.Nil,
				IsInternal:   false,
				Partitions:   []*handlers.DescribeTopicPartitionsResponsePartition{},
				TopicAuthOps: 0,
			},
		},
	}

	// Calculate expected size:
	// Size(4) + CorrelationID(4) + ThrottleTime(4) + TopicsArrayLen(4) + TopicData
	topicData := resp.Topics[0].Serialize()
	topicDataSize := len(topicData)
	// #nosec G115 -- Conversion is safe in this context
	expectedTotalSize := 4 + 4 + 4 + 4 + topicDataSize
	expectedMessageSize := uint32(expectedTotalSize - 4) // Size field value

	expectedBytes := new(bytes.Buffer)
	// Size
	err := binary.Write(expectedBytes, binary.BigEndian, expectedMessageSize)
	require.NoError(t, err)
	// CorrelationID
	err = binary.Write(expectedBytes, binary.BigEndian, correlationID)
	require.NoError(t, err)
	// ThrottleTimeMs
	err = binary.Write(expectedBytes, binary.BigEndian, int32(0))
	require.NoError(t, err)
	// Topics Array Length
	err = binary.Write(expectedBytes, binary.BigEndian, int32(1))
	require.NoError(t, err)
	// Topic Data
	_, err = expectedBytes.Write(topicData)
	require.NoError(t, err)

	actualBytes, err := resp.Serialize()
	require.NoError(t, err)
	assert.Equal(t, expectedBytes.Bytes(), actualBytes)
	// #nosec G115 -- Conversion is safe in this context
	assert.Equal(t, expectedTotalSize, len(actualBytes), "Total serialized length mismatch")
}

func TestHandleDescribeTopicPartitionsRequest(t *testing.T) {
	topicName := "my-test-topic"
	correlationID := uint32(9876)

	// Construct request payload bytes
	topicNameBytes := []byte(topicName)
	// #nosec G115 -- Conversion is safe in this context
	topicNameLen := uint16(len(topicNameBytes))
	payloadBuf := new(bytes.Buffer)
	// Topics Array Length = 1
	err := binary.Write(payloadBuf, binary.BigEndian, int32(1))
	require.NoError(t, err)
	// Topic Name Length
	err = binary.Write(payloadBuf, binary.BigEndian, topicNameLen)
	require.NoError(t, err)
	// Topic Name
	_, err = payloadBuf.Write(topicNameBytes)
	require.NoError(t, err)
	// Partition Index Array Length = 0
	err = binary.Write(payloadBuf, binary.BigEndian, int32(0))
	require.NoError(t, err)

	baseReq := &handlers.BaseRequest{
		Size:           uint32(payloadBuf.Len() + 10), // Size doesn't matter much here
		APIKey:         handlers.APIKeyDescribeTopicPartitions,
		APIVersion:     0,
		CorrelationID:  correlationID,
		RemainingBytes: payloadBuf.Bytes(),
	}

	response := handlers.HandleDescribeTopicPartitionsRequest(baseReq)
	require.NotNil(t, response)

	// Assertions on the response structure
	assert.Equal(t, correlationID, response.CorrelationID)
	assert.Equal(t, int32(0), response.ThrottleTimeMs)
	require.Len(t, response.Topics, 1, "Should contain one topic response")

	respTopic := response.Topics[0]
	assert.Equal(t, int16(protocol.ErrorUnknownTopicOrPartition), respTopic.ErrorCode)
	assert.Equal(t, topicName, respTopic.TopicName)
	assert.Equal(t, uuid.Nil, respTopic.TopicID)
	assert.False(t, respTopic.IsInternal)
	assert.Empty(t, respTopic.Partitions, "Partitions array should be empty")
	assert.Equal(t, int32(0), respTopic.TopicAuthOps)

	// Verify serialization works and matches expectations
	serializedResp, err := response.Serialize()
	require.NoError(t, err)

	// Construct expected serialized bytes manually for verification
	expectedTopicBytes := respTopic.Serialize()
	expectedTotalSize := 4 + 4 + 4 + 4 + len(expectedTopicBytes) // Size, CorrID, Throttle, ArrayLen, TopicData
	expectedMessageSize := uint32(expectedTotalSize - 4)

	expectedBuf := new(bytes.Buffer)
	err = binary.Write(expectedBuf, binary.BigEndian, expectedMessageSize)
	require.NoError(t, err)
	err = binary.Write(expectedBuf, binary.BigEndian, correlationID)
	require.NoError(t, err)
	err = binary.Write(expectedBuf, binary.BigEndian, int32(0)) // Throttle
	require.NoError(t, err)
	err = binary.Write(expectedBuf, binary.BigEndian, int32(1)) // Array Len
	require.NoError(t, err)
	_, err = expectedBuf.Write(expectedTopicBytes)
	require.NoError(t, err)

	assert.Equal(t, expectedBuf.Bytes(), serializedResp, "Serialized response bytes do not match expected")
}

func TestHandleDescribeTopicPartitionsRequest_ParseError(t *testing.T) {
	correlationID := uint32(1111)
	// Malformed payload (too short)
	malformedPayload := []byte{0x00, 0x00, 0x00, 0x01, 0x00} // Missing topic name length byte

	baseReq := &handlers.BaseRequest{
		Size:           uint32(len(malformedPayload) + 10),
		APIKey:         handlers.APIKeyDescribeTopicPartitions,
		APIVersion:     0,
		CorrelationID:  correlationID,
		RemainingBytes: malformedPayload,
	}

	// Expecting a non-nil response even on parse error, containing the correlation ID
	// and likely an empty topic list as a fallback.
	response := handlers.HandleDescribeTopicPartitionsRequest(baseReq)
	require.NotNil(t, response)

	assert.Equal(t, correlationID, response.CorrelationID)
	assert.Empty(t, response.Topics, "Topics should be empty on parse error")

	// Check serialization of the error response
	_, err := response.Serialize()
	require.NoError(t, err, "Serialization of error response should not fail")
}

// Helper function to create a DescribeTopicPartitions request payload
func createDescribeTopicPartitionsPayload(topicName string) ([]byte, error) {
	buf := new(bytes.Buffer)
	// Topics Array Length = 1
	if err := binary.Write(buf, binary.BigEndian, int32(1)); err != nil {
		return nil, fmt.Errorf("writing topic array len: %w", err)
	}
	// Topic Name Length
	topicNameBytes := []byte(topicName)
	// #nosec G115 -- Conversion is safe in this context
	if err := binary.Write(buf, binary.BigEndian, int16(len(topicNameBytes))); err != nil {
		return nil, fmt.Errorf("writing topic name len: %w", err)
	}
	// Topic Name
	if _, err := buf.Write(topicNameBytes); err != nil {
		return nil, fmt.Errorf("writing topic name: %w", err)
	}
	// Partition Index Array Length = 0
	if err := binary.Write(buf, binary.BigEndian, int32(0)); err != nil {
		return nil, fmt.Errorf("writing partition array len: %w", err)
	}
	return buf.Bytes(), nil
}
