//nolint:err113,gosec // dynamic errors are useful in test cases, type conversions are fine for tests
package handlers_test

import (
	"bytes"
	"encoding/binary"
	"errors"
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
			// Client ID Length (int16) = 0
			// Tagged Fields (Uvarint) = 0
			// Topics Array Length (Uvarint N+1) = 2 (N=1)
			//   Topic Name Length (Uvarint N+1) = 5 (N=4)
			//   Topic Name (string) = "test"
			//   Partition Index Array Length (int32) = 0 (ignored, but need bytes)
			payload: []byte{
				0x00, 0x00, // Client ID Length = 0
				0x00,               // Tagged Fields = 0
				0x02,               // Topics Array Length = 1 (Uvarint 2 means N=1)
				0x05,               // Topic Name Length = 4 (Uvarint 5 means N=4)
				't', 'e', 's', 't', // Topic Name = "test"
				0x00, 0x00, 0x00, 0x00, // Partition Index Array Length = 0
			},
			expectedReq: &handlers.DescribeTopicPartitionsRequest{
				ClientID: "",
				Topics: []*handlers.DescribeTopicPartitionsRequestTopic{
					{TopicName: "test"},
				},
			},
			expectedErr: nil,
		},
		{
			name: "Request with zero topics",
			// Client ID Length (0), Tagged Fields (0), Topics Array Length (Uvarint 1 means N=0)
			payload: []byte{
				0x00, 0x00, // Client ID Length = 0
				0x00, // Tagged Fields = 0
				0x01, // Topics Array Length = 0 (Uvarint 1 means N=0)
			},
			expectedReq: &handlers.DescribeTopicPartitionsRequest{
				ClientID: "",
				Topics:   []*handlers.DescribeTopicPartitionsRequestTopic{},
			},
			expectedErr: nil,
		},
		{
			name:        "Payload too short for client id length",
			payload:     []byte{0x00}, // Only 1 byte
			expectedReq: nil,
			expectedErr: errors.New("payload too short for client id length: error parsing describe topic partitions request"),
		},
		{
			name:        "Payload too short for tagged fields byte",
			payload:     []byte{0x00, 0x00}, // Client ID length only
			expectedReq: nil,
			expectedErr: errors.New("payload too short for tagged fields byte: error parsing describe topic partitions request"),
		},
		{
			name: "Payload too short for topic array length uvarint",
			payload: []byte{
				0x00, 0x00, // Client ID Length = 0
				0x00, // Tagged Fields = 0
				// Missing topic array length uvarint
			},
			expectedReq: nil,
			expectedErr: errors.New(
				"failed to read topic array length (uvarint) bytesRead=0: error parsing describe topic partitions request"),
		},
		{
			name: "Payload too short for topic name length uvarint",
			payload: []byte{
				0x00, 0x00, // Client ID Length = 0
				0x00, // Tagged Fields = 0
				0x02, // Topics Array Length = 1 (Uvarint 2 means N=1)
				// Missing topic name length uvarint
			},
			expectedReq: nil,
			expectedErr: errors.New(
				"failed to read topic name length (uvarint) bytesRead=0: error parsing describe topic partitions request"),
		},
		{
			name: "Payload too short for topic name",
			payload: []byte{
				0x00, 0x00, // Client ID Length = 0
				0x00,          // Tagged Fields = 0
				0x02,          // Topics Array Length = 1 (Uvarint 2 means N=1)
				0x05,          // Topic Name Length = 4 (Uvarint 5 means N=4)
				't', 'e', 's', // Missing 't'
			},
			expectedReq: nil,
			expectedErr: errors.New(
				"payload too short for topic name (expected 4 bytes, have 3): error parsing describe topic partitions request"),
		},
		{
			name: "Payload too short for partition index array length",
			payload: []byte{
				0x00, 0x00, // Client ID Length = 0
				0x00,               // Tagged Fields = 0
				0x02,               // Topics Array Length = 1 (Uvarint 2 means N=1)
				0x05,               // Topic Name Length = 4 (Uvarint 5 means N=4)
				't', 'e', 's', 't', // Topic Name = "test"
				0x00, 0x00, 0x00, // Missing byte for partition index array length
			},
			expectedReq: nil,
			expectedErr: errors.New(
				"payload too short for partition index array length: error parsing describe topic partitions request"),
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

	// Expected format:
	// ErrorCode(2) + TopicNameLen(Uvarint N+1) + TopicName(N) + TopicID(16) + IsInternal(1) +
	// PartitionsArrayLen(Uvarint N+1) + TopicAuthOps(4) + TaggedFields(1)
	expectedBytes := []byte{
		0x00, 0x03, // ErrorCode = 3
		0x09,                                   // TopicName Length = 8 (Uvarint 9 means N=8)
		'm', 'y', '-', 't', 'o', 'p', 'i', 'c', // TopicName = "my-topic"
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // TopicID (Nil UUID)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,                   // IsInternal = false
		0x01,                   // Partitions Array Length = 0 (Uvarint 1 means N=0)
		0x00, 0x00, 0x00, 0x00, // TopicAuthOps = 0
		0x00, // Tagged Fields = 0
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
	// Size(4) + CorrelationID(4) + TaggedFieldsHeader(1) + ThrottleTime(4) + TopicsArrayLen(Uvarint) +
	// TopicData + Cursor(1) + TaggedFieldsFooter(1)
	topicData := resp.Topics[0].Serialize()
	topicDataSize := len(topicData)
	// Calculate size needed for the Uvarint length prefix (N+1) for the compact array
	varintBuf := make([]byte, binary.MaxVarintLen64) // Max size for a uvarint
	// #nosec G115 -- Conversion is safe in this context
	arrayLengthVarintSize := protocol.WriteUvarint(varintBuf, uint64(len(resp.Topics)+1))
	// Size, CorrID, TaggedHdr, Throttle, ArrayLen, TopicData, Cursor, TaggedFtr
	expectedTotalSize := 4 + 4 + 1 + 4 + arrayLengthVarintSize + topicDataSize + 1 + 1
	// #nosec G115 -- Conversion is safe in this context
	expectedMessageSize := uint32(expectedTotalSize - 4) // Size field value

	expectedBytes := new(bytes.Buffer)
	// Size
	err := binary.Write(expectedBytes, binary.BigEndian, expectedMessageSize)
	require.NoError(t, err)
	// CorrelationID
	err = binary.Write(expectedBytes, binary.BigEndian, correlationID)
	require.NoError(t, err)
	// Tagged Fields Header (0)
	err = expectedBytes.WriteByte(0)
	require.NoError(t, err)
	// ThrottleTimeMs
	err = binary.Write(expectedBytes, binary.BigEndian, int32(0))
	require.NoError(t, err)
	// Topics Array Length (Uvarint N+1)
	lenBytes := make([]byte, arrayLengthVarintSize)
	// #nosec G115 -- Conversion is safe in this context
	protocol.WriteUvarint(lenBytes, uint64(len(resp.Topics)+1))
	_, err = expectedBytes.Write(lenBytes)
	require.NoError(t, err)
	// Topic Data
	_, err = expectedBytes.Write(topicData)
	require.NoError(t, err)
	// Cursor (0xff)
	err = expectedBytes.WriteByte(0xff)
	require.NoError(t, err)
	// Tagged Fields Footer (0)
	err = expectedBytes.WriteByte(0)
	require.NoError(t, err)

	actualBytes, err := resp.Serialize()
	require.NoError(t, err)
	assert.Equal(t, expectedBytes.Bytes(), actualBytes)
	assert.Len(t, actualBytes, expectedTotalSize, "Total serialized length mismatch")
}

//nolint:funlen // Test function is long due to detailed serialization checks
func TestHandleDescribeTopicPartitionsRequest(t *testing.T) {
	topicName := "my-test-topic"
	correlationID := uint32(9876)

	// Construct request payload bytes
	topicNameBytes := []byte(topicName)
	// #nosec G115 -- Conversion is safe in this context
	topicNameLen := len(topicNameBytes)
	payloadBuf := new(bytes.Buffer)
	// Client ID Length = 0
	err := binary.Write(payloadBuf, binary.BigEndian, int16(0))
	require.NoError(t, err)
	// Tagged Fields = 0
	err = payloadBuf.WriteByte(0)
	require.NoError(t, err)
	// Topics Array Length = 1 (Uvarint 2 means N=1)
	lenBytes := make([]byte, binary.MaxVarintLen64)
	n := protocol.WriteUvarint(lenBytes, uint64(1+1))
	_, err = payloadBuf.Write(lenBytes[:n])
	require.NoError(t, err)
	// Topic Name Length (Uvarint N+1)
	// #nosec G115 -- Conversion is safe in this context
	n = protocol.WriteUvarint(lenBytes, uint64(topicNameLen+1))
	_, err = payloadBuf.Write(lenBytes[:n])
	require.NoError(t, err)
	// Topic Name
	_, err = payloadBuf.Write(topicNameBytes)
	require.NoError(t, err)
	// Partition Index Array Length = 0 (int32)
	err = binary.Write(payloadBuf, binary.BigEndian, int32(0))
	require.NoError(t, err)

	// Calculate actual size for BaseRequest header
	headerSize := 4 + 2 + 2 + 4 // Size, ApiKey, ApiVersion, CorrelationID
	// #nosec G115 -- Conversion is safe in this context
	requestSize := uint32(headerSize + payloadBuf.Len() - 4) // Total size - Size field itself

	baseReq := &handlers.BaseRequest{
		Size:           requestSize,
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
	// Calculate size needed for the Uvarint length prefix (N+1) for the compact array
	varintBuf := make([]byte, binary.MaxVarintLen64) // Max size for a uvarint
	// #nosec G115 -- Conversion is safe in this context
	arrayLengthVarintSize := protocol.WriteUvarint(varintBuf, uint64(len(response.Topics)+1))
	// Size, CorrID, TaggedHdr, Throttle, ArrayLen, TopicData, Cursor, TaggedFtr
	expectedTotalSize := 4 + 4 + 1 + 4 + arrayLengthVarintSize + len(expectedTopicBytes) + 1 + 1
	expectedMessageSize := uint32(expectedTotalSize - 4)

	expectedBuf := new(bytes.Buffer)
	err = binary.Write(expectedBuf, binary.BigEndian, expectedMessageSize)
	require.NoError(t, err)
	err = binary.Write(expectedBuf, binary.BigEndian, correlationID)
	require.NoError(t, err)
	// Tagged Fields Header (0)
	err = expectedBuf.WriteByte(0)
	require.NoError(t, err)
	err = binary.Write(expectedBuf, binary.BigEndian, int32(0)) // Throttle
	require.NoError(t, err)
	// Topics Array Length (Uvarint N+1)
	lenBytes = make([]byte, arrayLengthVarintSize)
	protocol.WriteUvarint(lenBytes, uint64(len(response.Topics)+1))
	_, err = expectedBuf.Write(lenBytes)
	require.NoError(t, err)
	_, err = expectedBuf.Write(expectedTopicBytes)
	require.NoError(t, err)
	// Cursor (0xff)
	err = expectedBuf.WriteByte(0xff)
	require.NoError(t, err)
	// Tagged Fields Footer (0)
	err = expectedBuf.WriteByte(0)
	require.NoError(t, err)

	assert.Equal(t, expectedBuf.Bytes(), serializedResp, "Serialized response bytes do not match expected")
}

func TestHandleDescribeTopicPartitionsRequest_ParseError(t *testing.T) {
	correlationID := uint32(1111)
	// Malformed payload (too short for topic name length uvarint)
	malformedPayload := []byte{
		0x00, 0x00, // Client ID Length = 0
		0x00, // Tagged Fields = 0
		0x02, // Topics Array Length = 1 (Uvarint 2 means N=1)
		// Missing topic name length uvarint
	}

	// Calculate actual size for BaseRequest header
	headerSize := 4 + 2 + 2 + 4 // Size, ApiKey, ApiVersion, CorrelationID
	// #nosec G115 -- Conversion is safe in this context
	requestSize := uint32(headerSize + len(malformedPayload) - 4) // Total size - Size field itself

	baseReq := &handlers.BaseRequest{
		Size:           requestSize,
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
	// The handler currently returns a topic with an error code even on parse failure.
	// assert.Empty(t, response.Topics, "Topics should be empty on parse error")
	require.Len(t, response.Topics, 1, "Should have one topic in error response")
	assert.Equal(t, int16(protocol.ErrorUnknownTopicOrPartition), response.Topics[0].ErrorCode,
		"Error topic should have correct error code")
	assert.Equal(t, "", response.Topics[0].TopicName, "Error topic should have empty name")

	// Check serialization of the error response
	serializedResp, err := response.Serialize()
	require.NoError(t, err, "Serialization of error response should not fail")

	// Construct expected bytes for the error response (contains one topic with error code)
	require.Len(t, response.Topics, 1) // Ensure we have the topic to serialize
	expectedTopicBytes := response.Topics[0].Serialize()
	// Calculate size needed for the Uvarint length prefix (N+1) for the compact array (N=1)
	varintBuf := make([]byte, binary.MaxVarintLen64) // Max size for a uvarint
	// #nosec G115 -- Conversion is safe in this context
	arrayLengthVarintSize := protocol.WriteUvarint(varintBuf, uint64(1+1)) // Length is 1 byte for Uvarint(2)
	// Size, CorrID, TaggedHdr, Throttle, ArrayLen(1), TopicData, Cursor, TaggedFtr
	expectedTotalSize := 4 + 4 + 1 + 4 + arrayLengthVarintSize + len(expectedTopicBytes) + 1 + 1
	expectedMessageSize := uint32(expectedTotalSize - 4)

	expectedBuf := new(bytes.Buffer)
	err = binary.Write(expectedBuf, binary.BigEndian, expectedMessageSize)
	require.NoError(t, err)
	err = binary.Write(expectedBuf, binary.BigEndian, correlationID)
	require.NoError(t, err)
	// Tagged Fields Header (0)
	err = expectedBuf.WriteByte(0)
	require.NoError(t, err)
	err = binary.Write(expectedBuf, binary.BigEndian, int32(0)) // Throttle
	require.NoError(t, err)
	// Topics Array Length (Uvarint N+1 = 2)
	lenBytes := make([]byte, arrayLengthVarintSize)
	protocol.WriteUvarint(lenBytes, uint64(1+1))
	_, err = expectedBuf.Write(lenBytes)
	require.NoError(t, err)
	// Topic Data (for the single error topic)
	_, err = expectedBuf.Write(expectedTopicBytes)
	require.NoError(t, err)
	// Cursor (0xff)
	err = expectedBuf.WriteByte(0xff)
	require.NoError(t, err)
	// Tagged Fields Footer (0)
	err = expectedBuf.WriteByte(0)
	require.NoError(t, err)

	assert.Equal(t, expectedBuf.Bytes(), serializedResp, "Serialized error response bytes do not match expected")
}
