// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package l0

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Redpanda record batch header constants.
// Redpanda stores batches with a 61-byte header in little-endian format,
// which differs from the Kafka v2 wire format (big-endian, different field order).
const (
	// redpandaHeaderSize is the size of a Redpanda record batch header on disk.
	redpandaHeaderSize = 61

	// batchTypeRaftData is the Redpanda record_batch_type for user data.
	batchTypeRaftData = 1
)

// CreateL0Object creates an L0 object from Kafka records.
// Returns the serialized object bytes and extent metadata.
// The output format matches Redpanda's L0 format: 61-byte little-endian header
// followed by Kafka v2 varint-encoded record data.
func CreateL0Object(records []*kgo.Record) ([]byte, *ExtentMeta, error) {
	if len(records) == 0 {
		return nil, nil, ErrInvalidPlaceholder
	}

	// Group records into a single batch
	baseOffset := records[0].Offset
	lastOffset := records[len(records)-1].Offset

	// Build record data in Kafka v2 format
	// Each record has: length(varint) + attributes(int8) + timestampDelta(varint) +
	//                  offsetDelta(varint) + keyLen(varint) + key + valueLen(varint) + value +
	//                  headersCount(varint) + headers
	var recordsData bytes.Buffer
	for _, record := range records {
		offsetDelta := int32(record.Offset - baseOffset)
		timestampDelta := record.Timestamp.UnixMilli() - records[0].Timestamp.UnixMilli()

		// Build single record
		var recordBuf bytes.Buffer

		// attributes (int8) - always 0 for now (no control records)
		recordBuf.WriteByte(0)

		// timestampDelta (varint)
		writeVarint(&recordBuf, timestampDelta)

		// offsetDelta (varint)
		writeVarint(&recordBuf, int64(offsetDelta))

		// keyLen (varint) + key
		if record.Key == nil {
			writeVarint(&recordBuf, -1)
		} else {
			writeVarint(&recordBuf, int64(len(record.Key)))
			recordBuf.Write(record.Key)
		}

		// valueLen (varint) + value
		if record.Value == nil {
			writeVarint(&recordBuf, -1)
		} else {
			writeVarint(&recordBuf, int64(len(record.Value)))
			recordBuf.Write(record.Value)
		}

		// headersCount (varint) + headers
		writeVarint(&recordBuf, int64(len(record.Headers)))
		for _, header := range record.Headers {
			// headerKeyLen (varint) + headerKey
			writeVarint(&recordBuf, int64(len(header.Key)))
			recordBuf.WriteString(header.Key)
			// headerValueLen (varint) + headerValue
			if header.Value == nil {
				writeVarint(&recordBuf, -1)
			} else {
				writeVarint(&recordBuf, int64(len(header.Value)))
				recordBuf.Write(header.Value)
			}
		}

		// Write record length + record data
		recordBytes := recordBuf.Bytes()
		writeVarint(&recordsData, int64(len(recordBytes)))
		recordsData.Write(recordBytes)
	}

	recordsBytes := recordsData.Bytes()

	// Build Redpanda batch header (61 bytes, little-endian)
	// Format: header_crc(4) + size_bytes(4) + base_offset(8) + type(1) + crc(4) +
	//         attrs(2) + last_offset_delta(4) + first_timestamp(8) + max_timestamp(8) +
	//         producer_id(8) + producer_epoch(2) + base_sequence(4) + record_count(4) = 61
	header := make([]byte, redpandaHeaderSize)

	// size_bytes includes header size + records data
	sizeBytes := int32(redpandaHeaderSize + len(recordsBytes))
	binary.LittleEndian.PutUint32(header[0:4], 0)                                                    // header_crc (placeholder)
	binary.LittleEndian.PutUint32(header[4:8], uint32(sizeBytes))                                    // size_bytes
	binary.LittleEndian.PutUint64(header[8:16], uint64(baseOffset))                                  // base_offset
	header[16] = batchTypeRaftData                                                                    // type
	binary.LittleEndian.PutUint32(header[17:21], 0)                                                  // crc (placeholder)
	binary.LittleEndian.PutUint16(header[21:23], 0)                                                  // attrs (no compression)
	binary.LittleEndian.PutUint32(header[23:27], uint32(int32(lastOffset-baseOffset)))                // last_offset_delta
	binary.LittleEndian.PutUint64(header[27:35], uint64(records[0].Timestamp.UnixMilli()))            // first_timestamp
	binary.LittleEndian.PutUint64(header[35:43], uint64(records[len(records)-1].Timestamp.UnixMilli())) // max_timestamp
	binary.LittleEndian.PutUint64(header[43:51], ^uint64(0))                                         // producer_id (-1)
	binary.LittleEndian.PutUint16(header[51:53], ^uint16(0))                                         // producer_epoch (-1)
	binary.LittleEndian.PutUint32(header[53:57], ^uint32(0))                                         // base_sequence (-1)
	binary.LittleEndian.PutUint32(header[57:61], uint32(len(records)))                               // record_count

	// Combine header + records
	batchBytes := make([]byte, 0, redpandaHeaderSize+len(recordsBytes))
	batchBytes = append(batchBytes, header...)
	batchBytes = append(batchBytes, recordsBytes...)

	meta := &ExtentMeta{
		FirstByteOffset: 0,
		ByteRangeSize:   uint64(len(batchBytes)),
		BaseOffset:      baseOffset,
		LastOffset:      lastOffset,
	}

	return batchBytes, meta, nil
}

// DeserializeL0Object deserializes an L0 object written by Redpanda back to Kafka records.
// Redpanda stores L0 objects with its native record batch header format (61 bytes, little-endian),
// followed by Kafka v2 record data (varint-encoded).
func DeserializeL0Object(objectBytes []byte, placeholder *PlaceholderSerde) ([]*kgo.Record, error) {
	// Extract the byte range specified by the placeholder
	start := placeholder.Offset
	end := start + placeholder.SizeBytes

	if end > uint64(len(objectBytes)) {
		return nil, ErrInvalidPlaceholder
	}

	batchBytes := objectBytes[start:end]

	// Parse Redpanda batch header (61 bytes, little-endian)
	if len(batchBytes) < redpandaHeaderSize {
		return nil, fmt.Errorf("batch too small: %d bytes", len(batchBytes))
	}

	// Read Redpanda header fields in little-endian order:
	// header_crc(4) + size_bytes(4) + base_offset(8) + type(1) + crc(4) +
	// attrs(2) + last_offset_delta(4) + first_timestamp(8) + max_timestamp(8) +
	// producer_id(8) + producer_epoch(2) + base_sequence(4) + record_count(4) = 61
	_ = binary.LittleEndian.Uint32(batchBytes[0:4])   // header_crc
	_ = int32(binary.LittleEndian.Uint32(batchBytes[4:8]))   // size_bytes
	baseOffset := int64(binary.LittleEndian.Uint64(batchBytes[8:16]))
	_ = batchBytes[16]                                        // type (1 = raft_data)
	_ = binary.LittleEndian.Uint32(batchBytes[17:21])         // crc
	attributes := int16(binary.LittleEndian.Uint16(batchBytes[21:23]))
	_ = int32(binary.LittleEndian.Uint32(batchBytes[23:27]))  // last_offset_delta
	firstTimestamp := int64(binary.LittleEndian.Uint64(batchBytes[27:35]))
	_ = int64(binary.LittleEndian.Uint64(batchBytes[35:43]))  // max_timestamp
	_ = int64(binary.LittleEndian.Uint64(batchBytes[43:51]))  // producer_id
	_ = int16(binary.LittleEndian.Uint16(batchBytes[51:53]))  // producer_epoch
	_ = int32(binary.LittleEndian.Uint32(batchBytes[53:57]))  // base_sequence
	recordCount := int32(binary.LittleEndian.Uint32(batchBytes[57:61]))

	// Check for compression
	compressionType := attributes & 0x07
	if compressionType != 0 {
		return nil, fmt.Errorf("compressed batches not yet supported: type=%d", compressionType)
	}

	// Record data starts after the 61-byte header
	buf := bytes.NewReader(batchBytes[redpandaHeaderSize:])

	// Parse records (Kafka v2 varint format)
	records := make([]*kgo.Record, 0, recordCount)
	for i := int32(0); i < recordCount; i++ {
		// Read record length
		recordLen, err := readVarint(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to read record length: %w", err)
		}

		recordStart := buf.Len()

		// Read record attributes
		_, _ = buf.ReadByte()

		// Read timestampDelta
		timestampDelta, err := readVarint(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to read timestampDelta: %w", err)
		}

		// Read offsetDelta
		offsetDelta, err := readVarint(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to read offsetDelta: %w", err)
		}

		// Read key
		keyLen, err := readVarint(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to read keyLen: %w", err)
		}
		var key []byte
		if keyLen >= 0 {
			key = make([]byte, keyLen)
			buf.Read(key)
		}

		// Read value
		valueLen, err := readVarint(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to read valueLen: %w", err)
		}
		var value []byte
		if valueLen >= 0 {
			value = make([]byte, valueLen)
			buf.Read(value)
		}

		// Read headers
		headersCount, err := readVarint(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to read headersCount: %w", err)
		}
		var headers []kgo.RecordHeader
		for j := int64(0); j < headersCount; j++ {
			// Read header key
			headerKeyLen, err := readVarint(buf)
			if err != nil {
				return nil, fmt.Errorf("failed to read headerKeyLen: %w", err)
			}
			headerKey := make([]byte, headerKeyLen)
			buf.Read(headerKey)

			// Read header value
			headerValueLen, err := readVarint(buf)
			if err != nil {
				return nil, fmt.Errorf("failed to read headerValueLen: %w", err)
			}
			var headerValue []byte
			if headerValueLen >= 0 {
				headerValue = make([]byte, headerValueLen)
				buf.Read(headerValue)
			}

			headers = append(headers, kgo.RecordHeader{
				Key:   string(headerKey),
				Value: headerValue,
			})
		}

		// Create record
		record := &kgo.Record{
			Key:       key,
			Value:     value,
			Headers:   headers,
			Offset:    baseOffset + offsetDelta,
			Timestamp: time.UnixMilli(firstTimestamp + timestampDelta),
		}
		records = append(records, record)

		// Verify we consumed the right amount
		recordEnd := buf.Len()
		consumed := recordStart - recordEnd
		if int64(consumed) != recordLen {
			return nil, fmt.Errorf("record length mismatch: expected %d, got %d", recordLen, consumed)
		}
	}

	return records, nil
}

// EncodeAsKafkaBatch encodes records as a Kafka v2 record batch in wire format (big-endian).
// This is used when sending records back to Kafka clients in fetch responses.
func EncodeAsKafkaBatch(records []*kgo.Record) ([]byte, error) {
	if len(records) == 0 {
		return nil, ErrInvalidPlaceholder
	}

	baseOffset := records[0].Offset
	lastOffset := records[len(records)-1].Offset
	firstTimestamp := records[0].Timestamp.UnixMilli()
	maxTimestamp := records[len(records)-1].Timestamp.UnixMilli()

	// Build record data (varint-encoded, same as Kafka v2)
	var recordsData bytes.Buffer
	for _, record := range records {
		offsetDelta := int32(record.Offset - baseOffset)
		timestampDelta := record.Timestamp.UnixMilli() - firstTimestamp

		var recordBuf bytes.Buffer
		recordBuf.WriteByte(0) // attributes
		writeVarint(&recordBuf, timestampDelta)
		writeVarint(&recordBuf, int64(offsetDelta))

		if record.Key == nil {
			writeVarint(&recordBuf, -1)
		} else {
			writeVarint(&recordBuf, int64(len(record.Key)))
			recordBuf.Write(record.Key)
		}

		if record.Value == nil {
			writeVarint(&recordBuf, -1)
		} else {
			writeVarint(&recordBuf, int64(len(record.Value)))
			recordBuf.Write(record.Value)
		}

		writeVarint(&recordBuf, int64(len(record.Headers)))
		for _, header := range record.Headers {
			writeVarint(&recordBuf, int64(len(header.Key)))
			recordBuf.WriteString(header.Key)
			if header.Value == nil {
				writeVarint(&recordBuf, -1)
			} else {
				writeVarint(&recordBuf, int64(len(header.Value)))
				recordBuf.Write(header.Value)
			}
		}

		recordBytes := recordBuf.Bytes()
		writeVarint(&recordsData, int64(len(recordBytes)))
		recordsData.Write(recordBytes)
	}

	recordsBytes := recordsData.Bytes()

	// Kafka v2 batch header (big-endian):
	// baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1) + crc(4) +
	// attributes(2) + lastOffsetDelta(4) + firstTimestamp(8) + maxTimestamp(8) +
	// producerId(8) + producerEpoch(2) + baseSequence(4) + recordCount(4) = 61

	// Build header + records
	totalSize := 61 + len(recordsBytes)
	buf := make([]byte, totalSize)

	// baseOffset (int64 BE)
	binary.BigEndian.PutUint64(buf[0:8], uint64(baseOffset))
	// batchLength (int32 BE) = total - baseOffset(8) - batchLength(4) = total - 12
	binary.BigEndian.PutUint32(buf[8:12], uint32(totalSize-12))
	// partitionLeaderEpoch (int32 BE)
	binary.BigEndian.PutUint32(buf[12:16], 0xFFFFFFFF) // -1
	// magic (int8)
	buf[16] = 2 // Kafka v2
	// crc placeholder - will be computed below
	// attributes (int16 BE)
	binary.BigEndian.PutUint16(buf[21:23], 0)
	// lastOffsetDelta (int32 BE)
	binary.BigEndian.PutUint32(buf[23:27], uint32(int32(lastOffset-baseOffset)))
	// firstTimestamp (int64 BE)
	binary.BigEndian.PutUint64(buf[27:35], uint64(firstTimestamp))
	// maxTimestamp (int64 BE)
	binary.BigEndian.PutUint64(buf[35:43], uint64(maxTimestamp))
	// producerId (int64 BE)
	binary.BigEndian.PutUint64(buf[43:51], 0xFFFFFFFFFFFFFFFF) // -1
	// producerEpoch (int16 BE)
	binary.BigEndian.PutUint16(buf[51:53], 0xFFFF) // -1
	// baseSequence (int32 BE)
	binary.BigEndian.PutUint32(buf[53:57], 0xFFFFFFFF) // -1
	// recordCount (int32 BE)
	binary.BigEndian.PutUint32(buf[57:61], uint32(len(records)))

	// Copy records data
	copy(buf[61:], recordsBytes)

	// Compute CRC32-C over everything from attributes through end (offset 21 to end)
	crc := crc32.Checksum(buf[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(buf[17:21], crc)

	return buf, nil
}

// Helper functions for encoding

func writeVarint(buf *bytes.Buffer, val int64) {
	// ZigZag encoding for signed integers
	uval := uint64((val << 1) ^ (val >> 63))
	for uval >= 0x80 {
		buf.WriteByte(byte(uval) | 0x80)
		uval >>= 7
	}
	buf.WriteByte(byte(uval))
}

// Helper functions for decoding

func readVarint(buf *bytes.Reader) (int64, error) {
	var uval uint64
	var shift uint
	for {
		b, err := buf.ReadByte()
		if err != nil {
			return 0, err
		}
		uval |= uint64(b&0x7f) << shift
		if b < 0x80 {
			break
		}
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("varint overflow")
		}
	}
	// ZigZag decoding
	val := int64((uval >> 1) ^ -(uval & 1))
	return val, nil
}
