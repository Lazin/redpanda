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

// CreateL0Object creates an L0 object from Kafka records.
// Returns the serialized object bytes and extent metadata.
// The output format matches Redpanda's L0 format: raw Kafka record batches in wire format.
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
	for i, record := range records {
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

		_ = i // offsetDelta is computed above
	}

	// Build batch header (61 bytes fixed size)
	var headerBuf bytes.Buffer

	// baseOffset (int64)
	writeInt64(&headerBuf, baseOffset)

	// batchLength (int32) - will be updated after we know the size
	batchLengthPos := headerBuf.Len()
	writeInt32(&headerBuf, 0) // placeholder

	// partitionLeaderEpoch (int32)
	writeInt32(&headerBuf, -1)

	// magic (int8)
	headerBuf.WriteByte(2) // Kafka v2 format

	// crc (int32) - will be calculated over attributes through records
	crcPos := headerBuf.Len()
	writeInt32(&headerBuf, 0) // placeholder

	// attributes (int16) - bit 0-2: compression (0=none), bit 3: timestampType (0=createTime)
	writeInt16(&headerBuf, 0)

	// lastOffsetDelta (int32)
	writeInt32(&headerBuf, int32(lastOffset-baseOffset))

	// firstTimestamp (int64)
	writeInt64(&headerBuf, records[0].Timestamp.UnixMilli())

	// maxTimestamp (int64)
	writeInt64(&headerBuf, records[len(records)-1].Timestamp.UnixMilli())

	// producerId (int64)
	writeInt64(&headerBuf, -1)

	// producerEpoch (int16)
	writeInt16(&headerBuf, -1)

	// baseSequence (int32)
	writeInt32(&headerBuf, -1)

	// recordCount (int32)
	writeInt32(&headerBuf, int32(len(records)))

	// Append records data
	headerBuf.Write(recordsData.Bytes())

	// Update batchLength (everything after baseOffset field)
	batchBytes := headerBuf.Bytes()
	batchLength := len(batchBytes) - 12 // excluding baseOffset (8 bytes) and batchLength (4 bytes)
	binary.BigEndian.PutUint32(batchBytes[batchLengthPos:], uint32(batchLength))

	// Calculate CRC32-C over attributes through records (everything after CRC field)
	crcData := batchBytes[crcPos+4:]
	crc := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batchBytes[crcPos:], crc)

	meta := &ExtentMeta{
		FirstByteOffset: 0,
		ByteRangeSize:   uint64(len(batchBytes)),
		BaseOffset:      baseOffset,
		LastOffset:      lastOffset,
	}

	return batchBytes, meta, nil
}

// DeserializeL0Object deserializes an L0 object back to Kafka records.
func DeserializeL0Object(objectBytes []byte, placeholder *PlaceholderSerde) ([]*kgo.Record, error) {
	// Extract the byte range specified by the placeholder
	start := placeholder.Offset
	end := start + placeholder.SizeBytes

	if end > uint64(len(objectBytes)) {
		return nil, ErrInvalidPlaceholder
	}

	batchBytes := objectBytes[start:end]

	// Parse batch header (61 bytes)
	if len(batchBytes) < 61 {
		return nil, fmt.Errorf("batch too small: %d bytes", len(batchBytes))
	}

	buf := bytes.NewReader(batchBytes)

	// Read header fields
	baseOffset, _ := readInt64(buf)
	batchLength, _ := readInt32(buf)
	partitionLeaderEpoch, _ := readInt32(buf)
	magic, _ := buf.ReadByte()
	crc, _ := readInt32(buf)
	attributes, _ := readInt16(buf)
	lastOffsetDelta, _ := readInt32(buf)
	firstTimestamp, _ := readInt64(buf)
	maxTimestamp, _ := readInt64(buf)
	producerId, _ := readInt64(buf)
	producerEpoch, _ := readInt16(buf)
	baseSequence, _ := readInt32(buf)
	recordCount, _ := readInt32(buf)

	_ = batchLength
	_ = partitionLeaderEpoch
	_ = crc
	_ = maxTimestamp
	_ = producerId
	_ = producerEpoch
	_ = baseSequence

	if magic != 2 {
		return nil, fmt.Errorf("unsupported magic byte: %d", magic)
	}

	// Check for compression
	compressionType := attributes & 0x07
	if compressionType != 0 {
		return nil, fmt.Errorf("compressed batches not yet supported: type=%d", compressionType)
	}

	// Parse records
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

	_ = lastOffsetDelta // used for validation above

	return records, nil
}

// Helper functions for encoding

func writeInt64(buf *bytes.Buffer, val int64) {
	binary.Write(buf, binary.BigEndian, val)
}

func writeInt32(buf *bytes.Buffer, val int32) {
	binary.Write(buf, binary.BigEndian, val)
}

func writeInt16(buf *bytes.Buffer, val int16) {
	binary.Write(buf, binary.BigEndian, val)
}

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

func readInt64(buf *bytes.Reader) (int64, error) {
	var val int64
	err := binary.Read(buf, binary.BigEndian, &val)
	return val, err
}

func readInt32(buf *bytes.Reader) (int32, error) {
	var val int32
	err := binary.Read(buf, binary.BigEndian, &val)
	return val, err
}

func readInt16(buf *bytes.Reader) (int16, error) {
	var val int16
	err := binary.Read(buf, binary.BigEndian, &val)
	return val, err
}

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
