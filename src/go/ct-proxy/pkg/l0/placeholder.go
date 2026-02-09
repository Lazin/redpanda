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
	"encoding/binary"
)

// PlaceholderSerde represents cloud_topics::ctp_placeholder from C++.
// It uses serde::envelope format with version 0, compat_version 0.
type PlaceholderSerde struct {
	ID        ObjectID // object_id (has its own envelope)
	Offset    uint64   // first_byte_offset_t
	SizeBytes uint64   // byte_range_size_t
}

// Serialize serializes the placeholder to binary format matching C++ serde::envelope.
//
// Format:
// - Outer envelope (ctp_placeholder):
//   - version: 4 bytes (little endian) = 0
//   - compat_version: 4 bytes (little endian) = 0
// - Inner envelope (object_id):
//   - version: 4 bytes (little endian) = 1
//   - compat_version: 4 bytes (little endian) = 0
//   - epoch: 8 bytes (little endian)
//   - uuid: 16 bytes (raw)
//   - prefix: 2 bytes (little endian)
// - first_byte_offset: 8 bytes (little endian)
// - byte_range_size: 8 bytes (little endian)
func (p *PlaceholderSerde) Serialize() ([]byte, error) {
	// Calculate total size:
	// - outer envelope header: 8 bytes
	// - inner envelope header: 8 bytes
	// - epoch: 8 bytes
	// - uuid: 16 bytes
	// - prefix: 2 bytes
	// - offset: 8 bytes
	// - size_bytes: 8 bytes
	// Total: 58 bytes
	buf := make([]byte, 0, 58)

	// Outer envelope header: ctp_placeholder (version=0, compat=0)
	buf = binary.LittleEndian.AppendUint32(buf, 0) // version
	buf = binary.LittleEndian.AppendUint32(buf, 0) // compat_version

	// Inner envelope header: object_id (version=1, compat=0)
	buf = binary.LittleEndian.AppendUint32(buf, 1) // version
	buf = binary.LittleEndian.AppendUint32(buf, 0) // compat_version

	// object_id fields
	buf = binary.LittleEndian.AppendUint64(buf, uint64(p.ID.Epoch))
	uuidBytes, err := p.ID.Name.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf = append(buf, uuidBytes...) // 16 bytes UUID
	buf = binary.LittleEndian.AppendUint16(buf, p.ID.Prefix)

	// placeholder fields
	buf = binary.LittleEndian.AppendUint64(buf, p.Offset)
	buf = binary.LittleEndian.AppendUint64(buf, p.SizeBytes)

	return buf, nil
}

// Deserialize deserializes a placeholder from binary format.
func (p *PlaceholderSerde) Deserialize(data []byte) error {
	if len(data) < 58 {
		return ErrInvalidPlaceholder
	}

	offset := 0

	// Outer envelope header
	version := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	compatVersion := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if version != 0 || compatVersion != 0 {
		return ErrUnsupportedVersion
	}

	// Inner envelope header (object_id)
	objVersion := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	objCompatVersion := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if objVersion != 1 || objCompatVersion != 0 {
		return ErrUnsupportedVersion
	}

	// object_id fields
	p.ID.Epoch = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8

	var uuidBytes [16]byte
	copy(uuidBytes[:], data[offset:offset+16])
	if err := p.ID.Name.UnmarshalBinary(uuidBytes[:]); err != nil {
		return err
	}
	offset += 16

	p.ID.Prefix = binary.LittleEndian.Uint16(data[offset:])
	offset += 2

	// placeholder fields
	p.Offset = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	p.SizeBytes = binary.LittleEndian.Uint64(data[offset:])

	return nil
}
