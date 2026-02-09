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
	"crypto/rand"
	"fmt"

	"github.com/google/uuid"
)

// ObjectID represents a cloud_topics::object_id from C++.
// It matches the structure with serde::envelope<object_id, version<1>, compat_version<0>>.
type ObjectID struct {
	Epoch  int64     // cluster_epoch
	Name   uuid.UUID // uuid_t (16 bytes)
	Prefix uint16    // prefix_t (0-999)
}

const (
	// MaxPrefix is the maximum value for object prefix (999 in C++).
	MaxPrefix = 999
)

// GenerateObjectID creates a new ObjectID with the given epoch.
// It generates a random UUID and a random prefix between 0 and 999.
func GenerateObjectID(epoch int64) ObjectID {
	// Generate random UUID v4
	name := uuid.New()

	// Generate random prefix (0-999)
	var prefixBytes [2]byte
	rand.Read(prefixBytes[:])
	prefix := uint16(prefixBytes[0])<<8 | uint16(prefixBytes[1])
	prefix = prefix % (MaxPrefix + 1)

	return ObjectID{
		Epoch:  epoch,
		Name:   name,
		Prefix: prefix,
	}
}

// GetObjectPath returns the S3 path for this object ID.
// Format: level_zero/data/{prefix:03}/{epoch:018}/{uuid}
func GetObjectPath(id ObjectID) string {
	return fmt.Sprintf(
		"level_zero/data/%03d/%018d/%s",
		id.Prefix,
		id.Epoch,
		id.Name.String(),
	)
}

// ExtentMeta represents cloud_topics::extent_meta from C++.
type ExtentMeta struct {
	ID              ObjectID
	FirstByteOffset uint64
	ByteRangeSize   uint64
	BaseOffset      int64
	LastOffset      int64
}
