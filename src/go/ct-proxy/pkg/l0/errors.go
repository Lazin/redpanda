// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package l0

import "errors"

var (
	// ErrInvalidPlaceholder indicates the placeholder data is invalid or too short.
	ErrInvalidPlaceholder = errors.New("invalid placeholder data")

	// ErrUnsupportedVersion indicates an unsupported serde version.
	ErrUnsupportedVersion = errors.New("unsupported serde version")
)
