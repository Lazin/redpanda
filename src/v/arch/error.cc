/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "arch/error.h"
#include <stdexcept>

namespace arch {

manifest_error::manifest_error(const char* msg) 
: std::runtime_error(msg)
{
}

}