/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "arch/error.h"

#include <stdexcept>

namespace arch {

manifest_error::manifest_error(const char* msg)
  : std::runtime_error(msg) {}

} // namespace arch