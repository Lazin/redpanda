/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

#include <utility>

namespace experimental::cloud_topics {

using cmd_key = named_type<uint8_t, struct cmd_key_tag>;

struct advance_reconciled_offset_cmd
  : public serde::envelope<
      advance_reconciled_offset_cmd,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr cmd_key key = cmd_key(
      std::to_underlying(ctp_stm_key::advance_reconciled_offset));

    advance_reconciled_offset_cmd() noexcept = default;

    explicit advance_reconciled_offset_cmd(kafka::offset lro) noexcept
      : last_reconciled_offset(lro) {}

    auto serde_fields() { return std::tie(last_reconciled_offset); }

    kafka::offset last_reconciled_offset;
};

} // namespace experimental::cloud_topics
