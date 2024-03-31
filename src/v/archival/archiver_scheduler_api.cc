/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/archiver_scheduler_api.h"

#include "archival/types.h"

template<typename T>
struct fmt::formatter<std::optional<T>> final
  : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto format(const std::optional<T>& s, FormatContext& ctx) const {
        if (s.has_value()) {
            return format_to(ctx.out(), "{}", s.value());
        }
        return formatter<string_view>::format("<null>", ctx);
    }
};

namespace archival {

archiver_scheduler_api::~archiver_scheduler_api() = default;

std::ostream& operator<<(
  std::ostream& o, const archiver_scheduler_api::suspend_upload_arg& s) {
    fmt::print(
      o,
      "suspend_request({}, {}, {}, {}, {})",
      s.ntp,
      s.manifest_dirty,
      s.put_requests_used,
      s.uploaded_bytes,
      s.errc);
    return o;
}

std::ostream&
operator<<(std::ostream& o, archiver_scheduler_api::next_upload_action_type t) {
    switch (t) {
        using enum archiver_scheduler_api::next_upload_action_type;
    case segment_upload:
        return o << "segment_upload";
    case manifest_upload:
        return o << "manifest_upload";
    case segment_with_manifest:
        return o << "segment_with_manifest";
    }
    return o;
}

std::ostream&
operator<<(std::ostream& o, archiver_scheduler_api::next_upload_action_hint t) {
    fmt::print(o, "next_upload_action_hint({})", t.type);
    return o;
}

std::ostream& operator<<(
  std::ostream& o, const archiver_scheduler_api::suspend_housekeeping_arg& s) {
    fmt::print(
      o,
      "suspend_housekeeping_arg(ntp={}, delete_requests={}, manifest_dirty={}, "
      "errc={})",
      s.ntp,
      s.num_delete_requests_used,
      s.manifest_dirty,
      s.errc);
    return o;
}

std::ostream& operator<<(
  std::ostream& o, archiver_scheduler_api::next_housekeeping_action_type t) {
    switch (t) {
        using enum archiver_scheduler_api::next_housekeeping_action_type;
    case stm_housekeeping:
        return o << "stm_housekeeping";
    case archive_housekeeping:
        return o << "manifest_upload";
    }
    return o;
}

std::ostream& operator<<(
  std::ostream& o,
  archiver_scheduler_api::next_housekeeping_action_hint& hint) {
    fmt::print(
      o,
      "next_housekeeping_action_hint({}, {})",
      hint.type,
      hint.requests_quota);
    return o;
}

} // namespace archival
