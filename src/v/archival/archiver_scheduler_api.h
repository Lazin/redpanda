/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/outcome.h"
#include "model/fundamental.h"
#include "model/ktp.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

namespace ss = seastar;

namespace archival {

/// The object is responsible of managing execution of different
/// workflows. It controls backoff in situation when there is no
/// new data to upload. It can also apply throttling so one workflow
/// wouldn't be able to use too many requests.
/// It also acts as a scheduler that organizes interleaved execution
/// of different workflow that belong to the same NTP by guaranteeing
/// mutual exclusion. Only one workflow per-NTP is allowed to be active
/// at any moment in time. To guarantee this every workflow should start
/// by calling `maybe_suspend_*` and should also invoke `maybe_suspend_*`
/// periodically to allow interleaving.
/// Suspend methods are workflow specific.
class archiver_scheduler_api {
public:
    archiver_scheduler_api() = default;
    archiver_scheduler_api(const archiver_scheduler_api&) = delete;
    archiver_scheduler_api(archiver_scheduler_api&&) noexcept = delete;
    archiver_scheduler_api& operator=(const archiver_scheduler_api&) = delete;
    archiver_scheduler_api& operator=(archiver_scheduler_api&&) noexcept
      = delete;
    virtual ~archiver_scheduler_api();

    struct suspend_upload_arg {
        model::ktp ntp;
        // Set to true if the manifest was uploaded, false if not, nullopt if
        // there is no change from the previous state
        std::optional<bool> manifest_dirty{std::nullopt};
        // Number of PUT requests used
        size_t put_requests_used{0};
        // Number of bytes uploaded
        size_t uploaded_bytes{0};
        // Set if error occurred (other fields are ignored in this case)
        std::optional<std::error_code> errc;

        bool operator==(const suspend_upload_arg& o) const noexcept {
            return ntp == o.ntp && put_requests_used == o.put_requests_used
                   && manifest_dirty == o.manifest_dirty
                   && uploaded_bytes == o.uploaded_bytes && errc == o.errc;
        }
        friend std::ostream&
        operator<<(std::ostream& o, const suspend_upload_arg& s) {
            std::stringstream dirty;
            if (s.manifest_dirty.has_value()) {
                dirty << s.manifest_dirty.value();
            } else {
                dirty << "na";
            }
            std::stringstream errc;
            if (s.errc.has_value()) {
                errc << s.errc.value();
            } else {
                errc << "na";
            }
            fmt::print(
              o,
              "suspend_request({}, {}, {}, {}, {})",
              s.ntp,
              dirty.str(),
              s.put_requests_used,
              s.uploaded_bytes,
              errc.str());
            return o;
        }
    };

    enum class next_upload_action_type {
        // Low throughput mode
        segment_upload,
        manifest_upload,
        // High throughput mode
        segment_with_manifest,
    };

    /// Scheduler can suggest what should
    /// be uploaded next using this value. It can also
    /// communicate to the workflow the amount of resources
    /// the workflow should use to avoid being throttled.
    struct next_upload_action_hint {
        next_upload_action_type type;
        size_t requests_quota{0};
    };

    /// Applies throttling or backoff to uploads
    ///
    /// \param arg describes resources used by the partition
    /// \returns error or the hint for the workflow
    virtual ss::future<result<next_upload_action_hint>>
    maybe_suspend_upload(suspend_upload_arg arg) noexcept = 0;

    struct suspend_housekeeping_arg {
        model::ktp ntp;
        size_t num_delete_requests_used{0};
        std::optional<std::error_code> errc;

        bool operator==(const suspend_housekeeping_arg& o) const noexcept {
            return ntp == o.ntp
                   && num_delete_requests_used == o.num_delete_requests_used
                   && errc == o.errc;
        }

        friend std::ostream&
        operator<<(std::ostream& o, const suspend_housekeeping_arg& s) {
            std::stringstream err;
            if (s.errc.has_value()) {
                err << s.errc.value();
            } else {
                err << "na";
            }
            fmt::print(
              o,
              "suspend_housekeeping_arg(ntp={}, delete_requests={}, errc={})",
              s.ntp,
              s.num_delete_requests_used,
              err.str());
            return o;
        }
    };

    struct next_housekeeping_action_hint {
        // TODO: add resource quota for the housekeeping
    };

    /// Applies throttling or backoff to the housekeeping
    ///
    /// \param arg describes resources used by the partition
    /// \returns error or the hint for the workflow
    virtual ss::future<result<next_housekeeping_action_hint>>
    maybe_suspend_housekeeping(suspend_housekeeping_arg arg) noexcept = 0;
};

std::ostream&
operator<<(std::ostream& o, archiver_scheduler_api::next_upload_action_type t);

std::ostream&
operator<<(std::ostream& o, archiver_scheduler_api::next_upload_action_hint t);

} // namespace archival
