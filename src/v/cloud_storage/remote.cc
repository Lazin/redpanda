/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote.h"

#include "cloud_storage/logger.h"
#include "cloud_storage/types.h"
#include "s3/client.h"
#include "ssx/sformat.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timed_out_error.hh>

#include <asm-generic/errno.h>

#include <exception>
#include <variant>

namespace cloud_storage {

using namespace std::chrono_literals;

static constexpr ss::lowres_clock::duration initial_backoff = 10ms;
static constexpr ss::lowres_clock::duration backoff_jitter = 10ms;

/// Accumulates and deduplicates error messages.
/// The purpose of this is to silence the error messages
/// during retries.
class burst_logger {
public:
    burst_logger() = default;
    burst_logger(const burst_logger&) = delete;
    burst_logger& operator=(const burst_logger&) = delete;
    burst_logger(burst_logger&&) = delete;
    burst_logger& operator=(burst_logger&&) = delete;

    ~burst_logger() {
        if (std::uncaught_exceptions()) {
            // NOTE: if the object is destroyed before the content of the object
            // was logged we need to make an attempt to log everything.
            // The content of the object can't be logged twice.
            try {
                log_accumulated_errors("previously suspended error: ");
            } catch (...) {
            }
        }
    }

    /// \brief Log error message
    ///
    /// The interface is similar to vlog/fmt. Logs error message using
    /// debug level then deduplicates and saves it internally.
    template<typename... Args>
    void operator()(fmt::string_view format_str, Args&&... args) {
        auto msg = ssx::sformat(format_str, args...);
        vlog(cst_log.debug, "{}", msg);
        if (!_messages.empty() && msg != _messages.back()) {
            _messages.emplace_back(std::move(msg));
        }
    }

    /// \brief Log all accumulated messages using 'error' level
    ///
    /// \param msg_prefix prefix that will be added to every error message
    void log_accumulated_errors(const char* msg_prefix = "") {
        for (auto&& m : _messages) {
            vlog(cst_log.error, "{}\"{}\"", msg_prefix, std::move(m));
        }
        _messages.clear();
    }

private:
    std::vector<ss::sstring> _messages;
};

/// Total remaining time + time to wait during next backoff
struct time_budget {
    ss::lowres_clock::duration total;
    ss::lowres_clock::duration next_backoff;
};

/// Compute exp backoff and control execution time
struct backoff_helper {
    explicit backoff_helper(ss::lowres_clock::duration timeout)
      : _deadline(ss::lowres_clock::now() + timeout)
      , _backoff{initial_backoff}
      , _jitter{backoff_jitter} {}

    std::optional<time_budget> get_next() {
        auto now = ss::lowres_clock::now();
        if (now < _deadline) {
            auto total = _deadline - now;
            auto backoff = std::min(
              _backoff + _jitter.next_jitter_duration(), total);
            _backoff *= 2;
            struct time_budget res {
                .total = total, .next_backoff = backoff
            };
            return res;
        }
        return std::nullopt;
    }

    ss::lowres_clock::time_point _deadline;
    ss::lowres_clock::duration _backoff;
    simple_time_jitter<ss::lowres_clock> _jitter;
};

enum class error_outcome { retry, fail, notfound };

/// @brief Analyze exception
/// @return error outcome - retry, fail (with exception), or notfound (can only
/// be used with download)
static error_outcome categorize_error(
  const std::exception_ptr& err,
  burst_logger& blog,
  const s3::bucket_name& bucket,
  const s3::object_key& path) {
    try {
        std::rethrow_exception(err);
    } catch (const s3::rest_error_response& err) {
        if (err.code() == s3::s3_error_code::no_such_key) {
            vlog(cst_log.debug, "NoSuchKey response received {}", path);
            return error_outcome::notfound;
        } else if (err.code() == s3::s3_error_code::slow_down) {
            // This can happen when we're dealing with high request rate to
            // the manifest's prefix. Backoff algorithm should be applied.
            blog("SlowDown response received {}", path);
        } else {
            // Unexpected REST API error, we can't recover from this
            // because the issue is not temporary (e.g. bucket doesn't
            // exist)
            blog(
              "Accessing {}, unexpected REST API error \"{}\" detected, code: "
              "{}, request_id: {}, resource: {}",
              bucket,
              err.message(),
              err.code_string(),
              err.request_id(),
              err.resource());
            return error_outcome::fail;
        }
    } catch (const std::system_error& cerr) {
        // The system_error is type erased and not convenient for selective
        // handling. The following errors should be retried:
        // - connection refused, timed out or reset by peer
        // - network temporary unavailable
        // Shouldn't be retried
        // - any filesystem error
        // - broken-pipe
        // - any other network error (no memory, bad socket, etc)
        if (auto code = cerr.code(); code.value() != ECONNREFUSED
                                     && code.value() != ENETUNREACH
                                     && code.value() != ECONNRESET) {
            blog("System error {}", cerr);
            return error_outcome::fail;
        }
        blog("System error susceptible for retry {}", cerr.what());
    } catch (ss::timed_out_error& terr) {
        // This should happen when the connection pool was disconnected
        // from the S3 endpoint and subsequent connection attmpts failed.
        blog("Connection timeout {}", terr.what());
    }
    return error_outcome::retry;
}

remote::remote(
  s3_connection_limit limit,
  const s3::configuration& conf,
  service_probe& probe)
  : _pool(limit(), conf)
  , _probe(probe) {}

ss::future<> remote::start() { return ss::now(); }

ss::future<> remote::stop() {
    _as.request_abort();
    co_await _pool.stop();
    co_return co_await _gate.close();
}

ss::future<download_result> remote::download_manifest(
  const s3::bucket_name& bucket,
  base_manifest& manifest,
  ss::lowres_clock::duration timeout) {
    gate_guard guard{_gate};
    burst_logger blog;
    // NOTE: all log messages repeatedly generated by the retries
    // should go to he burst_log, everything else is logged as ususal
    backoff_helper backoff{timeout};
    auto key = manifest.get_manifest_path();
    vlog(cst_log.debug, "Download manifest {}", key());
    auto path = s3::object_key(key().string());
    auto [client, deleter] = co_await _pool.acquire();
    auto budget = backoff.get_next();
    while (!_gate.is_closed() && budget) {
        std::exception_ptr eptr = nullptr;
        try {
            auto resp = co_await client->get_object(bucket, path, timeout);
            vlog(cst_log.debug, "Receive OK response from {}", path);
            co_await manifest.update(resp->as_input_stream());
            switch (manifest.get_manifest_type()) {
            case manifest_type::partition:
                _probe.partition_manifest_upload();
                break;
            case manifest_type::topic:
                _probe.topic_manifest_upload();
                break;
            }
            co_return download_result::success;
        } catch (...) {
            eptr = std::current_exception();
        }
        auto outcome = categorize_error(eptr, blog, bucket, path);
        switch (outcome) {
        case error_outcome::retry:
            vlog(
              cst_log.debug,
              "Downloading manifest from {}, {}ms backoff required",
              bucket,
              budget->next_backoff.count());
            _probe.manifest_download_backoff();
            co_await ss::sleep_abortable(budget->next_backoff, _as);
            budget = backoff.get_next();
            break;
        case error_outcome::fail:
            blog.log_accumulated_errors("download manifest failure: ");
            std::rethrow_exception(eptr);
            break;
        case error_outcome::notfound:
            co_return download_result::notfound;
        }
    }
    vlog(
      cst_log.warn,
      "Downloading manifest from {}, backoff quota exceded, manifest at {} not "
      "available",
      bucket,
      path);
    blog.log_accumulated_errors("download manifest failure: ");
    _probe.failed_manifest_download();
    co_return download_result::timedout;
}

ss::future<upload_result> remote::upload_manifest(
  const s3::bucket_name& bucket,
  const base_manifest& manifest,
  ss::lowres_clock::duration timeout) {
    gate_guard guard{_gate};
    burst_logger blog;
    backoff_helper backoff{timeout};
    auto key = manifest.get_manifest_path();
    auto path = s3::object_key(key().string());
    vlog(cst_log.debug, "Uploading manifest {} to the {}", path, bucket());
    std::vector<s3::object_tag> tags = {{"rp-type", "partition-manifest"}};
    auto [client, deleter] = co_await _pool.acquire();
    auto budget = backoff.get_next();
    while (!_gate.is_closed() && budget) {
        std::exception_ptr eptr = nullptr;
        try {
            auto [is, size] = manifest.serialize();
            co_await client->put_object(
              bucket, path, size, std::move(is), tags, timeout);
            vlog(cst_log.debug, "Successfuly uploaded manifest to {}", path);
            switch (manifest.get_manifest_type()) {
            case manifest_type::partition:
                _probe.partition_manifest_upload();
                break;
            case manifest_type::topic:
                _probe.topic_manifest_upload();
                break;
            }
            co_return upload_result::success;
        } catch (...) {
            eptr = std::current_exception();
        }
        auto outcome = categorize_error(eptr, blog, bucket, path);
        switch (outcome) {
        case error_outcome::notfound:
            // not expected during upload
            [[fallthrough]];
        case error_outcome::retry:
            vlog(
              cst_log.debug,
              "Uploading manifest {} to {}, {}ms backoff required",
              path,
              bucket,
              budget->next_backoff.count());
            _probe.manifest_upload_backoff();
            co_await ss::sleep_abortable(budget->next_backoff, _as);
            budget = backoff.get_next();
            break;
        case error_outcome::fail:
            blog.log_accumulated_errors("upload manifest failure: ");
            std::rethrow_exception(eptr);
            break;
        }
    }
    vlog(
      cst_log.warn,
      "Uploading manifest {} to {}, backoff quota exceded, manifest not "
      "uploaded",
      path,
      bucket);
    blog.log_accumulated_errors("upload manifest failure: ");
    _probe.failed_manifest_upload();
    co_return upload_result::timedout;
}

ss::future<upload_result> remote::upload_segment(
  const s3::bucket_name& bucket,
  const segment_name& exposed_name,
  uint64_t content_length,
  const reset_input_stream& reset_str,
  manifest& manifest,
  ss::lowres_clock::duration timeout) {
    gate_guard guard{_gate};
    burst_logger blog;
    backoff_helper backoff{timeout};
    vlog(
      cst_log.debug,
      "Uploading segment for {}, exposed name {}, length {}",
      manifest.get_ntp(),
      exposed_name,
      content_length);
    auto s3path = manifest.get_remote_segment_path(exposed_name);
    std::vector<s3::object_tag> tags = {{"rp-type", "segment"}};
    auto budget = backoff.get_next();
    while (!_gate.is_closed() && budget) {
        auto [client, deleter] = co_await _pool.acquire();
        auto stream = reset_str();
        auto path = s3::object_key(s3path().string());
        vlog(
          cst_log.debug,
          "Uploading segment for {}, path {}",
          manifest.get_ntp(),
          s3path);
        std::exception_ptr eptr = nullptr;
        try {
            // Segment upload attempt
            co_await client->put_object(
              bucket, path, content_length, std::move(stream), tags, timeout);
            _probe.successful_upload(content_length);
            co_return upload_result::success;
        } catch (...) {
            eptr = std::current_exception();
        }
        auto outcome = categorize_error(eptr, blog, bucket, path);
        switch (outcome) {
        case error_outcome::notfound:
            // not expected during upload
            [[fallthrough]];
        case error_outcome::retry:
            vlog(
              cst_log.debug,
              "Uploading segment {} to {}, {}ms backoff required",
              path,
              bucket,
              budget->next_backoff.count());
            _probe.upload_backoff();
            co_await ss::sleep_abortable(budget->next_backoff, _as);
            budget = backoff.get_next();
            break;
        case error_outcome::fail:
            blog.log_accumulated_errors("upload segment failure: ");
            std::rethrow_exception(eptr);
            break;
        }
    }
    vlog(
      cst_log.warn,
      "Uploading segment {} to {}, backoff quota exceded, segment not uploaded",
      s3path,
      bucket);
    blog.log_accumulated_errors("upload segment failure: ");
    _probe.failed_upload(content_length);
    co_return upload_result::timedout;
}

ss::future<download_result> remote::download_segment(
  const s3::bucket_name& bucket,
  const segment_name& name,
  manifest& manifest,
  const try_consume_stream& cons_str,
  ss::lowres_clock::duration timeout) {
    gate_guard guard{_gate};
    burst_logger blog;
    backoff_helper backoff{timeout};
    auto s3path = manifest.get_remote_segment_path(name);
    auto path = s3::object_key(s3path().string());
    vlog(cst_log.debug, "Download segment {}", path);
    auto [client, deleter] = co_await _pool.acquire();
    auto budget = backoff.get_next();
    while (!_gate.is_closed() && budget) {
        std::exception_ptr eptr = nullptr;
        try {
            auto resp = co_await client->get_object(bucket, path, timeout);
            vlog(cst_log.debug, "Receive OK response from {}", path);
            uint64_t content_length = co_await cons_str(
              resp->as_input_stream());
            _probe.successful_download(content_length);
            co_return download_result::success;
        } catch (...) {
            eptr = std::current_exception();
        }
        auto outcome = categorize_error(eptr, blog, bucket, path);
        switch (outcome) {
        case error_outcome::retry:
            vlog(
              cst_log.debug,
              "Downloading segment from {}, {}ms backoff required",
              bucket,
              budget->next_backoff.count());
            _probe.download_backoff();
            co_await ss::sleep_abortable(budget->next_backoff, _as);
            budget = backoff.get_next();
            break;
        case error_outcome::fail:
            blog.log_accumulated_errors("download segment failure: ");
            std::rethrow_exception(eptr);
            break;
        case error_outcome::notfound:
            co_return download_result::notfound;
        }
    }
    vlog(
      cst_log.warn,
      "Downloading segment from {}, backoff quota exceded, segment at {} not "
      "available",
      bucket,
      path);
    blog.log_accumulated_errors("download segment failure: ");
    _probe.failed_download();
    co_return download_result::timedout;
}

} // namespace cloud_storage