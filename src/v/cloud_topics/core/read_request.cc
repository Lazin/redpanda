/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/core/read_request.h"

#include "cloud_topics/core/pipeline_stage.h"
#include "cloud_topics/logger.h"
#include "storage/types.h"

#include <variant>

namespace experimental::cloud_topics::core {

template<class Clock>
read_request<Clock>::read_request(
  model::ntp ntp,
  read_request_query query,
  std::chrono::milliseconds timeout,
  pipeline_stage stage)
  : ntp(std::move(ntp))
  , query(query)
  , ingestion_time(Clock::now())
  , expiration_time(Clock::now() + timeout)
  , stage(stage) {}

template<class Clock>
void read_request<Clock>::set_value(errc e) noexcept {
    try {
        response.set_value(e);
    } catch (const ss::broken_promise&) {
        vlog(
          cd_log.error,
          "Can't fail request for {}, error {} will be lost",
          ntp,
          e);
    }
}

template<class Clock>
bool read_request<Clock>::is_timequery() const noexcept {
    return std::holds_alternative<storage::timequery_config>(query);
}

template<class Clock>
void read_request<Clock>::set_value(read_request_result result) noexcept {
    try {
        response.set_value(std::move(result));
    } catch (const ss::broken_promise&) {
        vlog(cd_log.error, "Can't acknowledge request for {}", ntp);
    }
}

template<class Clock>
bool read_request<Clock>::has_expired() const noexcept {
    return Clock::now() > expiration_time;
}

template<class Clock>
storage::log_reader_config read_request<Clock>::get_log_reader_config() const {
    return std::get<storage::log_reader_config>(query);
}

template struct read_request<ss::lowres_clock>;
template struct read_request<ss::manual_clock>;
} // namespace experimental::cloud_topics::core
