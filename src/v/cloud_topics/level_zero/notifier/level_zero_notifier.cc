/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/notifier/level_zero_notifier.h"

#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/logger.h"
#include "cluster/partition.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "model/timeout_clock.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

namespace cloud_topics {

namespace {
constexpr auto replicate_timeout = std::chrono::seconds{30};
} // namespace

level_zero_notifier::level_zero_notifier(
  ss::sharded<cluster::shard_table>* shard_table,
  ss::sharded<cluster::partition_manager>* partition_manager,
  std::chrono::milliseconds retry_backoff)
  : _shard_table(shard_table)
  , _partition_manager(partition_manager)
  , _retry_backoff(retry_backoff) {}

ss::future<> level_zero_notifier::stop() {
    _as.request_abort();
    co_await _gate.close();
}

ss::future<std::expected<void, ctp_stm_api_errc>>
level_zero_notifier::set_allowed_local_start_offset(
  model::ntp ntp, kafka::offset new_floor) {
    auto shard = _shard_table->local().shard_for(ntp);
    if (!shard.has_value()) {
        // Partition is not hosted on this node: nothing to replicate.
        co_return std::expected<void, ctp_stm_api_errc>{};
    }
    co_return co_await container().invoke_on(
      *shard,
      [ntp = std::move(ntp), new_floor](level_zero_notifier& self) mutable {
          return self.replicate_on_home_shard(std::move(ntp), new_floor);
      });
}

ss::future<std::expected<void, ctp_stm_api_errc>>
level_zero_notifier::replicate_on_home_shard(
  model::ntp ntp, kafka::offset new_floor) {
    if (_gate.is_closed()) {
        co_return std::unexpected(ctp_stm_api_errc::shutdown);
    }
    auto holder = _gate.hold();
    auto units = co_await ss::get_units(_inflight, 1);

    auto partition = _partition_manager->local().get(ntp);
    if (!partition) {
        // Partition moved away after the shard lookup: nothing to do.
        co_return std::expected<void, ctp_stm_api_errc>{};
    }
    auto stm = partition->raft()->stm_manager()->get<ctp_stm>();
    if (!stm) {
        // Not a cloud-topic partition: nothing to do.
        co_return std::expected<void, ctp_stm_api_errc>{};
    }
    ctp_stm_api api(stm);
    co_return co_await replicate_with_retries(api, new_floor);
}

ss::future<std::expected<void, ctp_stm_api_errc>>
level_zero_notifier::replicate_with_retries(
  ctp_stm_api& api, kafka::offset new_floor) {
    auto last_error = ctp_stm_api_errc::timeout;
    for (int attempt = 0; attempt < max_attempts && !_as.abort_requested();
         ++attempt) {
        auto res = co_await api.set_allowed_local_start_offset(
          new_floor, model::timeout_clock::now() + replicate_timeout, _as);
        if (res.has_value()) {
            co_return std::expected<void, ctp_stm_api_errc>{};
        }
        last_error = res.error();
        if (last_error == ctp_stm_api_errc::shutdown) {
            // The stm is shutting down; retrying will not help.
            co_return std::unexpected(last_error);
        }
        if (attempt + 1 < max_attempts) {
            try {
                co_await ss::sleep_abortable<ss::lowres_clock>(
                  _retry_backoff, _as);
            } catch (const ss::sleep_aborted&) {
                co_return std::unexpected(ctp_stm_api_errc::shutdown);
            }
        }
    }
    co_return std::unexpected(last_error);
}

} // namespace cloud_topics
