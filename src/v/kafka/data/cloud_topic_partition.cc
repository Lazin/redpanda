/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/data/cloud_topic_partition.h"

#include "cloud_storage/types.h"
#include "cloud_topics/api.h"
#include "cluster/partition.h"
#include "cluster/rm_stm.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/errors.h"
#include "logger.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "storage/log_reader.h"
#include "storage/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>

namespace kafka {
cloud_topic_partition::cloud_topic_partition(
  ss::lw_shared_ptr<cluster::partition> p,
  experimental::cloud_topics::api* app) noexcept
  : _partition(p)
  , _ct_api(app) {}

const model::ntp& cloud_topic_partition::ntp() const {
    return _partition->ntp();
}

// Get start offset of the underlying Raft group
static model::offset get_kafka_start_offset(cluster::partition& p) {
    auto local_kafka_start_offset
      = p.get_offset_translator_state()->from_log_offset(p.raft_start_offset());
    return local_kafka_start_offset;
}

// Get start offset of the underlying Raft group but take possible
// start offset override into account (the override is set by DeleteRecords api)
static model::offset get_kafka_start_offset_with_override(
  cluster::partition& p, model::offset so_override) {
    auto so = get_kafka_start_offset(p);
    if (so_override == model::offset{}) {
        return so;
    }
    return std::max(so, so_override);
}

static model::offset get_log_end_offset(cluster::partition& p) {
    auto ot_state = p.get_offset_translator_state();
    // Local log is empty
    if (p.dirty_offset() < p.raft_start_offset()) {
        return ot_state->from_log_offset(p.raft_start_offset());
    }
    // Local log is not empty
    return ot_state->from_log_offset(model::next_offset(p.dirty_offset()));
}

static ss::future<std::vector<cluster::tx::tx_range>>
get_aborted_transactions_local(
  cluster::partition& p, cloud_storage::offset_range offsets) {
    // The reconciled data should have aborted transactions removed.
    // This means that we should only read aborted transactions for
    // recent offsets which are not reconciled yet.

    auto ot_state = p.get_offset_translator_state();
    auto source = co_await p.aborted_transactions(
      offsets.begin_rp, offsets.end_rp);

    std::vector<cluster::tx::tx_range> target;
    target.reserve(source.size());
    for (const auto& range : source) {
        target.emplace_back(
          range.pid,
          ot_state->from_log_offset(range.first),
          ot_state->from_log_offset(range.last));
    }

    co_return target;
}

ss::future<result<model::offset, error_code>>
cloud_topic_partition::sync_effective_start(
  model::timeout_clock::duration timeout) {
    // Ask partition for its start offset
    // TODO: ask dl_stm for its start offset
    auto kso = co_await _partition->sync_kafka_start_offset_override(timeout);
    if (kso.has_error()) {
        auto err = kso.error();
        auto error_code = error_code::unknown_server_error;
        if (err.category() == cluster::error_category()) {
            switch (cluster::errc(err.value())) {
            case cluster::errc::shutting_down:
            case cluster::errc::not_leader:
            case cluster::errc::timeout:
                error_code = error_code::not_leader_for_partition;
                break;
            default:
                error_code = error_code::unknown_server_error;
            }
        }
        co_return error_code;
    }

    co_return get_kafka_start_offset_with_override(*_partition, kso.value());
}

model::offset cloud_topic_partition::local_start_offset() const {
    auto ot_state = _partition->get_offset_translator_state();
    return ot_state->from_log_offset(_partition->raft_start_offset());
}

model::offset cloud_topic_partition::start_offset() const {
    const auto start_offset_override
      = _partition->kafka_start_offset_override();
    if (!start_offset_override.has_value()) {
        return get_kafka_start_offset(*_partition);
    }
    return get_kafka_start_offset_with_override(
      *_partition, start_offset_override.value());
}

model::offset cloud_topic_partition::high_watermark() const {
    if (_partition->is_read_replica_mode_enabled()) {
        if (_partition->cloud_data_available()) {
            return _partition->next_cloud_offset();
        } else {
            return model::offset(0);
        }
    }
    auto ot_state = _partition->get_offset_translator_state();
    return ot_state->from_log_offset(_partition->high_watermark());
}

checked<model::offset, error_code>
cloud_topic_partition::last_stable_offset() const {
    auto maybe_lso = _partition->last_stable_offset();
    if (maybe_lso == model::invalid_lso) {
        return error_code::offset_not_available;
    }
    auto ot_state = _partition->get_offset_translator_state();
    return ot_state->from_log_offset(maybe_lso);
}

bool cloud_topic_partition::is_leader() const {
    return _partition->is_leader();
}

ss::future<std::error_code> cloud_topic_partition::linearizable_barrier() {
    auto r = co_await _partition->linearizable_barrier();
    if (r) {
        co_return raft::errc::success;
    }
    co_return r.error();
}

cluster::partition_probe& cloud_topic_partition::probe() {
    return _partition->probe();
}

kafka::leader_epoch cloud_topic_partition::leader_epoch() const {
    return leader_epoch_from_term(_partition->raft()->confirmed_term());
}

// TODO: use previous translation speed up lookup
ss::future<storage::translating_reader> cloud_topic_partition::make_reader(
  storage::log_reader_config cfg,
  std::optional<model::timeout_clock::time_point> debounce_deadline) {
    // TODO: use cloud topics read path here
    auto ot_state = _partition->get_offset_translator_state();
    cfg.start_offset = ot_state->to_log_offset(cfg.start_offset);
    cfg.max_offset = ot_state->to_log_offset(cfg.max_offset);
    cfg.type_filter = {model::record_batch_type::raft_data};
    cfg.translate_offsets = storage::translate_offsets::yes;
    auto rdr = co_await _partition->make_reader(cfg, debounce_deadline);
    co_return storage::translating_reader(std::move(rdr), ot_state);
}

ss::future<std::vector<cluster::tx::tx_range>>
cloud_topic_partition::aborted_transactions(
  model::offset base,
  model::offset last,
  ss::lw_shared_ptr<const storage::offset_translator_state> ot_state) {
    auto base_rp = ot_state->to_log_offset(base);
    auto last_rp = ot_state->to_log_offset(last);
    cloud_storage::offset_range offsets = {
      .begin = model::offset_cast(base),
      .end = model::offset_cast(last),
      .begin_rp = base_rp,
      .end_rp = last_rp,
    };
    co_return co_await get_aborted_transactions_local(*_partition, offsets);
}

ss::future<std::optional<storage::timequery_result>>
cloud_topic_partition::timequery(storage::timequery_config cfg) {
    // cluster::partition::timequery returns a result in Kafka offsets,
    // no further offset translation is required here.
    return _partition->timequery(cfg);
}
ss::future<result<model::offset>> cloud_topic_partition::replicate(
  chunked_vector<model::record_batch> batches, raft::replicate_options opts) {
    using ret_t = result<model::offset>;
    // TODO: use cloud topics logic here
    return _partition->replicate(std::move(batches), opts)
      .then([](result<cluster::kafka_result> r) {
          if (!r) {
              return ret_t(r.error());
          }
          return ret_t(model::offset(r.value().last_offset()));
      });
}
ss::future<result<model::offset>> cloud_topic_partition::replicate(
  model::record_batch batch, raft::replicate_options opts) {
    return replicate(
      chunked_vector<model::record_batch>::single(std::move(batch)), opts);
}

raft::replicate_stages cloud_topic_partition::replicate(
  model::batch_identity batch_id,
  model::record_batch batch,
  raft::replicate_options opts) {
    using ret_t = result<raft::replicate_result>;
    // TODO: use cloud topics logic here
    if (_partition->is_read_replica_mode_enabled()) {
        return {
          ss::now(),
          ss::make_ready_future<result<raft::replicate_result>>(
            make_error_code(kafka::error_code::invalid_topic_exception))};
    }
    auto res = _partition->replicate_in_stages(
      batch_id, std::move(batch), opts);

    raft::replicate_stages out(raft::errc::success);
    out.request_enqueued = std::move(res.request_enqueued);
    out.replicate_finished = res.replicate_finished.then(
      [](result<cluster::kafka_result> r) {
          if (!r) {
              return ret_t(r.error());
          }
          return ret_t(
            raft::replicate_result{model::offset(r.value().last_offset())});
      });
    return out;
}

ss::future<std::optional<model::offset>>
cloud_topic_partition::get_leader_epoch_last_offset(
  kafka::leader_epoch epoch) const {
    auto ot_state = _partition->get_offset_translator_state();
    model::term_id term(epoch);
    auto first_local_offset = _partition->raft_start_offset();
    auto first_local_term = _partition->get_term(first_local_offset);
    auto last_local_term = _partition->term();

    if (term > last_local_term) {
        co_return std::nullopt;
    }

    if (term >= first_local_term) {
        auto last_offset = _partition->get_term_last_offset(term);
        if (last_offset) {
            co_return ot_state->from_log_offset(*last_offset);
        }
    }

    auto first_kafka_offset = ot_state->from_log_offset(first_local_offset);

    if (!_partition->kafka_start_offset_override().has_value()) {
        co_return first_kafka_offset;
    }
    co_return std::max(first_kafka_offset, start_offset());
}

ss::future<error_code> cloud_topic_partition::prefix_truncate(
  model::offset kafka_truncation_offset,
  ss::lowres_clock::time_point deadline) {
    if (kafka_truncation_offset <= start_offset()) {
        co_return kafka::error_code::none;
    }
    if (kafka_truncation_offset > high_watermark()) {
        co_return error_code::offset_out_of_range;
    }
    model::offset rp_truncate_offset{};
    auto ot_state = _partition->get_offset_translator_state();
    auto local_kafka_start_offset = ot_state->from_log_offset(
      _partition->raft_start_offset());
    if (kafka_truncation_offset > local_kafka_start_offset) {
        rp_truncate_offset = ot_state->to_log_offset(kafka_truncation_offset);
    }
    auto errc = co_await _partition->prefix_truncate(
      rp_truncate_offset,
      model::offset_cast(kafka_truncation_offset),
      deadline);

    // TODO: This behavior mimics the replicated_partition at the moment
    // but it will have to reach into the cloud topics machinery in the future.
    if (errc.category() == raft::error_category()) {
        vlog(kdlog.warn, "Raft error: {}", errc.value());
        switch (raft::errc(errc.value())) {
        case raft::errc::success:
            co_return kafka::error_code::none;
        case raft::errc::not_leader:
            co_return kafka::error_code::not_leader_for_partition;
        case raft::errc::shutting_down:
            co_return kafka::error_code::request_timed_out;
        default:
            co_return error_code::unknown_server_error;
        }
    } else if (errc.category() != cluster::error_category()) {
        vlog(kdlog.warn, "Cluster error: {}", errc.category().name());
        co_return error_code::unknown_server_error;
    }
    co_return map_topic_error_code(cluster::errc(errc.value()));
}

ss::future<error_code> cloud_topic_partition::validate_fetch_offset(
  model::offset fetch_offset,
  bool reading_from_follower,
  model::timeout_clock::time_point deadline) {
    if (reading_from_follower && !_partition->is_leader()) {
        // TODO: implement follower fetching for cloud topics
        co_return error_code::not_leader_for_partition;
    }

    auto timeout = deadline - model::timeout_clock::now();
    auto so = co_await sync_effective_start(timeout);
    if (!so) {
        co_return so.error();
    }

    if (
      fetch_offset < so.value()
      || fetch_offset > get_log_end_offset(*_partition)) {
        co_return error_code::offset_out_of_range;
    }

    co_return error_code::none;
}

result<partition_info> cloud_topic_partition::get_partition_info() const {
    auto ot_state = _partition->get_offset_translator_state();
    partition_info ret;
    ret.leader = _partition->get_leader_id();
    ret.replicas.reserve(_partition->raft()->get_follower_count() + 1);
    auto followers = _partition->get_follower_metrics();

    if (followers.has_error()) {
        return followers.error();
    }
    auto start_offset = _partition->raft_start_offset();

    auto clamped_translate = [ot_state,
                              start_offset](model::offset to_translate) {
        return to_translate >= start_offset
                 ? ot_state->from_log_offset(to_translate)
                 : ot_state->from_log_offset(start_offset);
    };

    for (const auto& follower_metric : followers.value()) {
        ret.replicas.push_back(replica_info{
          .id = follower_metric.id,
          .high_watermark = model::next_offset(
            clamped_translate(follower_metric.match_index)),
          .log_end_offset = model::next_offset(
            clamped_translate(follower_metric.dirty_log_index)),
          .is_alive = follower_metric.is_live,
        });
    }

    ret.replicas.push_back(replica_info{
      .id = _partition->raft()->self().id(),
      .high_watermark = high_watermark(),
      .log_end_offset = get_log_end_offset(*_partition),
      .is_alive = true,
    });

    return {std::move(ret)};
}

} // namespace kafka
