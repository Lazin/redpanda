/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/service.h"

#include "archival/logger.h"
#include "archival/ntp_archiver_service.h"
#include "cluster/partition_manager.h"
#include "cluster/topic_table.h"
#include "config/configuration.h"
#include "config/property.h"
#include "likely.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "s3/client.h"
#include "s3/error.h"
#include "s3/signature.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/log.h"
#include "utils/gate_guard.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>

#include <exception>
#include <stdexcept>

using namespace std::chrono_literals;

namespace archival::internal {

ntp_upload_queue::iterator ntp_upload_queue::begin() {
    return _archivers.begin();
}

ntp_upload_queue::iterator ntp_upload_queue::end() { return _archivers.end(); }

bool ntp_upload_queue::insert(ntp_upload_queue::value archiver) {
    auto key = archiver->get_ntp();
    auto [it, ok] = _archivers.insert(std::make_pair(
      std::move(key), upload_queue_item{.archiver = std::move(archiver)}));
    _upload_queue.push_back(it->second);
    return ok;
}

void ntp_upload_queue::erase(const ntp_upload_queue::key& ntp) {
    _archivers.erase(ntp);
}

bool ntp_upload_queue::contains(const key& ntp) const {
    return _archivers.contains(ntp);
}

size_t ntp_upload_queue::size() const noexcept { return _archivers.size(); }

ntp_upload_queue::value ntp_upload_queue::get_upload_candidate() {
    auto& candidate = _upload_queue.front();
    candidate._upl_hook.unlink();
    _upload_queue.push_back(candidate);
    return candidate.archiver;
}

ntp_upload_queue::value ntp_upload_queue::operator[](const key& ntp) const {
    auto it = _archivers.find(ntp);
    if (it == _archivers.end()) {
        return nullptr;
    }
    return it->second.archiver;
}

static ss::sstring get_value_or_throw(
  const config::property<std::optional<ss::sstring>>& prop, const char* name) {
    auto opt = prop.value();
    if (!opt) {
        vlog(
          archival_log.error,
          "Configuration property {} is required to enable archival storage",
          name);
        throw std::runtime_error(
          fmt::format("configuration property {} is not set", name));
    }
    return *opt;
}

/// Use shard-local configuration to generate configuration
ss::future<archival::configuration>
scheduler_service_impl::get_archival_service_config() {
    vlog(archival_log.debug, "Generating archival configuration");
    auto secret_key = s3::private_key_str(get_value_or_throw(
      config::shard_local_cfg().archival_storage_s3_secret_key,
      "archival_storage_s3_secret_key"));
    auto access_key = s3::public_key_str(get_value_or_throw(
      config::shard_local_cfg().archival_storage_s3_access_key,
      "archival_storage_s3_access_key"));
    auto region = s3::aws_region_name(get_value_or_throw(
      config::shard_local_cfg().archival_storage_s3_region,
      "archival_storage_s3_region"));
    auto s3_conf = co_await s3::configuration::make_configuration(
      access_key, secret_key, region);
    archival::configuration cfg{
      .client_config = std::move(s3_conf),
      .bucket_name = s3::bucket_name(get_value_or_throw(
        config::shard_local_cfg().archival_storage_s3_bucket,
        "archival_storage_s3_bucket")),
      .upload_policy = {}, // The values are ignored because only one policy
      .delete_policy{},    // is implemented for now.
      .interval
      = config::shard_local_cfg().archival_storage_upload_interval.value(),
      .gc_interval
      = config::shard_local_cfg().archival_storage_gc_interval.value(),
      .connection_limit = s3_connection_limit(
        config::shard_local_cfg().archival_storage_max_connections.value())};
    vlog(archival_log.debug, "Archival configuration generated: {}", cfg);
    co_return std::move(cfg);
}

scheduler_service_impl::scheduler_service_impl(
  const configuration& conf,
  ss::sharded<storage::api>& api,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::topic_table>& tt)
  : _conf(conf)
  , _partition_manager(pm)
  , _topic_table(tt)
  , _storage_api(api)
  , _jitter(conf.interval, 1ms)
  , _gc_jitter(conf.gc_interval, 1ms)
  , _conn_limit(conf.connection_limit())
  , _stop_limit(conf.connection_limit()) {}

scheduler_service_impl::scheduler_service_impl(
  ss::sharded<storage::api>& api,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::topic_table>& tt,
  ss::sharded<archival::configuration>& config)
  : scheduler_service_impl(config.local(), api, pm, tt) {}

void scheduler_service_impl::rearm_timer() {
    (void)ss::with_gate(_gate, [this] {
        return workflow()
          .finally([this] {
              if (_gate.is_closed()) {
                  return;
              }
              _timer.rearm(_jitter());
          })
          .handle_exception([](std::exception_ptr e) {
              vlog(archival_log.info, "Error in timer callback: {}", e);
          });
    });
}
void scheduler_service_impl::start() {
    _timer.set_callback([this] { rearm_timer(); });
    _timer.rearm(_jitter());
}

ss::future<> scheduler_service_impl::stop() {
    vlog(archival_log.info, "Scheduler service stop");
    _timer.cancel();
    _as.request_abort();
    std::vector<ss::future<>> outstanding;
    for (auto& it : _queue) {
        auto fut = ss::with_semaphore(
          _stop_limit, 1, [it] { return it.second.archiver->stop(); });
        outstanding.emplace_back(std::move(fut));
    }
    return ss::do_with(
      std::move(outstanding), [this](std::vector<ss::future<>>& outstanding) {
          return ss::when_all_succeed(outstanding.begin(), outstanding.end())
            .then([this] { return _gate.close(); });
      });
}

ss::future<> scheduler_service_impl::workflow() {
    static thread_local size_t niter = 0;
    vlog(archival_log.debug, "Start S3 maintanance cycle #{}", niter);
    co_await reconcile_archivers();
    co_await run_uploads();
    vlog(archival_log.debug, "Complete S3 maintanance cycle #{}", niter);
    niter++;
}

ss::lw_shared_ptr<ntp_archiver> scheduler_service_impl::get_upload_candidate() {
    return _queue.get_upload_candidate();
}

ss::future<> scheduler_service_impl::upload_topic_manifest(
  model::topic_namespace_view view, model::revision_id rev) {
    gate_guard gg(_gate);
    auto cfg = _topic_table.local().get_topic_cfg(view);
    if (cfg) {
        try {
            auto units = co_await ss::get_units(_conn_limit, 1);
            vlog(archival_log.error, "Uploading topic manifest {}", view);
            s3::client client(_conf.client_config, _as);
            topic_manifest tm(*cfg, rev);
            auto [istr, size_bytes] = tm.serialize();
            auto key = tm.get_manifest_path();
            std::vector<s3::object_tag> tags = {{"rp-type", "topic-manifest"}};
            co_await client.put_object(
              _conf.bucket_name,
              s3::object_key(key),
              size_bytes,
              std::move(istr),
              tags);
            co_await client.shutdown();
        } catch (const s3::rest_error_response& err) {
            vlog(
              archival_log.error,
              "REST API error occured during topic manifest upload: "
              "{}, code: {}, request-id: {}, resource: {}",
              err.message(),
              err.code(),
              err.request_id(),
              err.resource());
        } catch (...) {
            vlog(
              archival_log.error,
              "Exception occured during topic manifest upload: "
              "{}",
              std::current_exception());
        }
    }
}

ss::future<>
scheduler_service_impl::create_archivers(std::vector<model::ntp> to_create) {
    return ss::parallel_for_each(to_create, [this](const model::ntp& ntp) {
        storage::api& api = _storage_api.local();
        storage::log_manager& lm = api.log_mgr();
        auto log = lm.get(ntp);
        auto svc = ss::make_lw_shared<ntp_archiver>(log->config(), _conf);
        // TODO: run download_manifest, if error is 'slowdown', wait a while
        // and repeat. Use exponential backoff.
        return svc->download_manifest()  // TODO: handle SlowDown
          .then([this, svc](download_manifest_result result) {
              _queue.insert(svc);
              if (found) {
                  vlog(
                    archival_log.info,
                    "Found manifest for partition {}",
                    svc->get_ntp());
              } else {
                  vlog(
                    archival_log.info,
                    "Start archiving new partition {}",
                    svc->get_ntp());
                  model::topic_namespace_view view(svc->get_ntp());
                  (void)upload_topic_manifest(view, svc->get_revision_id());
              }
              return ss::now();
          })
          .handle_exception_type([ntp](const s3::rest_error_response& err) {
              vlog(
                archival_log.error,
                "Manifest download for partition {}, failed, "
                "error: ",
                ntp.path(),
                err.what());
              return ss::now();
          });
    });
} // namespace archival::internal

ss::future<>
scheduler_service_impl::remove_archivers(std::vector<model::ntp> to_remove) {
    return ss::parallel_for_each(
      to_remove, [this](const model::ntp& ntp) -> ss::future<> {
          vlog(archival_log.info, "removing archiver for {}", ntp.path());
          auto archiver = _queue[ntp];
          return ss::with_semaphore(
                   _conn_limit, 1, [archiver] { return archiver->stop(); })
            .finally([this, ntp] {
                vlog(archival_log.info, "archiver stopped {}", ntp.path());
                _queue.erase(ntp);
            });
      });
}

ss::future<> scheduler_service_impl::reconcile_archivers() {
    gate_guard g(_gate);
    cluster::partition_manager& pm = _partition_manager.local();
    storage::api& api = _storage_api.local();
    storage::log_manager& lm = api.log_mgr();
    auto snapshot = lm.get();
    std::vector<model::ntp> to_remove;
    std::vector<model::ntp> to_create;
    // find ntps that exist in _svc_per_ntp but no longer present in log_manager
    _queue.copy_if(
      std::back_inserter(to_remove), [&snapshot, &pm](const model::ntp& ntp) {
          return !snapshot.contains(ntp) || !pm.get(ntp)
                 || !pm.get(ntp)->is_leader();
      });
    // find new ntps that present in the snapshot only
    std::copy_if(
      snapshot.begin(),
      snapshot.end(),
      std::back_inserter(to_create),
      [this, &pm](const model::ntp& ntp) {
          return ntp.ns != model::redpanda_ns && !_queue.contains(ntp)
                 && pm.get(ntp) && pm.get(ntp)->is_leader();
      });
    // epxect to_create & to_remove be empty most of the time
    if (unlikely(!to_remove.empty() || !to_create.empty())) {
        // run in parallel
        ss::gate reconcile_gate;
        if (!to_remove.empty()) {
            (void)ss::with_gate(
              reconcile_gate,
              [this, to_remove = std::move(to_remove)]() mutable {
                  return remove_archivers(std::move(to_remove));
              });
        }
        if (!to_create.empty()) {
            (void)ss::with_gate(
              reconcile_gate,
              [this, to_create = std::move(to_create)]() mutable {
                  return create_archivers(std::move(to_create));
              });
        }
        co_await reconcile_gate.close();
    }
    co_return;
}

ss::future<> scheduler_service_impl::run_uploads() {
    // Start uploads
    gate_guard g(_gate);
    ss::gate upload_gate;
    ntp_archiver::batch_result total{};
    int quota = _queue.size();
    while (_conn_limit.available_units() && quota-- > 0) {
        auto archiver = _queue.get_upload_candidate();
        (void)ss::with_gate(
          upload_gate, [this, archiver, &total]() -> ss::future<> {
              storage::api& api = _storage_api.local();
              storage::log_manager& lm = api.log_mgr();
              vlog(
                archival_log.debug,
                "Checking {} for S3 upload candidates",
                archiver->get_ntp());
              return archiver->upload_next_candidate(_conn_limit, lm)
                .then([archiver,
                       &total](const ntp_archiver::batch_result& res) {
                    total.num_failed += res.num_failed;
                    total.num_succeded += res.num_succeded;
                    if (res.num_failed != 0) {
                        vlog(
                          archival_log.error,
                          "Failed to upload {} segments out of {} that belongs "
                          "to {}",
                          res.num_failed,
                          res.num_succeded,
                          archiver->get_ntp());
                    } else if (res.num_succeded) {
                        vlog(
                          archival_log.info,
                          "{} segments that belongs to {} are uploaded",
                          res.num_succeded,
                          archiver->get_ntp());
                    }
                    return ss::now();
                });
          });
    }
    co_await upload_gate.close();
    if (total.num_failed != 0) {
        vlog(
          archival_log.error,
          "Failed to upload {} segments out of {}",
          total.num_failed,
          total.num_succeded);
    }
    co_return;
}

} // namespace archival::internal
