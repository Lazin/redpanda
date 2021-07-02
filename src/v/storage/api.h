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

#pragma once

#include "s3/client.h"
#include "seastarx.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/tiered_storage.h"

#include <seastar/core/shared_ptr.hh>

namespace storage {

class api {
public:
    api(kvstore_config kv_conf, log_config log_conf) noexcept
      : _kv_conf(std::move(kv_conf))
      , _log_conf(std::move(log_conf))
      , _recovery_mgr(std::make_unique<partition_recovery_manager>()) {}

    ss::future<> start() {
        _kvstore = std::make_unique<kvstore>(_kv_conf);
        return _kvstore->start().then([this] {
            _log_mgr = std::make_unique<log_manager>(_log_conf, kvs());
        });
    }

    void set_remote(s3::bucket_name bucket, cloud_storage::remote& remote) {
        _recovery_mgr->set_remote(std::move(bucket), &remote);
    }

    ss::future<> stop() {
        auto f = ss::now();
        if (_log_mgr) {
            f = _log_mgr->stop();
        }
        if (_recovery_mgr) {
            f = f.then([this] { return _recovery_mgr->stop(); });
        }
        if (_kvstore) {
            f = f.then([this] { return _kvstore->stop(); });
        }
        return f;
    }

    kvstore& kvs() { return *_kvstore; }
    log_manager& log_mgr() { return *_log_mgr; }
    partition_recovery_manager& downloader() { return *_recovery_mgr; }

private:
    kvstore_config _kv_conf;
    log_config _log_conf;

    std::unique_ptr<kvstore> _kvstore;
    std::unique_ptr<log_manager> _log_mgr;
    std::unique_ptr<partition_recovery_manager> _recovery_mgr;
};

} // namespace storage
