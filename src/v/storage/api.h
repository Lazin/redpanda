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
    api(
      kvstore_config kv_conf, log_config log_conf, size_t max_conn = 3) noexcept
      : _kv_conf(std::move(kv_conf))
      , _log_conf(std::move(log_conf))
      , _max_s3_connections(max_conn)
      , _downloader(std::make_unique<topic_downloader>(_max_s3_connections)) {}

    ss::future<> start() {
        _kvstore = std::make_unique<kvstore>(_kv_conf);
        return _kvstore->start().then([this] {
            _log_mgr = std::make_unique<log_manager>(_log_conf, kvs());
        });
    }

    void set_remote(s3::bucket_name bucket, cloud_storage::remote& remote) {
        _downloader->set_remote(std::move(bucket), &remote);
    }

    ss::future<> stop() {
        auto f = ss::now();
        if (_log_mgr) {
            f = _log_mgr->stop();
        }
        if (_downloader) {
            f = f.then([this] { return _downloader->stop(); });
        }
        if (_kvstore) {
            f = f.then([this] { return _kvstore->stop(); });
        }
        return f;
    }

    kvstore& kvs() { return *_kvstore; }
    log_manager& log_mgr() { return *_log_mgr; }
    topic_downloader& downloader() { return *_downloader; }

private:
    kvstore_config _kv_conf;
    log_config _log_conf;
    size_t _max_s3_connections;

    std::unique_ptr<kvstore> _kvstore;
    std::unique_ptr<log_manager> _log_mgr;
    std::unique_ptr<topic_downloader> _downloader;
};

} // namespace storage
