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

#include "seastarx.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/tiered_storage.h"

namespace storage {

class api {
public:
    api(kvstore_config kv_conf, log_config log_conf) noexcept
      : _kv_conf(std::move(kv_conf))
      , _log_conf(std::move(log_conf)) {}

    ss::future<> start() {
        _kvstore = std::make_unique<kvstore>(_kv_conf);
        _downloader = std::make_unique<s3_downloader>(_dl_conf);
        return _kvstore->start().then([this] {
            _log_mgr = std::make_unique<log_manager>(_log_conf, kvs());
        });
    }

    /// Provide downloader configuration.
    /// Configuration initialization is futurized, so we can't initialize
    /// it in c-tor (because api c-tor is invoked indirectly by the ss::sharded)
    void configure_downloader(s3_downloader_configuration cfg) {
        _dl_conf = std::move(cfg);
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
    s3_downloader& downloader() { return *_downloader; }

private:
    kvstore_config _kv_conf;
    log_config _log_conf;
    s3_downloader_configuration _dl_conf;

    std::unique_ptr<kvstore> _kvstore;
    std::unique_ptr<log_manager> _log_mgr;
    std::unique_ptr<s3_downloader> _downloader;
};

} // namespace storage
