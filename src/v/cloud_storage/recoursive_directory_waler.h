/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cloud_storage/logger.h"
#include "seastarx.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>

#include <chrono>
#include <memory>
#include <utility>

namespace cloud_storage {

struct recoursive_directory_walker {
    using walker_type = std::function<ss::future<>(ss::directory_entry)>;

    struct file_list_item {
        ss::lowres_clock::time_point access_time;
        ss::sstring filename;
        uint64_t size;
    };

    std::deque<file_list_item> files;

    ss::future<> walk(ss::sstring dirname) {
        // Rebuild files list
        files.clear();

        std::deque<ss::sstring> dirlist = {dirname};

        while (!dirlist.empty()) {
            auto target = dirlist.back();
            dirlist.pop_back();
            ss::file dir = co_await open_directory(target);
            auto sub = dir.list_directory(
              [this, &dirlist, target](
                ss::directory_entry ent) -> ss::future<> {
                  auto path = std::filesystem::path(target)
                              / std::filesystem::path(ent.name);
                  vlog(cst_log.debug, "Looking at directory {}", target);
                  if (
                    ent.type && ent.type == ss::directory_entry_type::regular) {
                      vlog(cst_log.debug, "Regular file found {}", path);
                      ss::file f = co_await ss::open_file_dma(
                        path.string(), ss::open_flags::rw);
                      auto s = co_await f.stat();
                      // convert to time_point
                      auto sec = std::chrono::seconds(s.st_atim.tv_sec);
                      auto nan = std::chrono::nanoseconds(s.st_atim.tv_nsec);
                      auto ts = sec + nan;
                      auto d = std::chrono::duration_cast<
                        ss::lowres_clock::duration>(ts);
                      auto t = ss::lowres_clock::time_point(d);
                      files.push_back(
                        {t, ent.name, static_cast<uint64_t>(s.st_size)});
                  } else if (
                    ent.type
                    && ent.type == ss::directory_entry_type::directory) {
                      vlog(cst_log.debug, "Dir found {}", path);
                      dirlist.push_front(path.string());
                  }
                  co_return;
              });
            co_await sub.done().finally(
              [dir]() mutable { return dir.close(); });
        }
    }
};

} // namespace cloud_storage
