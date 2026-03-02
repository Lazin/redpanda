/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "random/generators.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/noncopyable_function.hh>

#include <gmock/gmock.h>

#include <chrono>
#include <exception>
#include <stdexcept>

using namespace std::chrono_literals;

struct remote_mock_download {
    virtual ~remote_mock_download() = default;
    virtual std::pair<iobuf, cloud_io::download_result>
    _do_download_object(cloud_storage_clients::object_key key) = 0;
};

class remote_mock final
  : public cloud_io::remote_api<ss::lowres_clock>
  , public remote_mock_download {
public:
    using reset_input_stream
      = cloud_io::remote_api<ss::lowres_clock>::reset_input_stream;

    MOCK_METHOD(
      ss::future<cloud_io::download_result>,
      object_exists,
      (const cloud_storage_clients::bucket_name&,
       const cloud_storage_clients::object_key&,
       retry_chain_node&,
       std::string_view),
      (override));

    MOCK_METHOD(
      ss::future<cloud_io::upload_result>,
      upload_object,
      (cloud_io::basic_upload_request<ss::lowres_clock>),
      (override));

    MOCK_METHOD(
      ss::future<cloud_io::upload_result>,
      upload_stream,
      (cloud_io::basic_transfer_details<ss::lowres_clock>,
       uint64_t,
       const reset_input_stream&,
       lazy_abort_source&,
       const std::string_view,
       std::optional<size_t>),
      (override));

    MOCK_METHOD(
      ss::future<cloud_io::download_result>,
      download_stream,
      (cloud_io::basic_transfer_details<ss::lowres_clock>,
       const cloud_io::try_consume_stream&,
       const std::string_view,
       bool,
       std::optional<cloud_storage_clients::http_byte_range>,
       std::function<void(size_t)>),
      (override));

    MOCK_METHOD(
      (std::pair<iobuf, cloud_io::download_result>),
      _do_download_object,
      (cloud_storage_clients::object_key key),
      (override));

    ss::future<cloud_io::download_result> download_object(
      cloud_io::basic_download_request<ss::lowres_clock> req) override {
        auto [buf, err] = _do_download_object(req.transfer_details.key);
        req.payload = std::move(buf);
        co_return err;
    }

    void expect_download_object(
      cloud_storage_clients::object_key key,
      cloud_io::download_result res,
      iobuf body) {
        EXPECT_CALL(*this, _do_download_object(std::move(key)))
          .Times(1)
          .WillOnce(::testing::Return(std::make_pair(std::move(body), res)));
    }

    template<class Exception>
    void expect_download_object_throw(
      cloud_storage_clients::object_key key, Exception err) {
        EXPECT_CALL(*this, _do_download_object(std::move(key)))
          .Times(1)
          .WillOnce(::testing::Throw(err));
    }
};
