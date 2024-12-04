// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/interfaces/cluster_partition_manager.h"
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>

#include <gmock/gmock.h>

#include <exception>

class partition_mock final
  : public experimental::cloud_topics::cluster_partition_api {
public:
    MOCK_METHOD(
      ss::future<fragmented_vector<model::tx_range>>,
      aborted_transactions,
      (model::offset base, model::offset last),
      (const override));

    MOCK_METHOD(
      ss::future<model::record_batch_reader>,
      make_reader,
      (storage::log_reader_config config,
       std::optional<model::timeout_clock::time_point> debounce_deadline),
      (override));
};

class partition_manager_mock
  : public experimental::cloud_topics::cluster_partition_manager_api {
public:
    MOCK_METHOD(
      std::unique_ptr<experimental::cloud_topics::cluster_partition_api>,
      get_partition,
      (const model::ntp&),
      (override));
};
