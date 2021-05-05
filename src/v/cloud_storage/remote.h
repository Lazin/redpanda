/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/manifest.h"
#include "cloud_storage/types.h"
#include "random/simple_time_jitter.h"
#include "s3/client.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>

namespace cloud_storage {

/// \brief Represents remote endpoint
///
/// The `remote` is responsible for remote data
/// transfer and storage. It 'knows' how to upload and
/// download data. Also, it's responsible for maintaining
/// correct naming in S3. The remote takes into account
/// things like reconnects, backpressure and backoff.
class remote {
public:
    /// Functor that returns fresh input_stream object that can be used
    /// to re-upload and will return all data that needs to be uploaded
    using reset_input_stream = std::function<ss::input_stream<char>()>;

    /// \brief Initialize 'remote'
    ///
    /// \param limit is a number of simultaneous connections
    /// \param conf is an S3 configuration
    explicit remote(s3_connection_limit limit, const s3::configuration& conf);

    /// \brief Start the remote
    ss::future<> start();

    /// \brief Stop the remote
    ///
    /// Wait until all background operations complete
    ss::future<> stop();

    /// \brief Download manifest from pre-defined S3 location
    ///
    /// Method downloads the manifest and handles backpressure and
    /// errors. It retries multiple times until timeout excedes.
    /// \param bucket is a bucket name
    /// \param manifest is a manifest to download
    /// \param timeout is a time limit for retrying the download
    /// \return future that returns success code
    ss::future<download_result> download_manifest(
      const s3::bucket_name& bucket,
      base_manifest& manifest,
      ss::lowres_clock::duration timeout);

    /// \brief Upload manifest to the pre-defined S3 location
    ///
    /// \param bucket is a bucket name
    /// \param manifest is a manifest to upload
    /// \param timeout is a time limit for retrying the upload
    /// \return future that returns success code
    ss::future<upload_result> upload_manifest(
      const s3::bucket_name& bucket,
      const base_manifest& manifest,
      ss::lowres_clock::duration timeout);

    /// \brief Upload segment to S3
    ///
    /// The method uploads the segment while tolerating some errors. It can
    /// retry after some errors. 
    /// \param reset_str is a functor that returns an input_stream that returns
    ///                  segment's data
    /// \param exposed_name is a segment's name in S3
    /// \param manifest is a manifest that should have the segment metadata
    /// \param timeout is a time limit for retrying the upload
    ss::future<upload_result> upload_segment(
      const s3::bucket_name& bucket,
      const segment_name& exposed_name,
      uint64_t content_length,
      const reset_input_stream& reset_str,
      manifest& manifest,
      ss::lowres_clock::duration timeout);

    // TBD: download

private:
    s3::client_pool _pool;
    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace cloud_storage