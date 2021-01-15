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
#include "arch/manifest.h"
#include "model/fundamental.h"
#include "s3/client.h"
#include "storage/log_manager.h"
#include "storage/api.h"
#include "cluster/partition_manager.h"
#include <seastar/core/abort_source.hh>

namespace arch {

/// This class performs per-ntp arhcival workload. Every ntp can be
/// processed independently, without the knowledge about others. All
/// 'ntp_archiver' instances that the shard posesses are supposed to be
/// aggregated on a higher level in the 'archiver_service'.
///
/// The 'ntp_archiver' is responsible for manifest manitpulations and
/// generation of per-ntp candidate set. The actual file uploads are
/// handled by 'archiver_service'.
class ntp_archiver {
public:
    /// Create new instance
    ///
    /// \param ntp is an ntp that archiver is responsible for
    /// \param conf is an S3 client configuration
    ntp_archiver(model::ntp ntp, const s3::configuration& conf);

    /// Start archiver
    ss::future<> start();

    /// Stop archiver
    ss::future<> stop();

private:
    ss::future<> get_remote_manifest();
    ss::future<> upload_manifest();

    model::ntp _ntp;
    s3::client _client;
    manifest _local;
    manifest _remote;
    ss::gate _gate;
    ss::abort_source _abort;
};

/*
The idea here is:

archiver_service has a link to log_manager and can get the list of all ntp's managed by this service
it can fetch the list by timer every n-seconds
then it creates a list of ntp_archiver instances (or reuses previously created)

it runs jitted timer and in callback, for every ntp it fetches the list of candidates,
and runs the algorithm provided in RFC

the ntp_archiver is responsible for maintaining manifest (per-ntp) and uploading individual log segments
to be able to do this it should have it's own storage::log::impl reference

we can do dynamic_cast<disk_log_impl>(log->get_impl())->segment_set() to get the list of segments for ntp

-- sharding --
initially no sharding is used, archiver_service uploads all logs that it has, this will work for 1 node
cluster that I can easily test

the next iteration is to use partition_manager to check if the shard is a leader for particular ntp:
partition = partition_manager.get(ntp)
partition.is_leader()

every log_manager on a node will have unique set of ntp, but for some of them it might not be a leader (some
other node in the cluste will be a leader)
we only need to upload logs for which partion.is_leader() is true, so the algorithm is the same, we will
only filter out some ntps from upload process

the ntp sharding on a node is done by using jump hash, the leader2ntp mapping is based on raft

-- big picture --

archiver_service is a service initialized in application.cc
it depends on storage::api and partition_manager services
it runs a timer(s) to do the housekeeping

ntp_service is a component of the archiver_service responsible for individual
ntp. it can participate in algorihm described in RFC (maintain manifest and upload
file)

*/

/// Per-shard archiver service
class archiver_service {
public:
    archiver_service(const s3::configuration& conf, const storage::log_manager& lm);

    /// Start archiver
    ss::future<> start();

    /// Stop archiver
    ss::future<> stop();
private:
    // ss::sharded<cluster::partition_manager>& _partition_manager;
    const storage::log_manager& _log_manager;
    simple_time_jitter<ss::lowres_clock> _jitter;
    ss::timer<ss::lowres_clock> _timer;
};


} // end namespace arch
