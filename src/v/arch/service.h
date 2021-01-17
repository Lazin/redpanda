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
#include "arch/manifest.h"
#include "cluster/partition_manager.h"
#include "model/fundamental.h"
#include "s3/client.h"
#include "storage/api.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"
#include "storage/segment.h"
#include "storage/segment_set.h"

#include <seastar/core/abort_source.hh>

namespace arch {

/*
The idea here is:

archiver_service has a link to log_manager and can get the list of all ntp's
managed by this service it can fetch the list by timer every n-seconds then it
creates a list of ntp_archiver instances (or reuses previously created)

it runs jitted timer and in callback, for every ntp it fetches the list of
candidates, and runs the algorithm provided in RFC

the ntp_archiver is responsible for maintaining manifest (per-ntp) and uploading
individual log segments to be able to do this it should have it's own
storage::log::impl reference

we can do dynamic_cast<disk_log_impl>(log->get_impl())->segment_set() to get the
list of segments for ntp

-- sharding --
initially no sharding is used, archiver_service uploads all logs that it has,
this will work for 1 node cluster that I can easily test

the next iteration is to use partition_manager to check if the shard is a leader
for particular ntp: partition = partition_manager.get(ntp) partition.is_leader()

every log_manager on a node will have unique set of ntp, but for some of them it
might not be a leader (some other node in the cluste will be a leader) we only
need to upload logs for which partion.is_leader() is true, so the algorithm is
the same, we will only filter out some ntps from upload process

the ntp sharding on a node is done by using jump hash, the leader2ntp mapping is
based on raft

-- big picture --

archiver_service is a service initialized in application.cc
it depends on storage::api and partition_manager services
it runs a timer(s) to do the housekeeping

ntp_service is a component of the archiver_service responsible for individual
ntp. it can participate in algorihm described in RFC (maintain manifest and
upload file)

*/

/// Shard-local archiver service
class archiver_service {
public:
    archiver_service(
      const s3::configuration& conf,
      const storage::log_manager& lm,
      const cluster::partition_manager& pm);

    /// Start archiver
    ss::future<> start();

    /// Stop archiver
    ss::future<> stop();

private:
    const cluster::partition_manager& _partition_manager;
    const storage::log_manager& _log_manager;
    simple_time_jitter<ss::lowres_clock> _jitter;
    ss::timer<ss::lowres_clock> _timer;
};

} // end namespace arch
