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

#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"
#include "storage/ntp_config.h"

#include <boost/property_tree/ptree_fwd.hpp>

#include <iterator>

namespace arch {

/// Segment file name without working directory,
/// expected format: <base-offset>-<term-id>-<revision>.log
using segment_name = named_type<ss::sstring, struct arch_segment_name_t>;
/// Segment path in S3, expected format:
/// <prefix>/<ns>/<topic>/<part-id>_<rev>/<base-offset>-<term-id>-<revision>.log
using remote_segment_path
  = named_type<ss::sstring, struct arch_remote_segment_path_t>;
using remote_manifest_path
  = named_type<ss::sstring, struct arch_remote_manifest_path_t>;
/// Local segment path, expected format:
/// <work-dir>/<ns>/<topic>/<part-id>_<rev>/<base-offset>-<term-id>-<revision>.log
using local_segment_path
  = named_type<ss::sstring, struct arch_local_segment_path_t>;

/// Manifest file stored in S3
class manifest final {
    using ptree = boost::property_tree::ptree;

public:
    struct segment_meta {
        bool is_compacted;
        size_t size_bytes;
        model::offset base_offset;
        model::offset committed_offset;
        /// Set to true if the file was deleted
        bool is_deleted_locally; // NOTE: because S3 doesn't delete files
                                 // immediately we will still be able to read
                                 // the deleted file for a while, to prevent
                                 // confusion we mark such file as deleted in
                                 // manifest in order for GC alg. to remove the
                                 // record eventually

        bool operator==(const segment_meta& other) const;
        bool operator<(const segment_meta& other) const;
    };
    using key = segment_name;
    using value = segment_meta;
    using segments_map = std::map<key, value>;
    using const_iterator = segments_map::const_iterator;

    /// Create empty manifest that supposed to be updated later
    manifest();

    /// Create manifest for specific ntp
    explicit manifest(model::ntp ntp, model::revision_id rev);

    /// Manifest object name in S3
    remote_manifest_path get_manifest_path() const;

    /// Segment file name in S3
    remote_segment_path get_remote_segment_path(const segment_name& name) const;

    /// Get NTP
    model::ntp get_ntp() const;

    /// Get revision
    model::revision_id get_revision_id() const;

    /// Return iterator to the begining(end) of the segments list
    const_iterator begin() const;
    const_iterator end() const;
    size_t size() const;

    /// Check if the manifest contains particular segment
    bool contains(const segment_name& obj) const;

    /// Add new segment to the manifest
    bool add(const segment_name& key, const segment_meta& meta);

    /// Get segment if available or nullopt
    std::optional<std::reference_wrapper<const segment_meta>>
    get(const segment_name& key) const;

    /// Get insert iterator for segments set
    std::insert_iterator<segments_map> get_insert_iterator();

    /// Return new manifest that contains only those segments that present
    /// in local manifest and not found in 'remote_set'.
    ///
    /// \param remote_set the manifest to compare to
    /// \return manifest with segments that doesn't present in 'remote_set'
    manifest difference(const manifest& remote_set) const;

    /// Update manifest file from input_stream (remote set)
    ss::future<> update(ss::input_stream<char>&& is);

    /// Serialize manifest object
    ///
    /// \return asynchronous input_stream with the serialized json
    std::tuple<ss::input_stream<char>, size_t> serialize() const;

    /// Serialize manifest object
    ///
    /// \param out output stream that should be used to output the json
    /// \param pretty pretty print json
    void serialize(std::ostream& out, bool pretty = false) const;

    /// Compare two manifests for equality
    bool operator==(const manifest& other) const;

    /// Remove segment record from manifest
    ///
    /// \param name is a segment name
    /// \return true on success, false on failure (no such segment)
    bool delete_permanently(const segment_name& name);

    /// Remove segment as deleted in manifest
    ///
    /// \param name is a segment name
    /// \return true on success, false on failure (no such segment or already
    /// marked)
    bool mark_as_deleted(const segment_name& name);

private:
    /// Update manifest content from ptree object that supposed to be generated
    /// from manifest.json file
    void update(const ptree& m);

    model::ntp _ntp;
    model::revision_id _rev;
    segments_map _segments;
};

} // namespace arch
