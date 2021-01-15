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

#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <boost/property_tree/ptree_fwd.hpp>

#include <iterator>

namespace arch {

using remote_segment_name
  = named_type<ss::sstring, struct arch_remote_segment_name_t>;

class manifest {
    using ptree = boost::property_tree::ptree;

public:
    using value = remote_segment_name;
    using segments_set = std::set<value>;
    using const_iterator = segments_set::const_iterator;

    /// Create empty manifest that supposed to be updated later
    manifest();
    /// Create manifest for specific ntp
    explicit manifest(model::ntp ntp);

    /// Manifest object name in S3
    ss::sstring get_object_name() const;

    /// Get NTP
    model::ntp get_ntp() const;

    /// Return iterator to the begining(end) of the segments list
    const_iterator begin() const;
    const_iterator end() const;
    size_t size() const;

    /// Check if the manifest contains particular segment
    bool contains(const value& obj) const;

    /// Add new segment to the manifest
    bool add(const value& obj);

    /// Get insert iterator for segments set
    std::insert_iterator<segments_set> get_insert_iterator();

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
    ss::input_stream<char> serialize() const;

    /// Serialize manifest object
    ///
    /// \param out output stream that should be used to output the json
    /// \param pretty pretty print json
    void serialize(std::ostream& out, bool pretty = false) const;

    /// Compare two manifests for equality
    bool operator==(const manifest& other) const;

private:
    /// Update manifest content from ptree object that supposed to be generated
    /// from manifest.json file
    void update(const ptree& m);

    model::ntp _ntp;
    segments_set _objects;
};

} // namespace arch
