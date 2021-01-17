/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "arch/manifest.h"

#include "arch/error.h"
#include "arch/logger.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_istreambuf.h"
#include "bytes/iobuf_ostreambuf.h"
#include "hashing/murmur.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "storage/ntp_config.h"

#include <seastar/core/coroutine.hh>

#include <boost/algorithm/string.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <algorithm>
#include <array>

namespace arch {

manifest::manifest()
  : _ntp()
  , _rev() {}

manifest::manifest(model::ntp ntp, model::revision_id rev)
  : _ntp(std::move(ntp))
  , _rev(rev) {}

remote_manifest_path manifest::get_manifest_path() const {
    constexpr uint32_t bitmask = 0xF0000000;
    auto path = fmt::format("{}_{}", _ntp.path(), _rev());
    uint32_t hash = bitmask & murmurhash3_x86_32(path.data(), path.size());
    return remote_manifest_path(fmt::format(
      "{:08x}/meta/{}_{}/manifest.json", hash, _ntp.path(), _rev()));
}

remote_segment_path
manifest::get_remote_segment_path(const segment_name& name) const {
    auto path = fmt::format("{}_{}/{}", _ntp.path(), _rev(), name());
    uint32_t hash = murmurhash3_x86_32(path.data(), path.size());
    return remote_segment_path(fmt::format("{:08x}/{}", hash, path));
}

model::ntp manifest::get_ntp() const { return _ntp; }

model::revision_id manifest::get_revision_id() const { return _rev; }

manifest::const_iterator manifest::begin() const { return _segments.begin(); }

manifest::const_iterator manifest::end() const { return _segments.end(); }

size_t manifest::size() const { return _segments.size(); }

bool manifest::contains(const segment_name& obj) const {
    return _segments.count(obj) != 0;
}

bool manifest::add(const segment_name& key, const segment_meta& meta) {
    auto [it, ok] = _segments.insert(std::make_pair(key, meta));
    return ok;
}

std::optional<std::reference_wrapper<const manifest::segment_meta>>
manifest::get(const segment_name& key) const {
    auto it = _segments.find(key);
    if (it == _segments.end()) {
        return std::nullopt;
    }
    return std::cref(it->second);
}

std::insert_iterator<manifest::segments_map> manifest::get_insert_iterator() {
    return std::inserter(_segments, _segments.begin());
}

manifest manifest::difference(const manifest& remote_set) const {
    if (_ntp != remote_set._ntp && _rev != remote_set._rev) {
        throw manifest_error("NTPs doesn't match");
    }
    manifest result(_ntp, _rev);
    std::set_difference(
      begin(),
      end(),
      remote_set.begin(),
      remote_set.end(),
      result.get_insert_iterator());
    return result;
}

ss::future<> manifest::update(ss::input_stream<char>&& is) {
    iobuf result;
    auto os = make_iobuf_ref_output_stream(result);
    co_await ss::copy(is, os);
    iobuf_istreambuf ibuf(result);
    std::istream stream(&ibuf);
    ptree m;
    boost::property_tree::json_parser::read_json(stream, m);
    update(m);
    co_return;
}

void manifest::update(const manifest::ptree& m) {
    // ntp from json
    auto ns = model::ns(m.get<ss::sstring>("namespace"));
    auto tp = model::topic(m.get<ss::sstring>("topic"));
    auto pt = model::partition_id(m.get<int32_t>("partition"));
    _rev = model::revision_id(m.get<int64_t>("revision"));
    _ntp = model::ntp(ns, tp, pt);
    segments_map tmp;
    if (m.count("segments") != 0) {
        for (const ptree::value_type& it : m.get_child("segments")) {
            auto coffs = it.second.get<int64_t>("committed_offset");
            auto boffs = it.second.get<int64_t>("base_offset");
            segment_meta meta{
              .is_compacted = it.second.get<bool>("is_compacted"),
              .size_bytes = it.second.get<size_t>("size_bytes"),
              .base_offset = model::offset(boffs),
              .committed_offset = model::offset(coffs),
              .is_deleted_locally = it.second.get<bool>("deleted"),
            };
            tmp.insert(std::make_pair(segment_name(it.first.data()), meta));
        }
    }
    std::swap(tmp, _segments);
}

std::tuple<ss::input_stream<char>, size_t> manifest::serialize() const {
    iobuf serialized;
    iobuf_ostreambuf obuf(serialized);
    std::ostream os(&obuf);
    serialize(os, false);
    size_t size_bytes = serialized.size_bytes();
    return std::make_tuple(
      make_iobuf_input_stream(std::move(serialized)), size_bytes);
}

void manifest::serialize(std::ostream& out, bool pretty) const {
    ptree m;
    m.put("namespace", _ntp.ns());
    m.put("topic", _ntp.tp.topic());
    m.put("partition", _ntp.tp.partition());
    m.put("revision", _rev());
    ptree segments;
    for (const auto& [sn, meta] : _segments) {
        ptree it;
        it.put("is_compacted", meta.is_compacted);
        it.put("size_bytes", meta.size_bytes);
        it.put("committed_offset", meta.committed_offset());
        it.put("base_offset", meta.base_offset());
        it.put("deleted", meta.is_deleted_locally);
        segments.push_back(std::make_pair(sn(), std::move(it)));
    }
    m.add_child("segments", segments);
    boost::property_tree::write_json(out, m, pretty);
}

bool manifest::segment_meta::operator==(const segment_meta& other) const {
    return is_compacted == other.is_compacted && size_bytes == other.size_bytes
           && base_offset == other.base_offset
           && committed_offset == other.committed_offset;
}

bool manifest::delete_permanently(const segment_name& name) {
    auto it = _segments.find(name);
    if (it != _segments.end()) {
        _segments.erase(it);
        return true;
    }
    return false;
}

bool manifest::mark_as_deleted(const segment_name& name) {
    auto it = _segments.find(name);
    if (it != _segments.end() && it->second.is_deleted_locally == false) {
        it->second.is_deleted_locally = true;
        return true;
    }
    return false;
}

bool manifest::segment_meta::operator<(const segment_meta& o) const {
    return std::make_tuple(
             is_compacted, size_bytes, base_offset, committed_offset)
           < std::make_tuple(
             o.is_compacted, o.size_bytes, o.base_offset, o.committed_offset);
}

bool manifest::operator==(const manifest& other) const {
    return _ntp == other._ntp && _rev == other._rev
           && _segments == other._segments;
}

} // namespace arch