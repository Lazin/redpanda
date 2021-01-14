#include "arch/manifest.h"

#include "arch/logger.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_istreambuf.h"
#include "bytes/iobuf_ostreambuf.h"
#include "model/fundamental.h"

#include <seastar/core/coroutine.hh>

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <algorithm>

namespace arch {

manifest::manifest()
  : _ntp() {}

manifest::manifest(model::ntp ntp)
  : _ntp(std::move(ntp)) {}

ss::sstring manifest::get_object_name() const {
    return fmt::format(
      "redpanda/meta/{}/{}/{}/manifest.json",
      _ntp.ns(),
      _ntp.tp.topic(),
      _ntp.tp.partition());
}

model::ntp manifest::get_ntp() const { return _ntp; }

manifest::const_iterator manifest::begin() const { return _objects.begin(); }

manifest::const_iterator manifest::end() const { return _objects.end(); }

bool manifest::contains(const manifest::value& obj) const {
    return _objects.count(obj) != 0;
}

bool manifest::add(const manifest::value& obj) {
    auto [it, ok] = _objects.insert(obj);
    return ok;
}

std::insert_iterator<manifest::segments_set> manifest::get_insert_iterator() {
    return std::inserter(_objects, _objects.begin());
}

manifest manifest::difference(const manifest& remote_set) const {
    manifest result(_ntp);
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
    auto os = make_iobuf_output_stream(result);
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
    auto ns = model::ns(m.get<ss::sstring>("ns"));
    auto tp = model::topic(m.get<ss::sstring>("tp"));
    auto pt = model::partition_id(m.get<int32_t>("pt"));
    _ntp = model::ntp(ns, tp, pt);
    if (m.count("segments") != 0) {
        for (const ptree::value_type& it : m.get_child("segments")) {
            _objects.insert(remote_segment_name(it.first.data()));
        }
    }
}

ss::input_stream<char> manifest::serialize() const {
    iobuf serialized;
    iobuf_ostreambuf obuf(serialized);
    std::ostream os(&obuf);
    serialize(os, false);
    return make_iobuf_input_stream(std::move(serialized));
}

void manifest::serialize(std::ostream& out, bool pretty) const {
    ptree m;
    m.put("ns", _ntp.ns());
    m.put("tp", _ntp.tp.topic());
    m.put("pt", _ntp.tp.partition());
    ptree segments;
    for (const auto& sn : _objects) {
        ptree item;
        segments.push_back(std::make_pair(sn(), std::move(item)));
    }
    m.add_child("segments", segments);
    boost::property_tree::write_json(out, m, pretty);
}

bool manifest::operator == (const manifest& other) const {
    return _ntp == other._ntp && _objects == other._objects;
}

} // namespace arch