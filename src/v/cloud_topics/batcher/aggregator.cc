/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batcher/aggregator.h"
#include "cloud_topics/logger.h"

#include "bytes/iostream.h"
#include "cloud_topics/batcher/serializer.h"
#include "cloud_topics/batcher/write_request.h"
#include "model/record.h"
#include "storage/record_batch_builder.h"

namespace cloud_topics::details {

template<class Clock>
aggregator<Clock>::aggregator(object_id id)
  : _id(id) {}

template<class Clock>
aggregator<Clock>::~aggregator() {
    if (!_staging.empty()) {
        for (auto& [key, list] : _staging) {
            std::ignore = key;
            for (auto& req : list) {
                // TODO: revisit error code used here
                req.set_value(errc::unexpected_failure);
            }
        }
    }
}

template<class Clock>
struct prepared_placeholder_batches {
    const object_id id;
    chunked_vector<std::unique_ptr<batches_for_req<Clock>>> placeholders;
    uint64_t size_bytes{0};
};

namespace {
/// Convert multiple chunk elements into placeholder batches
///
/// Byte offsets in the chunk are zero based. Because we're
/// concatenating multiple chunks the offset has to be corrected.
/// This is done using the `base_byte_offset` parameter.
template<class Clock>
void make_dl_placeholder_batches(
  prepared_placeholder_batches<Clock>& ctx,
  write_request<Clock>& req,
  const serialized_chunk& chunk) {
    auto result = std::make_unique<batches_for_req<Clock>>();
    vlog(cd_log.info, "NEEDLE");
    for (const auto& b : chunk.batches) {
        dl_placeholder placeholder{
          .id = ctx.id,
          .offset = first_byte_offset_t(ctx.size_bytes),
          .size_bytes = byte_range_size_t(b.size_bytes),
        };
    vlog(cd_log.info, "NEEDLE");

        storage::record_batch_builder builder(
          model::record_batch_type::
            version_fence /*TODO: use dl_placeholder batch type*/,
          b.base);

    vlog(cd_log.info, "NEEDLE");
        // TX data (producer id, control flag) are not copied from 'src' yet.

        // Put the payload
        builder.add_raw_kv(
          serde::to_iobuf(dl_placeholder_record_key::payload),
          serde::to_iobuf(placeholder));

    vlog(cd_log.info, "NEEDLE");
        // TODO: fix this
        for (int i = 1; i < b.num_records - 1; i++) {
            iobuf empty;
            builder.add_raw_kv(
              serde::to_iobuf(dl_placeholder_record_key::empty),
              std::move(empty));
        }
    vlog(cd_log.info, "NEEDLE");
        result->placeholders.push_back(std::move(builder).build());
        ctx.size_bytes += b.size_bytes;
    vlog(cd_log.info, "NEEDLE");
    }
    vlog(cd_log.info, "NEEDLE");
    req._hook.unlink();
    vlog(cd_log.info, "NEEDLE");
    result->ref.push_back(req);
    vlog(cd_log.info, "NEEDLE");
    ctx.placeholders.push_back(std::move(result));
    vlog(cd_log.info, "NEEDLE");
}
} // namespace

template<class Clock>
chunked_vector<std::unique_ptr<batches_for_req<Clock>>> aggregator<Clock>::get_placeholders() {
    prepared_placeholder_batches<Clock> ctx{
      .id = _id,
    };
    for (auto& [key, list] : _staging) {
    vlog(cd_log.info, "NEEDLE {}", key);
        for (auto& req : list) {
    vlog(cd_log.info, "NEEDLE {} {}", req.ntp, req.index);
            vassert(
              !req.data_chunk.payload.empty(),
              "Empty write request for ntp: {}",
              key);
            make_dl_placeholder_batches(ctx, req, req.data_chunk);
        }
    }
    vlog(cd_log.info, "NEEDLE");
    return std::move(ctx.placeholders);
}

template<class Clock>
ss::input_stream<char> aggregator<Clock>::get_stream() {
    iobuf concat;
    for (auto& p: _aggregated) {
        concat.append(std::move(p->ref.back().data_chunk.payload));
    }
    // NOTE(Evgeny): if the delimiter or header is added
    // the 'size_bytes' and 'get_placeholders' methods
    // should also be updated.
    vassert(
      _size_bytes == concat.size_bytes(),
      "Serialized size {} doesn't match the expected value {}",
      concat.size_bytes(),
      _size_bytes);
    return make_iobuf_input_stream(std::move(concat));
}

template<class Clock>
ss::input_stream<char> aggregator<Clock>::prepare() {
    // Move data from staging to aggregated
    _aggregated = get_placeholders();
    // Produce input stream
    return get_stream();
}

template<class Clock>
void aggregator<Clock>::ack() {
    for (auto& p: _aggregated) {
        p->ref.back().set_value(std::move(p->placeholders));
    }
}

template<class Clock>
void aggregator<Clock>::ack_error(errc e) {
    for (auto& p: _aggregated) {
        p->ref.back().set_value(e);
    }
}

template<class Clock>
void aggregator<Clock>::add(write_request_list<Clock>& pending) {
    while (!pending.empty()) {
        auto& req = pending.back();
        auto it = _staging.find(req.ntp);
        if (it == _staging.end()) {
            _staging.insert(std::make_pair(req.ntp, write_request_list<Clock>()));
            it = _staging.find(req.ntp);
            vassert(it != _staging.end(), "Write request can't be inserted");
        }
        req._hook.unlink();
        it->second.push_back(req);
        _size_bytes += req.size_bytes();
    }
}

template<class Clock>
size_t aggregator<Clock>::size_bytes() const noexcept {
    return _size_bytes;
}

template class aggregator<ss::lowres_clock>;
template class aggregator<ss::manual_clock>;
} // namespace cloud_topics::details
