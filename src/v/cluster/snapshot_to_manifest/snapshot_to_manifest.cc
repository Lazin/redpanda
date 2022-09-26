// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/partition_manifest.h"
#include "cluster/archival_metadata_stm.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"
#include "storage/ntp_config.h"
#include "storage/snapshot.h"
#include "syschecks/syschecks.h"
#include "utils/string_switch.h"
#include "vlog.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>

#include <exception>
#include <stdexcept>
#include <string>

static ss::logger logger{"main"};

enum class operation_type {
    generate_manifest,
    generate_snapshot,
    get_insync_offset,
};

void cli_opts(boost::program_options::options_description_easy_init opt) {
    namespace po = boost::program_options;

    opt(
      "operation",
      po::value<ss::sstring>()->default_value(ss::sstring("generate_manifest")),
      "Type of the operation");

    opt(
      "snapshot",
      po::value<ss::sstring>()->default_value(
        ss::sstring("archival_metadata.snapshot")),
      "Input file name");

    opt(
      "manifest",
      po::value<ss::sstring>()->default_value(ss::sstring("manifest.json")),
      "Output manifest file");

    opt(
      "namespace",
      po::value<ss::sstring>()->default_value(ss::sstring("kafka")),
      "Namespace name");

    opt("topic", po::value<ss::sstring>(), "Topic name");

    opt("partition", po::value<uint32_t>(), "Partition id");

    opt("revision", po::value<uint32_t>(), "Revision id");

    opt("insync", po::value<uint32_t>(), "Snapshot in-sync offset");
}

struct app_conf {
    operation_type type;
    ss::sstring snapshot_path;
    ss::sstring manifest_path;
    model::ntp ntp;
    model::initial_revision_id rev;
    model::offset insync_offset;
};

static operation_type parse_operation_type(ss::sstring op) {
    return string_switch<operation_type>(op)
      .match("manifest", operation_type::generate_manifest)
      .match("snapshot", operation_type::generate_snapshot)
      .match("print", operation_type::get_insync_offset);
}

app_conf cfg_from(boost::program_options::variables_map& m) {
    return app_conf{
      .type = parse_operation_type(m["operation"].as<ss::sstring>()),
      .snapshot_path = m["snapshot"].as<ss::sstring>(),
      .manifest_path = m["manifest"].as<ss::sstring>(),
      .ntp = model::ntp(
        model::ns(m["namespace"].as<ss::sstring>()),
        model::topic(m["topic"].as<ss::sstring>()),
        model::partition_id(m["partition"].as<uint32_t>())),
      .rev = model::initial_revision_id(m["revision"].as<uint32_t>())};
}

namespace cluster::details {
class archival_metadata_stm_accessor {
public:
    static cloud_storage::partition_manifest load_manifest(
      ss::sstring snapshot_path,
      model::ntp ntp,
      model::initial_revision_id rev) {
        auto path = std::filesystem::path(snapshot_path);
        auto file = path.filename().string();
        auto dir = path.remove_filename();
        storage::simple_snapshot_manager manager(
          dir, file, ss::default_priority_class());
        auto opt_reader = manager.open_snapshot().get();
        if (!opt_reader.has_value()) {
            vlog(logger.error, "can't open snapshot file");
            throw std::runtime_error("can't open snapshot file");
        }
        auto& reader = *opt_reader;
        iobuf meta_buf = reader.read_metadata().get();
        iobuf_parser meta_parser(std::move(meta_buf));
        auto version = reflection::adl<int8_t>{}.from(meta_parser);
        vlog(logger.info, "snapshot version {}", version);

        if (version == 0) {
            vlog(logger.warn, "can't load snapshot, old format");
            throw std::runtime_error("can't load snapshot, old format");
        }

        auto hdr_offset = reflection::adl<int64_t>{}.from(meta_parser);
        auto hdr_version = reflection::adl<int8_t>{}.from(meta_parser);
        auto hdr_snapshot_size = reflection::adl<int32_t>{}.from(meta_parser);
        vlog(
          logger.info,
          "snapshot offset {}, version {}, size {}",
          hdr_offset,
          hdr_version,
          hdr_snapshot_size);
        auto data = read_iobuf_exactly(reader.input(), hdr_snapshot_size).get();
        reader.close().get();

        return archival_metadata_stm::manifest_from_snapshot_data(
          std::move(data), std::move(ntp), rev);
    }

    static model::offset get_insync_offset(ss::sstring snapshot_path) {
        auto path = std::filesystem::path(snapshot_path);
        auto file = path.filename().string();
        auto dir = path.remove_filename();
        storage::simple_snapshot_manager manager(
          dir, file, ss::default_priority_class());
        auto opt_reader = manager.open_snapshot().get();
        if (!opt_reader.has_value()) {
            vlog(logger.error, "can't open snapshot file");
            throw std::runtime_error("can't open snapshot file");
        }
        auto& reader = *opt_reader;
        iobuf meta_buf = reader.read_metadata().get();
        iobuf_parser meta_parser(std::move(meta_buf));
        auto version = reflection::adl<int8_t>{}.from(meta_parser);
        vlog(logger.info, "snapshot version {}", version);

        if (version == 0) {
            vlog(logger.warn, "can't load snapshot, old format");
            throw std::runtime_error("can't load snapshot, old format");
        }

        auto hdr_offset = reflection::adl<int64_t>{}.from(meta_parser);
        auto hdr_version = reflection::adl<int8_t>{}.from(meta_parser);
        auto hdr_snapshot_size = reflection::adl<int32_t>{}.from(meta_parser);
        vlog(
          logger.debug,
          "stm header offset {}, version {}, size {}",
          hdr_offset,
          hdr_version,
          hdr_snapshot_size);
        return model::offset(hdr_offset);
    }
};
} // namespace cluster::details

static void generate_manifest_from_snapshot(app_conf cfg) {
    vlog(
      logger.info,
      "input: {}, output: {}",
      cfg.snapshot_path,
      cfg.manifest_path);
    auto manifest
      = cluster::details::archival_metadata_stm_accessor::load_manifest(
        cfg.snapshot_path, cfg.ntp, cfg.rev);

    // Serializing the manifest
    auto [is, size] = manifest.serialize();
    ss::file outf = ss::open_file_dma(
                      cfg.manifest_path,
                      ss::open_flags::create | ss::open_flags::rw)
                      .get();
    auto os = ss::make_file_output_stream(outf).get();
    ss::copy(is, os).get();
    os.flush().get();
    os.close().get();
    is.close().get();
    vlog(logger.info, "done");
}

static void generate_snapshot_from_manifest(app_conf cfg) {
    vlog(
      logger.info,
      "input: {}, output: {}",
      cfg.snapshot_path,
      cfg.manifest_path);
    // Load manifest
    auto file = ss::open_file_dma(cfg.snapshot_path, ss::open_flags::ro).get();
    cloud_storage::partition_manifest manifest(cfg.ntp, cfg.rev);
    manifest.update(ss::make_file_input_stream(file)).get();
    file.close().get();
    //
    auto path = std::filesystem::path(cfg.manifest_path);
    auto fname = path.filename().string();
    auto dir = path.remove_filename();

    storage::ntp_config ntpc(cfg.ntp, dir.string());

    cluster::archival_metadata_stm::make_snapshot(
      ntpc, manifest, cfg.insync_offset)
      .get();
}

static void dump_snapshot_information(app_conf cfg) {
    vlog(
      logger.info,
      "input: {}, output: {}",
      cfg.snapshot_path,
      cfg.manifest_path);
    auto offset
      = cluster::details::archival_metadata_stm_accessor::get_insync_offset(
        cfg.snapshot_path);

    vlog(logger.info, "in-sync offset: {}", offset);
    // Serializing the manifest
    vlog(logger.info, "done");
}

int main(int args, char** argv, char** env) {
    syschecks::initialize_intrinsics();
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
    ss::app_template app;
    cli_opts(app.add_options());
    return app.run(args, argv, [&] {
        vlog(logger.info, "starting");
        auto cfg = cfg_from(app.configuration());
        return ss::async([cfg] {
            switch (cfg.type) {
            case operation_type::generate_manifest:
                generate_manifest_from_snapshot(cfg);
                break;
            case operation_type::generate_snapshot:
                generate_snapshot_from_manifest(cfg);
                break;
            case operation_type::get_insync_offset:
                dump_snapshot_information(cfg);
                break;
            };
        });
    });
}
