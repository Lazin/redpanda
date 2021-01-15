#include "arch/service.h"
#include "arch/logger.h"

namespace arch {

ntp_archiver::ntp_archiver(model::ntp ntp, const s3::configuration& conf) 
: _ntp(std::move(ntp))
, _client(conf)
{
}

ss::future<> ntp_archiver::start() {
    return ss::with_gate(_gate, [this] {
        vlog(arch_log.info, "starting arhival service for {}", _ntp);
        // TODO: run
    });
}

ss::future<> ntp_archiver::stop() {
    return _gate.close();
}

ss::future<> ntp_archiver::get_remote_manifest() {
    return ../
}

archiver_service::archiver_service(const s3::configuration& conf, const storage::log_manager& lm) {

}

ss::future<> archiver_service::start() {
    // TODO: move to timer cb
    auto ntps = _log_manager.get();
    for (auto ntp: ntps) {
        // TODO: add or keep ntp_archiver
    }
    return ss::now();
}

ss::future<> archiver_service::stop() {
    return ss::now();
}

}  // end namespace arch
