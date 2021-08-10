#pragma once

#include "prometheus/prometheus_sanitize.h"
#include "utils/hdr_hist.h"
#include "utils/human.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/future.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/log.hh>

#include <fmt/core.h>
#include <fmt/format.h>

#include <iostream>
#include <string_view>

enum class hist_type {
    latency,
    memory,
    queue_depth,
};

inline std::ostream& operator<<(std::ostream& o, hist_type t) {
    switch (t) {
    case hist_type::latency:
        o << 'L';
        break;
    case hist_type::memory:
        o << 'M';
        break;
    case hist_type::queue_depth:
        o << 'Q';
        break;
    }
    return o;
}

class hist_helper {
    static constexpr unsigned memory_tracking_precision_bits = 4;

public:
    explicit hist_helper(
      std::string_view name, hist_type t = hist_type::latency)
      : _log(fmt::format("h{}-{}-{}", t, name, ss::this_shard_id()))
      , _qlog(fmt::format(
          "h{}-{}.queuedepth-{}",
          hist_type::queue_depth,
          name,
          ss::this_shard_id()))
      , _type(t) {
        _helpers.push_back(this);
        if (!_printing_timer.armed()) {
            using namespace std::chrono_literals;
            _printing_timer.set_callback([] {
                for (auto h : _helpers) {
                    if (h->_type != hist_type::queue_depth) {
                        vlog(h->_log.info, "hist: {}", h->print());
                    }
                    if (h->_queing_hist.has_value()) {
                        vlog(
                          h->_qlog.info, "hist: {}", h->print_queuing_hist());
                    }
                }
            });
            _reset_timer.set_callback([] {
                for (auto h : _helpers) {
                    h->_hist = hdr_hist{};
                    vlog(h->_log.info, "hist: clear");
                }
            });
            _printing_timer.arm_periodic(5s);
            _reset_timer.arm_periodic(60s);
        }
    }

    template<typename T>
    auto measure(T&& t) {
        vassert(
          _type == hist_type::latency,
          "this hist_helper shouldn't be used to track latency");
        _incnt++;
        return t.then([this, m = _hist.auto_measure()](auto t) {
            _outcnt++;
            auto d = _incnt - _outcnt;
            if (_queing_hist || d) {
                // only record second histogram if we saw queue filled up
                // at any moment
                if (!_queing_hist.has_value()) {
                    _queing_hist.emplace();
                }
                _queing_hist->record(d);
            }
            return t;
        });
    }

    auto measure(ss::future<>&& f) {
        vassert(
          _type == hist_type::latency,
          "this hist_helper shouldn't be used to track latency");
        _incnt++;
        return f.then([this, m = _hist.auto_measure()] {
            _outcnt++;
            auto d = _incnt - _outcnt;
            if (_queing_hist || d) {
                // only record second histogram if we saw queue filled up
                // at any moment
                if (!_queing_hist.has_value()) {
                    _queing_hist.emplace();
                }
                _queing_hist->record(d);
            }
        });
    }

    void record(uint64_t value) {
        if (_type == hist_type::memory) {
            value = value >> memory_tracking_precision_bits;
        }
        _hist.record(value);
    }

    hdr_hist& get_hist() { return _hist; }

private:
    ss::sstring print() {
        fmt::memory_buffer buf;
        constexpr std::string_view fmt_str
          = "{{p10={},p50={},p90={},p99={},p999={},max={},cnt={}}}";
        switch (_type) {
        case hist_type::latency:
            fmt::format_to(
              buf,
              fmt_str,
              human::latency{(double)_hist.get_value_at(10.0)},
              human::latency{(double)_hist.get_value_at(50.0)},
              human::latency{(double)_hist.get_value_at(90.0)},
              human::latency{(double)_hist.get_value_at(99.0)},
              human::latency{(double)_hist.get_value_at(99.9)},
              human::latency{(double)_hist.get_value_at(100.)},
              _hist.get_sample_count());
            break;
        case hist_type::memory:
            fmt::format_to(
              buf,
              fmt_str,
              human::bytes{(
                double)(_hist.get_value_at(10.0) << memory_tracking_precision_bits)},
              human::bytes{(
                double)(_hist.get_value_at(50.0) << memory_tracking_precision_bits)},
              human::bytes{(
                double)(_hist.get_value_at(90.0) << memory_tracking_precision_bits)},
              human::bytes{(
                double)(_hist.get_value_at(99.0) << memory_tracking_precision_bits)},
              human::bytes{(
                double)(_hist.get_value_at(99.9) << memory_tracking_precision_bits)},
              human::bytes{(
                double)(_hist.get_value_at(100.) << memory_tracking_precision_bits)},
              _hist.get_sample_count());
            break;
        case hist_type::queue_depth:
            break;
        }
        return ss::sstring(buf.data(), buf.size());
    }

    ss::sstring print_queuing_hist() {
        fmt::memory_buffer buf;
        constexpr std::string_view fmt_str
          = "{{p10={},p50={},p90={},p99={},p999={},max={},cnt={}}}";
        fmt::format_to(
          buf,
          fmt_str,
          _queing_hist->get_value_at(10.0),
          _queing_hist->get_value_at(50.0),
          _queing_hist->get_value_at(90.0),
          _queing_hist->get_value_at(99.0),
          _queing_hist->get_value_at(99.9),
          _queing_hist->get_value_at(100.),
          _queing_hist->get_sample_count());
        return ss::sstring(buf.data(), buf.size());
    }

    hdr_hist _hist;

    std::optional<hdr_hist> _queing_hist;
    uint64_t _incnt{0};
    uint64_t _outcnt{0};

    static thread_local ss::timer<> _printing_timer;
    static thread_local ss::timer<> _reset_timer;
    ss::logger _log;
    ss::logger _qlog;
    ss::metrics::metric_groups _metrics;
    static thread_local std::vector<hist_helper*> _helpers;
    hist_type _type;
};
