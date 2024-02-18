/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "container/intrusive_list_helpers.h"
#include "io/io_queue.h"

#include <seastar/core/future.hh>

#include <filesystem>

namespace experimental::io {

class persistence;

/**
 * Manager of all file I/O queues.
 *
 * The scheduler tracks and controls the lifecycle of each I/O queue. The role
 * of the scheduler is to implement high-level policies to accompolish goals
 * like avoiding I/O queue starvation, or providing I/O priority.
 *
 * Currently the only policy that is implemented in the scheduler is maintaining
 * a maximum number of open files by limiting the number of I/O queues that are
 * open at any given time.
 */
class scheduler {
public:
    /**
     * The queue is a wrapper around the low-level `io_queue`, and provides
     * hooks and metadata for being managed by the scheduler.
     */
    class queue {
    public:
        /**
         * Construct a new queue.
         */
        queue(
          persistence* storage,
          std::filesystem::path path,
          io_queue::completion_callback_type complete);

        /**
         * Return the path of the backing file.
         */
        [[nodiscard]] const std::filesystem::path& path() const noexcept;

    private:
        friend class scheduler;

        io_queue io_queue_;

        /*
         * all queues are tracked by the scheduler, as well as a cache to select
         * a queue to close in order to enforce the maximum open queue limit.
         */
        intrusive_list_hook sched_hook_;
        intrusive_list_hook cache_hook_;

        /*
         * a queue monitor controls the open/close lifecycle of the I/O queue.
         * see scheduler::monitor implementation for more details.
         */
        seastar::future<> monitor_{seastar::make_ready_future<>()};
        seastar::semaphore sem_{0};
        seastar::semaphore_units<> units_;
        bool stop_{false};
    };

    /**
     * Construct a new scheduler.
     *
     * The scheduler will limit the number of concurrent open queues to a
     * maximum of \p nofiles.
     */
    explicit scheduler(size_t nofiles) noexcept;

    /**
     * Add a queue to the scheduler.
     *
     * The queue must not have been previously added.
     */
    void add_queue(queue*) noexcept;

    /**
     * Remove a queue from the scheduler.
     *
     * The returned future completes when the background monitor has exited.
     */
    static seastar::future<> remove_queue(queue*) noexcept;

    /**
     * Submit an I/O request to a queue.
     */
    void submit_read(queue* queue, page* page) noexcept;
    void submit_write(queue* queue, page* page) noexcept;

private:
    intrusive_list<queue, &queue::sched_hook_> queues_;

    /*
     * maximum open file handling. see implementation of `monitor` for more
     * detailed description.
     */
    size_t waiters_{0};
    seastar::semaphore nofiles_;
    intrusive_list<queue, &queue::cache_hook_> lru_;
    seastar::future<> monitor(queue*) noexcept;
};

} // namespace experimental::io
