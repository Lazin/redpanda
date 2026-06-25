# supervisor_stm Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `persisted_stm`-derived `supervisor_stm` that creates and removes other STMs on a partition at runtime via replicated commands, with children becoming first-class participants in `raft::state_machine_manager` and the storage `stm_hookset`.

**Architecture:** Children are registered in two places — `state_machine_manager::_machines` (apply loop + `managed_snapshot`) and `storage::stm_hookset::_stms` (log-truncation floor). CREATE is enacted **synchronously** inside the supervisor's `do_apply` (pins the truncation floor with no gap); REMOVE teardown is **deferred** to a drain step at the top of `try_apply_in_foreground`. Recovery is driven by the supervisor (source of truth): restart reconstruction after static STMs start, and a supervisor-first two-phase Raft snapshot install.

**Tech Stack:** C++23, Seastar (coroutines/futures), Bazel/Bazelisk, GoogleTest (`redpanda_cc_gtest`), serde envelopes, the `raft_fixture` multi-node test harness.

**Design doc:** `docs/rfcs/20260625_supervisor_stm.md`

## Global Constraints

- C++23; `ss::` prefix for Seastar types; snake_case identifiers; CamelCase concepts.
- Use `vassert`/`dassert` for assertions, `vlog` for logging. No new `operator<<`.
- Do not call `future::get_exception()` inside a `vlog`/`vassert` argument; bind it to a variable first.
- Format with `bazel run //tools:clang_format` before every commit.
- Build: `bazel build //src/v/...`. Tests via `bazel test //...` targets shown per task.
- PRs target `dev`; this work is on branch `exp/hierarchical-stms`.
- Commit sign-off: `Signed-off-by: Evgeny Lazin <4lazin@gmail.com>`, no `Co-Authored-By`.
- At most one child per type per partition; a child STM must derive from `raft::persisted_stm` (so it is a `storage::snapshotable_stm`).
- Invariant: after a REMOVE command for a type, no further commands for that type are written to the log.

## File Structure

**New**
- `src/v/cluster/supervisor_stm.h` / `.cc` — the STM: command codec, active-set local snapshot, CREATE/REMOVE apply, restart + Raft-snapshot reconciliation, `supervisor_stm_factory`.
- `src/v/cluster/tests/supervisor_stm_test.cc` — end-to-end tests via `raft_fixture` + a test creatable factory.
- `src/v/raft/tests/stm_dynamic_membership_test.cc` — manager-level dynamic membership tests using test-only STMs.

**Modified**
- `src/v/model/record_batch_types.h` / `record_batch_types.cc` — new `supervisor_stm_command` batch type.
- `src/v/raft/state_machine_base.h` — `provides_dynamic_membership()` + `prepare_created_at()` virtuals.
- `src/v/raft/persisted_stm.h` / `.cc` — `write_empty_snapshot_at()` + `prepare_created_at()` override.
- `src/v/storage/types.h` / `types.cc` — `stm_hookset::remove_stm()`.
- `src/v/raft/state_machine_manager.h` / `.cc` — apply-context register, removal drain, restart reconstruction, supervisor-first snapshot install.
- `src/v/cluster/state_machine_registry.h` — `type_id()`, `is_creatable()`, lookup-by-type-id, create-and-register-against-live-manager.
- `src/v/raft/tests/stm_test_fixture.h` — test-only creatable child + membership-provider STM.
- `src/v/raft/tests/BUILD`, `src/v/cluster/tests/BUILD` — test targets.

---

## Phase 0 — Primitives

### Task 1: `supervisor_stm_command` record batch type

**Files:**
- Modify: `src/v/model/record_batch_types.h:65-68`
- Modify: `src/v/model/record_batch_types.cc` (the `format_to` switch)

**Interfaces:**
- Produces: `model::record_batch_type::supervisor_stm_command` (value `43`); `model::record_batch_type::MAX` becomes `supervisor_stm_command`.

- [ ] **Step 1: Add the enum value**

In `src/v/model/record_batch_types.h`, change the tail of the enum:

```cpp
    l1_stm = 41,      // cloud_topics::l1::*
    ct_read_replica_stm = 42, // cloud_topics::read_replica::*
    supervisor_stm_command = 43, // supervisor_stm create/remove commands
    MAX = supervisor_stm_command,
};
```

- [ ] **Step 2: Add the `format_to` case**

In `src/v/model/record_batch_types.cc`, locate the `switch` in `format_to(record_batch_type, ...)` and add, alongside the other cases:

```cpp
    case record_batch_type::supervisor_stm_command:
        return fmt::format_to(out, "supervisor_stm_command");
```

- [ ] **Step 3: Build to verify the enum compiles and the switch is exhaustive**

Run: `bazel build //src/v/model/...`
Expected: success (a missing switch case would warn/error under the repo's `-Werror` switch handling).

- [ ] **Step 4: Commit**

```bash
bazel run //tools:clang_format
git add src/v/model/record_batch_types.h src/v/model/record_batch_types.cc
git commit -s -m "model: add supervisor_stm_command record batch type"
```

---

### Task 2: `state_machine_base` membership + creation hooks

**Files:**
- Modify: `src/v/raft/state_machine_base.h:118-127`

**Interfaces:**
- Produces:
  - `virtual bool state_machine_base::provides_dynamic_membership() const` — default `false`.
  - `virtual ss::future<> state_machine_base::prepare_created_at(model::offset)` — default `ss::now()`.

- [ ] **Step 1: Add the two virtuals**

In `src/v/raft/state_machine_base.h`, after `supports_snapshot_at_offset()` (line ~121), add:

```cpp
    /**
     * Returns true if this state machine drives dynamic membership of the
     * state_machine_manager (i.e. it can register/deregister other STMs). The
     * manager applies such an STM's Raft snapshot portion before all others so
     * it can reconstruct the dynamic children first.
     */
    virtual bool provides_dynamic_membership() const { return false; }

    /**
     * Called by the manager when this STM is registered as a freshly created
     * child at `creation_offset`. Implementations must arrange that, after this
     * call, the STM's next() == creation_offset + 1 and that state is durable so
     * restart recovery resumes from there. Default is a no-op for STMs that are
     * never created dynamically.
     */
    virtual ss::future<> prepare_created_at(model::offset /*creation_offset*/) {
        return ss::now();
    }
```

- [ ] **Step 2: Build**

Run: `bazel build //src/v/raft/...`
Expected: success.

- [ ] **Step 3: Commit**

```bash
bazel run //tools:clang_format
git add src/v/raft/state_machine_base.h
git commit -s -m "raft: add dynamic-membership and created-at hooks to state_machine_base"
```

---

### Task 3: `stm_hookset::remove_stm`

**Files:**
- Modify: `src/v/storage/types.h:155-163` (next to `add_stm`)

**Interfaces:**
- Consumes: existing `stm_hookset::add_stm(ss::shared_ptr<snapshotable_stm>)`, member `std::vector<ss::shared_ptr<snapshotable_stm>> _stms;`, `ss::shared_ptr<snapshotable_stm> _tx_stm;`.
- Produces: `void stm_hookset::remove_stm(const ss::shared_ptr<snapshotable_stm>& stm)`.

- [ ] **Step 1: Add `remove_stm` after `add_stm`**

In `src/v/storage/types.h`, immediately after `add_stm` (line ~163):

```cpp
    void remove_stm(const ss::shared_ptr<snapshotable_stm>& stm) {
        if (_tx_stm == stm) {
            _tx_stm = nullptr;
        }
        std::erase(_stms, stm);
    }
```

This is synchronous (no `co_await`), so it is mutually atomic with `max_removable_local_log_offset()` under Seastar's cooperative scheduling.

- [ ] **Step 2: Build**

Run: `bazel build //src/v/storage/...`
Expected: success.

- [ ] **Step 3: Commit**

```bash
bazel run //tools:clang_format
git add src/v/storage/types.h
git commit -s -m "storage: add stm_hookset::remove_stm"
```

---

### Task 4: `persisted_stm::write_empty_snapshot_at`

**Files:**
- Modify: `src/v/raft/persisted_stm.h:180-228` (declare method + override)
- Modify: `src/v/raft/persisted_stm.cc` (define near `do_write_local_snapshot`, ~line 333)
- Test: `src/v/raft/tests/persisted_stm_test.cc`

**Interfaces:**
- Consumes: `BaseT::set_next`, `do_write_local_snapshot()`, `_snapshot_hydrated`, `_on_snapshot_hydrated`, `_last_snapshot_offset`, `BaseT::last_applied_offset()`.
- Produces:
  - `ss::future<> persisted_stm_base<BaseT, T>::write_empty_snapshot_at(model::offset)`.
  - `ss::future<> persisted_stm_base<BaseT, T>::prepare_created_at(model::offset)` (override of the Task 2 virtual).

- [ ] **Step 1: Write the failing test**

In `src/v/raft/tests/persisted_stm_test.cc`, add (the `simple_kv` test STM and `state_machine_fixture` come from `stm_test_fixture.h`):

```cpp
TEST_F_CORO(state_machine_fixture, write_empty_snapshot_at_pins_next) {
    create_nodes();
    ss::shared_ptr<simple_kv> stm;
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        stm = builder.create_stm<simple_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }
    co_await wait_for_leader(10s);

    // Replicate a few batches so there is a log to (not) replay.
    co_await build_random_state(20, wait_for_each_batch::yes);

    // Create an empty snapshot at offset 10: next() must become 11 and the
    // applied state must remain empty.
    co_await stm->write_empty_snapshot_at(model::offset{10});

    ASSERT_EQ_CORO(stm->next(), model::offset{11});
    ASSERT_TRUE_CORO(stm->state.empty());
    ASSERT_EQ_CORO(stm->last_locally_snapshotted_offset(), model::offset{10});
}
```

Note: `next()` is `protected` in `state_machine_base`; expose a test accessor on `simple_kv` if needed, or assert via `last_applied()` (`== 10`). Prefer `ASSERT_EQ_CORO(stm->last_applied(), model::offset{10});`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `bazel test //src/v/raft/tests:persisted_stm_test --test_filter='*write_empty_snapshot_at_pins_next*'`
Expected: FAIL (no member `write_empty_snapshot_at`).

- [ ] **Step 3: Declare the methods in the header**

In `src/v/raft/persisted_stm.h`, in the `public:` section near `write_local_snapshot()` (line ~180):

```cpp
    /**
     * Persists an empty-state local snapshot whose last included offset is
     * `o`, and sets next() to o + 1. Used to create a child STM that must start
     * applying at o + 1 and recover through the normal snapshot-load path.
     * Requires the STM to be hydrated with empty state (freshly started).
     */
    ss::future<> write_empty_snapshot_at(model::offset o);
```

And override the Task 2 hook (next to `name()` override, line ~193):

```cpp
    ss::future<> prepare_created_at(model::offset o) override {
        return write_empty_snapshot_at(o);
    }
```

- [ ] **Step 4: Define `write_empty_snapshot_at` in the .cc**

In `src/v/raft/persisted_stm.cc`, after `do_write_local_snapshot` (line ~333):

```cpp
template<typename BaseT, supported_stm_snapshot T>
ss::future<>
persisted_stm_base<BaseT, T>::write_empty_snapshot_at(model::offset o) {
    auto holder = _gate.hold();
    auto lock_holder = co_await _op_lock.get_units();
    // The STM must be created empty: there must be nothing applied yet.
    vassert(
      BaseT::last_applied_offset() < model::offset{0}
        || BaseT::last_applied_offset() <= o,
      "[{} ({})] write_empty_snapshot_at({}) called after applying past it; "
      "last_applied={}",
      _raft->ntp(),
      name(),
      o,
      BaseT::last_applied_offset());
    // Mark hydrated so snapshotting is permitted, and pin next() to o + 1.
    _snapshot_hydrated = true;
    _on_snapshot_hydrated.broadcast();
    BaseT::set_next(model::next_offset(o));
    co_await do_write_local_snapshot();
}
```

`do_write_local_snapshot` calls `take_local_snapshot`, which serializes the
current (empty) state at `last_applied_offset() == o`, and updates
`_last_snapshot_offset`.

- [ ] **Step 5: Run the test to verify it passes**

Run: `bazel test //src/v/raft/tests:persisted_stm_test --test_filter='*write_empty_snapshot_at_pins_next*'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
bazel run //tools:clang_format
git add src/v/raft/persisted_stm.h src/v/raft/persisted_stm.cc src/v/raft/tests/persisted_stm_test.cc
git commit -s -m "raft: add persisted_stm::write_empty_snapshot_at"
```

---

## Phase 1 — state_machine_manager dynamic membership

These tasks use test-only STMs (added in Task 5) to drive the manager API in
isolation, before the real `supervisor_stm` exists.

### Task 5: Test-only creatable child + membership-provider STMs

**Files:**
- Modify: `src/v/raft/tests/stm_test_fixture.h`

**Interfaces:**
- Consumes: `simple_kv_base`, `raft_node_instance`, `state_machine_manager` API from Task 6+.
- Produces (for later tasks):
  - `creatable_kv` — a `persisted_stm`-based child whose `name = "creatable_kv"`.
  - `membership_provider_stm` — a test STM (`name = "membership_provider"`, `provides_dynamic_membership() == true`) that, on applying a batch of `record_batch_type::supervisor_stm_command`, calls the manager's create/remove API. Exposes `set_manager(state_machine_manager*)`.

Note: `creatable_kv` must be a real `persisted_stm` so it participates in the
hookset and local snapshots. Model it on the existing `persisted_stm`-based STM
in `persisted_stm_test.cc` (find the `persisted_stm` test STM there and mirror
its snapshot methods). It records applied offsets in a `std::vector<model::offset>`.

- [ ] **Step 1: Add `creatable_kv`**

Add a `persisted_stm`-derived STM to `stm_test_fixture.h`:

```cpp
class creatable_kv final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "creatable_kv";
    explicit creatable_kv(raft::consensus* raft, ss::logger& logger)
      : raft::persisted_stm<>("creatable_kv.snapshot", logger, raft) {}

    ss::future<> do_apply(const model::record_batch& b) override {
        applied.push_back(b.last_offset());
        co_return;
    }
    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units apply_units) override {
        iobuf data = serde::to_iobuf(applied);
        auto offset = last_applied_offset();
        apply_units.return_all();
        co_return raft::stm_snapshot::create(0, offset, std::move(data));
    }
    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&& b) override {
        applied = serde::from_iobuf<std::vector<model::offset>>(std::move(b));
        co_return raft::local_snapshot_applied::yes;
    }
    ss::future<> apply_raft_snapshot(const iobuf&) override { co_return; }
    ss::future<iobuf> take_raft_snapshot(model::offset) override {
        co_return iobuf{};
    }
    raft::stm_initial_recovery_policy get_initial_recovery_policy() const override {
        return raft::stm_initial_recovery_policy::skip_to_end;
    }
    std::vector<model::offset> applied;
};
```

- [ ] **Step 2: Add `membership_provider_stm`**

```cpp
class membership_provider_stm final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "membership_provider";
    explicit membership_provider_stm(raft::consensus* raft, ss::logger& logger)
      : raft::persisted_stm<>("membership_provider.snapshot", logger, raft)
      , _logger(logger) {}

    bool provides_dynamic_membership() const override { return true; }

    // Decodes a single-record command: key = "create"|"remove", value = type id.
    ss::future<> do_apply(const model::record_batch& b) override;

    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units u) override {
        auto data = serde::to_iobuf(_active);
        auto offset = last_applied_offset();
        u.return_all();
        co_return raft::stm_snapshot::create(0, offset, std::move(data));
    }
    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&& b) override {
        _active = serde::from_iobuf<absl::flat_hash_set<ss::sstring>>(std::move(b));
        co_return raft::local_snapshot_applied::yes;
    }
    ss::future<> apply_raft_snapshot(const iobuf&) override { co_return; }
    ss::future<iobuf> take_raft_snapshot(model::offset) override {
        co_return iobuf{};
    }
    raft::stm_initial_recovery_policy get_initial_recovery_policy() const override {
        return raft::stm_initial_recovery_policy::read_everything;
    }
    ss::logger& _logger;
    absl::flat_hash_set<ss::sstring> _active;
};
```

`do_apply` body (added in Task 6 once the manager API exists; for now leave a
minimal body that records into `_active` so the file compiles):

```cpp
inline ss::future<> membership_provider_stm::do_apply(const model::record_batch& b) {
    if (b.header().type != model::record_batch_type::supervisor_stm_command) {
        co_return;
    }
    // Filled in Task 6: call _raft->stm_manager() create/remove API.
    co_return;
}
```

- [ ] **Step 3: Build the fixture library**

Run: `bazel build //src/v/raft/tests:stm_test_fixture`
Expected: success.

- [ ] **Step 4: Commit**

```bash
bazel run //tools:clang_format
git add src/v/raft/tests/stm_test_fixture.h
git commit -s -m "raft/tests: add creatable_kv and membership_provider test STMs"
```

---

### Task 6: Apply-context CREATE (`register_created_stm`)

**Files:**
- Modify: `src/v/raft/state_machine_manager.h:137-270` (public API + members)
- Modify: `src/v/raft/state_machine_manager.cc`
- Modify: `src/v/raft/tests/stm_test_fixture.h` (fill `membership_provider_stm::do_apply` CREATE path)
- Test: `src/v/raft/tests/stm_dynamic_membership_test.cc` (new)
- Modify: `src/v/raft/tests/BUILD`

**Interfaces:**
- Consumes: `_machines`, `entry_ptr`, `state_machine_entry`, `_supports_snapshot_at_offset`, `_raft->log()->stm_hookset()`, `state_machine_base::start()` (friend access), `prepare_created_at()`.
- Produces:
  - `ss::future<> state_machine_manager::register_created_stm(ss::sstring name, ss::shared_ptr<state_machine_base> stm, model::offset creation_offset)` — apply-context (must NOT reacquire `_apply_mutex`).
  - `state_machine_base* state_machine_manager::membership_provider()` helper (returns the single `provides_dynamic_membership()` STM or nullptr).

- [ ] **Step 1: Write the failing test**

Create `src/v/raft/tests/stm_dynamic_membership_test.cc`:

```cpp
#include "raft/tests/stm_test_fixture.h"
#include "test_utils/test.h"

namespace {
model::record_batch make_supervisor_cmd(ss::sstring op, ss::sstring type_id) {
    storage::record_batch_builder b(
      model::record_batch_type::supervisor_stm_command, model::offset(0));
    b.add_raw_kv(serde::to_iobuf(std::move(op)), serde::to_iobuf(std::move(type_id)));
    return std::move(b).build();
}
} // namespace

TEST_F_CORO(state_machine_fixture, create_child_starts_after_create_offset) {
    create_nodes();
    std::vector<ss::shared_ptr<membership_provider_stm>> providers;
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto p = builder.create_stm<membership_provider_stm>(
          node->raft().get(), logger());
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        providers.push_back(p);
    }
    auto leader = co_await wait_for_leader(10s);

    // Replicate some data, then a CREATE command, then more data.
    co_await build_random_state(10, wait_for_each_batch::yes);
    auto create_res = co_await replicate_batch(
      make_supervisor_cmd("create", "creatable_kv"));
    auto create_offset = create_res.value().last_offset;
    co_await build_random_state(10, wait_for_each_batch::yes);
    co_await wait_for_apply();

    co_await parallel_for_each_node([&](raft_node_instance& n) -> ss::future<> {
        auto child = n.raft()->stm_manager()->get<creatable_kv>();
        ASSERT_NE_CORO(child, nullptr);
        // The child must not have applied anything at or before create_offset.
        for (auto o : child->applied) {
            ASSERT_GT_CORO(o, create_offset);
        }
        co_return;
    });
}
```

Add a `replicate_batch(model::record_batch)` helper to `state_machine_fixture`
(mirrors `replicate`, but takes a prebuilt batch):

```cpp
ss::future<result<raft::replicate_result>>
replicate_batch(model::record_batch b) {
    co_return co_await retry_with_leader(
      10s + model::timeout_clock::now(),
      [b = std::move(b)](raft_node_instance& leader) mutable {
          return leader.raft()->replicate(
            b.share(),
            raft::replicate_options(raft::consistency_level::quorum_ack));
      });
}
```

- [ ] **Step 2: Add the BUILD target and run the test to verify it fails**

In `src/v/raft/tests/BUILD`, add a `redpanda_cc_gtest` named `stm_dynamic_membership_test` with the same `deps` as `stm_manager_test` (copy the block at lines 549-564, change `name` and `srcs`).

Run: `bazel test //src/v/raft/tests:stm_dynamic_membership_test`
Expected: FAIL (no `register_created_stm`; provider `do_apply` does nothing yet).

- [ ] **Step 3: Declare `register_created_stm` + `membership_provider` in the header**

In `src/v/raft/state_machine_manager.h`, public section (after `remove_local_state()`, line ~137):

```cpp
    /**
     * Registers a freshly created child STM. MUST be called from within a
     * managed STM's do_apply (the apply mutex is already held); it does NOT
     * reacquire _apply_mutex. Starts the child, initializes it to begin
     * applying at creation_offset + 1 (empty snapshot pinned), and adds it to
     * both this manager and the storage stm_hookset.
     */
    ss::future<> register_created_stm(
      ss::sstring name,
      ss::shared_ptr<state_machine_base> stm,
      model::offset creation_offset);
```

Private section (near `all_state_machines()`, line ~223):

```cpp
    // Returns the single membership-providing STM, or nullptr.
    state_machine_base* membership_provider() const;
    // Adds an entry to _machines and the stm_hookset; not lifecycle-managing.
    entry_ptr add_machine_entry(ss::sstring name, stm_ptr stm);
```

- [ ] **Step 4: Implement in the .cc**

In `src/v/raft/state_machine_manager.cc`:

```cpp
state_machine_manager::entry_ptr
state_machine_manager::add_machine_entry(ss::sstring name, stm_ptr stm) {
    _supports_snapshot_at_offset = _supports_snapshot_at_offset
                                   && stm->supports_snapshot_at_offset();
    auto snapshotable = ss::dynamic_pointer_cast<storage::snapshotable_stm>(stm);
    vassert(
      snapshotable != nullptr,
      "dynamically registered STM '{}' must be a snapshotable persisted_stm",
      name);
    _raft->log()->stm_hookset()->add_stm(snapshotable);
    auto [it, _] = _machines.try_emplace(
      name, ss::make_lw_shared<state_machine_entry>(name, std::move(stm)));
    return it->second;
}

ss::future<> state_machine_manager::register_created_stm(
  ss::sstring name,
  ss::shared_ptr<state_machine_base> stm,
  model::offset creation_offset) {
    vlog(
      _log.info,
      "registering created stm '{}' at offset {}",
      name,
      creation_offset);
    // start() hydrates with empty state (no snapshot exists yet).
    co_await stm->start();
    // Pin the truncation floor and next() before adding to the hookset.
    co_await stm->prepare_created_at(creation_offset);
    add_machine_entry(std::move(name), std::move(stm));
}

state_machine_base* state_machine_manager::membership_provider() const {
    for (const auto& [_, entry] : _machines) {
        if (entry->stm->provides_dynamic_membership()) {
            return entry->stm.get();
        }
    }
    return nullptr;
}
```

- [ ] **Step 5: Fill the provider's CREATE path in the fixture**

In `stm_test_fixture.h`, implement `membership_provider_stm::do_apply` CREATE branch:

```cpp
inline ss::future<> membership_provider_stm::do_apply(const model::record_batch& b) {
    if (b.header().type != model::record_batch_type::supervisor_stm_command) {
        co_return;
    }
    auto offset = b.last_offset();
    chunked_vector<std::pair<ss::sstring, ss::sstring>> cmds;
    b.for_each_record([&](model::record r) {
        cmds.emplace_back(
          serde::from_iobuf<ss::sstring>(r.key().copy()),
          serde::from_iobuf<ss::sstring>(r.value().copy()));
    });
    for (auto& [op, type_id] : cmds) {
        if (op == "create" && !_active.contains(type_id)) {
            _active.insert(type_id);
            // Test only supports creatable_kv.
            auto child = ss::make_shared<creatable_kv>(_raft, _logger);
            co_await _raft->stm_manager()->register_created_stm(
              type_id, child, offset);
        }
        // remove handled in Task 7
    }
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `bazel test //src/v/raft/tests:stm_dynamic_membership_test --test_filter='*create_child_starts_after_create_offset*'`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
bazel run //tools:clang_format
git add src/v/raft/state_machine_manager.h src/v/raft/state_machine_manager.cc \
        src/v/raft/tests/stm_test_fixture.h src/v/raft/tests/stm_dynamic_membership_test.cc \
        src/v/raft/tests/BUILD
git commit -s -m "raft: support synchronous dynamic STM creation in state_machine_manager"
```

---

### Task 7: REMOVE drain step (`request_remove_stm`)

**Files:**
- Modify: `src/v/raft/state_machine_manager.h` (API + pending-removal member)
- Modify: `src/v/raft/state_machine_manager.cc` (`try_apply_in_foreground`, `drain_pending_removals`, `do_stop_stm`)
- Modify: `src/v/raft/tests/stm_test_fixture.h` (provider REMOVE path)
- Test: `src/v/raft/tests/stm_dynamic_membership_test.cc`

**Interfaces:**
- Consumes: `_machines`, `do_stop_stm`, `_raft->log()->stm_hookset()->remove_stm`, `state_machine_base::remove_local_state()`.
- Produces:
  - `void state_machine_manager::request_remove_stm(ss::sstring name)` — enqueue-only, callable from `do_apply`.
  - `ss::future<> state_machine_manager::drain_pending_removals()` — called at the top of `try_apply_in_foreground` under `_apply_mutex`.

- [ ] **Step 1: Write the failing test**

In `stm_dynamic_membership_test.cc`:

```cpp
TEST_F_CORO(state_machine_fixture, remove_child_deregisters_and_clears_snapshot) {
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<membership_provider_stm>(node->raft().get(), logger());
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }
    co_await wait_for_leader(10s);

    co_await replicate_batch(make_supervisor_cmd("create", "creatable_kv"));
    co_await build_random_state(10, wait_for_each_batch::yes);
    co_await replicate_batch(make_supervisor_cmd("remove", "creatable_kv"));
    co_await build_random_state(5, wait_for_each_batch::yes);
    co_await wait_for_apply();

    co_await parallel_for_each_node([&](raft_node_instance& n) -> ss::future<> {
        ASSERT_EQ_CORO(n.raft()->stm_manager()->get<creatable_kv>(), nullptr);
        co_return;
    });
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bazel test //src/v/raft/tests:stm_dynamic_membership_test --test_filter='*remove_child_deregisters*'`
Expected: FAIL (child still present; no remove path).

- [ ] **Step 3: Declare API + member in the header**

In `state_machine_manager.h` public section:

```cpp
    /**
     * Requests removal of a dynamically registered child. Enqueue-only and
     * callable from within do_apply; the actual teardown (stop, deregister,
     * delete snapshot) happens at the next apply-loop drain, never mid-batch.
     */
    void request_remove_stm(ss::sstring name);
```

Private section:

```cpp
    ss::future<> drain_pending_removals();
    std::vector<ss::sstring> _pending_removals;
```

- [ ] **Step 4: Implement in the .cc**

```cpp
void state_machine_manager::request_remove_stm(ss::sstring name) {
    vlog(_log.info, "requesting removal of stm '{}'", name);
    _pending_removals.push_back(std::move(name));
}

ss::future<> state_machine_manager::drain_pending_removals() {
    if (_pending_removals.empty()) {
        co_return;
    }
    auto pending = std::exchange(_pending_removals, {});
    for (auto& name : pending) {
        auto it = _machines.find(name);
        if (it == _machines.end()) {
            continue;
        }
        auto entry = it->second;
        _machines.erase(it);
        auto snapshotable = ss::dynamic_pointer_cast<storage::snapshotable_stm>(
          entry->stm);
        if (snapshotable) {
            _raft->log()->stm_hookset()->remove_stm(snapshotable);
        }
        co_await do_stop_stm(entry);
        co_await entry->stm->remove_local_state();
    }
}
```

Call the drain at the top of `try_apply_in_foreground`, right after acquiring
the apply mutex (`state_machine_manager.cc:488`):

```cpp
        auto u = co_await _apply_mutex.get_units();
        co_await drain_pending_removals();
```

- [ ] **Step 5: Fill the provider REMOVE path**

In `stm_test_fixture.h` `membership_provider_stm::do_apply`, extend the loop:

```cpp
        else if (op == "remove" && _active.contains(type_id)) {
            _active.erase(type_id);
            _raft->stm_manager()->request_remove_stm(type_id);
        }
```

- [ ] **Step 6: Run to verify it passes**

Run: `bazel test //src/v/raft/tests:stm_dynamic_membership_test --test_filter='*remove_child_deregisters*'`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
bazel run //tools:clang_format
git add src/v/raft/state_machine_manager.h src/v/raft/state_machine_manager.cc \
        src/v/raft/tests/stm_test_fixture.h src/v/raft/tests/stm_dynamic_membership_test.cc
git commit -s -m "raft: deferred dynamic STM removal drain in state_machine_manager"
```

---

### Task 8: Restart reconstruction

**Files:**
- Modify: `src/v/raft/state_machine_manager.h` (API)
- Modify: `src/v/raft/state_machine_manager.cc` (`start()`)
- Modify: `src/v/raft/tests/stm_test_fixture.h` (provider reconstructs children on start)
- Test: `src/v/raft/tests/stm_dynamic_membership_test.cc`

**Interfaces:**
- Consumes: `register_created_stm` (for first-time children with no snapshot), and a sibling for already-snapshotted children.
- Produces:
  - `ss::future<> state_machine_manager::register_reconstructed_stm(ss::sstring name, ss::shared_ptr<state_machine_base> stm)` — start (loads existing snapshot) + add to both registries; no empty-snapshot write.
  - `ss::future<> state_machine_manager::reconstruct_dynamic_children()` — calls the provider to re-register its children; invoked during `start()` after static STMs start, before the apply loop.

- [ ] **Step 1: Write the failing test**

```cpp
TEST_F_CORO(state_machine_fixture, child_survives_restart) {
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<membership_provider_stm>(node->raft().get(), logger());
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }
    co_await wait_for_leader(10s);
    co_await replicate_batch(make_supervisor_cmd("create", "creatable_kv"));
    co_await build_random_state(20, wait_for_each_batch::yes);
    co_await wait_for_apply();

    // Snapshot the provider's active set + the child's local snapshot, then
    // restart one node and verify the child comes back and keeps applying.
    auto target = *all_vnodes().begin();
    co_await stop_node(target.id());
    co_await restart_node(
      node(target.id()),
      [&](raft::state_machine_manager_builder& b, raft_node_instance& n) {
          b.create_stm<membership_provider_stm>(n.raft().get(), logger());
      });

    co_await build_random_state(5, wait_for_each_batch::yes);
    co_await wait_for_apply();
    auto child = node(target.id()).raft()->stm_manager()->get<creatable_kv>();
    ASSERT_NE_CORO(child, nullptr);
}
```

Note: use the fixture's existing restart helpers; find their exact names in
`raft_fixture.h` (e.g. `stop_node`/`restart_node` patterns used in
`stm_manager_test.cc` `test_recovery_from_snapshot`). Adapt the call shape to
match.

- [ ] **Step 2: Run to verify it fails**

Run: `bazel test //src/v/raft/tests:stm_dynamic_membership_test --test_filter='*child_survives_restart*'`
Expected: FAIL (child not reconstructed after restart).

- [ ] **Step 3: Declare API in the header**

```cpp
    ss::future<> register_reconstructed_stm(
      ss::sstring name, ss::shared_ptr<state_machine_base> stm);
```

Private:

```cpp
    ss::future<> reconstruct_dynamic_children();
```

And add a provider-facing virtual on `state_machine_base` (Task 2 file) the
manager calls during reconstruction:

```cpp
    /**
     * For membership-providing STMs: re-create and re-register the dynamic
     * children this STM is responsible for, after its own local snapshot has
     * been loaded. Default no-op.
     */
    virtual ss::future<> reconstruct_children(class state_machine_manager&) {
        return ss::now();
    }
```

(Forward-declare `state_machine_manager` in `state_machine_base.h`.)

- [ ] **Step 4: Implement in the .cc**

```cpp
ss::future<> state_machine_manager::register_reconstructed_stm(
  ss::sstring name, ss::shared_ptr<state_machine_base> stm) {
    vlog(_log.info, "registering reconstructed stm '{}'", name);
    co_await stm->start();
    add_machine_entry(std::move(name), std::move(stm));
}

ss::future<> state_machine_manager::reconstruct_dynamic_children() {
    auto* provider = membership_provider();
    if (provider == nullptr) {
        co_return;
    }
    co_await provider->reconstruct_children(*this);
}
```

In `start()`, after `parallel_for_each(_machines, start)` and the `_next`
computation but **before** spawning the apply loop (`state_machine_manager.cc:231`,
right after `apply_initial_recovery_policy()`):

```cpp
    co_await apply_initial_recovery_policy();
    // Re-register dynamic children (kept out of initial recovery policy).
    co_await reconstruct_dynamic_children();
    ssx::spawn_with_gate(_gate, [this] { ... });
```

- [ ] **Step 5: Implement provider `reconstruct_children` in the fixture**

```cpp
ss::future<> reconstruct_children(raft::state_machine_manager& mgr) override {
    for (const auto& type_id : _active) {
        auto child = ss::make_shared<creatable_kv>(_raft, _logger);
        co_await mgr.register_reconstructed_stm(type_id, child);
    }
}
```

- [ ] **Step 6: Run to verify it passes**

Run: `bazel test //src/v/raft/tests:stm_dynamic_membership_test --test_filter='*child_survives_restart*'`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
bazel run //tools:clang_format
git add src/v/raft/state_machine_manager.h src/v/raft/state_machine_manager.cc \
        src/v/raft/state_machine_base.h src/v/raft/tests/stm_test_fixture.h \
        src/v/raft/tests/stm_dynamic_membership_test.cc
git commit -s -m "raft: reconstruct dynamic children on state_machine_manager start"
```

---

### Task 9: Supervisor-first Raft snapshot install

**Files:**
- Modify: `src/v/raft/state_machine_manager.cc` (`do_apply_raft_snapshot`)
- Modify: `src/v/raft/tests/stm_test_fixture.h` (provider reconcile in `apply_raft_snapshot`)
- Test: `src/v/raft/tests/stm_dynamic_membership_test.cc`

**Interfaces:**
- Consumes: `managed_snapshot`, `apply_snapshot_to_stm`, `register_created_stm`/`register_reconstructed_stm`, `request_remove_stm`, `membership_provider()`.
- Produces: two-phase install — provider portion applied first, children re-collected, remaining portions applied.

- [ ] **Step 1: Write the failing test**

```cpp
TEST_F_CORO(state_machine_fixture, snapshot_install_reconstructs_children) {
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<membership_provider_stm>(node->raft().get(), logger());
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }
    auto leader = co_await wait_for_leader(10s);
    co_await replicate_batch(make_supervisor_cmd("create", "creatable_kv"));
    co_await build_random_state(50, wait_for_each_batch::yes);
    co_await wait_for_apply();

    // Take a managed snapshot on the leader at the committed offset, then add a
    // fresh node that must install it and reconstruct the child.
    auto& l = node(leader);
    co_await l.raft()->write_snapshot(raft::write_snapshot_cfg(
      l.raft()->committed_offset(), iobuf{}));

    auto& new_node = add_node(model::node_id(99), model::revision_id(0));
    raft::state_machine_manager_builder b;
    b.create_stm<membership_provider_stm>(new_node.raft().get(), logger());
    co_await new_node.init_and_start(all_vnodes(), std::move(b));

    co_await wait_for_apply();
    ASSERT_NE_CORO(
      new_node.raft()->stm_manager()->get<creatable_kv>(), nullptr);
}
```

Note: the exact `write_snapshot` / managed-snapshot trigger may differ; mirror
how `test_recovery_from_snapshot` in `stm_manager_test.cc` forces a non-empty
managed snapshot and a follower install. Adapt accordingly.

- [ ] **Step 2: Run to verify it fails**

Run: `bazel test //src/v/raft/tests:stm_dynamic_membership_test --test_filter='*snapshot_install_reconstructs_children*'`
Expected: FAIL (child not present on the new node).

- [ ] **Step 3: Make `do_apply_raft_snapshot` supervisor-first**

Replace the non-empty branch of `do_apply_raft_snapshot`
(`state_machine_manager.cc:448-458`) with provider-first ordering:

```cpp
    } else {
        iobuf_parser parser(std::move(snapshot_content));
        auto snap = co_await serde::read_async<managed_snapshot>(parser);

        // Phase 1: apply the membership provider's portion first so it can
        // reconstruct/reconcile dynamic children before their portions apply.
        entry_ptr provider;
        for (auto& entry : state_machines) {
            if (entry->stm->provides_dynamic_membership()) {
                provider = entry;
                break;
            }
        }
        if (provider) {
            co_await apply_snapshot_to_stm(provider, snap, last_offset);
        }

        // Phase 2: apply the remaining portions to the (now complete) set of
        // machines, skipping the provider already handled above.
        auto machines = all_state_machines();
        co_await ss::coroutine::parallel_for_each(
          machines,
          [this, &snap, last_offset, provider](entry_ptr& entry) -> ss::future<> {
              if (provider && entry->name == provider->name) {
                  return ss::now();
              }
              return apply_snapshot_to_stm(entry, snap, last_offset);
          });
    }
```

- [ ] **Step 4: Implement provider reconcile in `apply_raft_snapshot`**

In `stm_test_fixture.h`, make `membership_provider_stm::apply_raft_snapshot`
decode the active set and reconcile membership (the provider's portion is its
serialized `_active`; have `take_raft_snapshot` serialize `_active`):

```cpp
ss::future<iobuf> take_raft_snapshot(model::offset) override {
    co_return serde::to_iobuf(_active);
}
ss::future<> apply_raft_snapshot(const iobuf& b) override {
    auto incoming = b.empty()
      ? absl::flat_hash_set<ss::sstring>{}
      : serde::from_iobuf<absl::flat_hash_set<ss::sstring>>(b.copy());
    auto& mgr = *_raft->stm_manager();
    // add children present in snapshot but not registered
    for (const auto& type_id : incoming) {
        if (!_active.contains(type_id)) {
            auto child = ss::make_shared<creatable_kv>(_raft, _logger);
            co_await mgr.register_reconstructed_stm(type_id, child);
        }
    }
    // remove children registered but absent from snapshot
    for (const auto& type_id : _active) {
        if (!incoming.contains(type_id)) {
            mgr.request_remove_stm(type_id);
        }
    }
    _active = std::move(incoming);
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `bazel test //src/v/raft/tests:stm_dynamic_membership_test --test_filter='*snapshot_install_reconstructs_children*'`
Expected: PASS.

- [ ] **Step 6: Run the whole dynamic-membership suite**

Run: `bazel test //src/v/raft/tests:stm_dynamic_membership_test`
Expected: PASS (all tasks 6-9 green).

- [ ] **Step 7: Commit**

```bash
bazel run //tools:clang_format
git add src/v/raft/state_machine_manager.cc src/v/raft/tests/stm_test_fixture.h \
        src/v/raft/tests/stm_dynamic_membership_test.cc
git commit -s -m "raft: supervisor-first dynamic membership Raft snapshot install"
```

---

## Phase 2 — Registry / factory extensions

### Task 10: Creatable-STM registry support

**Files:**
- Modify: `src/v/cluster/state_machine_registry.h`

**Interfaces:**
- Consumes: existing `state_machine_factory`, `state_machine_registry`, `raft::state_machine_manager`.
- Produces:
  - `virtual std::string_view state_machine_factory::type_id() const` (= the STM `name`).
  - `virtual bool state_machine_factory::is_creatable() const` — default `false`.
  - `virtual ss::future<> state_machine_factory::create_new(raft::state_machine_manager&, raft::consensus*, const stm_instance_config&, model::offset creation_offset)` — build + `register_created_stm` (writes empty snapshot). Default `vassert(false)`.
  - `virtual ss::future<> state_machine_factory::create_existing(raft::state_machine_manager&, raft::consensus*, const stm_instance_config&)` — build + `register_reconstructed_stm` (loads existing snapshot). Default `vassert(false)`.
  - `state_machine_factory* state_machine_registry::find_creatable(std::string_view type_id)`.

- [ ] **Step 1: Extend `state_machine_factory`**

In `state_machine_registry.h`, add to the `state_machine_factory` interface
(after `create`, line ~54):

```cpp
    /// Stable type id of the STM this factory builds (== the STM name).
    virtual std::string_view type_id() const = 0;

    /// True if this STM may be created dynamically by a supervisor_stm.
    virtual bool is_creatable() const { return false; }

    /**
     * Build this STM and register it with a live state_machine_manager as a
     * freshly created child at creation_offset (writes an empty snapshot via
     * register_created_stm). Only valid when is_creatable().
     */
    virtual ss::future<> create_new(
      raft::state_machine_manager&,
      raft::consensus*,
      const stm_instance_config&,
      model::offset creation_offset) {
        vassert(false, "create_new not supported by this factory");
        co_return;
    }

    /**
     * Build this STM and re-register an already-existing child (loads its own
     * on-disk snapshot via register_reconstructed_stm). Only valid when
     * is_creatable().
     */
    virtual ss::future<> create_existing(
      raft::state_machine_manager&,
      raft::consensus*,
      const stm_instance_config&) {
        vassert(false, "create_existing not supported by this factory");
        co_return;
    }
```

- [ ] **Step 2: Add `find_creatable` to the registry**

```cpp
    state_machine_factory* find_creatable(std::string_view type_id) {
        for (auto& f : _stm_factories) {
            if (f->is_creatable() && f->type_id() == type_id) {
                return f.get();
            }
        }
        return nullptr;
    }
```

- [ ] **Step 3: Build**

Run: `bazel build //src/v/cluster/...`
Expected: FAIL — every existing `state_machine_factory` subclass now needs a
`type_id()`. This is expected; the next step fixes them.

- [ ] **Step 4: Add `type_id()` to all existing factories**

Each existing factory (`rm_stm_factory`, `tm_stm_factory`,
`id_allocator_stm_factory`, `log_eviction_stm_factory`,
`archival_metadata_stm_factory`, `partition_properties_stm_factory`, the
cloud-topics/datalake/kafka factories, etc.) gains:

```cpp
    std::string_view type_id() const override { return T::name; }
```

where `T` is the STM the factory builds (e.g. `rm_stm::name`). Find them with:

```bash
grep -rln "public cluster::state_machine_factory\|: state_machine_factory\|state_machine_factory {" src/v
```

- [ ] **Step 5: Build**

Run: `bazel build //src/v/...`
Expected: success.

- [ ] **Step 6: Commit**

```bash
bazel run //tools:clang_format
git add -A
git commit -s -m "cluster: add creatable-STM support to state_machine_registry"
```

---

## Phase 3 — supervisor_stm

### Task 11: `supervisor_stm` command codec + active-set snapshot

**Files:**
- Create: `src/v/cluster/supervisor_stm.h`, `src/v/cluster/supervisor_stm.cc`
- Create: `src/v/cluster/tests/supervisor_stm_test.cc`
- Modify: `src/v/cluster/BUILD`, `src/v/cluster/tests/BUILD`

**Interfaces:**
- Consumes: `raft::persisted_stm`, `cluster::state_machine_registry`, `model::record_batch_type::supervisor_stm_command`.
- Produces:
  - `cluster::supervisor_stm` with `static constexpr std::string_view name = "supervisor_stm";`.
  - `static model::record_batch supervisor_stm::make_create_command(std::string_view type_id);`
  - `static model::record_batch supervisor_stm::make_remove_command(std::string_view type_id);`
  - serde envelope `supervisor_snapshot { absl::flat_hash_map<ss::sstring, model::offset> active; }` (type_id → creation_offset).

- [ ] **Step 1: Write the failing test (codec round-trip)**

`src/v/cluster/tests/supervisor_stm_test.cc`:

```cpp
#include "cluster/supervisor_stm.h"
#include "test_utils/test.h"

TEST(supervisor_stm, command_codec_roundtrip) {
    auto create = cluster::supervisor_stm::make_create_command("creatable_kv");
    ASSERT_EQ(
      create.header().type, model::record_batch_type::supervisor_stm_command);
    auto decoded = cluster::supervisor_stm::decode_command(create);
    ASSERT_EQ(decoded.op, cluster::supervisor_stm::op_type::create);
    ASSERT_EQ(decoded.type_id, "creatable_kv");
}
```

- [ ] **Step 2: Add the BUILD target; run to verify it fails**

In `src/v/cluster/tests/BUILD`, add a `redpanda_cc_gtest` `supervisor_stm_test`
depending on `//src/v/cluster` and `//src/v/test_utils:gtest`.

Run: `bazel test //src/v/cluster/tests:supervisor_stm_test --test_filter='*command_codec_roundtrip*'`
Expected: FAIL (no such header).

- [ ] **Step 3: Write the header with the codec + snapshot types**

`src/v/cluster/supervisor_stm.h` (codec + types only for now):

```cpp
#pragma once
#include "cluster/state_machine_registry.h"
#include "model/record.h"
#include "raft/persisted_stm.h"
#include "serde/envelope.h"

namespace cluster {

class supervisor_stm final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "supervisor_stm";

    enum class op_type : uint8_t { create = 0, remove = 1 };
    struct command {
        op_type op;
        ss::sstring type_id;
    };

    static model::record_batch make_create_command(std::string_view type_id);
    static model::record_batch make_remove_command(std::string_view type_id);
    static command decode_command(const model::record& r);
    // convenience: decode the single record of a command batch
    static command decode_command(const model::record_batch& b);

    struct supervisor_snapshot
      : serde::envelope<
          supervisor_snapshot,
          serde::version<0>,
          serde::compat_version<0>> {
        absl::flat_hash_map<ss::sstring, model::offset> active;
        auto serde_fields() { return std::tie(active); }
    };

    // (constructor + persisted_stm overrides added in Task 12)
};

} // namespace cluster
```

- [ ] **Step 4: Implement the codec in the .cc**

`src/v/cluster/supervisor_stm.cc`:

```cpp
#include "cluster/supervisor_stm.h"
#include "storage/record_batch_builder.h"

namespace cluster {

static model::record_batch
make_command(supervisor_stm::op_type op, std::string_view type_id) {
    storage::record_batch_builder b(
      model::record_batch_type::supervisor_stm_command, model::offset(0));
    b.add_raw_kv(
      serde::to_iobuf(static_cast<uint8_t>(op)),
      serde::to_iobuf(ss::sstring(type_id)));
    return std::move(b).build();
}

model::record_batch
supervisor_stm::make_create_command(std::string_view type_id) {
    return make_command(op_type::create, type_id);
}
model::record_batch
supervisor_stm::make_remove_command(std::string_view type_id) {
    return make_command(op_type::remove, type_id);
}

supervisor_stm::command
supervisor_stm::decode_command(const model::record& r) {
    return command{
      .op = static_cast<op_type>(serde::from_iobuf<uint8_t>(r.key().copy())),
      .type_id = serde::from_iobuf<ss::sstring>(r.value().copy())};
}

supervisor_stm::command
supervisor_stm::decode_command(const model::record_batch& b) {
    command out;
    b.for_each_record([&](model::record r) { out = decode_command(r); });
    return out;
}

} // namespace cluster
```

- [ ] **Step 5: Run to verify it passes**

Run: `bazel test //src/v/cluster/tests:supervisor_stm_test --test_filter='*command_codec_roundtrip*'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
bazel run //tools:clang_format
git add src/v/cluster/supervisor_stm.h src/v/cluster/supervisor_stm.cc \
        src/v/cluster/tests/supervisor_stm_test.cc src/v/cluster/BUILD \
        src/v/cluster/tests/BUILD
git commit -s -m "cluster: add supervisor_stm command codec and snapshot types"
```

---

### Task 12: `supervisor_stm` apply, snapshot, reconciliation

**Files:**
- Modify: `src/v/cluster/supervisor_stm.h`, `src/v/cluster/supervisor_stm.cc`

**Interfaces:**
- Consumes: `raft::persisted_stm` virtuals (`do_apply`, `take_local_snapshot`, `apply_local_snapshot`, `apply_raft_snapshot`, `take_raft_snapshot`, `get_initial_recovery_policy`), `state_machine_registry::find_creatable`, `state_machine_manager::register_created_stm`/`register_reconstructed_stm`/`request_remove_stm`, `state_machine_base::reconstruct_children`/`provides_dynamic_membership`.
- Produces: a fully-functional `supervisor_stm`.

- [ ] **Step 1: Add constructor + members + override declarations to the header**

Add to the `public:` section of `supervisor_stm`:

```cpp
    supervisor_stm(
      raft::consensus* raft,
      ss::logger& logger,
      state_machine_registry& registry);

    bool provides_dynamic_membership() const override { return true; }
    ss::future<> reconstruct_children(raft::state_machine_manager&) override;

protected:
    ss::future<> do_apply(const model::record_batch&) override;
    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units) override;
    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;
    ss::future<> apply_raft_snapshot(const iobuf&) override;
    ss::future<iobuf> take_raft_snapshot(model::offset) override;
    raft::stm_initial_recovery_policy get_initial_recovery_policy() const override {
        return raft::stm_initial_recovery_policy::read_everything;
    }

private:
    ss::future<> create_child(ss::sstring type_id, model::offset at);
    state_machine_registry& _registry;
    absl::flat_hash_map<ss::sstring, model::offset> _active;
```

- [ ] **Step 2: Implement `do_apply` (synchronous create / deferred remove)**

```cpp
ss::future<> supervisor_stm::do_apply(const model::record_batch& b) {
    if (b.header().type != model::record_batch_type::supervisor_stm_command) {
        co_return;
    }
    auto offset = b.last_offset();
    auto cmd = decode_command(b);
    switch (cmd.op) {
    case op_type::create:
        if (_active.contains(cmd.type_id)) {
            co_return; // idempotent
        }
        co_await create_child(cmd.type_id, offset);
        _active.emplace(cmd.type_id, offset);
        co_return;
    case op_type::remove:
        if (!_active.contains(cmd.type_id)) {
            co_return; // idempotent
        }
        _active.erase(cmd.type_id);
        _raft->stm_manager()->request_remove_stm(cmd.type_id);
        co_return;
    }
}

ss::future<>
supervisor_stm::create_child(ss::sstring type_id, model::offset at) {
    auto* factory = _registry.find_creatable(type_id);
    vassert(
      factory != nullptr,
      "[{}] CREATE for unknown/non-creatable STM type '{}'",
      _raft->ntp(),
      type_id);
    co_await factory->create_new(
      *_raft->stm_manager(), _raft, stm_instance_config{nullptr}, at);
}
```

- [ ] **Step 3: Implement snapshots + reconciliation**

```cpp
ss::future<raft::stm_snapshot>
supervisor_stm::take_local_snapshot(ssx::semaphore_units u) {
    supervisor_snapshot snap;
    snap.active = _active;
    auto offset = last_applied_offset();
    u.return_all();
    co_return raft::stm_snapshot::create(0, offset, serde::to_iobuf(std::move(snap)));
}

ss::future<raft::local_snapshot_applied>
supervisor_stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&& b) {
    auto snap = serde::from_iobuf<supervisor_snapshot>(std::move(b));
    _active = std::move(snap.active);
    co_return raft::local_snapshot_applied::yes;
}

ss::future<iobuf> supervisor_stm::take_raft_snapshot(model::offset) {
    supervisor_snapshot snap;
    snap.active = _active;
    co_return serde::to_iobuf(std::move(snap));
}

ss::future<> supervisor_stm::apply_raft_snapshot(const iobuf& b) {
    auto incoming = b.empty()
      ? supervisor_snapshot{}
      : serde::from_iobuf<supervisor_snapshot>(b.copy());
    auto& mgr = *_raft->stm_manager();
    for (auto& [type_id, at] : incoming.active) {
        if (!_active.contains(type_id)) {
            auto* factory = _registry.find_creatable(type_id);
            vassert(factory, "unknown creatable STM '{}' in snapshot", type_id);
            // The child's portion is applied right after (Phase 2 of install),
            // which sets next = last_included + 1; here we just register it.
            co_await factory->create_existing(
              mgr, _raft, stm_instance_config{nullptr});
        }
    }
    for (auto& [type_id, _] : _active) {
        if (!incoming.active.contains(type_id)) {
            mgr.request_remove_stm(type_id);
        }
    }
    _active = std::move(incoming.active);
}

ss::future<> supervisor_stm::reconstruct_children(raft::state_machine_manager& mgr) {
    for (auto& [type_id, at] : _active) {
        auto* factory = _registry.find_creatable(type_id);
        vassert(factory, "unknown creatable STM '{}' on reconstruct", type_id);
        // On restart the child has its own on-disk snapshot (the empty one
        // written at creation, or a later real one), so load it.
        co_await factory->create_existing(
          mgr, _raft, stm_instance_config{nullptr});
    }
}
```

The two factory methods keep the paths explicit: `create_new` (CREATE +
snapshot-install add-branch) writes the empty snapshot via
`register_created_stm`; `create_existing` (restart reconstruction) loads the
child's own on-disk snapshot via `register_reconstructed_stm`.

- [ ] **Step 4: Build**

Run: `bazel build //src/v/cluster/...`
Expected: success.

- [ ] **Step 5: Commit**

```bash
bazel run //tools:clang_format
git add src/v/cluster/supervisor_stm.h src/v/cluster/supervisor_stm.cc
git commit -s -m "cluster: implement supervisor_stm apply, snapshot, reconciliation"
```

---

### Task 13: `supervisor_stm_factory` + a creatable test child + integration test

**Files:**
- Modify: `src/v/cluster/supervisor_stm.h`, `src/v/cluster/supervisor_stm.cc` (factory)
- Modify: `src/v/cluster/tests/supervisor_stm_test.cc` (integration test with a creatable test child factory)
- Modify: `src/v/cluster/tests/BUILD`

**Interfaces:**
- Consumes: `supervisor_stm`, `state_machine_factory`, `state_machine_registry`, `raft_fixture`.
- Produces: `cluster::supervisor_stm_factory` (static factory) + the split `create_new`/`create_existing` factory methods finalized.

- [ ] **Step 1: Add `supervisor_stm_factory`**

In `supervisor_stm.h`:

```cpp
class supervisor_stm_factory : public state_machine_factory {
public:
    explicit supervisor_stm_factory(state_machine_registry& registry)
      : _registry(registry) {}
    bool is_applicable_for(const storage::ntp_config&) const override;
    void create(
      raft::state_machine_manager_builder&,
      raft::consensus*,
      const stm_instance_config&) override;
    std::string_view type_id() const override { return supervisor_stm::name; }

private:
    state_machine_registry& _registry;
};
```

In `supervisor_stm.cc` implement `is_applicable_for` (return true for partitions
that should host a supervisor — for the test, gate on a config/ntp; keep it
conservative) and `create` (`builder.create_stm<supervisor_stm>(raft, clusterlog, _registry);`).

- [ ] **Step 2: Write the failing integration test**

In `supervisor_stm_test.cc`, add a `raft_fixture`-based test that registers a
`supervisor_stm` plus a creatable test-child factory in a registry, replicates a
CREATE command via `supervisor_stm::make_create_command`, and asserts the child
is registered and applies only after the create offset; then a REMOVE and assert
deregistration. Reuse `creatable_kv` from `stm_test_fixture.h` exposed via a
small test `state_machine_factory` whose `is_creatable()` returns true and whose
`create_new`/`create_existing` build a `creatable_kv` and call the matching
manager register method.

```cpp
TEST_F_CORO(supervisor_fixture, create_and_remove_child_end_to_end) {
    // build registry: creatable child factory only (supervisor added per-node)
    // create_nodes(); start each node with a supervisor_stm + the registry;
    // replicate make_create_command("creatable_kv"); wait_for_apply;
    // assert get<creatable_kv>() != nullptr on all nodes;
    // replicate make_remove_command("creatable_kv"); wait_for_apply;
    // assert get<creatable_kv>() == nullptr on all nodes.
}
```

(Model the fixture on `state_machine_fixture`; supply the registry to the
`supervisor_stm` constructor.)

- [ ] **Step 3: Run to verify it fails, then implement `create_new`/`create_existing`**

Finalize the factory methods from Task 10/12: `create_new` builds the STM and
calls `mgr.register_created_stm(type_id, stm, creation_offset)`; `create_existing`
builds it and calls `mgr.register_reconstructed_stm(type_id, stm)`.

Run: `bazel test //src/v/cluster/tests:supervisor_stm_test`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
bazel run //tools:clang_format
git add src/v/cluster/supervisor_stm.h src/v/cluster/supervisor_stm.cc \
        src/v/cluster/tests/supervisor_stm_test.cc src/v/cluster/tests/BUILD
git commit -s -m "cluster: add supervisor_stm_factory and end-to-end create/remove test"
```

---

### Task 14: Truncation-safety + idempotency tests; full suite

**Files:**
- Modify: `src/v/raft/tests/stm_dynamic_membership_test.cc`

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Write the truncation-safety test**

```cpp
TEST_F_CORO(state_machine_fixture, child_blocks_truncation_until_caught_up) {
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<membership_provider_stm>(node->raft().get(), logger());
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }
    auto leader = co_await wait_for_leader(10s);
    co_await replicate_batch(make_supervisor_cmd("create", "creatable_kv"));
    auto state = co_await build_random_state(200, wait_for_each_batch::no);
    // Aggressively snapshot/evict on the leader while the child catches up.
    auto& l = node(leader);
    co_await l.raft()->write_snapshot(raft::write_snapshot_cfg(
      l.raft()->committed_offset(), iobuf{}));
    co_await wait_for_apply();
    // The child must have observed a contiguous tail of offsets > create_offset
    // with no gap (no data lost to truncation).
    co_await parallel_for_each_node([&](raft_node_instance& n) -> ss::future<> {
        auto child = n.raft()->stm_manager()->get<creatable_kv>();
        ASSERT_NE_CORO(child, nullptr);
        // applied offsets must be strictly increasing and contiguous by batch
        for (size_t i = 1; i < child->applied.size(); ++i) {
            ASSERT_GT_CORO(child->applied[i], child->applied[i - 1]);
        }
        co_return;
    });
}
```

- [ ] **Step 2: Write the idempotency test**

```cpp
TEST_F_CORO(state_machine_fixture, duplicate_create_remove_are_idempotent) {
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<membership_provider_stm>(node->raft().get(), logger());
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }
    co_await wait_for_leader(10s);
    co_await replicate_batch(make_supervisor_cmd("create", "creatable_kv"));
    co_await replicate_batch(make_supervisor_cmd("create", "creatable_kv"));
    co_await wait_for_apply();
    co_await parallel_for_each_node([&](raft_node_instance& n) -> ss::future<> {
        ASSERT_NE_CORO(n.raft()->stm_manager()->get<creatable_kv>(), nullptr);
        co_return;
    });
    co_await replicate_batch(make_supervisor_cmd("remove", "creatable_kv"));
    co_await replicate_batch(make_supervisor_cmd("remove", "creatable_kv"));
    co_await wait_for_apply();
    co_await parallel_for_each_node([&](raft_node_instance& n) -> ss::future<> {
        ASSERT_EQ_CORO(n.raft()->stm_manager()->get<creatable_kv>(), nullptr);
        co_return;
    });
}
```

- [ ] **Step 3: Run the full suite**

Run:
```bash
bazel test //src/v/raft/tests:stm_dynamic_membership_test \
           //src/v/raft/tests:persisted_stm_test \
           //src/v/raft/tests:stm_manager_test \
           //src/v/cluster/tests:supervisor_stm_test
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
bazel run //tools:clang_format
git add src/v/raft/tests/stm_dynamic_membership_test.cc
git commit -s -m "raft/tests: add truncation-safety and idempotency tests for dynamic STMs"
```

---

## Self-Review notes (for the executor)

- **Spec coverage:** every spec section maps to a task — primitives (Tasks 1-4),
  manager mechanism (Tasks 5-9), registry/factory (Task 10), supervisor_stm
  (Tasks 11-13), tests incl. truncation safety (Tasks 6-9, 13-14).
- **Reference implementations:** C++ blocks are reference implementations against
  the code as read on 2026-06-25; exact insertion points (line numbers) may drift
  — locate by the quoted surrounding code and adapt to compile. Treat the
  **Interfaces** blocks and **test code** as the contract; adjust bodies to the
  compiler/fixture as needed.
- **Open implementation decisions resolved here:** the factory exposes
  `create_new` (writes empty snapshot via `register_created_stm`) and
  `create_existing` (loads snapshot via `register_reconstructed_stm`); the
  `stm_hookset` remove key is the `snapshotable_stm` shared_ptr identity.
- **Fixture API caveat:** restart/add-node/snapshot helper names
  (`restart_node`, `add_node`, `write_snapshot_cfg`) must be confirmed against
  `raft/tests/raft_fixture.h` and `stm_manager_test.cc::test_recovery_from_snapshot`;
  adapt call shapes to the real signatures.
