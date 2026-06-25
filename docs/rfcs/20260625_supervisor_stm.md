- Feature Name: supervisor_stm — dynamically creating and removing STMs at runtime
- Status: in-progress
- Start Date: 2026-06-25
- Authors: Evgeny Lazin
- Branch: `exp/hierarchical-stms`

# supervisor_stm: dynamically creating and removing STMs at runtime

## 1. Motivation

Today the set of state machines (STMs) running on a partition is fixed at
partition startup. `raft::state_machine_manager` is built once via
`state_machine_manager_builder::create_stm<T>()` (see
`raft/state_machine_manager.h`), and there is no mechanism to add or remove an
STM at runtime, nor to enable/disable one.

We want a new STM, derived from `raft::persisted_stm`, that can **create** and
**remove** other STMs on the same partition in response to replicated commands.
This is the foundation for "hierarchical STMs": a partition starts with a small
static set of STMs, one of which can spin up additional STMs on demand and
retire them later, with all transitions driven through the Raft log so every
replica converges to the same membership.

This first piece of work delivers the mechanism as **general-purpose
infrastructure**, validated with a throwaway test child STM. No production
child STM is wired up yet.

## 2. Terminology

- **`supervisor_stm`** — the new STM introduced here. It is an ordinary,
  statically-registered `persisted_stm` that applies create/remove commands and
  thereby manages the lifecycle of *child* STMs on the same partition. It is
  **not** related to the cluster-wide Redpanda controller
  (`controller`/`controller_stm`/`controller_backend`).
- **child STM** — an STM created dynamically by `supervisor_stm` in response to
  a CREATE command. A child is a normal `persisted_stm`.
- **creatable STM type** — an STM type registered as eligible to be created
  dynamically. Identified by its STM `name`.

## 3. Goals / Non-goals

**Goals**

- A `supervisor_stm` (derived from `persisted_stm`) integrated with
  `raft::state_machine_manager`.
- Two replicated operations:
  - **Create STM** — a command encoding the STM type. On apply, a new child STM
    of that type is created, started, and begins applying commands with offsets
    **strictly greater** than the offset of the CREATE command.
  - **Remove STM** — a command encoding the STM type. On apply, the targeted
    child applies all commands up to the REMOVE offset, then is deregistered,
    stopped, and its on-disk snapshot removed.
- A primitive that writes a **snapshot at a given offset with empty state** so a
  freshly created child recovers through the normal snapshot-load path and
  starts at `creation_offset + 1`.
- Correct interaction with **log truncation/eviction**: a child's start offset
  must pin the truncation floor from the instant the CREATE command is applied.
- Full support for the Raft **`managed_snapshot`** install / fast-reconfiguration
  path with dynamic membership.

**Non-goals (this iteration)**

- Wiring up any production child STM type.
- Multiple concurrent instances of the same child type on one partition (see
  §4 decision: at most one instance per type).
- A general enable/disable (pause) API beyond create/remove.

## 4. Key decisions

These were settled during design and constrain the implementation:

1. **Integration model: extend `state_machine_manager`.** Children are
   first-class STMs in the manager's apply loop and `managed_snapshot`, rather
   than being hidden inside a composite STM. `supervisor_stm` reaches the
   manager via `_raft->stm_manager()`.
2. **Type → instance mapping: reuse `cluster::state_machine_registry`.** The
   existing factory concept is extended (rather than introducing a separate
   "creatable STM" registry).
3. **Instance identity: at most one child per type.** A child is keyed by its
   STM `name`, consistent with how `_machines` and `managed_snapshot` are keyed
   today. CREATE of an already-active type is an idempotent no-op; REMOVE
   targets the type.
4. **Recovery source of truth: `supervisor_stm`.** It persists the active child
   set in its own snapshot and rebuilds it by replaying create/remove commands.
   The manager holds no persisted membership of its own; the Raft log is the
   single source of truth.
5. **Command model: two generic commands parameterized by type id.**
   `supervisor_stm` owns one `record_batch_type`; a CREATE record carries the
   type id, a REMOVE record carries the type id. The type id is the STM `name`.
6. **Empty-snapshot primitive: generic method on `persisted_stm_base`,
   `write_empty_snapshot_at(offset)`**, reusing the STM's existing
   `take_local_snapshot` over empty state. No per-STM boilerplate; any
   `persisted_stm` whose `take_local_snapshot` works on empty state qualifies.
7. **Raft snapshot scope: full support now, with supervisor-first ordering** on
   install.

## 5. Architecture overview

A child must be registered in **two** independent registries:

- `raft::state_machine_manager::_machines` — drives the apply loop and the
  `managed_snapshot`.
- `storage::stm_hookset::_stms` — gates log truncation. Before truncating at
  `truncation_point`, `consensus` calls
  `stm_hookset()->ensure_snapshot_exists(truncation_point)`
  (`raft/consensus.cc:4355`), which for each registered STM does
  `wait(truncation_point)` then snapshots (`persisted_stm.cc:354`). An STM in
  the hookset therefore blocks truncation until it has applied + snapshotted
  past the truncation point; an STM **not** in the hookset gets no such
  protection. STMs are added to the hookset today by their factories
  (`add_stm`, e.g. `cluster/rm_stm.cc:2508`). The hookset currently has no
  remove method.

This two-registry reality drives the central correctness requirement: **a
child's start offset must pin the truncation floor at the instant the CREATE
command is applied**, with no gap. Hence CREATE is enacted synchronously inside
`do_apply`; deferring it would allow truncation to advance past offsets the new
child must still apply.

### Components

- **`cluster::supervisor_stm`** (new) — a `persisted_stm`. Owns one
  `record_batch_type`. Applies CREATE/REMOVE. Holds a reference to the creatable
  factories (via the registry) and reaches the manager via `_raft->stm_manager()`.
  Persisted state: the active child set (`{type_id, creation_offset}`).
- **`cluster::supervisor_stm_factory`** (new) — statically registered like
  `rm_stm_factory`, so every applicable partition gets a `supervisor_stm`.
- **`raft::state_machine_manager` extensions** — apply-context register /
  deregister entry points, a REMOVE drain step, start-time child
  reconstruction, two-phase supervisor-first snapshot install.
- **`raft::persisted_stm_base::write_empty_snapshot_at(offset)`** (new) — the
  empty-snapshot primitive plus a "construct-as-created-at-offset" init path.
- **`storage::stm_hookset::remove_stm(...)`** (new).
- **`cluster::state_machine_registry` / `state_machine_factory` extensions** —
  stable `type_id()` (= STM `name`), lookup-by-type-id, a create-and-register
  path against a live manager, and a "creatable" flag.

### Layering

`supervisor_stm` lives in the `cluster` layer (it needs the registry to
construct children). The register/deregister mechanism and
`write_empty_snapshot_at` live in the `raft` layer (generic). `stm_hookset`
lives in `storage`. `cluster` depends on `raft` depends on `storage`, so the
dependency direction is preserved.

## 6. Command model & data flow

`supervisor_stm` owns a dedicated `model::record_batch_type` (new enum value).
Each command is a single-record batch, serialized with `serde` (versioned
envelope):

- **CREATE**: payload `{ type_id }`. Creation offset = the command batch offset.
- **REMOVE**: payload `{ type_id }`.

### CREATE flow — applying CREATE at offset `X`

Enacted **synchronously** inside `supervisor_stm::do_apply(X)` (the apply loop
already holds the manager's `_apply_mutex`):

1. Decode `create{type_id}`. If `type_id` is unknown / not creatable: this is a
   committed command we cannot honor — log loudly and (likely) `vassert`. If
   `type_id` is already active: idempotent no-op (covers re-apply after
   restart).
2. Construct the child via the factory. Initialize it as "created at `X`": mark
   it hydrated, `set_next(X+1)`, and `write_empty_snapshot_at(X)` to persist an
   empty local snapshot at `X` (`_last_snapshot_offset = X`).
3. `add_stm` into `stm_hookset`. From this instant, `ensure_snapshot_exists`
   will `wait` for the child before any truncation past `X`.
4. Insert into `state_machine_manager::_machines` via the apply-context register
   entry point (which must **not** re-acquire `_apply_mutex`).
5. Record `{type_id, X}` in the supervisor's in-memory active set.

The child is absent from the current `batch_applicator` pass (the applicator
iterates a local vector built at `try_apply_in_foreground` lines 500–513), which
is correct — it must not apply batch `X`. The child then catches up
`X+1 … _next` via the normal background-apply fiber.

### REMOVE flow — applying REMOVE at offset `Y` for `type_id`

Enacted via a **drain step** at the top of `try_apply_in_foreground` (after
acquiring `_apply_mutex`, before selecting machines), because a child cannot be
`stop()`ed while it is concurrently applying the current batch. Relaxing the
truncation floor carries no data-loss risk, so the small deferral is safe.

1. `do_apply(Y)` decodes `remove{type_id}`. If not active: idempotent no-op.
   Otherwise remove `type_id` from the active set and enqueue a removal request.
2. At the next drain: `remove_stm` from the hookset, `stop()` the child, erase
   from `_machines`, delete its on-disk local snapshot
   (`remove_persistent_state()`).

By the manager's batch ordering, the child has already applied every batch
`< Y` before the supervisor processes `Y`. The REMOVE batch uses the
supervisor's `record_batch_type`, so the child no-op-applies it.

**Invariant (documented):** once a REMOVE command for a type is written to the
log, no further commands for that type may be written. The supervisor owns the
lifecycle, so this holds; it is what makes "apply up until the REMOVE offset"
crisp.

Both commands are idempotent on re-apply (restart / replay safety).

## 7. `state_machine_manager` extensions & concurrency

**New apply-context entry points** (callable only from within an STM's
`do_apply`, where `_apply_mutex` is already held — they must not re-acquire it):

- register: write nothing to `_apply_mutex`; `add_stm` to hookset, insert into
  `_machines`, recompute `_supports_snapshot_at_offset`, `co_await stm->start()`
  when reconstructing (on the CREATE path the child is already initialized via
  `write_empty_snapshot_at`).
- deregister request: enqueue for the drain step.

**Drain step** (top of `try_apply_in_foreground`, under `_apply_mutex`, no
active consume, no STM mid-apply): enact pending removals
(`remove_stm` + `stop` + erase + delete snapshot).

**Why `_machines` mutation here is safe:** `batch_applicator` iterates its own
local `apply_state` vector; the snapshot/`take_snapshot` paths and `wait`
iterate `_machines` only while holding `_apply_mutex`; the apply loop is a
single fiber, so `maybe_start_background_apply` (which iterates `_machines`)
never runs concurrently with a foreground consume. Background-apply fibers
operate on a single captured `entry_ptr` and do not iterate `_machines`.

**`stm_hookset` add/remove safety:** `add_stm`, the new `remove_stm`, and
`max_removable_local_log_offset` are all synchronous (no `co_await`), so
seastar's cooperative scheduling makes them mutually atomic on a shard. No new
locking is required.

## 8. `persisted_stm` extension

`write_empty_snapshot_at(model::offset o)`:

- Sets the next offset so `last_applied() == o` (`set_next(o+1)`).
- Persists a local snapshot via the existing `take_local_snapshot` over the
  STM's current (empty) state, with header offset `o`, updating
  `_last_snapshot_offset`.
- A "construct-as-created-at-offset" init path marks the STM hydrated so
  subsequent normal operations behave correctly without a full `start()` replay.

On restart the child loads this snapshot through the normal path
(`load_local_snapshot`), yielding `next = o + 1`.

Requirement on creatable child STMs: their `take_local_snapshot` /
`apply_local_snapshot` must round-trip empty state. This is the "the STM being
created must support this operation" constraint.

## 9. `storage::stm_hookset` extension

Add `remove_stm(...)` (by `shared_ptr` identity or by name). Synchronous, no
`co_await`. Used by the REMOVE drain step.

## 10. `state_machine_registry` / factory extensions

- `state_machine_factory` gains a stable `type_id()` returning the STM `name`,
  and a flag indicating whether the type is **creatable** dynamically.
- A create-and-register path that targets a **live** `state_machine_manager`
  (today `create()` only targets a `state_machine_manager_builder`).
- `state_machine_registry` gains lookup-by-type-id so `supervisor_stm` can map a
  command's `type_id` to the right factory.

`supervisor_stm` holds a reference to the registry (passed in by
`supervisor_stm_factory`, which captures it at partition startup).

## 11. Recovery

### A. Restart (local-snapshot path)

`manager.start()` ordering (mutating `_machines` during
`parallel_for_each(_machines, …)` is UB, so reconstruction happens after it):

1. `parallel_for_each(_machines, start)` starts the static STMs.
   `supervisor_stm::start()` loads its own local snapshot, learns the active
   child set, constructs each child (it holds the registry reference), and
   **enqueues** them (no `_machines` mutation yet).
2. Manager computes `_next` and runs `apply_initial_recovery_policy` over the
   **static** STMs only.
3. Manager drains the enqueued child registrations: `add_stm` to hookset, insert
   into `_machines`, `co_await child->start()` (each child loads its own on-disk
   snapshot → `next = snapshot_offset + 1`). Children are deliberately **kept
   out of** `initial_recovery_policy`; they have a definite start offset and
   catch up `snapshot+1 … _next` via background apply.
4. Spawn the apply loop.

Children join the hookset before the apply loop / eviction activate, so there is
no truncation gap on restart.

### B. Raft `managed_snapshot` install (supervisor-first)

`do_apply_raft_snapshot` (`raft/state_machine_manager.cc:408`) becomes
two-phase:

1. **Supervisor portion first.** The manager identifies the membership-providing
   STM via a new virtual `state_machine_base::provides_dynamic_membership()`
   (default `false`, overridden by `supervisor_stm`) and applies *its* portion
   before the others. The supervisor's `apply_raft_snapshot` **reconciles**:
   construct + register children present in the snapshot but not yet registered
   (into hookset + `_machines`); deregister children registered but absent from
   the snapshot. This runs under `_apply_mutex` (install happens inside
   `try_apply_in_foreground` / the background apply fiber). Unlike a steady-state
   REMOVE, teardown here is performed **synchronously** during install: no batch
   application is concurrent with a snapshot install, so a child being removed
   cannot be mid-apply and the drain-step deferral (§7) is unnecessary on this
   path.
2. **Remaining portions.** The manager re-collects `_machines` (now including the
   freshly-registered children) and applies each remaining `name → iobuf`
   portion via `apply_snapshot_to_stm`, setting every child's
   `next = last_included + 1`.

### C. `take_raft_snapshot(offset)` (fast reconfig)

Once children are first-class in `_machines`, they are already included in the
snapshot map. `supervisor_stm::take_raft_snapshot` additionally serializes the
active child set so install (B) can reconstruct them.

## 12. Testing

Test child STM: a minimal `persisted_stm` modeled on `simple_kv`
(`raft/tests/stm_test_fixture.h`) that records the offsets it applied, declares a
stable `name`, and is registered via a creatable factory. Lives in test code.

Tests, built on the `raft_fixture` multi-node harness
(`raft/tests/raft_fixture.h`) and `stm_manager_test.cc` patterns:

1. **Create** — replicate CREATE@X; assert child registered in `_machines` +
   hookset, applied only offsets `> X`, empty local snapshot exists at `X`.
2. **Remove** — replicate REMOVE@Y; assert child applied through `Y-1`, then
   deregistered from both registries, stopped, snapshot file deleted.
3. **Restart recovery** — create child, replicate data, restart; assert
   supervisor reconstructs child, child resumes from its snapshot (no replay
   from 0, no missed offsets).
4. **Truncation safety (core concern)** — create child, force
   eviction/`write_snapshot`; assert truncation blocks at the child's applied
   offset via `ensure_snapshot_exists` and the child loses no data.
5. **Raft snapshot install** — build a `managed_snapshot` containing a dynamic
   child on one node, install on a lagging/fresh node; assert supervisor-first
   reconstruction + child receives its portion (`next = last_included+1`); cover
   reconcile-remove (child absent from snapshot gets deregistered).
6. **Idempotency** — re-apply CREATE/REMOVE (replay) → no double-register, no
   crash.
7. **`supervisor_stm` unit tests** — command encode/decode, active-set snapshot
   round-trip.

## 13. Change inventory

**New**

- `src/v/cluster/supervisor_stm.{h,cc}` — the STM, command types
  (`record_batch_type`), active-set local snapshot, create/remove apply logic,
  restart + raft-snapshot reconciliation.
- `cluster::supervisor_stm_factory` + a new `model::record_batch_type` enum
  value.
- Test child STM + tests (under `src/v/raft/tests/` and/or
  `src/v/cluster/tests/`).

**Modified — raft**

- `state_machine_manager.{h,cc}` — apply-context register/deregister entry
  points (no `_apply_mutex` re-acquire), REMOVE drain step in
  `try_apply_in_foreground`, start-time child-reconstruction drain, two-phase
  supervisor-first `do_apply_raft_snapshot`.
- `state_machine_base.h` — `provides_dynamic_membership()` virtual (default
  `false`).
- `persisted_stm.{h,cc}` — `write_empty_snapshot_at(offset)` +
  construct-as-created-at-offset init path.

**Modified — storage**

- `storage/types.h` (`stm_hookset`) — add `remove_stm(...)`.

**Modified — cluster**

- `state_machine_registry.h` / `state_machine_factory` — stable `type_id()`
  (= `name`), lookup-by-type-id, create-and-register-against-live-manager path,
  "creatable" flag.

## 14. Invariants & assumptions

- At most one child per type per partition.
- After a REMOVE command for a type, no further commands for that type are
  written to the log.
- A creatable child STM's `take_local_snapshot` / `apply_local_snapshot`
  round-trips empty state.
- CREATE is enacted synchronously at apply time so the truncation floor is
  pinned with no gap; REMOVE teardown is deferred to a safe drain point.
- Apply-context register/deregister entry points are called only while
  `_apply_mutex` is held and therefore never re-acquire it.

## 15. Open questions for the implementation plan

- Exact `remove_stm` lookup key in `stm_hookset` (pointer identity vs. name).
- Whether `write_empty_snapshot_at` is best expressed as a single method or a
  small "created-at" init helper plus the existing snapshot write.
- Precise placement of the REMOVE drain and start-time reconstruction drain so
  they compose cleanly with the existing `apply()` / `try_apply_in_foreground`
  structure.
- How `provides_dynamic_membership()` STMs are ordered when (hypothetically)
  more than one exists; for now exactly one (`supervisor_stm`).
