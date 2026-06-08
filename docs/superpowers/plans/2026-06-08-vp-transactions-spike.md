# VP transactions — verification spike

> **For agentic workers:** this is a *spike* (time-boxed investigation), not a
> feature implementation. Tasks are code-reading findings and throwaway probe
> tests whose output is *knowledge* + spec corrections, not production code.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** De-risk the three "verify against the running system" open questions in
`docs/superpowers/specs/2026-06-08-virtual-partitions-transactions-design.md`
before committing to a full implementation plan.

**Approach:** A prior code recon already resolved items 1 and 3 and strongly
de-risked item 2 (see "Already established"). The remaining live questions need
one combined `rm_stm` + `ctp_stm` probe fixture and an offset-inspection probe.

**Time-box:** ~1–1.5 days. If the commit/abort probes pass and the offset
behavior is as expected, proceed to the implementation plan. If the probe
surfaces a crash or a read-path that can't represent per-VP markers, escalate
back to design.

---

## Already established by code recon (no experiment needed)

Record these in the spec; they do not need a probe.

- **Offset translator filter set** = `model::offset_translator_batch_types()`
  (`src/v/model/record_batch_types.h:78-88`): `raft_configuration`,
  `archival_metadata`, `version_fence`, `prefix_truncate`,
  `partition_properties_update`, `datalake_translation_state`, `group_block`,
  `ctp_stm_command`. Chosen per-partition at log creation via
  `raft::offset_translator_batch_types(ntp)` (`src/v/raft/consensus.cc:86-93`),
  which returns this set for the kafka namespace and `{}` otherwise.
  - `tx_fence` (value 10) is **not** filtered → **consumes a Kafka offset.**
    ⚠️ **Spec correction:** §6 wrongly claimed the translator skips `tx_fence`.
    It does not. (Design impact below.)
  - commit/abort markers (`raft_data` + control attr) are **not** filtered →
    consume a Kafka offset and are visible as control records. Matches spec.
  - `ctp_placeholder` is not filtered → consumes a Kafka offset. Matches spec.
- **Cloud-topic components already tolerate native transaction batches:**
  - `ctp_stm::do_apply` early-returns for any type other than `ctp_placeholder`
    / `ctp_stm_command` (`src/v/cloud_topics/level_zero/stm/ctp_stm.cc:275-280`).
  - The L0 reader preserves `raft_data` (incl. control/markers) batches
    (`src/v/cloud_topics/level_zero/reader/.../level_zero_reader.cc:299-303` and
    `:485-490`).
  - The reconciler reads through `kafka::read_committed_reader`
    (`src/v/cloud_topics/reconciler/reconciliation_source.cc:152-164`).
  - The frontend already passes transaction control batches through to the local
    raft log unchanged (`src/v/cloud_topics/frontend/frontend.cc:1286-1292`).
  - Both `rm_stm` and `ctp_stm` are applicable to a cloud-topic partition
    (`rm_stm_factory` / `ctp_stm_factory::is_applicable_for`).
- **Coordinator participant ops are raw-raft + idempotent:** `do_begin_tx`,
  `do_commit_tx`, `do_abort_tx` call `_raft->replicate()` directly
  (`src/v/cluster/rm_stm.cc:448,613,783`); commit/abort detect already-resolved
  state and return success without a duplicate marker (`rm_stm.cc:560-608`,
  `:726-779`); the coordinator retries (`tx_gateway_frontend.cc:2157-2272`).

### Design impact of the `tx_fence` correction

LF builds its **own** per-VP offset translation; it is free to skip the
`tx_fence` in each VP's client-offset space regardless of what the partition's
raft→kafka translator does. So the spec's *conclusion* (the fence does not occupy
a VP client offset) can stand — but the *justification* must change: it's LF's
own translation choice, **not** because the raft→kafka translator skips
`tx_fence` (it doesn't). The fence is one native batch per `(pid, shard)`, shared
across the VPs of the txn; the offset-inspection probe (Task 4) confirms what a
`read_committed` reader does with fence/marker offsets and finalizes whether the
fence must be skipped, assigned to one VP, or fanned out like the marker.

---

## Probe harness (the only real build cost)

There is **no existing test that puts `rm_stm` and `ctp_stm` on one partition** —
the spike must create one. The variadic `raft::stm_raft_fixture<STM...>`
(`src/v/raft/tests/raft_fixture.h`) supports it. Crib STM construction and
dependency wiring from:

- `src/v/cloud_topics/level_zero/stm/tests/ctp_stm_test.cc` — `ctp_stm_fixture`
  (`raft::stm_raft_fixture<ct::ctp_stm>`, gtest, 3-node), helpers
  `make_record_batch`, `replicate_record_batch`, `replicate_with_epoch`.
- `src/v/cluster/tests/rm_stm_test_fixture.h` — `rm_stm` dependency setup
  (`producer_state_manager`, `_feature_table`, `tx_gateway_frontend`,
  `producer_expiration_ms`, `max_concurent_producers`) and the
  `begin_tx`/`commit_tx`/`abort_tx`/`aborted_transactions` call patterns
  (`src/v/cluster/tests/rm_stm_tests.cc:148-175`).

---

## Task 1: combined `rm_stm` + `ctp_stm` probe fixture

**Files:**
- Create: `src/v/cloud_topics/level_zero/stm/tests/vp_txn_spike_test.cc`
- Modify: `src/v/cloud_topics/level_zero/stm/tests/BUILD` (add a
  `redpanda_cc_gtest` target `vp_txn_spike_test` depending on the `ctp_stm`,
  `rm_stm`, and raft test-fixture libs; copy deps from the existing
  `ctp_stm_test` target and add `//src/v/cluster:rm_stm` + the rm_stm test deps).

- [ ] **Step 1: Define the fixture** subclassing
  `raft::stm_raft_fixture<cluster::rm_stm, ct::ctp_stm>`. In `create_stms`,
  create both: `builder.create_stm<cluster::rm_stm>(logger, node.raft().get(),
  tx_gateway_frontend, _feature_table, producer_state_manager, std::nullopt)`
  and `builder.create_stm<ct::ctp_stm>(ct::cd_log, node.raft().get())`. Wire the
  `rm_stm` sharded dependencies in the fixture ctor/`start()` exactly as
  `rm_stm_test_fixture` does (copy verbatim; confirm exact signatures there).
  Call `enable_offset_translation()` (as `ctp_stm_fixture::start` does) so the
  raft→kafka translator is active.

- [ ] **Step 2: Add a `make_txn_placeholder` helper** that builds a
  `ctp_placeholder` batch carrying a producer identity + the transactional attr +
  a chosen `base_sequence`:

```cpp
// Build a header like a client's transactional data batch, then wrap it.
storage::record_batch_builder b(model::record_batch_type::raft_data, model::offset{0});
b.set_producer_identity(pid.id, pid.epoch);
b.set_transactional_type();
b.add_raw_kv(iobuf{}, iobuf{} /* one record */);
auto data_hdr = std::move(b).build().header();
data_hdr.base_sequence = seq;                 // LF-generated per-(pid,shard) seq
cloud_topics::extent_meta ext{ /* object_id, byte range, base/last offset */ };
auto ph = cloud_topics::encode_placeholder_batch(data_hdr, ext);
// (encode_placeholder_batch copies pid/epoch/txn-attr; sequence comes from the header)
```

Confirm `encode_placeholder_batch`'s exact behavior at
`src/v/cloud_topics/level_zero/stm/placeholder.cc:17` (it copies pid/epoch + txn
attr; the spec wants LF to set `base_sequence`).

- [ ] **Step 3: Build it** — `bazel build
  //src/v/cloud_topics/level_zero/stm/tests:vp_txn_spike_test`. Expected: compiles
  (this proves the two STMs co-instantiate on one partition).

## Task 2: probe — commit path tolerated end-to-end

- [ ] **Step 1: Write the probe** `TEST_F_CORO(vp_txn_spike, commit_tolerated)`:
  1. `co_await start(); co_await wait_for_leader(...)`.
  2. `begin_tx(pid, tx_seq=0, timeout, partition_id{0})` on the leader's
     `rm_stm` → writes a `tx_fence`.
  3. replicate two `make_txn_placeholder(pid, seq=0)` and `(pid, seq=1)` batches
     (representing two VPs' data under the one pid) via the raft replicate path.
  4. `commit_tx(pid, tx_seq=0, timeout)` → writes a commit marker.
  5. `co_await` apply on all nodes.

- [ ] **Step 2: Assert no crash + state correct.** Expected observations:
  - neither STM `vasserts`; apply completes on all replicas;
  - `rm_stm.last_stable_offset()` advances past the marker (txn resolved);
  - `rm_stm.aborted_transactions(begin,end)` returns empty (committed);
  - `ctp_stm` state (start/last-reconciled offsets, size estimator) reflects only
    the two placeholders, ignoring fence/marker.

  Run: `bazel test //src/v/cloud_topics/level_zero/stm/tests:vp_txn_spike_test
  --test_arg=--gtest_filter='*commit_tolerated*'`.

- [ ] **Step 3: Record** pass/fail + any surprising apply behavior in Task 5.

## Task 3: probe — abort path + aborted-range visibility

- [ ] **Step 1: Write** `TEST_F_CORO(vp_txn_spike, abort_visible)`: same as Task 2
  steps 1–3 but call `abort_tx(pid, tx_seq=0, timeout)`.

- [ ] **Step 2: Assert:**
  - `rm_stm.aborted_transactions(begin,end)` returns one `tx_range{pid, first,
    last}` covering the two placeholders;
  - `last_stable_offset()` advances past the abort marker;
  - `ctp_stm` again reflects only the placeholders.

  Run: `... --test_arg=--gtest_filter='*abort_visible*'`.

- [ ] **Step 3: Record** the exact `first`/`last` offsets of the returned range
  (needed for the per-VP translation rules) in Task 5.

## Task 4: probe — fence/marker offset placement (design-deciding)

This resolves the `tx_fence` correction's open end and the marker fan-out
offset model.

- [ ] **Step 1: Write** `TEST_F_CORO(vp_txn_spike, offset_layout)` that, after a
  commit txn with two placeholders (two distinct `virtual_partition_id`s), dumps
  the raft log in order and, for each batch, prints: `base_offset`,
  `last_offset`, `type`, `is_control`, and the kafka offset via the partition's
  `offset_translator_state` (use the fixture's translation as `fetch.cc` does).

- [ ] **Step 2: Read a `read_committed` reader over the range** (wrap the log
  reader in `kafka::read_committed_reader`, as
  `reconciliation_source.cc:152-164` does) and print which batches it yields and
  at which kafka offsets — i.e. does the fence appear? does the marker appear?

- [ ] **Step 3: Record the answers** to:
  - Does `tx_fence` occupy a kafka offset here (expected: yes, per recon)?
  - Does a `read_committed` reader surface the fence and/or the marker?
  - Given one fence + one marker per `(pid, shard)` but two VPs, decide the LF
    per-VP rule: **skip fence in per-VP translation; fan the marker into each
    touched VP** (confirm this is representable, i.e. one shard batch → a
    client-offset slot in each VP's map), or document the deviation found.

## Task 5: write up findings and correct the spec

**Files:**
- Modify: `docs/superpowers/specs/2026-06-08-virtual-partitions-transactions-design.md`
- Create: `docs/superpowers/specs/2026-06-08-vp-transactions-spike-findings.md`

- [ ] **Step 1: Fix the spec** — in §6 replace the claim that the offset
  translator skips `tx_fence` with the corrected statement (translator does not
  skip it; LF's per-VP translation skips it by its own choice). Update open
  questions 1–3 to "resolved (see spike findings)" with one-line conclusions.

- [ ] **Step 2: Write the findings doc** — for each of items 1/2/3 and Task 4:
  the question, the evidence (file:line and/or probe result), the conclusion, and
  the implication for the implementation plan (e.g. "implementation plan needs a
  per-VP marker-fan-out apply step that observes the shard's native marker and
  appends a client-offset slot to each touched VP's translation map").

- [ ] **Step 3: Commit** spike test + findings + spec fix:

```bash
git add src/v/cloud_topics/level_zero/stm/tests/vp_txn_spike_test.cc \
        src/v/cloud_topics/level_zero/stm/tests/BUILD \
        docs/superpowers/specs/2026-06-08-virtual-partitions-transactions-design.md \
        docs/superpowers/specs/2026-06-08-vp-transactions-spike-findings.md
git commit -m "spike: verify VP transaction batches on a cloud-topic partition"
```

---

## Decision gates

- **All probes pass + offset layout as expected** → the integration is sound;
  proceed to the implementation plan. The marker fan-out becomes a concrete
  apply-time step in the plan.
- **A probe crashes (vassert) in `ctp_stm`/reader/reconciler** → a real
  integration gap; capture the exact site and return to design before planning.
- **`read_committed` reader cannot represent per-VP markers from one shared
  marker** → the marker fan-out can't be a pure read/translation concern;
  reconsider writing per-VP marker placeholders instead (revisit spec §5).

## Self-review

- Spec coverage: spike targets exactly the spec's open questions 1–5 (1–3 here;
  4–5 are proxy/Kafka-client integration, deferred to a later integration test,
  noted in the findings doc).
- No placeholders: probe steps reference concrete fixtures/helpers and exact
  run commands; signatures to be confirmed against the cited fixture files.
- Consistency: helper/type names (`make_txn_placeholder`, `encode_placeholder_batch`,
  `begin_tx`/`commit_tx`/`abort_tx`, `aborted_transactions`) are used uniformly.
