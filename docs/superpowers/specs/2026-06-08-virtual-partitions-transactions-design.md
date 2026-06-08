# virtual partitions: transaction support

Date: 2026-06-08
Author: Evgeny Lazin (design captured collaboratively)
Status: approved-pending-review

## Motivation

Virtual partitions (VPs) let a single cloud-topic partition simulate many
thousands of logical Kafka partitions. A dedicated cloud-topic partition (the
**MTZ**) stores placeholder batches from many VPs interleaved; the actual data
lives in cloud-storage L0 objects uploaded by an external proxy. The
**leaderless frontend (LF)** owns VP metadata and replicates placeholders to the
MTZ through the normal write path minus the `write_pipeline` (L0 is already
uploaded). A proxy speaks the Kafka protocol to external clients and drives LF
through an admin API.

This document designs **transaction support** for VPs: specifically, what state
must exist for a Kafka transactional producer (through the proxy) to use
transactions correctly across VPs, and where that state lives. Idempotency
(client-facing sequence dedup) and transactional consumer-offset commits are
explicitly deferred (see Non-goals).

Key environment facts established during design:

- The MTZ is **sharded** into several cloud-topic partitions `MTZ_0..MTZ_k`,
  each a real Raft partition with its own `rm_stm` and `ctp_stm`.
- Each **VP is pinned to exactly one shard.** A VP never spans shards.
- A **transaction may span multiple VPs and therefore multiple shards**, but
  **never spans VPs and normal (non-VP) partitions** — a txn is all-VP or
  all-normal.
- `MTZ_i` offset of a placeholder batch == the VP offset of that batch on the
  physical log; per-VP offsets therefore have gaps. The LF hides the gaps from
  the client via per-VP **offset translation** (client-gapless ↔ shard offset).

## Scope

In scope:

- How an all-VP transaction is coordinated, made durable, committed/aborted, and
  read back under `read_committed`.
- The metadata the LF must maintain, and the state it delegates to existing
  components.
- The single new field required on the placeholder struct.
- The cloud-topic integration points that must tolerate transaction batches.

Out of scope / deferred:

- **Client-facing idempotency** (per-`(pid, VP)` sequence dedup of producer
  retries). The LF rewrites sequences for the MTZ (below); the client-facing
  dedup that this displaces is deferred to the idempotency work.
- **Transactional consumer-offset commits** (`AddOffsetsToTxn` /
  `TxnOffsetCommit`). Produce side only.
- The LF metadata storage mechanism (assumed durable; out of scope by
  requirement).
- The proxy and the LF admin API.

## Constraints

- **No changes to `rm_stm`.** Confirmed possible (see Design §4).
- **No new fields on `model::record_batch_header`.** The Kafka batch header
  format is fixed. Setting existing fields to chosen values is allowed.
- **The placeholder struct (`ctp_placeholder`) may gain fields.**
- **Reuse the existing transaction coordinator** (`tm_stm` /
  `tx_gateway_frontend`) unmodified.

## The decision that shapes everything: reuse the coordinator ⟹ one pid per txn

`tm_stm` carries exactly one producer id per transaction (`tx_metadata.pid`,
`src/v/cluster/tm_stm_types.h`) and drives `commit_tx(ntp, that_pid, tx_seq)` for
each enrolled ntp (`tx_gateway_frontend::commit_data` →
`rm_partition_frontend::commit_tx` → `rm_stm::commit_tx`). A real Kafka
transactional producer also has exactly one pid. Therefore the on-shard data and
markers must all be under that single pid, or the coordinator's `commit_tx`
finds no matching transaction on the shard and writes no marker.

An earlier candidate — relabeling each VP's slice with its own per-`(producer,
VP)` pid so each VP becomes an independent `rm_stm` producer — would have given
clean per-VP markers, sequences, and aborted ranges directly from `rm_stm`. But
it requires a pid *per participant*, which the existing coordinator cannot
express. **Reusing the coordinator and per-VP pids are mutually exclusive.** We
choose coordinator reuse, hence the **one-pid model**. The two facilities that
per-VP pids would have provided for free move into the LF translation layer:
sequence rewriting and marker fan-out (Design §5, §6).

This model is essentially "the client's pid passes through; each shard's
`rm_stm` tracks the producer's transaction" — correct here because each VP is
pinned to one shard and the coordinator participates per shard.

## Design

### 1. Identity (pid / epoch)

The client's pid+epoch are allocated by the existing cluster `id_allocator`
(fronted to the proxy via LF) and **pass through unchanged**: `id_allocator → LF
→ proxy → Kafka client`. There is no virtual↔real pid mapping. The same pid is
used:

- by the client on every produce, on every VP;
- in the placeholder header on every shard the txn touches;
- by the coordinator in `tx_metadata.pid` and in every `commit_tx`/`abort_tx`;
- in the `rm_stm` aborted-range entries and therefore in the consumer-visible
  `aborted_transactions` list.

Because the consumer-visible pid (taken from the placeholder header by the read
merge, see §6) equals the coordinator pid equals the aborted-list pid, all three
are automatically consistent. Fencing (epoch bumps, zombie rejection) is handled
entirely by the coordinator + per-shard `rm_stm`, unchanged.

### 2. Coordinator integration (proxy translates VP ntps → shard ntps)

The coordinator must only ever see **real** ntps. The proxy/LF performs the
translation at the protocol boundary:

- `InitProducerId(transactional.id)` → unmodified coordinator; returns the
  client pid/epoch.
- `AddPartitionsToTxn(vp_ntps)` → the proxy maps each VP ntp to its **shard**
  ntp and **dedups** (e.g. `VP1, VP7 → MTZ_0` registers `MTZ_0` once). The
  coordinator stores shard ntps in `tx_metadata.partitions`. The coordinator's
  per-partition `begin_tx` writes a `tx_fence` batch for the pid on each shard.
  The proxy fabricates the per-VP acks the client expects.
- `Produce` → see §3.
- `EndTxn(commit|abort)` → unmodified coordinator runs standard 2PC across the
  enrolled shards: durable decision (`tx_status` FSM), one marker per
  `(pid, shard)` via `commit_tx`/`abort_tx`, durable completion, crash recovery
  by re-driving (the partition-side ops are idempotent; the coordinator already
  relies on retrying them).

Because txns never span VP and normal partitions, the coordinator never sees a
mixed participant set; an all-VP txn's participants are exactly the MTZ shards
its VPs live on.

### 3. Produce path and per-`(pid, shard)` sequence rewriting

The proxy uploads the L0 object (the client's original batch, carrying the
client pid/epoch, the client's per-VP sequence, and the transactional bit) and
passes metadata to the LF. The LF builds a placeholder and replicates it to the
target shard via the normal cloud-topic write path minus `write_pipeline`.

On a shard, VP1's and VP7's batches share the one pid; their independent
client per-VP sequences interleave and would look out-of-order to the shard's
`rm_stm`. The LF therefore assigns a **monotonic sequence per `(pid, shard)`**
to the placeholder header (the LF is the sole writer to the shard, so it can
generate these deterministically). Consequences:

- `rm_stm` sees a clean monotonic sequence per pid and accepts the batch on its
  transactional replicate path.
- The value is harmless to consumers (consumers do not validate sequences); the
  read merge already takes `base_sequence` from the placeholder header.
- The client-facing dedup of producer retries that `rm_stm` would normally do on
  the client's sequence now belongs to the LF — deferred to idempotency work.

The placeholder header thus carries: client pid, client epoch, **LF-generated**
`base_sequence`, the transactional attr, and the matching offset/record-count
shape. `encode_placeholder_batch` (`src/v/cloud_topics/level_zero/stm/
placeholder.cc:17`) is extended to set the sequence from the LF rather than copy
it from the original header, and to stamp `virtual_partition_id` (§7).

### 4. Why `rm_stm` needs no change (per shard)

The placeholder batch is type `ctp_placeholder`, which `rm_stm::do_apply`
already routes exactly like `raft_data` (`src/v/cluster/rm_stm.cc:1789-1805`):
`tx_fence` → `apply_fence`; control attr → `apply_control` (commit/abort);
otherwise → `apply_data`. With the transactional attr + a real pid + a monotonic
sequence, the placeholder flows through `do_transactional_replicate` and is
tracked as ordinary transactional data. The coordinator's fence and marker
batches are native (`make_fence_batch` / `make_tx_control_batch`,
`src/v/cluster/rm_stm_types.cc`) and `rm_stm` applies them as today. The shard's
`rm_stm` thus computes the (shard-global) LSO (`rm_stm.cc:1273`) and records
aborted ranges (`tx_range{pid, first, last}`, `rm_stm.cc` `_aborted_tx_state`),
unmodified.

### 5. Marker fan-out (the price of one pid)

The coordinator writes exactly one marker per `(pid, shard)`, but every VP that
the txn touched on that shard needs to *see* that marker, or a `read_committed`
consumer never terminates abort-filtering for that pid.

When the native marker for `(pid, tx_seq)` lands on `MTZ_i`, the LF fans it into
the per-VP offset map of **each VP the txn touched on `MTZ_i`** — one shard
marker yields one client offset in each such VP. The VP-set is derivable from
the shard log between the txn's fence and its marker (the pid's placeholders in
that range, grouped by `virtual_partition_id`), or accumulated incrementally as
the LF replicates the txn's data. It is bounded by the number of *active* txns,
needs no per-VP state machine, and is reconstructable after restart from the
log.

At read time the LF serves the single native marker batch (already a valid Kafka
control record) at the fanned-in client offset in each touched VP. No
synthesis — just re-offsetting.

### 6. Offset translation rules

Per VP, the client-gapless offset sequence is built, in shard-log order, from:

- **data placeholders** with `virtual_partition_id == VP` — consume a client
  offset (and merge with L0 data on read);
- **commit/abort markers fanned into this VP** (§5) — consume a client offset,
  visible to the consumer as a control record.

Everything else is skipped (no client offset): other VPs' placeholders,
`tx_fence` batches, and any marker for a txn that did not touch this VP. A
shard's native marker is never assigned to a VP by its physical log position; it
reaches a VP only through the §5 fan-out, which places it in exactly the VPs the
txn touched. Note the partition's own Raft→Kafka translator does **not** skip
`tx_fence` — it is absent from `offset_translator_batch_types()`
(`src/v/model/record_batch_types.h:78`), so on the shard a fence consumes a
Kafka offset; LF nonetheless skips it in *per-VP* translation by its own choice,
since LF defines the per-VP mapping independently. (Earlier drafts wrongly
attributed the skip to the translator.) The exact fence/marker offset treatment
is confirmed by the verification spike,
`docs/superpowers/plans/2026-06-08-vp-transactions-spike.md`.

The marker assignment is fixed when the LF processes the marker (apply-time), so
client offsets are stable. The read merge (`apply_placeholder_to_batch`,
`placeholder.cc:77`) takes pid/epoch/sequence from the placeholder header, so
consumer-visible pid == client pid.

`read_committed` fetch for a VP:

1. translate the client fetch range → shard offset range;
2. data from VP-attributed placeholders → L0 → merge, limited by the per-VP LSO;
3. **LSO** = the shard's `rm_stm::last_stable_offset()` translated into the VP's
   client space (shard-global; see Risks);
4. **aborted_transactions** = `rm_stm::aborted_transactions(shard_range)`
   (`partition.cc:1560`) with each `{pid, first}` translated into the VP's
   client space; over-inclusion of pids with no data in this VP is harmless;
5. fanned-in markers (§5) interleaved at their client offsets so the consumer
   terminates abort-filtering.

### 7. Placeholder struct change

`ctp_placeholder` (`src/v/cloud_topics/level_zero/stm/placeholder.h`) gains one
field:

```cpp
struct ctp_placeholder : serde::envelope<...> {
    object_id id;
    first_byte_offset_t offset;
    byte_range_size_t size_bytes;
    model::partition_id virtual_partition_id;   // NEW: which VP this batch belongs to
    auto serde_fields() { return std::tie(id, offset, size_bytes, virtual_partition_id); }
};
```

This is the only struct change required for transactions. Markers and fences are
native batches with no placeholder payload; they are attributed to VPs by the
fan-out in §5, not by a placeholder field. (`virtual_partition_id` is foundational
to the VP feature generally, not transactions specifically.)

## Metadata maintained by the LF

For transactions, the LF maintains only the **routing / translation** layer:

1. **VP → shard assignment** (which `MTZ_i` hosts each VP).
2. **Per-VP offset translation** map (client-gapless ↔ shard offset), including
   the fanned-in marker entries from §5.
3. **Per-`(pid, shard)` monotonic sequence counter** for placeholder sequence
   rewriting (§3).
4. **(transient, derivable) per-active-txn VP-set per shard** used for marker
   fan-out (§5) — reconstructable from the shard log, so storage is optional.

### State explicitly delegated, NOT maintained by the LF

- **Coordinator (`tm_stm` / `tx_gateway_frontend`):** transactional.id↔pid/epoch
  registry, `tx_seq`, the transaction status FSM, the enrolled (shard) partition
  set, the durable commit/abort decision, and crash recovery.
- **Per-shard `rm_stm`:** per-pid transaction begin offset, LSO, aborted ranges,
  fence/marker application, and producer fencing.

This is the payoff of reuse: the LF holds no transaction state machine, only
translation/identity-routing data.

## End-to-end flows (summary)

- **InitProducerId** → coordinator → pid/epoch (passes through to client).
- **AddPartitionsToTxn(VPs)** → proxy maps→shards, dedups, registers shards;
  coordinator `begin_tx` writes a fence per shard; proxy fabricates per-VP acks.
- **Produce(VP)** → proxy uploads L0; LF replicates placeholder (client pid,
  LF `(pid,shard)` sequence, txn attr, `virtual_partition_id`) to the shard;
  shard `rm_stm` tracks it transactionally.
- **EndTxn(commit)** → coordinator 2PC: prepare (durable) → one commit marker per
  shard → complete (durable). LF fans each shard marker into the touched VPs'
  offset maps.
- **EndTxn(abort)** → same, abort markers; shard `rm_stm` records aborted ranges.
- **Fetch(VP, read_committed)** → per §6.

## Rejected alternatives

- **Per-`(producer, VP)` pid + custom LF coordinator.** Cleanest per-VP markers /
  sequences / aborted ranges straight from `rm_stm`, but requires the LF to build
  and operate a durable distributed-transaction coordinator (2PC + recovery +
  per-txn-id ownership). Rejected in favor of reusing the proven coordinator.
- **Opaque placeholders, LF owns the entire per-VP transaction state machine.**
  `rm_stm` uninvolved in txns; LF reimplements per-VP LSO + aborted ranges +
  markers + fencing. Maximum LF code; rejected.
- **Single unsharded MTZ.** One `rm_stm` LSO for the whole partition couples
  `read_committed` liveness across all VPs, and one marker per txn cannot serve
  multiple VPs. Sharding both bounds the LSO blast radius and is required for
  scale.

## Costs, risks, mitigations

- **`read_committed` liveness coupling (accepted).** A shard's LSO is a single
  value; one open txn on a shard stalls `read_committed` for all VPs on that
  shard. Mitigation: sharding bounds the blast radius (more shards → smaller
  coupling); the user accepts residual coupling.
- **Marker fan-out + sequence rewriting** add moderate read/write-path logic in
  the LF (bounded by active txns); not a per-VP state machine.
- **Write amplification:** a fence + a marker per `(pid, shard)` per txn, across
  the shards a txn touches — same shape as Kafka's per-partition overhead.
- **Cloud-topic integration (largest implementation risk):** the shard log now
  mixes placeholders with native `tx_fence` and commit/abort batches. `ctp_stm`,
  the reconciler / L0→L1 path, and the LF read path must tolerate non-placeholder
  batches (they carry no L0 payload), and `begin_tx`/`commit_tx`/`abort_tx` must
  function on a cloud-topic partition (they replicate native batches via Raft).

## Open questions to verify during implementation

1. Confirm the exact set of batch types skipped by Kafka offset translation, in
   particular that `tx_fence` does not consume a client offset while commit/abort
   markers do.
2. Confirm a cloud-topic partition accepts native (non-placeholder) replicates
   for fence/marker batches, and that `ctp_stm` / reconciler / read path tolerate
   them end-to-end.
3. Confirm `rm_stm::commit_tx`/`abort_tx` are safely re-drivable for coordinator
   recovery against a cloud-topic partition.
4. Validate the proxy's `AddPartitionsToTxn` dedup + fabricated per-VP acks
   against real Kafka clients.
5. End-to-end `read_committed` correctness test with a real Kafka client across a
   multi-shard, multi-VP transaction (commit and abort), verifying abort-filtering
   terminates via fanned-in markers.
