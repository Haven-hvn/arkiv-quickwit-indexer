# arkiv-quickwit-bridge

External daemon that pipes an [arkiv-op-reth] node's `EntityOperation` event
stream into a stock [Quickwit] cluster, making Quickwit the full-text /
ranking / aggregation "findability layer" on top of arkiv's authoritative,
Merkle-committed corpus. Implements `ARKIV_TO_QUICKWIT_INGESTION_DESIGN.md`.

**Strictly out-of-tree.** The bridge talks to Quickwit only through the
public HTTP surface (`/api/v1/{index}/ingest`, `/api/v1/{index}/delete-tasks`)
and to arkiv only through the standard `eth_*` / `arkiv_*` JSON-RPC
namespaces. Neither codebase is modified.

## How it works

```
arkiv-op-reth ──eth_getLogs/arkiv_query──► bridge ──HTTP ingest/delete-tasks──► Quickwit
                                             │
                                        SQLite state
                                 (cursor, reorg buffer, dedup)
```

- **Tailer** polls `eth_getLogs` for entity mutation events, trailing head
  by `confirmation_depth` blocks. Two on-chain schemas are supported via
  `arkiv.schema`: `entity_operation` (default — arkiv-op-reth's single
  `EntityOperation` event at `0x4400…0044`, with historical `atBlock`
  hydration) and `legacy` (deployed networks such as the hoodi testnet —
  six `ArkivEntityCreated/Updated/Deleted/Expired/BTLExtended/OwnerChanged`
  events at `0x…61726b6976`; those nodes ignore `atBlock`, so hydration
  reads head state, which is correct in tail-only mode). Verified live
  against `https://braga.hoodi.arkiv.network/rpc`.
  `start_block` controls the first launch: a number backfills from that
  block, `"head"` is tail-only mode — the bridge resolves the current chain
  height automatically and indexes forward only, never touching historical
  state (which also sidesteps retention limits entirely). Restarts resume
  from the persisted cursor either way, so downtime never creates a gap.
- **Hydrator** calls `arkiv_query($key = …, atBlock)` per mutation log and
  joins in the block timestamp.
- **Batcher/sender** POSTs ND-JSON batches to the ingest API with 429/5xx
  backoff.
- **Delete scheduler** maps `DELETE`/`EXPIRE` ops (and superseded versions of
  upserted entities) to Quickwit delete tasks. Deletes are persisted in
  SQLite before submission, so a crash cannot lose a retraction.
- **Reorg handling**: confirmation-depth trailing + a rolling 256-block hash
  buffer re-checked every iteration. On a fork: binary-search the fork
  point, enqueue retraction delete tasks, roll back the cursor, re-crawl.
  Reorgs deeper than `retract_max_depth` halt the daemon.
- **Sampling audits** re-fetch a random sample of emitted docs via
  `eth_getCode` (the trie-committed path) and verify the recorded payload
  hash appears in the entity's code bytes. Mismatch → halt.
- **Entity filtering** (`entity_filters`): index everything (default), or
  only entities carrying particular string annotations. Each rule is either
  a single condition (`string_annotation_key`, optionally pinned to a
  `string_annotation_value`) or an `all_of` list of conditions that must
  all hold on the same entity. Rules OR together. Updates that move an
  entity out of the filtered set tombstone its previously indexed docs;
  skips are counted in `arkiv_bridge_docs_skipped_filtered_total`.
- **Retention awareness**: arkiv entities carry a BTL/expiry, and nodes are
  not expected to retain state history for entities that have expired.
  When hydration at a historical block fails, the bridge checks the entity
  at head: dead at head → the history is legitimately gone, the doc is
  skipped (counted in `arkiv_bridge_docs_skipped_unretained_total`) and the
  backfill moves on; alive at head → the failure is a real node problem and
  the block is retried. A skipped doc loses nothing durable — the eventual
  `DELETE`/`EXPIRE` log tombstones every version of that entity anyway.
- **Paranoid mode**: set `arkiv.rpc_url_backup` to hydrate every entity from
  two independent nodes and halt on discrepancy.

### Exactly-once semantics

Every doc carries a deterministic
`_doc_id = keccak256(entity_key || block_be8 || log_index_be4 || op_type_be1)`.
The cursor advances block-by-block only after Quickwit accepts the block's
docs, so a crash replays at most one block — re-emitted docs share their
`_doc_id`, and consumers dedup by grouping on it. (Quickwit delete tasks
remove *all* matches for a query, so `_doc_id`-targeted deletes would drop
the surviving copy too; tolerated duplicates bounded to one block is the
deliberate trade-off.)

### Verifiability

Docs carry `entity_key`, `entity_address`, `code_hash`
(= keccak256 of the raw payload bytes), and `block_number`. A client that
trusts nothing can hydrate a hit via `eth_getCode(entity_address, block)` /
`arkiv_query`, re-hash the payload, and Merkle-prove the account against the
block's `stateRoot` with `eth_getProof`. The bridge never weakens arkiv's
verifiability — Quickwit only ever serves pointers.

## Running

```bash
# 1. Install the index (once):
curl -XPOST http://127.0.0.1:7280/api/v1/indexes \
  -H 'content-type: application/yaml' \
  --data-binary @index-config/arkiv-index.yaml

# 2. Configure:
cp arkiv-quickwit-bridge.example.yaml config.yaml
#   … edit rpc_url / base_url / state_store_path …

# 3. Run:
cargo run --release -- --config config.yaml
```

The daemon refuses to start if the Quickwit index is missing, a URL is
malformed, or an extractor rule is ambiguous. A systemd unit is provided in
`systemd/`.

### Storage backend note

The bridge is storage-agnostic: whatever Quickwit's `index_uri` points at
(S3, local, `ipfs://` if the cluster was built with the `ipfs` feature) is
transparent behind the ingest API.

## Searching

```bash
curl 'http://127.0.0.1:7280/api/v1/arkiv/search' \
  -H 'content-type: application/json' \
  -d '{"query": "body:hello AND op_type:create", "max_hits": 10}'
```

Hits return provenance (`entity_key`, `entity_address`, `code_hash`,
`block_number`) — hydrate the payload from arkiv, not from Quickwit
(`body` is deliberately not stored).

## Metrics

Prometheus on `bridge.metrics_listen_addr` (`/metrics`). Alert on:

| Condition | Meaning |
| --- | --- |
| `arkiv_bridge_lag_blocks > 500` | search is falling behind |
| `arkiv_bridge_audit_mismatch_total > 0` | data integrity broken — halted |
| `rate(arkiv_bridge_reorgs_observed_total) spike` | chain instability |
| sustained `arkiv_bridge_quickwit_http_error_total{code=~"5.."}` | Quickwit unhealthy |

## Tests

```bash
cargo test
```

Unit tests cover config validation, log decoding, doc construction, payload
extraction, the state store, reorg retraction, and the audit verifier. For
an end-to-end run, point the bridge at a devnet arkiv-op-reth node and a
local Quickwit (`./quickwit run`) and watch `arkiv_bridge_docs_emitted_total`.

[arkiv]: https://arkiv.network/
[Quickwit]: https://quickwit.io
