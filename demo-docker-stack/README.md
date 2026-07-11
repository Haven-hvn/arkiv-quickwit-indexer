# arkiv-quickwit-stack

Self-contained Docker stack: search the [arkiv](https://arkiv.network) braga
hoodi testnet entity stream locally, with Quickwit storing its index splits
on a **fully offline IPFS node**.

```
braga hoodi RPC ──HTTPS──► bridge ──ingest──► Quickwit ──splits──► kubo (offline IPFS)
                                                  ▲
                    localhost:8080 ── frontend ───┘  (search page + API proxy)
```

Everything runs on your machine. The only network traffic that leaves it is
the bridge's outbound HTTPS to the arkiv RPC endpoint. The only port
published to the host is `127.0.0.1:8080` — the search page.

## Prerequisites

- Docker with the compose plugin (Docker Desktop, colima, OrbStack, …)
- ~6 GB free RAM and ~15 GB disk during the Quickwit image build
- This folder checked out **next to** its two build contexts:

```
workingfolder/
├── arkiv-quickwit-stack/      ← this folder (compose file lives here)
├── arkiv-quickwit-bridge/     ← bridge source (build context)
└── quickwit-main/             ← quickwit source with ipfs backend (build context)
```

## Run it

```bash
cd arkiv-quickwit-stack
docker compose up -d --build
```

The first build compiles Quickwit from source in release mode — expect
30–60+ minutes depending on hardware. Subsequent `up`s reuse the images.

Then open **http://localhost:8080** and search. The bridge starts at the
chain tip (no backfill) and trails it by 32 confirmation blocks, so the
first documents appear roughly a minute after startup, as new entities are
written on-chain. Try `*`, `op_type:create`, or
`annotations.string.type:w3pups-telemetry`.

## What each service is

| Service | Image | Role |
| --- | --- | --- |
| `kubo` | `ipfs/kubo:v0.32.1` | IPFS node in `test` profile + `--offline`: zero bootstrap peers, zero outbound traffic. Quickwit splits are stored here content-addressed (each split has a CID). RPC port 5001 is reachable only inside the compose network. |
| `quickwit` | built from `../quickwit-main` | Quickwit with the `ipfs://` storage backend compiled in (`release-feature-set`). Metastore is file-backed on a named volume; splits go to kubo's MFS. |
| `qw-init` | `curlimages/curl` | One-shot: creates the `arkiv` index from `quickwit-config/arkiv-index.yaml`. Idempotent — "already exists" is success. |
| `bridge` | built from `../arkiv-quickwit-bridge` | Tails `EntityOperation`-family events from `https://braga.hoodi.arkiv.network/rpc` (legacy/OP-stack schema — braga has not migrated to the reth version), hydrates each entity, ingests into Quickwit, maps deletes/expiries to delete tasks. `start_block: "head"` = tip-only, no backfill. |
| `frontend` | nginx | Static search page; proxies `/api/*` to Quickwit so the browser only ever talks to `localhost:8080`. |

## Configuration

- `bridge-config/config.yaml` — RPC URL, chain id, schema, confirmation
  depth. To point at a different arkiv network, change `rpc_url`,
  `chain_id`, and possibly `schema` (`legacy` for deployed OP-stack
  networks, `entity_operation` for arkiv-op-reth nodes).
- `quickwit-config/quickwit.yaml` — Quickwit node config (IPFS endpoint,
  metastore URI).
- `quickwit-config/arkiv-index.yaml` — the index schema (doc mapping,
  tokenizers, tag fields). Edit before first launch; changing it later
  requires deleting and recreating the index.

State lives in named volumes: `ipfs-data` (splits), `qw-data` (metastore +
caches), `bridge-state` (cursor / reorg buffer / dedup). `docker compose
down` keeps them; `docker compose down -v` wipes everything for a fresh
start.

## Verifying results (trust model)

Quickwit hits are *pointers*, not authoritative data: each carries
`entity_key`, `owner`, `block_number`, and `code_hash` =
keccak256(payload). To verify a hit, hydrate the entity from any arkiv node
and re-hash:

```bash
curl -s https://braga.hoodi.arkiv.network/rpc -X POST \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"arkiv_query","params":["$key = <entity_key>", {"resultsPerPage":"0x1","includeData":{"key":true,"payload":true}}]}'
# keccak256(result.data[0].value bytes) must equal the hit's code_hash
```

## Operations

```bash
docker compose logs -f bridge          # ingestion progress
docker compose exec bridge sh -c 'wget -qO- 127.0.0.1:9464/metrics' | grep arkiv_bridge
docker compose exec kubo ipfs files ls /quickwit-indexes/arkiv   # splits as IPFS files
docker compose exec kubo ipfs config Bootstrap                   # [] — offline, always
```

The bridge halts (exits nonzero, container restarts) only on
operator-attention conditions: a reorg deeper than 512 blocks. Transient
RPC/Quickwit errors are retried with backoff indefinitely.

## Notes

- braga hoodi runs the pre-reth-migration (OP-stack) contract, which emits
  six `ArkivEntity*` events and does not support historical `atBlock`
  queries — the bridge's `schema: legacy` handles both differences. When
  the network migrates to arkiv-op-reth, switch to
  `schema: entity_operation`.
- Kubo never garbage-collects by default. Deleted/merged-away splits free
  disk only when GC runs; for long-lived deployments add `--enable-gc` to
  the kubo command.
- The published search page binds to `127.0.0.1` only. To expose it on a
  LAN, change the port mapping in `docker-compose.yml` deliberately.
