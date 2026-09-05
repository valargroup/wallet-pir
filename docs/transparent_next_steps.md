# Transparent sync: state and next steps

Date: 2026-09-05. Written as a handoff. What is deployed, what is measured, what
is decided, and what the next agent should do.

Read [the findings index](transparent_pir_findings.md) first for the
measurement conclusions; this document is the operational and forward-looking
half.

## 1. What is deployed right now

**BIP 158 transparent activity filters, live and public.**

- `transparent-filter-server` runs on the Enhance PIR coordinator
  (`enhance-pir.valargroup.dev`, DigitalOcean droplet `167.99.42.60`), bound to
  `127.0.0.1:8090`, beside `zakurad`.
- Caddy routes `/v1/filters/*` to it. `/metrics` and `/ready` are blocked at the
  edge and return 404. `/v1/health` on the public host is the *enhance
  coordinator's*, not the filter service's.
- Coverage begins at Ironwood activation, height 3,428,143, and tracks the tip.
  Store lives at `/srv/zakura/transparent-filter-data`, a few MB.
- Host-side ingest is 110 blocks/s; a full backfill from activation takes about
  seven minutes.

The public surface is specified in [the API document](transparent_filter_api.md):
`info`, `chain`, `digests`, `range`. Requests carry chain, profile and height
range only; no endpoint accepts a script, address or match.

**Not deployed, and deliberately so:** private history retrieval. It exists as
`server/transparent-history-server` and `pir/transparent-history`, runs on
loopback for measurement, and is enabled for no wallet.

**Deprecated:** transparent-spend PIR. The crate and journal remain in the tree,
no worker is provisioned, and the coordinator does not publish those tables.

## 2. Decisions already taken

Do not reopen these without new evidence; each was decided deliberately.

- **Single-operator trust is accepted for now.** A negative filter result
  advances coverage, so an operator omitting one script produces a wrong balance
  rather than a visible failure. `/v1/filters/digests` exists so independent
  operators can be compared, and nobody performs that comparison because there
  is one operator. Recorded in the API document.
- **Coverage starts at Ironwood activation.** A genesis reindex was measured
  before deciding: 16.4 blocks/s at height 200,000 against 110 after activation,
  roughly one to three days and under a gigabyte. The boundary stands on need,
  not cost. Revisit only if earlier-birthday wallets become a target.
- **No filter header chain is published.** The construction exists in
  `pir/transparent-filter/src/digest.rs`, but coverage starts at activation
  rather than genesis, and an all-zero predecessor part-way up a chain is not a
  genesis-derived header chain. Publishing one would overstate what it anchors.
- **Vizor wallet integration was deferred**, on purpose, so the filter layer
  could be finished first.

## 3. The state of private retrieval

Measured end to end over HTTP against a live service, every workload verified
against the independent ledger oracle before its bytes were recorded. Ordinary
retrieval for the same 1,152-block interval is 2,888,097 bytes.

| Workload | Queries | Fresh | Reuse | vs ordinary (fresh) |
|---|---:|---:|---:|---:|
| 100 unchanged scripts | 0 | 174,466 | 174,466 | 0.06x |
| One script, 2 events | 1 / 4 | 400,798 | 821,684 | 0.14x |
| Ten median histories | 10 / 12 | 1,405,558 | 1,198,584 | 0.49x |
| Largest, 9,152 events | 51 / 56 | 7,006,798 | 4,336,494 | 2.43x |

A query is 106,504 bytes up. **Packing keys are 86,016 of that, 80.8%**; the
body is 20,480 and scales with rows, not row bytes, so both tables cost the same
to query.

Four-way key reuse cuts a query to 41,988 bytes but front-loads 458,768 bytes of
published parameters. Break-even is 2 queries for the directory table and 8 for
pages. **A sparse wallet pays 2.05x with reuse on**, which is why it must not be
a global switch.

## 4. Next step: the per-table reuse policy

This is the concrete work the measurement demands, and it is the only retrieval
change with a known payoff.

**Problem.** Reuse is currently all-or-nothing per service. A sync that makes
one directory query and no page queries still downloads four published sets for
both tables: 382,304 bytes of the pages table's parameters are pure waste.

**Shape of the fix.** Decide reuse per table, from the query count that sync
will make against that table, comparing `(n - ceil(n/4)) * 86,016` of key saving
against the extra published bytes for that table. The prior art is the `auto`
policy in `tools/transparent-pir-reuse-bench/src/main.rs`, which does exactly
this comparison; that bench is the reference, not a starting point to copy,
since it is in-process and has its own workspace.

**Where the code is.** Branch `experiment/key-reuse-measurement` carries the
reuse client and batching server. It is a measurement branch, not mergeable as
it stands: it was cut before the workspace moved to the merged ipir-sp revision,
so rebase it onto main first and drop its now-redundant revision bump.

- `pir/transparent-history/src/client.rs` — `TableClient` holds a `QueryPool`;
  `prepare_batch`, `decode_batch`, `fetch_rows`. The policy decision belongs
  here, per table, before the first batch.
- `server/transparent-history-server/src/service.rs` — `TableRuntime::build`
  precomputes `PUBLIC_SETS` sets unconditionally. It will need to publish one or
  four per table, and say which in the session.
- The session type must carry the per-table set count so a client cannot assume
  four.

**Do not remove the padding.** Batches round a sync up to whole batches, and
those padding queries are real cost. A batch that shrank to fit the number of
matches would publish how many blocks matched, which is most of what the local
filter match exists to keep private. The same applies to the server's rejection
of a batch naming one set twice: two queries under one set share both matrix and
secret, the case `same_matrix_same_secret_exposes_selector_difference` in
`ipir-sp` shows leaks the selector difference by subtraction.

**Pass condition.** Reuse must not make any profile worse than fresh. Concretely
the sparse profile must return to roughly 400,798 bytes rather than 821,684,
while the heavy profile keeps its 4,336,494. Verify against the ledger oracle,
as every prior measurement did.

## 5. How to run the measurement

The harness is the same one the existing evidence came from; only the transport
differs.

```
# 1. Build a generation from the checked-in mainnet day
python3 -c "import sys; sys.path.insert(0,'tools'); \
  import transparent_pir_incremental as inc; \
  m,b = inc.load_sample('docs/transparent-pir-evaluation/mainnet-study/mainnet-day.jsonl.gz'); \
  print(inc.build_generation('main', b, '<outdir>', page_bytes=17920))"

# 2. Serve it
cargo build --release -p transparent-history-server
target/release/transparent-history-server --listen 127.0.0.1:8092 \
  --generation-dir <outdir>/<generation-id>

# 3. Measure
cargo build --release -p transparent-history-pir --bin transparent-history-cli
python3 tools/transparent_pir_http_run.py \
  --sample docs/transparent-pir-evaluation/mainnet-study/mainnet-day.jsonl.gz \
  --base-url http://127.0.0.1:8092 \
  --client-binary target/release/transparent-history-cli \
  --output <results-dir>
```

The generation over the full day is directory 4,096 x 3,584 and pages 4,096 x
17,920; table runtimes take about seven seconds to build and the process holds
both resident.

## 6. Traps this work already fell into

Each of these produced plausible numbers rather than an error. Two flattered the
result under test.

- **Charging a count as bytes.** `public_params` became a list of sets and the
  client charged its length: 4 rather than 458,768. Setup is exactly what reuse
  trades against. `tools/transparent_pir_http_transport.py` now recomputes the
  expected charge from the session it fetched itself and refuses to run on a
  mismatch. Keep that check.
- **Charging setup per process rather than per session.** Each harness call runs
  a fresh client that re-fetches published parameters; a wallet holds one session
  and caches them. This affected both the reuse measurement and, once audited,
  the fresh baseline.
- **Committing after a source change without re-running tests.** A manifest type
  changed from integer to string while a fixture still wrote integers; the
  change was committed on the strength of an earlier green run and reached main.
  The fixture now references the exported `SCHEMA`/`GENERATION_SCHEMA` constants
  rather than restating them.
- **`ipir-sp` revisions must move for the whole workspace at once.** Two
  revisions in one build produce two distinct copies of the same types and fail
  in ways that point anywhere but the cause.

## 7. Open items not on the critical path

- **`ipir-sp` CI does not run.** Its workflow needs a self-hosted AVX-512 runner
  that is offline; jobs have queued for hours and at least #15 and #16 merged
  without it. Verify locally and say so until the runner is restored.
- **The reuse construction needs specialist review.** Its security argument is
  conditional, not a reduction: it requires joint pseudorandomness of the stacked
  samples *given the evaluation key*, and evaluation keys encrypt
  secret-dependent automorphic images. See `ipir-sp/KEY_REUSE_EXPERIMENT.md`.
  Nothing enables the feature today.
- **`bitcoin` pulls `secp256k1`** into `pir/transparent-filter` through its `std`
  feature, and the crate never uses it. That weight matters for a mobile build.
- **`enhance-pir-server`'s health reports a stale `current_height`** during
  ingest, the same defect fixed in the filter service in #61.
- **`tools/__pycache__/*.pyc` are tracked** and churn on every harness run.

## 8. What would change the conclusion

The retrieval track passes its byte gate for sparse and median profiles at day
scale and fails for heavy histories and short intervals. Two things would move
that materially, and neither is a tuning exercise:

1. **A smaller published-parameter footprint**, which is what makes reuse lose at
   short intervals.
2. **A resolution of the completeness-trust question**, which outranks the byte
   comparison entirely. Strong retrieval privacy over an unverified indexer is
   unbalanced, and no measurement here addresses it.
