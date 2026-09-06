# The per-table key policy

Date: 2026-09-05. Status: research measurement against a live service. Loopback
only, no wallet enabled. Depends on the `experimental-key-reuse` feature of
`ipir-sp`, whose security argument is conditional and unreviewed, so this
configuration is not shippable as it stands.

[Bounded key reuse](transparent_pir_key_reuse.md) measured sharing packing keys
across a batch and found it made the sparse profile 2.05x worse while making the
heavy profile 0.62x. It was a single switch for the whole service. This makes
the choice per table, from the number of queries that sync will make against
that table, and the profile that reuse hurt no longer pays for it.

## What changed

**The decision is per table, and it is not binary.** A table publishes four
public matrix sets; a client downloads the prefix of them it will use, and that
count is also its batch size. So the choice is over 1, 2, 3 or 4 sets rather
than "fresh" against "four-way", which matters because the two are not the only
sensible plans: at four page queries a two-set plan beats both.

**Published parameters are fetched per table, on demand.** The session document
now carries a commitment to each set rather than the sets themselves, and
`GET /v1/transparent-history/{table}/params/{slot}` serves one. The init
document fell from about 459 KB to 2,459 bytes. A sync that never queries the
pages table never downloads its parameters.

**The commitment changed shape to make that safe.** The digest was taken over
the concatenated parameter bytes, which only a client holding every set can
check. It is now taken over the per-set digests, so a client that downloaded one
set verifies that set against the session and the session against the epoch that
every response carries.

## Result

Same generation, same workloads and the same independent ledger oracle as the
earlier measurements: heights 3,470,268–3,471,419, directory 4,096 x 3,584,
pages 4,096 x 17,920. Ordinary retrieval for the interval is 2,888,097 bytes.
Every run recovered 0, 2, 20 and 9,152 events exactly before its bytes were
recorded.

All three columns are this build, with the batch size forced for the baselines,
so they differ only in the decision under test:

| Workload | Fresh keys | Always four | **Policy** | Sets used (dir, pages) |
|---|---:|---:|---:|---|
| unused_100 | 174,466 | 174,466 | **174,466** | none |
| sparse_1 | 300,449 | 420,260 | **300,449** | 1, 0 |
| median_10 | 1,305,272 | 797,160 | **797,160** | 4, 0 |
| large_1 | 6,978,479 | 4,221,790 | **4,101,979** | 1, 4 |

The policy matches the better baseline on every workload and beats both on the
large history, because that sync is the one where the two tables want different
answers: one directory lookup on fresh keys, fifty page lookups shared four
ways. A single switch cannot express that, which is the whole point.

Against ordinary retrieval the policy runs 0.06x, 0.10x, 0.28x and 1.42x.

## What the policy actually compares

For `n` padded selections against one table, a plan of `s` sets sends
`ceil(n/s)` batches of `s` queries and holds `s` published sets:

```
s * set_bytes + ceil(n/s) * (batch_header + key_bytes + response_header)
              + ceil(n/s) * s * (slot_byte + selector_bytes + response_bytes)
```

The cheapest `s` wins; ties go to the smaller plan. Two things in that formula
are easy to leave out and both flatter sharing:

- **A larger batch pads more.** Rounding to whole batches adds queries, and each
  carries a full selector and a full response. Fifty page selections at four
  sets become fifty-two queries.
- **The extra sets are paid for once, but the keys are saved per batch.** A set
  only repays itself when there are enough batches to amortize it.

Counting both moves the thresholds. For this geometry both tables upload the
same 86,016-byte keys and the same 20,480-byte selector, because both hold 4,096
rows; only the published set differs, at 14,336 bytes for the directory and
71,680 for pages. A four-set batch first beats fresh keys at 3 directory queries
and 4 page queries, and first becomes the *cheapest* plan at 4 and 7
respectively. Below those a two- or three-set batch wins, which an
all-or-nothing switch had nothing to offer.

The earlier report's break-evens of 2 and 8 were computed from key bytes against
published bytes alone, without the padding a larger batch forces.

## Where the sparse improvement comes from

The sparse profile is 300,449 bytes where the previous fresh baseline recorded
400,798. That 100,349-byte difference is **not** the batch-size policy, which
chooses fresh keys for that sync in both builds. It is the on-demand fetch and
the encoding:

- **95,576 bytes**: the pages table's set, which the old session document
  inlined and this sync never uses.
- **4,780 bytes**: the directory set now travels as raw bytes over its own
  endpoint rather than base64 inside JSON.
- **−7 bytes**: batch framing, which costs slightly more than a bare query.

The batch-size policy's own contribution is visible in the fresh column against
the policy column: nothing on sparse, 508,112 bytes on median_10, and 2,876,500
on large_1.

## Where this stops working

The measured workloads bracket the limits rather than state them. This section
extends them with a cost model built from the same per-unit constants the client
charges; it reproduces both single-address runs to the byte before it reports
anything, and `tools/transparent_pir_break_even.py --check` fails if it drifts.
Everything below is a calculation from measured constants, not a run.

**By history size: about 5,950 events.** One address over the measured day wins
up to 5,954 events (32 page lookups, 0.95x ordinary retrieval) and loses from
5,955 (33 lookups, 1.03x). The two measured endpoints are 2 events at 0.10x and
9,152 at 1.42x.

**By sync frequency: about 91 blocks, and this is the binding limit.** Ordinary
retrieval gets cheaper the more often a wallet syncs. Private retrieval does not,
because its floor is per sync: filters, then the reference data for the tables it
touches, then a whole first lookup. So the two cross on frequency, not only on
size.

| Interval | Retrieval budget | What it buys |
|---|---:|---|
| 12 blocks (0.2 h) | 8,415 | nothing — one lookup is 125,983, fifteen times over |
| 288 blocks (6 h) | 417,487 | 6 index lookups, or 1 index and 1 page |
| 1,152 blocks (24 h) | 2,749,381 | 56 index lookups, or 1 index and 32 pages |

The shortest interval affording one index lookup is roughly 91 blocks, about two
hours; roughly 229 blocks, about five hours, if that lookup finds history needing
a page. Before this change those were about 159 blocks and about 260. The
improvement is real and does not change the shape: a wallet that syncs every few
minutes is nowhere near affordable, and no amount of key sharing takes it there.

Those two figures interpolate between measured intervals that are far apart, and
retrieval is not linear in blocks. They are "about two hours" and "about five",
not thresholds.

**By how much sharing can still save: saturated by about 16 lookups.** The
marginal cost of a page lookup falls from 203,807 bytes at one lookup to 85,512
at sixteen and 76,031 at fifty. Groups of four are the largest published; eight
would double the reference data and is untested. Below three or four lookups
sharing does not pay at all, which is what the policy is for. So sharing helps
within a band and is close to irrelevant outside it.

## What did not change, deliberately

- **Padding.** A batch is rounded up to its full size and every padding query is
  issued and decoded. A batch that shrank to fit the matches would publish how
  many blocks matched, which is most of what the local filter match keeps
  private. The rounding is charged, not hidden.
- **One query per set per batch.** The server refuses a batch naming a set
  twice. Two queries under one set share both matrix and secret, the case
  `same_matrix_same_secret_exposes_selector_difference` in `ipir-sp` shows
  exposes the selector difference by subtraction. The client's slot allocator
  cannot produce it; the server does not rely on that.
- **The setup cross-check.** The harness still recomputes the published-parameter
  charge from the session it fetched itself and refuses to run on a mismatch. It
  now checks the unit price and the set count separately, because the charge is
  a product and a wrong factor still looks like a number.

## Limitations

- Depends on `ipir-sp`'s `experimental-key-reuse`. Conditional security
  argument, unreviewed, gated off by default in every other crate.
- Loopback on one host. Not TLS, not a wide-area network, not a mobile device.
  Nothing here is a latency claim.
- One bounded-range generation over 1,152 blocks, not lifetime history.
- Single runs. Byte counts are deterministic; wall times are not a timing claim.
- Workload scripts are drawn from one mainnet day by fixed rank. They are not
  wallet-population percentiles.
- The policy decides from the queries one call makes against one table. A sync
  that reaches a table repeatedly — private page navigation does — decides per
  call and pays for additional sets only when a later call wants more. That is
  correct but not optimal: a client that knew its whole page workload up front
  could choose once.
- The heavy profile is still 1.42x ordinary retrieval. This makes it cheaper; it
  does not make it win.
- Trusted indexer, unchanged. This is a byte result, not an answer to the
  completeness question.

## Reproduction

```sh
python3 -c "import sys; sys.path.insert(0,'tools'); \
  import transparent_pir_incremental as inc; \
  m,b = inc.load_sample('docs/transparent-pir-evaluation/mainnet-study/mainnet-day.jsonl.gz'); \
  print(inc.build_generation('main', b, '<outdir>/generations', page_bytes=17920))"

cargo build --release -p transparent-history-server -p transparent-history-pir
target/release/transparent-history-server --listen 127.0.0.1:8092 \
  --generation-dir <outdir>/generations/<generation-id>

# Policy, then the two baselines under the same accounting.
for mode in "" 1 4; do
  PIR_KEY_SETS=$mode python3 tools/transparent_pir_http_run.py \
    --sample docs/transparent-pir-evaluation/mainnet-study/mainnet-day.jsonl.gz \
    --base-url http://127.0.0.1:8092 \
    --client-binary target/release/transparent-history-cli \
    --output <results>/${mode:-policy}
done

cargo test --workspace --release
python3 -m unittest discover -s tools -p 'test_transparent_pir_*.py'
python3 tools/transparent_pir_break_even.py --check
python3 tools/transparent_pir_break_even.py   # the limits above
```

`PIR_KEY_SETS` is a measurement override only; unset is the policy a wallet
would run. Preserved results and digests are under
[`transparent-pir-evaluation/per-table-reuse/`](transparent-pir-evaluation/per-table-reuse/).
