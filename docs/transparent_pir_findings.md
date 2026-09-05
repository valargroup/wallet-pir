# Transparent sync: what the measurements settled

Date: 2026-09-05. Index of the transparent filter and private retrieval work,
and what each measurement established. Component reports carry the detail; this
records the conclusions and the decisions they force.

## Where the work stands

| Layer | State |
|---|---|
| BIP 158 activity filters | Implemented, deployed on the coordinator, public API under `/v1/filters/` |
| Private history retrieval | Research service, loopback, measured over real transport, not enabled for any wallet |
| Bounded key reuse | Measured against an unmerged `ipir-sp` revision; not shippable as measured |

## 1. Filters carry the benefit, and they now win at every interval

Range-scoped BIP 158 delivery removed the prototype's worst failure. The old
174,379-byte full-day bundle floor, which made short catch-up hopeless against a
9,492-byte compact suffix, was an artefact of full-day manifest delivery rather
than of filters.

| Interval | Ordinary | Delivered filters | Ratio |
|---|---:|---:|---:|
| 12 blocks | 9,492 | 1,077 | 8.8x |
| 116 blocks | 224,877 | 11,014 | 20.4x |
| 288 blocks | 446,142 | 28,655 | 15.6x |
| 1,152 blocks | 2,888,097 | 138,716 | 20.8x |

Matches are rare: a 1,000-script mostly-inactive wallet matched 12 blocks over a
full day, two of them false positives. The ~25% gain over the previous Bloom
encoding is encoding alone — every script in the sample day is P2PKH or P2SH, so
there was no coverage difference to measure. Details in
[the BIP 158 report](transparent_pir_bip158.md).

Host-side filter ingest is 110 blocks/s; the whole 45,051-block backfill from
Ironwood activation takes about seven minutes and 3.2 MB. An earlier figure of
1,045 s for 1,152 blocks measured an SSH tunnel, not the service.

## 2. Transport was not the unknown

Every earlier retrieval figure came from in-process round trips excluding HTTP,
TLS, retries and framing. Measured over real transport against a live service,
totals run 0.4-3% above those figures: framing is negligible against a
106,504-byte query. The earlier evidence was substantially right and its
conclusion unchanged. Details in
[the real-transport measurement](transparent_pir_http.md).

That is a useful negative result. It removes a suspected source of error rather
than confirming a hope, and it means the remaining uncertainty is in the
construction, not the plumbing.

## 3. Per-query cost is the binding constraint

A query is 106,504 bytes up and 5,136 (directory) or 25,616 (pages) down.
**Packing keys are 86,016 of the upload, 80.8%**; the query body is 20,480 and
scales with rows rather than row bytes, so both tables cost the same to query.

Subtracting filter delivery from ordinary retrieval leaves the budget retrieval
must fit inside:

| Interval | Ordinary | Filters | Retrieval budget |
|---|---:|---:|---:|
| 12 blocks | 9,492 | 1,077 | 8,415 |
| 288 blocks | 446,142 | 28,655 | 417,487 |
| 1,152 blocks | 2,888,097 | 138,716 | 2,749,381 |

At twelve blocks a single query exceeds the budget twelve times over. Frequent
sync is not winnable and should stop being treated as a target.

## 4. Key reuse loses where it was most needed

The plan assumed four-way key reuse was required, because the 288-block budget
needed roughly 50 KB per query. Reuse does deliver that: **41,988 bytes per
query against 106,504**, a 2.54x reduction matching the key share to within four
bytes.

But it front-loads **458,768 bytes** of published parameters — four sets per
table instead of one — and that exceeds the entire 288-block retrieval budget
before a single query.

| Workload | Queries | Fresh | Reuse | vs ordinary |
|---|---:|---:|---:|---:|
| 100 unchanged | 0 | 174,466 | 174,466 | 0.06x |
| One script, 2 events | 4 | 400,798 | 821,684 | 0.28x |
| Ten median histories | 12 | 1,405,558 | 1,198,584 | 0.42x |
| Largest, 9,152 events | 56 | 7,006,798 | 4,336,494 | 1.50x |

The sparse wallet — the profile this design exists to serve — pays **2.05x**
what fresh queries cost. Reuse repays only once a sync amortises those sets:
break-even is 2 queries for the directory table and 8 for pages.

**So reuse belongs behind a per-table policy keyed on query count, not a global
switch.** That is what the earlier reuse experiment's "auto" mode did, and this
measurement shows why it existed. Details in
[the key reuse measurement](transparent_pir_key_reuse.md).

## 5. What is conceded

- Heavy histories lose. The largest sampled history is 1.50x ordinary retrieval
  even with reuse, and lifetime restoration is not addressed at all.
- Short intervals lose. Below roughly a day, retrieval cannot fit its budget.
- Query counts leak activity volume unless padded, and padding rounds a sync up
  to whole batches, which is real cost rather than waste to optimise away.
- Range requests reveal the interval being synchronised, and when.
- Filters rest on a trusted indexer. A negative result advances coverage, so an
  omitted script produces a wrong balance rather than a visible failure, and a
  digest from the same operator commits to the false filter rather than exposing
  it. Single-operator trust is currently accepted; cross-operator digest
  comparison is enabled by the API and performed by nobody.

## 6. Accounting errors found, and what they cost

Recorded because each produced plausible numbers rather than failures, and two
of them flattered the result under test.

- **Published sets counted instead of their bytes.** `public_params` became a
  list, and the client charged its length: 4 rather than 458,768. Setup is
  exactly what reuse trades against, so omitting it made reuse look like a win
  for sparse wallets when it is a 2x loss.
- **Setup charged per client process, not per session.** Affected the reuse
  measurement and, once audited, the fresh baseline too: the largest-history
  total fell from 7,121,490 to 7,006,798, and 2.47x to 2.43x.
- **A test fixture left behind by a type change.** Manifest schema fields became
  strings while the fixture still wrote integers, and the change was committed
  without re-running the tests it broke. This reached main.

The harness now recomputes the setup charge from the session it fetched itself
and refuses to run on a mismatch, and the fixture references the exported schema
constants rather than restating them. Byte counts predating these fixes should
be read from the corrected evidence files rather than from earlier summaries.

## 7. What follows

1. **Filters are the deliverable.** They are deployed, publicly served, and
   worth 8.8-20.8x independently of whether retrieval ever ships. Wallet
   integration is the next product step.
2. **Retrieval stays research.** It passes its byte gate for sparse and median
   profiles at day scale with exact ledger equality, and fails for heavy
   histories and short intervals.
3. **A per-table reuse policy is the next retrieval experiment**, since a global
   switch makes the common case worse.
4. **The completeness-trust question outranks all of it.** Strong retrieval
   privacy over an unverified indexer is unbalanced, and no measurement here
   addresses it.
