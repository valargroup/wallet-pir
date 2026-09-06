# Updated transparent PIR recommendation

Date: 2026-09-05. Status: design recommendation, not a deployed protocol.

Generation boundaries and the figures supporting them are measured over the
Ironwood-to-tip event journal; see *What is measured and what is not* for the
boundary between what has been established and what has not.

The implementation calls a generation a *shard*, and the implementation plan
follows the code. The two words name the same object; this document uses
*generation* throughout, and *segment* always means a part of one generation,
never a generation itself.

## Goal

Construct a private transparent-history service from which a wallet can recover
every confirmed receive and spend for its supported scripts, then derive its
current UTXO set, balance, address-use state, and transaction history.

The database should publish chain facts rather than calculated wallet balances.
The wallet remains responsible for address derivation, accepting a chain anchor,
confirmation policy, coinbase maturity, key availability, and coin selection.
Anchor acquisition is supplied through an independent interface; transparent
recovery must not require shielded scanning or regular compact-block sync.

The recommended organization is an immutable, time-sharded event ledger keyed
by exact raw locking script, partitioned into generations whose boundaries are
decided by content rather than by a fixed block width:

```text
accepted chain anchor
    -> published height-to-generation map
    -> public activity filter per generation
    -> private script directory per generation
    -> private event pages per generation
    -> optional private transaction-detail table
    -> wallet-validated event replay
    -> UTXOs, balance and history
```

## Recovery scope

The minimum confirmed ledger consists of:

1. Every transparent output controlled by a supported wallet script.
2. Every later input that consumes one of those outputs.

This must include outputs received and spent while the wallet was offline and
zero-balance histories. An empty current UTXO set is not evidence that a script
has never been used.

For full restoration, the database must cover the wallet's required history.
The current deployment's Ironwood start cannot reconstruct earlier transparent
history. A snapshot at Ironwood could establish a starting balance, but it
cannot reconstruct complete earlier history.

Mempool state, pending transactions, transaction broadcast, and wallet-local
metadata are separate capabilities. Labels, contacts, payment intent, fiat
values, and unbroadcast transactions cannot be recovered from the chain.

## Canonical event model

For every transparent output, index a receive event under the output's exact raw
locking script:

```text
ReceiveEvent {
    height
    block_hash
    txid
    transaction_index
    output_index
    value
    script
    coinbase
}
```

For every non-coinbase input, resolve the previous output and index a spend event
under that previous output's exact raw locking script:

```text
SpendEvent {
    height
    block_hash
    spending_txid
    transaction_index
    input_index
    spent_txid
    spent_output_index
    script
}
```

Previous-output resolution is mandatory. An input identifies an outpoint but
does not directly identify the script whose wallet must discover the spend.
Failure to resolve any previous output makes that indexed range incomplete and
must fail construction rather than publish a partial result.

Use stable event identities so retries and duplicate delivery are idempotent:

```text
receive identity = (txid, output_index)
spend identity   = (spending_txid, input_index, spent_txid, spent_output_index)
```

Events have one canonical order. It is protocol data rather than an
implementation detail, because pages are stored in it and wallets replay in it:

```text
order key =
    (height, transaction_index, receive before spend,
     output_index or input_index)
```

A transparent output cannot be consumed by the transaction that creates it, so
ordering by transaction index within a block already resolves a receive and its
spend in the same block: the creating transaction has the lower index. Ordering
receives before spends within one transaction makes the key total rather than
merely sufficient, so two builders cannot produce different page bytes for the
same events.

Duplicate delivery is idempotent on these identities. A repeat that differs in
any field is not a newer version of an event; it is a contradiction, and must be
rejected rather than settled by taking the later copy.

The first private profile may cover P2PKH and P2SH scripts, but unsupported
script classes must be reported as outside coverage rather than absent. Logical
identity is always exact script bytes, not an address string.

## Content-sealed generations

Partition the chain into immutable generations. A generation's boundaries are
decided by what it *contains*, not by a fixed block width:

```text
Generation 42
    heights: decided by content, published in the generation map
    parent block hash
    terminal block hash
    public activity filter
    segments: one or more, normally one
        private script directory
        private event pages
    optional private transaction details
```

Each generation manifest binds:

- network and genesis hash;
- schema and profile versions;
- seal parameters;
- start and end heights;
- parent and terminal block hashes;
- previous-generation commitment;
- lifecycle state, and for a tail its revision number and the revision it
  supersedes;
- public-filter digest;
- PIR table dimensions, salts and row sizes;
- the complete segment list, with each segment's directory, page and
  transaction-table digests.

A generation's identity is the digest of its canonical manifest, and its
published directory is named by that digest. A manifest edited after
publication is refused rather than served under the identity of what it
replaced. Because the digest binds the segment list, a generation cannot gain,
lose or reorder a segment without becoming a different generation.

### Why not a fixed block width

Nothing bounds what a height range contains. A fixed-width generation must
therefore size its tables from expected occupancy, and fail closed when a busy
range exceeds them.

Measurement over Ironwood activation to height 3,473,474 — 45,332 blocks,
869,283 events — shows the variation is not marginal. Density ranges from 12.0
to 38.5 events per block across 4,096-block buckets. Expressed the way that
matters, holding occupancy fixed instead of width: a generation sized to hold
about 4,100 distinct scripts spans anywhere from **124 to 1,746 blocks**. A
fixed-width generation would vary by that same factor in how full its tables
are.

That variation is not affordable, because uniform table geometry is what makes
the scheme cheap. PIR parameters are a function of `(rows, item_size_bits)`
alone, so generations sharing a geometry share one parameter set: a wallet
validates parameters and derives its query setup **once for all generations**,
rather than once per generation. Sealing on content converts chain-density
variation into differing block spans, which cost nothing, instead of differing
table fullness, which costs the shared parameter set.

### Sealing on the limits that bind

The tables are keyed differently, so no single quantity bounds them:

| Table | Sized by | Seals? |
|---|---|---|
| activity filter | distinct scripts | yes |
| script directory | distinct scripts | yes |
| event pages | page rows | yes |
| transaction detail | distinct transaction ids | only if built |

Page rows are *not* a function of the event count. Pages are per script and
padded, so many short histories cost far more rows than the same number of
events concentrated in a few long ones. Sealing on events alone would leave the
page table unbounded in the direction that actually hurts.

Each sealing quantity is monotone as blocks stream in, so one incremental pass
decides every boundary. Seal at the first block boundary where any limit binds.

Distinct transaction ids size only the optional transaction-detail table. Where
that table is not published, they are counted and reported but do not seal. Over
the Ironwood-to-tip journal the transaction-id limit never bound under any
candidate policy — every generation sealed on scripts or page rows — so
publishing without that table moves no boundary.

### Capacity and target are separate numbers

A single threshold is not enough. Blocks arrive whole, and one block can add
thousands of scripts, so a generation just under a threshold can land past what
one segment's pinned geometry actually holds. Each quantity therefore carries
two numbers:

- **capacity** — what one segment's tables hold. A hard limit.
- **target** — where sealing is preferred. Must leave room for one more block.

A block that would breach capacity seals the generation *before* it is added;
reaching a target seals *after*. Measured overshoot past target is real but
modest: at a 4,096-script target, generations landed at up to 4,506 scripts, so
capacity needs roughly 15% headroom rather than a doubling.

Above roughly eight thousand scripts the scripts limit stops binding altogether
and page rows seals every generation. Whichever limit binds is the one actually
being chosen; the others are inert and should not be mistaken for tuning.

### Segments, so every valid block is published

Sealing before an oversized block is not always available. The block may be the
generation's first, and a block whose own content exceeds a segment's capacity
has nowhere to be sealed to. Refusing to publish it would let a single block —
which anyone can pay to produce — halt the service, so that is not an option
either. Publication must handle every valid block automatically.

A generation therefore contains **one or more segments**, each carrying the
standard directory and page geometry. The normal case is one segment, and a
single-segment generation is exactly what the preceding paragraphs describe.
When a generation's content does not fit the segments it has, the builder adds
another. That is the deterministic fallback that guarantees progress: a fresh
segment's rows are empty, so placement terminates. Two independently salted
candidate buckets do not supply that guarantee — they reduce collisions, they
do not bound them — which is why the fallback is segment count rather than
more buckets.

Segment count is decided when the generation is built, as the smallest count
that admits a placement, and is published in the manifest with each segment's
geometry and digest. Placement hashes over the generation's whole logical row
space, `directory_rows * segment_count`, so occupancy stays even as segments are
added instead of piling into a spillover segment.

A wallet advances coverage for the generation's range only after processing
every segment. A partial segment set is incomplete work, not a shorter answer.

Segments must not become publicly selected shards. A wallet issues one request
per generation and table; the service evaluates that request against every
segment of the generation and returns the per-segment results, which the wallet
decodes and matches on exact script bytes. What becomes public is the segment
*count*, which the manifest publishes anyway. Which segment holds a script is
not disclosed by the request. Letting a wallet ask for the segment its script
hashes into would reveal part of the script's identity before PIR begins, which
is the script-prefix sharding rejected under *Private script directory* below.

The cost is paid in queries and bytes during exceptional periods: a generation
with `n` segments costs `n` times the directory and page work of an ordinary
one, on both sides. That belongs in the cost and privacy analysis rather than
being assumed away. It is still preferable to giving oversized ranges a larger
geometry, which would oblige every client to support a second parameter set —
and then a third — because one unusual block arrived.

### Boundaries are published, not re-derived

Content-derived boundaries depend on the indexer's event set. Two operators
disagreeing about a single event would produce different boundaries, and the
disagreement would cascade into every later generation — destroying the
cross-operator digest comparison that publishing digests exists to support,
because no two generations would be comparable at all.

The height-to-generation map is therefore published as committed protocol data.
An operator must reproduce the published boundaries rather than derive its own,
so a disagreement stays localized to one generation's digests, exactly as it
would under fixed widths. The same map is what a wallet binary-searches to turn
its birthday height into a first generation; with content sealing there is no
width to divide by, so the map is not a convenience index but a requirement.

Seal parameters are schema, not tuning. Two generation sets built under
different parameters are different partitions of the same chain, and a wallet
holding both would hold incompatible coverage. Changing them re-partitions;
since sealed generations are immutable, new parameters can in practice apply
only to new ranges.

Generation *density* becomes public — a generation covering few blocks means
high activity in that period. That is public chain data already, but it should
be stated rather than discovered.

### The tail, and its revisions

Recent blocks still need a shorter unsealed tail, so publication latency and
ordinary reorgs do not require rebuilding a large sealed generation. A
generation that has reached no limit is the tail by definition. The tail is
published rather than withheld, so coverage reaches the accepted anchor rather
than stopping at the last seal boundary. A wallet that stopped there would
report a balance correct only as of an older height, with no way to express the
difference.

A tail grows, so its bytes change while the range it covers is still being
decided. Publication resolves that by making every published tail an immutable
**revision** rather than a mutable object. A generation is in one of three
states:

- **open** — being built. Not published, and not addressable.
- **provisional** — published, immutable under its own manifest digest, and
  expected to be superseded by a later revision covering more blocks. Each
  revision records its revision number and the digest it supersedes.
- **sealed** — final. A sealed generation is never superseded, and only sealed
  generations enter the previous-generation commitment chain, so a provisional
  revision can never fork that chain.

Publication is append-only in digests. A revision's bytes never change under a
digest that has been served; superseding one publishes a new digest beside it
rather than replacing what is there. Promotion to sealed is the last such step,
and like every other it must not mix a directory from one revision with pages
from another.

A wallet may use a provisional revision for a current balance, but the coverage
it takes from one is **provisional coverage**: recorded together with the
revision digest that produced it, never cached as immutable, and re-derived
from the superseding revision or from the sealed generation when one appears. A
balance derived from a provisional revision may be displayed as current. It must
not be reported as settled coverage.

## Chain anchors without regular-sync coupling

Every reconstructed balance is relative to a precise chain state: a network,
height and block hash. The transparent protocol therefore needs an accepted
anchor, but it must not prescribe regular wallet synchronization as the way to
obtain one.

The client consumes a wallet-owned abstraction:

```text
ChainAnchor {
    network
    genesis_hash
    height
    block_hash
    observed_tip_height
}
```

Possible providers include the wallet's existing sync engine, a minimal
header/checkpoint client, comparison across independent block-hash sources, or a
trusted signed checkpoint service. A research POC may accept the indexer's
declared anchor under the existing trusted-indexer assumption, but it must
record that choice and compare the anchor with an independent source during
evaluation.

Historical generations form a committed, gapless sequence. Each manifest binds
its range, parent and terminal block hashes, content digests, and the previous
generation commitment. A wallet can validate that sequence against one accepted
terminal anchor without retaining or downloading ordinary compact data for
every covered block.

This structural validation establishes network, range, ordering, freshness and
consistent generation selection. It does not prove that the indexer included
every event. Completeness remains the separate trust decision described below.

Only the recent provisional tail normally requires active reorg tracking.
Historical generations may be sealed after an explicit confirmation depth. A
deep reorg crossing a sealed boundary requires a replacement committed sequence
and rollback to an anchor accepted through the same provider interface.

## Public activity filter

Each generation has a public probabilistic filter containing every exact script
with at least one receive or spend in that generation. A wallet downloads the
same filter bytes as every other wallet and tests its derived scripts locally.

- A negative result lets the wallet skip private retrieval for that script and
  generation, under the complete-indexer assumption.
- A positive result is only a candidate. The wallet privately retrieves exact
  records and rejects false positives.

The filter may use BIP-158 GCS encoding, but a generation filter is a new
application profile rather than a canonical per-block Bitcoin basic filter. It
needs deterministic generation-key derivation and explicit chain-range
metadata.

Larger generations trade precision for deduplication. They remove repeated
scripts across many blocks and reduce filter overhead, but a match locates only
an epoch. This is acceptable when the fallback retrieves a script's exact
history from the generation; it is poor when the fallback scans every block in
the generation.

The Golomb-Rice parameters do not need retuning for a wider generation. An
element is mapped into `[0, N*M)`, so the per-tested-element false-positive rate
is `1/M` regardless of how many elements the filter holds: a generation filter
over twelve thousand scripts is exactly as precise per query as a block filter
over twelve. What grows with the element count is the filter's *size*, which is
what cross-block deduplication pays for.

Measured over Ironwood activation to height 3,473,474, the complete set of
generation filters — every byte a restoring wallet downloads unconditionally
— is **0.40 to 0.51 MB**, against roughly 108 MB for coverage-matched compact
scanning of the same range. Wider generations deduplicate better: 19 generations
cost 0.40 MB where 50 cost 0.51 MB. This is the public half of the cost only;
whether the private retrieval it gates is also cheaper is the open question.

Filter bytes must be globally identical, immutable, cacheable, and committed by
public digests. A service must not personalize filters. Otherwise a malicious
service could insert chosen scripts into wallet-specific filters and observe
whether a private query follows.

## Private script directory

The logical directory maps a script to its event storage:

```text
script -> {
    exact_script
    total_event_count
    inline_events
    first_event_page
    event_page_count
}
```

Physically place records into fixed-size PIR rows:

```text
directory_row =
    H(generation_salt || raw_script) mod (directory_row_count * segment_count)

segment = directory_row / directory_row_count
row     = directory_row mod directory_row_count
```

A script's pages are a contiguous extent, not a list of locators: the record
carries the first page and the page count, and the pages follow in order. A list
would make the record's size depend on the history's length, which a fixed-width
row cannot express, and would force either a truncated record or a second
geometry. A generation's page space is its segments' page tables concatenated in
segment order, so an extent crossing a segment boundary is ordinary addressing
rather than a special case.

Every record must carry the exact script bytes. The client must reject hash
collisions, misplaced records, duplicate records, and filter false positives.
Rows contain a fixed number of uniformly padded slots; empty and occupied slots
must have identical response geometry.

Bucket overflow must have a bounded private resolution strategy, such as a fixed
number of independently salted candidate rows. It must never trigger a public
script, address, outpoint, or transaction lookup.

If the physical database is further sharded, temporal sharding is preferable to
script-prefix sharding. Selecting a script-hash shard reveals part of the
script's identity before PIR begins. Selecting a temporal generation reveals a
chain range but preserves the full script anonymity set within that generation.

Note that a generation's *row count* is pinned and identical everywhere, so the
directory's physical geometry carries no information about how busy its range
was. Only the generation's block span does, and that is published anyway.

## Private event pages

Histories that do not fit inline in the directory record are stored in
fixed-size pages:

```text
EventPage {
    generation_id
    exact_script
    page_ordinal
    page_count
    event_count
    minimum_height
    maximum_height
    events[]
    padding
}
```

The private directory response supplies a page extent. Follow-up page selection
must itself use PIR; a page index must never appear in a plaintext request.
Otherwise the directory is private while its page access pattern identifies the
selected history.

Pages are ordered by the canonical order key. Height bounds allow
a previously synchronized wallet to navigate to the first page newer than its
checkpoint. Navigation must either be private or use a public layout that is
identical for all scripts; a free plaintext index would disclose history shape.

Large or spammed histories require more pages. Query budgets must leave work
resumable and incomplete rather than truncating history or silently advancing
coverage.

## Optional private transaction-detail table

**Not built in the first implementation.** The script event ledger is sufficient
for current balance and net history, which is what the first measurement has to
establish. Publishing this table as well would add a third table per generation,
a third published parameter set, and a third seal limit, and would make the
headline byte comparison harder to read rather than stronger. It stays specified
here, and the seal logic still counts transaction ids, so adding it later is a
build change rather than a re-partition.


The script event ledger is sufficient for current balance and basic net history.
It is not always sufficient for full transaction presentation. A restored
wallet may know that one of its outputs was spent without knowing all external
recipients, change outputs, or enough input values to calculate the exact fee.

If those features are required, publish a second private table:

```text
txid -> CompactTransparentTransaction {
    txid
    transparent_input_outpoints[]
    transparent_outputs[] { value, script }
    other wallet-required transaction metadata
}
```

After discovering a relevant transaction ID through the script ledger, the
wallet retrieves its compact transaction privately from the same temporal
generation. Transaction IDs and transaction-table locators must not appear in
plaintext requests.

## Transparent as Separate Ledger 

Transparent funds are treated as an independent subledger from shielded. It is cleaner and removes unnecessary coupling.

For each transaction:
```
transparent credits = owned transparent outputs created
transparent debits  = owned transparent outputs consumed
transparent delta   = credits - debits
```

Then,
- Transparent → shielded is simply a transparent debit/withdrawal.
- Shielded → transparent is a transparent credit/deposit.
- Transparent → another user is also a transparent debit.
- Self-transfer between transparent addresses produces matching debit and credit.
- The transparent balance remains fully reconstructible without scanning shielded pools.

The transparent ledger does not need to determine whether an outflow was shielding, payment, or another mixed-pool operation. Those are wallet-wide semantic classifications performed later, if desired.

## Wallet reconstruction

The wallet replays validated events into an outpoint-indexed map:

```text
on ReceiveEvent:
    reject duplicate or conflicting outpoint
    utxos[(txid, output_index)] = {
        value,
        script,
        creation_height,
        creation_block_hash,
        coinbase
    }

on SpendEvent:
    require the consumed outpoint to exist
    remove utxos[(spent_txid, spent_output_index)]
    record the confirmed spender
```

An unexplained spend of an unknown receive is unresolved work. It may indicate
missing historical coverage, an incomplete index, an unsupported script, or a
malformed response. It must not be ignored.

After complete replay through an accepted anchor:

```text
current UTXOs = recovered receives - recovered spends

confirmed balance = sum(values of current UTXOs)

spendable balance = sum(values of current UTXOs allowed by wallet policy)
```

Spendability remains wallet-owned policy. The wallet applies confirmation depth,
coinbase maturity, script support, key availability, locks, dust policy, and
coin selection. Recovery of a receive does not by itself make an output
spendable.

For history presentation, group events by transaction ID:

```text
net value =
    sum(wallet-owned outputs created by the transaction)
    - sum(wallet-owned outputs consumed by the transaction)
```

Transactions with both receives and spends may be self-transfers or change.
Exact recipient, change and fee presentation can use the optional private
transaction-detail table plus the wallet's derivation metadata.

## Address discovery

PIR does not remove the wallet's HD discovery problem. The wallet must:

1. Derive its normal external and change script window.
2. Test those scripts against the relevant generation filters.
3. Privately retrieve exact events for candidates.
4. Advance its existing gap-limit rules only on validated real activity.
5. Repeat until the wallet's normal discovery policy is satisfied.

Do not introduce a separate PIR gap rule. Newly derived or imported scripts must
be checked over their required earlier chain range before inheriting current
coverage. Imported keys absent from the wallet backup cannot be reconstructed.

## Synchronization and atomic progress

For each generation covered by an accepted anchor, the wallet:

1. Verifies network and genesis identity, generation commitment continuity,
   gapless ranges, and the terminal commitment against its accepted anchor.
2. Validates and locally matches the public activity filter.
3. Privately retrieves directory rows for candidate scripts, across every
   segment of the generation.
4. Validates exact script identities and directory invariants.
5. Privately retrieves every required event page and transaction-detail row.
6. Validates event identities, canonical order, range, uniqueness and
   references.
7. Applies events to pending ledger state.
8. Atomically commits ledger state and per-script generation coverage, once
   every segment of the generation has been processed.

A crash, timeout, malformed response, stale generation, incomplete segment set,
query-budget exhaustion, or authentication failure leaves coverage unadvanced.
Completed private rows may be journaled for idempotent resumption. Coverage
taken from a provisional tail revision is recorded with that revision's digest
and re-derived when a later revision or the sealed generation appears.

Coverage is tracked by script/discovery scope and chain range, separately from
shielded synchronization. A partial result may display known events as
incomplete, but it must not claim a synchronized balance.

On a reorg reported by the anchor provider, rewind events and coverage to an
accepted common ancestor, rebuild the UTXO map from retained events, and discard
affected pending work. Immutable historical generations are reusable only while
their terminal commitments remain in the sequence rooted at the accepted
anchor. This mechanism depends on the chain-anchor interface, not on shielded or
compact-block synchronization.

## Timing and shard-selection privacy

PIR hides the selected row within the selected database. It does not inherently
hide:

- which temporal generation was selected;
- when the wallet queried it;
- how many queries it issued;
- request and response sizes;
- whether a filter hit triggered contact;
- linkage between repeated sessions.

If the wallet queries a generation only after a filter hit, the PIR server can
infer probable activity in that generation. Repeated selections reveal a coarse
activity timeline that may correlate with public transparent transactions.

Content sealing sharpens this slightly. Generation spans vary with chain
activity, and the map that publishes them is public, so a busy period is covered
by a narrower generation than a quiet one. Selecting a generation therefore
localizes the wallet's activity in time more precisely during busy periods than
during quiet ones — by roughly the same factor the spans differ, which
measurement puts at about fourteen. This is a property of the partition, not of
the wallet's behaviour, and it applies equally to every wallet that selects that
generation.

An exceptional generation's segment count is public as well, and a wallet
querying such a generation pays a proportionally larger amount of work. That
discloses nothing about which script was selected, but it does make those
generations distinguishable by traffic volume, which the analysis counts rather
than ignores.

The initial design may explicitly accept generation, timing and query-count
leakage, but it must not describe that as hiding whether the wallet had activity.
Possible stronger modes include:

- fixed schedules with indistinguishable dummy queries;
- fixed query counts and response geometry;
- delayed batching of matches;
- querying a fixed group of generations with decoys;
- an unlinkable relay or privacy network;
- a global PIR namespace that hides generation selection at higher cost.

There is a fundamental trade-off: suppressing PIR work after a negative filter
reveals, to a linked observer, whether a later query occurred. Cover traffic can
hide that bit only by paying some of the work the filter was intended to avoid.

## Trust and completeness

The initial profile trusts both the publisher and the PIR retrieval service to
provide correct, complete, mutually consistent data for the declared chain
anchor. Wallet validation checks structural consistency; it does not
cryptographically prove event inclusion, completeness, or non-equivocation.
Users who do not accept that trust can run their own infrastructure or verify
the result through independent chain replay. Query privacy remains a separate
requirement, subject to the generation, timing and query-count leakage described
above.

PIR provides query privacy; it does not prove database completeness. A filter
negative is safe only if the indexer included every covered receive and resolved
spend. Block hashes and table digests bind returned data but do not prove the
absence of omitted events.

Production use for balance or spendability therefore requires either:

1. an explicit trusted-indexer policy accepted by the wallet owner; or
2. a separately specified completeness mechanism.

Under this initial trust policy, evaluate the reconstructed ledger in shadow mode
against an independent archive-derived result at the same anchor. Require exact
event and UTXO equality, not only equal final balances.

## Recommended first implementation

Build on the existing integrated prototype rather than replacing its model:

- immutable content-sealed generations, with a published height-to-generation
  map;
- one public activity filter per generation, under its own keying;
- a fixed-row private script directory, identical in geometry across
  generations;
- two candidate directory buckets, independently salted per generation, with
  an added segment as the fallback that guarantees placement;
- a small number of inline events, holding a script's *newest* history so an
  active script needs no page query;
- fixed-size, privately selected event pages;
- exact event replay with atomic checkpoint advancement;
- immutable provisional tail revisions alongside the sealed historical
  generations.

Do not build the private compact-transaction table yet. Build it only if the
wallet adapter confirms that its existing transaction-display path cannot
reconstruct required details from script events and locally retained data.

Choose the seal parameters from a census over the real event journal rather than
from an estimate, since they are schema and re-partition the chain when changed.
The first such census over Ironwood activation to height 3,473,474 found that
the scripts limit stops binding above roughly eight thousand: past that, page
rows seal every generation and the scripts limit is inert. Whichever limit binds
is the one that is actually being chosen.

Evaluate candidate seal parameters using:

- filter bytes after cross-block script deduplication;
- directory and page PIR upload and response bytes;
- per-generation published setup, multiplied across the generations a restoring
  wallet touches;
- false-positive private queries;
- active, inactive, restoration and catch-up workloads;
- large and externally spammed histories;
- realized occupancy distributions, including overshoot past target;
- multi-segment generations, and the query and byte multiple they cost on both
  sides;
- tail-revision churn: bytes re-fetched when a revision is superseded, and again
  when its range seals;
- client memory and latency on the minimum supported device;
- server CPU, concurrency, and resident bytes per loaded generation, as a
  function of its segment count;
- reorg, interruption and tail-publication behavior;
- timing and generation-selection leakage.

The architecture succeeds only when the complete cost is lower than the same
coverage through transparent compact scanning:

```text
public filters
+ PIR setup
+ directory queries
+ event-page queries
+ optional transaction-detail queries
+ retries and cover traffic
< coverage-matched compact scanning
```

### What is measured and what is not

Measured: the event journal from Ironwood activation to height 3,473,474;
chain-density variation and the generation spans it produces; the complete
public filter cost; realized occupancy distributions and overshoot; which limit
binds at each candidate parameter set.

Not yet measured, and each capable of overturning the result: the private
retrieval cost of a real restoration workload against these generations;
per-generation published setup summed over the generations a wallet touches;
resident server memory per loaded generation; and end-to-end behaviour on a
minimum supported device.

No generation in the measured journal needed more than one segment, and the
measurement covers no tail-revision churn. Both paths are specified and built so
that publication cannot stall, but their cost is projected from the geometry
rather than observed.

Deliberately out of the first implementation, so its cost is neither measured
nor claimed: the private transaction-detail table, and bounded evaluation-key
reuse across generations. Both are specified; neither is built. A result that
depends on either must say so.

This design can reconstruct confirmed balance and history while hiding exact
script and page selections. It does not, without additional measures, hide the
queried time range, the occurrence of activity-triggered contact, wallet network
identity, or omissions by the indexer.
