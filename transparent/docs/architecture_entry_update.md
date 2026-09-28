# Transparent PIR entry layout update

Date: 2026-09-28. Status: implemented in source as `transparent-shard-v9`
on the current 4,096-byte native rows. Not accepted as a contract change,
not published, and not deployed. The live fleet and the version-1 production
journal are unchanged.

This document records the `transparent-shard-v7` directory and page entry
layouts it was written against, then defines a candidate compact layout. The
candidate replaces the exact raw script in each private record with a 14-byte
salted cryptographic script tag. It does not change the PIR construction, row
width, table row counts, filter profile, directory placement algorithm or
choice table. The slot and event counts in the body assume 3,584-byte rows.
The implementation keeps the 4,096-byte row, so those counts do not apply.

## Implementation

`transparent-shard-v9` uses the tag and the 87-byte event defined below, on
4,096-byte rows:

- directory entry: 192 bytes, 21 slots per row (was 16 under v8);
- page fragment: 46 events (was 41);
- shared rows: 33 / 19 / 13 entries at 1 / 2 / 3 paged events (was 25 / 15 / 11);
- PIR query and response sizes: unchanged at a given row count;
- journal: version 2, about 7% smaller after a separate re-ingest into a new
  directory. Opening a version-1 journal returns an error and leaves the files
  in place.

The builder increments `tag_salt_counter` and recomputes every tag when two
indexed scripts collide. It does not publish a duplicate and does not drop a
script. The wallet derives the tag from the verified manifest and accepts only
that tag. Inline occupancy is a zero-free prefix. For `first_page >= 1` the
wallet fetches that row, reads `K`, then fetches the rest. Completeness is
structural: ordinals `0..K-1`, non-final fragments full, canonical order.

The candidate changes deterministic exact-script attribution into computational
attribution: a 112-bit tag under a per-revision salt that cannot be predicted
before the revision's transactions are mined. It therefore requires correctness
and privacy review, a new shard schema, complete republication and
wallet/server compatibility gates before it can be selected.

## Current layout

The current schema is `transparent-shard-v7`. Directory and page tables both
use fixed 3,584-byte rows. Fixed row width and zero padding keep response size
independent of row occupancy. All integers are little-endian.

### Directory rows

A directory row contains:

- bytes 0–3: occupied-entry count (`u32`);
- up to 14 fixed 248-byte directory entries;
- 108 trailing zero bytes when all 14 slots are occupied, or more zero padding
  when fewer entries are present.

Each directory entry contains:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 2 | Raw script length |
| 2 | 40 | Exact raw script, zero-padded |
| 42 | 4 | Total events for the script in this shard |
| 46 | 1 | Inline event count |
| 47 | 1 | Reserved, zero |
| 48 | 4 | First page row |
| 52 | 4 | Page fragment count |
| 56 | 192 | Up to two fixed 96-byte events |

The newest two events are inline. A history of zero, one or two events requires
no page lookup. Older events are stored in ascending canonical order in the
page table. `total_events`, the inline count and the page count let the wallet
reject a truncated or internally inconsistent history.

The directory uses two independently salted candidate rows per script.
Without a valid public directory-choice table, the wallet privately retrieves
both rows and searches for the exact raw script. With a choice table, it
retrieves the selected candidate only. The row bytes are identical in both
cases.

### Page rows

A page row contains:

- bytes 0–3: entry count (`u32`);
- one or more variable-length page entries;
- zero padding through byte 3,583.

Each page entry has a 64-byte header:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 2 | Raw script length |
| 2 | 40 | Exact raw script, zero-padded |
| 42 | 4 | Fragment ordinal |
| 46 | 4 | Fragment count |
| 50 | 4 | Event count |
| 54 | 4 | Minimum event height |
| 58 | 4 | Maximum event height |
| 62 | 2 | Reserved, zero |
| 64 | `96 × event count` | Events in canonical order |

An entry carries between 1 and 36 events. Short histories of equal paged length
share rows: a row holds 22 one-event entries, 13 two-event entries or 10
three-event entries. A full 36-event fragment uses 3,520 bytes plus the 4-byte
row header. Histories longer than one fragment receive a private contiguous run
of rows and do not share their final row.

The exact raw script in every page entry independently binds a page fragment to
the wallet's requested script. The wallet checks it rather than trusting the
directory's page locator.

### Event records

Both receive and spend records are exactly 96 bytes. They carry chain height,
transaction position and the receive or spend identity, but no script. The
enclosing directory or page entry is therefore the event-to-script binding.
Fields unused by an event kind, reserved bytes, unused slots and trailing row
bytes must all be zero.

## Proposed Design

Replace the variable-length raw-script representation in both private entry
types with one fixed 14-byte salted cryptographic tag:

```text
tag_salt =
    SHA-256(
        "transparent-shard-tag-salt-v1\0"
        || u64_le(shard_id)
        || terminal_block_hash          // 32 bytes, internal byte order
        || u32_le(tag_salt_counter)
    )

script_tag =
    first_14_bytes(
        SHA-256(
            "transparent-shard-script-tag-v2\0"
            || tag_salt
            || u16_le(raw_script_length)
            || raw_script
        )
    )
```

This is a hash of the complete raw script, not the first 14 bytes of the
script. A raw prefix would preserve common standard-script prefixes and would
provide a weaker, script-shape-dependent binding.

The tag is public-data-derived and is not a secret or a password hash. Domain
separation prevents another SHA-256 use from being interpreted as this record
identity. The length prefix makes the input framing explicit.

### Tag salt

The salt exists so that nobody can precompute two scripts with the same tag
and place both in a shard. Without it, a 14-byte tag collision costs about
\(2^{56}\) SHA-256 evaluations, and two cheap transactions would then make a
shard unpublishable under a fail-closed collision rule.

- `shard_id` and `terminal_block_hash` are the manifest's existing fields for
  this revision. The terminal block hash is converted from its display hex to
  the 32-byte internal order before hashing.
- `tag_salt_counter` is a new manifest field, bound by the manifest digest. It
  starts at 0 and increases only when the builder finds a tag collision.
- The salt is not stored as a free value. The wallet recomputes it from the
  three inputs and never accepts a salt supplied separately, so a publisher
  cannot choose a predictable one.
- Every transaction in a revision is mined at or before its terminal block, so
  the salt is unknown when any indexed script enters the chain. A moving-tip
  revision gets a new terminal block, and therefore a new salt, on each
  republication. A sealed shard's salt is fixed, but no script can join it.
- A miner can grind the terminal block hash, but each candidate salt yields a
  collision among the shard's scripts with probability about
  \(n^2 / 2^{113}\) (about \(4 \times 10^{-24}\) at \(n = 2 \times 10^{5}\)),
  so grinding gives no practical advantage.
- The tag salt is independent of the directory bucket salt and the choice-table
  hashes, which stay keyed on the raw script. Changing the counter changes
  only tags: row placement, page allocation and the choice table are unchanged.
- The salt is a deterministic function of chain data and the counter, so a
  rebuild of the same revision reproduces the same bytes.

The proposed layout deliberately retains today's 3,584-byte row width. The
separate native/4,096-byte-row proposal can be evaluated after this change's
storage, correctness and compatibility effects are isolated.

Integer fields after the tag are packed without alignment padding. Parsers must
load them with explicit little-endian reads rather than aligned casts. Unused
entry slots and trailing row bytes must still be zero.

### Proposed event codec

The event shrinks from 96 to 87 bytes. Kind lives in the flags byte; there is
no separate kind byte and no trailing reserved padding. Fields after flags are
packed without alignment:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 1 | Flags: bit 0 = kind (`0` receive, `1` spend); bit 1 = coinbase; bits 2–7 reserved zero |
| 1 | 2 | Transaction index in the block |
| 3 | 4 | Chain height |
| 7 | 8 | Value (receives only; spends leave this zero) |
| 15 | 32 | Txid (receive’s creating tx, or spend’s spending tx) |
| 47 | 4 | Output index (receive) or input index (spend) |
| 51 | 32 | Spent txid (spends only; receives leave this zero) |
| 83 | 4 | Spent output index (spends only) |

Coinbase (bit 1) is valid only on receives; spends must leave it clear. Unknown
flag bits are rejected. The codec also gains one rule: an all-zero record is
not a valid event. `TransparentEvent::from_bytes` must reject it; encoders must
never emit it. Directory inline slots use that pattern as an empty sentinel:
all-zero means the slot is unoccupied; any other bytes decode as an event.
Honest chain events are never all-zero, so the sentinel does not collide with
real receives or spends. Packing kind into flags alone does not create the
sentinel — a non-coinbase receive still has flags `0` — so the explicit
all-zero reject remains required.

### Proposed directory rows

A proposed directory row contains:

- bytes 0–3: occupied-entry count (`u32`);
- up to 18 fixed 192-byte directory entries;
- 124 trailing zero bytes when all 18 slots are occupied.

Each proposed directory entry contains:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 14 | Script tag |
| 14 | 4 | First page row (1-based; 0 = none) |
| 18 | 174 | Up to two fixed 87-byte events |

The entry shrinks from 248 to 192 bytes. A row consequently holds 18 entries
instead of 14, increasing directory capacity by approximately 28.6% without
changing PIR query or response geometry. It does not by itself reduce the
fixed-size response. The tag length remains 14 bytes under the salted-tag
policy from Change 6; with 87-byte events, tags from 14 to 20 bytes all fit 18
directory slots (\(18 \times 198 + 4 = 3{,}568\) at 20 bytes).

There is no stored `total_events`, inline count or page fragment count.

Inline occupancy is the longest zero-free prefix of the two event slots under
the empty-sentinel rule: slot bytes all zero means empty; otherwise decode.
Occupied slots must form a prefix (slot 0 empty and slot 1 occupied is
malformed).

Page extent uses a 1-based locator so “no pages” does not collide with PIR
row 0:

```text
first_page_field = 0           → no page queries
first_page_field = r (r ≥ 1)   → contiguous run starts at PIR row (r - 1)
```

- `first_page_field = 0`: the history is exactly the occupied inline slots
  (0, 1 or 2 events).
- `first_page_field = r ≥ 1`: both inline slots are occupied and older events
  live in a private contiguous page run starting at PIR row `base = r - 1`.
  The wallet fetches that first page, matches the script tag, reads
  `fragment count = K` and `ordinal = 0` from the entry, then fetches
  `base + 1 .. base + K - 1`. Every fragment must report the same `K` and an
  ordinal in `[0, K)`.

Completeness is structural rather than against a directory total: non-final
fragments carry 40 events, the final fragment carries 1–40, ordinals cover
`0 .. K-1` exactly, and inline/page event order matches the canonical sort.
The page query budget for a paged script is known after the first page
response, not from the directory alone.

The wallet derives the requested script's tag locally and accepts only the
matching entry.

### Proposed page rows

Each proposed page entry has a 34-byte header:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 14 | Script tag |
| 14 | 4 | Fragment ordinal |
| 18 | 4 | Fragment count |
| 22 | 4 | Event count |
| 26 | 4 | Minimum event height |
| 30 | 4 | Maximum event height |
| 34 | `87 × event count` | Events in canonical order |

The maximum rises from 36 to 40 events per fragment: 41 events still do not
fit in a 3,584-byte row. Packing improves for short histories: one-event
capacity rises from 22 to 29 entries, two-event capacity from 13 to 17 and
three-event capacity from 10 to 12. A full fragment uses 3,514 bytes plus the
4-byte row header, leaving 66 zero bytes.

The page tag must equal the locally derived requested tag and agree with the
directory entry. Fragment ordinals, the shared `fragment count` learned from
the first page, heights, event order and zero padding retain their checks.
Events inside a fragment still use an explicit `event count`; the all-zero
sentinel applies to directory inline slots. Pages are not chained by next-row
pointers: the 1-based `first page row` field plus the first fragment's `K`
define the contiguous zero-based PIR extent.

### Collision and failure policy

The proposed format cannot provide the current format's unconditional
byte-for-byte script equality. The tag is 112 bits, and \(2^{112} \approx
5.2 \times 10^{33}\). The estimates below use \(n = 2 \times 10^{5}\) indexed
scripts per shard, near observed heavy-archive sizes.

- **Indexed scripts colliding with each other.** The builder checks every tag
  in the revision for duplicates. On a duplicate it increments
  `tag_salt_counter`, recomputes every tag and checks again. It never selects
  one of the colliding scripts and never publishes a revision with duplicate
  tags. The accidental probability is about \(n^2 / 2^{113} \approx 4 \times
  10^{-24}\) per revision, so the counter is expected to stay at 0. Publication
  fails only if the counter reaches a fixed limit, which no honest build
  reaches.
- **Duplicate tags within a decoded row** must be rejected by the wallet.
- **A wallet script absent from the shard colliding with an indexed script.**
  The builder cannot detect this because it does not know the absent script.
  Such a collision could turn a filter false positive into incorrect event
  attribution. The wallet compares tags only within the candidate directory
  rows it retrieves, and reaches pages only through a directory match, so the
  probability per lookup is about \(18 / 2^{112} \approx 4 \times 10^{-33}\)
  with a choice table and \(36 / 2^{112} \approx 8 \times 10^{-33}\) without
  one. Across \(10^{12}\) lookups (for example, \(10^6\) wallets, each making
  1,000 syncs with 1,000 filter false positives) the total is about
  \(10^{-20}\). The effect would be a wrong event attributed to the wallet,
  not a privacy leak.
- **Adversarial collisions.** Because the salt is unknown when scripts are
  mined, an attacker cannot precompute a colliding pair. Flooding a shard with
  \(m\) scripts raises the accidental probability to about
  \((n + m)^2 / 2^{113}\), roughly \(10^{-16}\) even at \(m = 10^{9}\), and a
  success only forces a counter increment and rebuild. This protection depends
  on the salt rules above; if the salt were predictable, a collision would cost
  about \(2^{56}\) work.
- **Wallet salt check.** The wallet recomputes `tag_salt` from the manifest's
  `shard_id`, `terminal_block_hash` and `tag_salt_counter`, and derives its
  requested tags from that salt. The terminal block hash is the one the wallet
  already checks against its chain view.
- **Malicious servers are out of scope for tag length.** Rows carry no
  integrity authentication, so a server that lies can already return wrong
  events at any tag length. The tag bounds honest-data misattribution only.
- A malformed tag, duplicate tag, inconsistent directory/page tag or any
  existing count/order/padding failure must stop recovery for the shard. It
  must not fall back to prefix matching or public address retrieval.

Acceptance therefore requires an explicit decision that computational
attribution at these probabilities is sufficient for wallet recovery.

The schema must use a new opaque name. Existing `transparent-shard-v7`
manifests and rows must retain their current meaning. A rollout requires new
golden encodings, collision tests, malformed-record tests, exact recovery
comparison, full republication, and coordinated wallet/server deployment with
the predecessor retained for rollback.

The salt adds three required tests:

- the salt changes when the terminal block hash changes and when the counter
  changes, and is reproduced exactly by a rebuild of the same revision;
- a fixture with a forced tag collision makes the builder increment the
  counter and publish a collision-free revision rather than fail or select a
  script;
- the wallet rejects a manifest whose tags were built under a salt other than
  the one recomputed from its `shard_id`, `terminal_block_hash` and
  `tag_salt_counter`.

## Change Log

### 2026-09-28 — Change 1: replace exact scripts with 20-byte script tags

Proposed replacing the exact raw script in directory and page entries with a
domain-separated SHA-256 tag truncated to 20 bytes.

Motivation:

- reduce repeated per-entry script identity from 42 bytes (length plus padded
  script) to 20 bytes;
- increase directory slots per 3,584-byte row from 14 to 15;
- improve packing density for short page histories;
- preserve a fixed identity derived from the complete script rather than using
  a raw prefix.

Semantic change:

- current records authenticate ownership by exact byte equality;
- proposed records authenticate ownership under a 160-bit collision-resistance
  assumption;
- construction-time collision rejection does not cover an absent wallet script
  colliding with an indexed script.

No implementation or deployment decision is recorded by this entry.

### 2026-09-28 — Change 2: shorten tags to 18 bytes and pack without reserved padding

Revised the candidate from Change 1: truncate the same domain-separated SHA-256
digest to 18 bytes, drop directory and page reserved alignment bytes, and pack
integer fields immediately after the tag and counts.

Motivation:

- fit 16 directory entries per 3,584-byte row (223-byte entries, 12 trailing
  zeros) instead of 15 at 20 bytes with reserved padding;
- a 19-byte tag still yields a 224-byte packed entry, which does not fit 16
  slots;
- raise one-event shared page packing from 25 to 26 entries per row;
- accept the weaker 144-bit tag because only accidental collision probability
  is in scope, and at observed shard sizes that residual remains negligible
  (\(\sim n / 2^{144}\)).

Layout effect relative to Change 1:

- directory: 228-byte / 15-slot / 3 reserved → 223-byte / 16-slot / no reserved;
- page header: 44 bytes with 4 reserved → 38 bytes with no reserved;
- collision assumption: 160-bit → 144-bit.

No implementation or deployment decision is recorded by this entry.

### 2026-09-28 — Change 3: drop stored inline event count

Removed the 1-byte inline event count from the proposed directory entry.
Inline occupancy is derived as `min(total_events, 2)` from the packing rule
that the newest two events are inline and longer histories always fill both
slots.

Motivation:

- the stored count was redundant with `total_events` under that invariant;
- unused slots remain zero-padded, but zero bytes cannot denote emptiness
  because an all-zero event record decodes as a valid receive;
- save one byte per directory entry without changing directory slot count.

Layout effect relative to Change 2:

- directory: 223-byte entries with explicit inline count → 222-byte entries
  with derived occupancy; still 16 slots (28 trailing zeros instead of 12).

No implementation or deployment decision is recorded by this entry.

### 2026-09-28 — Change 4: drop stored directory page fragment count

Removed the 4-byte page fragment count from the proposed directory entry.
Page extent length is derived from `total_events` with the same rule the
builder already uses (`page_count = ceil((total_events - 2) / 36)` when
paged, else 0). The directory still stores `first page row` as the start of
the contiguous private run; pages are not linked by next-row pointers.

Motivation:

- under fixed `INLINE_EVENTS` and `EVENTS_PER_PAGE`, the stored count was
  redundant with `total_events`;
- the wallet still knows the query budget before any page fetch;
- each page entry continues to carry its own `fragment count`, which must
  equal the derived directory value;
- save four bytes per directory entry without changing directory slot count.

Layout effect relative to Change 3:

- directory: 222-byte entries → 218-byte entries; still 16 slots (92 trailing
  zeros instead of 28).

No implementation or deployment decision is recorded by this entry.

### 2026-09-28 — Change 5: drop total_events; empty-sentinel inline; learn K from first page

Removed `total_events` from the proposed directory entry. Paired two rules so
occupancy and page extent no longer need a directory length field:

1. Event codec: all-zero 96-byte records are invalid as events and serve as the
   empty inline-slot sentinel; occupied inline slots are a zero-free prefix.
2. Paged histories: the directory stores a 1-based first-page locator
   (`0` = no pages, `r ≥ 1` → PIR row `r - 1`) so absence does not collide with
   zero-indexed row 0. A non-zero locator implies both inline slots are full;
   the wallet fetches that row, reads `fragment count = K`, then fetches the
   rest of the contiguous run. Completeness is structural (ordinals, shared
   `K`, full non-final fragments).

Motivation:

- `total_events` existed mainly to derive inline/page counts and checksum
  recovery; the sentinel plus first-page `K` replace both roles;
- save four more bytes per directory entry;
- accept that the page query budget for a paged script is known after the
  first page response rather than from the directory alone.

Layout effect relative to Change 4:

- directory: 218-byte entries → 214-byte entries; still 16 slots (156 trailing
  zeros instead of 92);
- `first page row` encoding changes from zero-based (with a separate count
  saying whether pages exist) to 1-based with `0` as the absent sentinel.

No implementation or deployment decision is recorded by this entry.

### 2026-09-28 — Change 6: 14-byte salted tag and 17 directory slots

Replaced the unsalted 18-byte tag with a 14-byte tag under a per-revision salt
derived from `shard_id`, the revision's terminal block hash and a new manifest
field, `tag_salt_counter`. Changed the builder collision rule from failing
publication to incrementing the counter and rebuilding.

Motivation:

- after Changes 3–5 removed nine bytes, the 18-byte tag no longer sat at a
  directory slot boundary: tags from 15 to 21 bytes all give 16 slots, and 18
  to 21 bytes give identical page packing, so 18 bytes saved nothing over 21;
- 14 bytes is the longest tag that fits 17 directory entries per row;
- an unsalted tag lets an attacker precompute two colliding scripts (about
  \(2^{72}\) work at 18 bytes, \(2^{56}\) at 14) and pay to both, which under
  the fail-closed rule would make the shard permanently unpublishable;
- a salt that cannot be predicted before the revision's transactions are mined
  prevents that precomputation, and the counter lets the builder recover from
  any collision without failing;
- the earlier "144-bit collision resistance" wording conflated collision and
  second-preimage resistance; the policy now states each case separately.

Layout effect relative to Change 5:

- directory: 214-byte entries / 16 slots / 156 trailing zeros → 210-byte
  entries / 17 slots / 10 trailing zeros;
- page header: 38 bytes → 34 bytes; one-event entries per row 26 → 27,
  three-event 10 → 11, two-event unchanged at 15; full fragment leaves 90
  trailing zeros instead of 86;
- manifest: new `tag_salt_counter` field;
- tag strength: 144-bit unsalted → 112-bit salted.

No implementation or deployment decision is recorded by this entry.

### 2026-09-28 — Change 7: pack kind into flags (bit 0 kind, bit 1 coinbase)

Moved event kind from its own leading byte into the flags byte. Bit 0 is kind
(`0` receive, `1` spend); bit 1 is coinbase (receives only). The former kind
byte is reserved zero. `EVENT_BYTES` stays 96.

Motivation:

- two kinds and one coinbase bit fit in one byte; a separate kind byte was
  wasted width;
- keep fixed 96-byte events so directory inline slots (192 bytes) and page
  packing (`36 × 96`) are unchanged;
- the all-zero empty-sentinel rule from Change 5 is unchanged: packing kind
  into flags does not make a non-coinbase receive distinguishable from the
  empty pattern by flags alone.

Layout effect relative to Change 6:

- event header: separate kind byte + flags → flags with kind in bit 0 and
  coinbase in bit 1, plus one extra reserved zero byte;
- no change to directory or page entry sizes, slot counts or row geometry.

No implementation or deployment decision is recorded by this entry.

### 2026-09-28 — Change 8: shrink events to 87 bytes

Removed the Change 7 reserved stand-in for the kind byte and the trailing
8-byte reserved pad. Fields after the flags byte pack without alignment.
`EVENT_BYTES` becomes 87.

Motivation:

- neither reserved region held a defined field; both existed only to keep a
  round 96-byte width;
- a fixed width is required, but 96 is not; shrinking improves directory and
  page packing inside the same 3,584-byte PIR row without changing response
  size.

Layout effect relative to Change 7:

- event: 96 bytes → 87 bytes (flags with kind in bit 0 / coinbase in bit 1,
  then packed ledger fields through spent output index; no reserved bytes);
- directory: 210-byte / 17-slot / 10 trailing zeros → 192-byte / 18-slot /
  124 trailing zeros; capacity vs current v7 rises from ~21.4% to ~28.6%;
- page: max events per fragment 36 → 40; one-event entries per row 27 → 29,
  two-event 15 → 17, three-event 11 → 12; full fragment leaves 66 trailing
  zeros instead of 90;
- tags from 14 to 20 bytes now all fit 18 directory slots; the candidate keeps
  the 14-byte salted tag from Change 6.

No implementation or deployment decision is recorded by this entry.
