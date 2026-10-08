# Transparent PIR activity metadata implementation plan

Status: proposed implementation plan. No codec, journal, wallet, or Vizor change
is implemented by this document; no publication, deployment, or qualification is
claimed. The current schema remains described in the
[transparent architecture](../transparent/docs/architecture.md). The
[recovery contract](../transparent/docs/contract.md) continues to govern coverage,
privacy, and spendability.

## Goal and accepted coverage

Recover enough transaction metadata through existing script directory/page PIR
to format ordinary confirmed transparent activity without a separate fee lookup.
Full transaction details are a separate future capability. "Fully displayed"
means an accurate activity title, amount semantics, pool label, date, and status;
it does not mean a complete recipient list, memo, or transaction-detail screen.
Confirmation and display-detail completeness are independent.

| Case | Covered before txid enrichment | Explicitly accepted limitation |
| --- | --- | --- |
| Transparent-only send funded entirely by the selected account | Aggregate amount leaving the account, fee, transparent classification, chain status/date, after complete owned-effect recovery | Recipient addresses and individual external outputs wait until opening |
| Several owned transparent inputs or external recipients | Group events by spending txid; show the aggregate outgoing amount under the same complete-recovery and ownership conditions | No per-recipient breakdown |
| Ordinary transparent-only receive | Owned received amount, transparent classification, chain status/date | Sender details come from [txid display v2](../transparent/docs/txid-display.md): the first address-shaped source, with further source scripts named as an omission |
| Locally created transaction with retained records | Preserve its existing payment details and classification | Do not replace rich records with partial summaries |
| Shared funding across accounts or parties | Known selected-account movement and whole-transaction fee | Do not infer that account's payment amount or fee share |
| Self-transfer or cross-account transfer | Ownership/scope-based presentation where supported | Net movement alone does not reproduce gross self-payment presentation |
| Shielding or unshielding involving owned outputs | Combine transparent and shielded recovery | Classification stays provisional when other effects could change it |
| Shielded send to an external transparent address | Preserve facts from existing shielded recovery | Owned-script TPIR does not supply the external output; the new metadata alone cannot complete this case |
| Other mixed-pool transaction | Known owned effects and independent chain status | Exact payment breakdown and pool classification may remain incomplete |
| TEX or another multi-transaction operation | Individual recovered transactions | Original grouping and combined operation fee are not guaranteed |
| Restored swap/gift-card operation | Underlying recovered financial activity | Application intent and labels require retained records or separate evidence |
| Pending, expired, or conflicted transaction | Existing local-send and status paths | Confirmed TPIR does not provide pending history |
| Incomplete ledger coverage | Visible known activity marked partial | No final account movement, payment amount, or spendability claim from incomplete evidence |

These are recovery capabilities, not guarantees that every account has completed
recovery. Preserve richer local-send records and classify only from evidence.

## Minimal metadata and encoding

This proposed extension adds only:

| Transaction metadata | Meaning | Proposed encoding |
| --- | --- | --- |
| Optional exact fee | Actual whole-transaction fee calculated by the publisher; unknown, zero, and not applicable remain distinct | Canonical unsigned base-128 variable-length integer when present; a presence bit |
| Transparent input count | Complete count of non-coinbase transparent inputs, not merely inputs discovered for this wallet; zero for coinbase | Canonical unsigned base-128 variable-length integer |
| Has shielded components | Presence of any supported shielded transaction component, including historical Sprout, Sapling, Orchard, and Ironwood; not proof of real input/output roles or ownership | One versioned flag bit |

Reuse spare event flag bits only in a newly versioned codec. Do not reinterpret
existing v10 bytes. A 10,000-zatoshi fee takes two bytes and an input count below
128 takes one byte: typically **three additional bytes per event**, with no
additional flag byte. For today's 51/79/43-byte compact receive/spend/local-spend
forms, that example becomes 54/82/46 bytes. Larger values take more bytes.
This is encoding arithmetic, not a measured storage, page-count, or latency claim.

Metadata belongs to the creating transaction for a receive and the spending
transaction for a spend. Initially repeat it on events, verifying agreement for
one transaction identity across scripts and pages. An adjacent receive/local-spend
pair generally refers to two transactions and must not share their fee. Defer
fragment-local metadata deduplication until measurement justifies its complexity.

Exclude transparent output counts/totals, separate or aggregate shielded value
balances, and per-pool presence flags from this minimum proposal. Obtain dates
from wallet-accepted block data using event height; do not repeat timestamps in
events. Keep recipient addresses, memos, and external output breakdowns in detail
recovery. Counts are bounded by the decoded type and supported transaction rules;
monetary values must preserve exact zatoshis and protocol bounds, not a fixed u16
or rounded-unit approximation. Reject noncanonical, overflowing, or truncated
variable-length encodings and unknown version/flag combinations.

The publisher supplies these facts under the existing trusted-indexer accuracy
and completeness model. A txid, publication digest, or successful note decryption
does not authenticate an asserted fee or count. Preserve independently known
local fees; contradictory metadata is an integrity failure, not permission to
overwrite local facts or silently choose one assertion.

## Wallet and activity behavior

Group spend events by spending txid and deduplicate by input index and consumed
outpoint. Join owned receives under that transaction identity. Grouping collects
known inputs; it does not prove the absence of another party's input.

For a non-coinbase transaction, derive the amount leaving the selected account
only when the exact fee is known, there are no shielded components, required
owned-effect coverage and input values are complete, and the number of distinct
inputs owned by that account equals the published complete transparent input
count. Conflicting identities or metadata invalidate the calculation.

`amount leaving account = owned input total - owned output total - fee`

For a 1 ZEC input, 0.5999 ZEC owned change, and 0.0001 ZEC fee, this is
0.4 ZEC. A positive amount supports an aggregate outgoing payment in the ordinary
case; it does not identify recipients. Preserve local intent and ownership/scope
evidence for explicit self-transfers and gross payment presentation. Do not
assign the whole fee to one account in a shared-funding transaction or apply this
formula to mixed transactions. Coinbase is not an ordinary fee-paying send.

When classification or attribution is unresolved, keep a tappable transaction
row with explicit incomplete details. A complete known account movement may be
shown as a debit/credit, including fees, rather than labeled as the payment
amount. If owned recovery itself is incomplete, the movement is also partial or
unavailable. Keep payment amount, account movement, fee availability, detail
completeness, and chain status separate. Do not use missing outgoing output rows
to suppress a known spend. Existing locally constructed transaction details take
precedence over a reconstructed summary.

## Separate future transaction-details PIR

A future **txid PIR** capability retrieves private transaction details separately
from owned-script history discovery. This proposal does not implement that
service or assert that current Enhance PIR is a generic txid payload store.

Opening a transaction prioritizes its missing detail obligation. Render cached
facts immediately after navigation; do not block opening the screen on a network
request. One durable enrichment path can serve both explicitly scheduled
background work and on-demand priority, deduplicated by transaction identity.
The initial policy does not require eager enrichment of every historical row.

Retrieve canonical transaction data privately and validate its identity with the
version-aware transaction library. A txid binds transaction effects according to
its format; do not claim that matching it authenticates all authorizing bytes or
proves mining. Maintain independent accepted-chain placement evidence. Fetch
missing parent information privately only when independent exact-fee calculation
requires prevout values not already known. Verify the parent identity and referenced
output; resolving that output does not require calculating the parent's fee or
recursively recovering its ancestors.

Persist validated facts and bounded, resumable obligations; reuse caches and
refresh the detail screen and activity row after commits. Keep an incomplete,
retryable or explicitly unsupported state on failure. Under `PrivateRequired`,
never fall back to public txid, script, address, outpoint, or parent lookups.
Query identity and protected locators remain private across paging and retries;
variable request counts/timing still require a composed privacy assessment.

Transaction data does not guarantee recovery of contact names, original payment
intent, collaborative payment attribution, or swap/gift-card/TEX grouping. Keep
payload work necessary to recover owned financial effects independent of optional
display enrichment, so closing a detail screen never stops financial recovery.
Recipient, fee-verification, and ledger completion remain distinct capabilities.

## Implementation sequence

1. **Publisher and protocol:** calculate actual fees with canonical,
   version-aware transaction logic and resolved prevouts, including supported
   legacy and mixed formats; never substitute a conventional-fee estimate.
   Produce the complete input count and shielded-components bit. Specify new
   journal and shard publication versions, fresh ingestion/publication lineage,
   decoder bounds, and byte-aware packing. Keep existing journal/publications
   available until explicit consumer cutover; do not rewrite them in place.
2. **Wallet libraries:** add durable metadata and provenance to the proposed
   ledger/history contracts, preserve local records, and apply metadata and
   projection updates atomically. Compare counts and owned effects from a
   consistent snapshot. Contradictions cannot advance trusted coverage; retain
   partial facts and pending work honestly. Reorgs invalidate source-bound
   contributions without deleting independent local facts.
3. **Vizor activity:** expose account movement separately from payment amount and
   make unknown fee distinct from zero. Replace enrichment-dependent history
   suppression and premature classification with visible partial activity.
   Continue existing status, local-send, and application-operation paths.
4. **Qualification:** compare events, fees, counts, and shape assertions with an
   independently operated block-derived oracle on the exact candidate
   publication. Measure directory/page packing, query counts, resource use, and
   whole-wallet activity behavior before accepting the new encoding. Publisher
   verification is an operational control, not an omission/completeness proof.

Executable milestone checklists belong in
[remaining work](../transparent/docs/remaining-work.md) when implementation
tracking is added. This document specifies the proposal rather than duplicating
that authoritative checklist. Fee-only script enrichment and txid detail retrieval
have separate qualification and rollout gates; neither enables private financial
authority or sending by itself.

## Tests and acceptance criteria

- Compare ordinary sends/receives, several owned inputs, several recipients,
  shared funding, cross-account/self-transfers, shielding, external unshielding,
  mixed-pool transactions, and coinbase against independent chain reconstruction.
- Retain exact local-send output/fee data; preserve application grouping where
  independently known and accept missing grouping after seed restoration.
- Exercise incomplete coverage, unresolved input values, both discovery orders,
  contradictory transaction metadata, unknown/zero/non-applicable fees, restart,
  page resumption, and reorgs. Never fabricate payment amounts or hide activity.
- Test canonical variable-length boundaries, overflow/truncation, reserved bits,
  unsupported versions, and metadata attribution to parent receive versus child
  spend. Verify directory inline/page equivalence and greedy fragment boundaries.
- Validate that complete fee metadata needs no txid or parent request. For future
  detail retrieval, capture requests during success, failure, retry, cancellation,
  and policy transitions; prove there is no public fallback and cache reuse avoids
  duplicate retrieval. Parent resolution must stop at required output evidence.
- Report typical encoding examples separately from measured storage and latency.
  Release acceptance requires the versioned candidate, independent oracle,
  consumer compatibility, honest activity states, and measured packing impact.
