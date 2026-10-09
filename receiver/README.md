# Receiver directory

The receiver directory lets a restored wallet find Ironwood payments to keys it
cannot rescan for, such as its dynamic IVK refund and incoming keys. It maps each
canonical 43-byte Ironwood receiver to the payments recoverable with the all-zero
outgoing viewing key (OVK). Coinbase recipients are excluded. Every Ironwood
Action still counts toward note positions, including coinbase and unrecoverable
outputs.

`receiver-directory` holds zero-OVK extraction, records, publications, common
witnesses and, with the `store` feature, the indexer's SQLite store.

## Records and publications

A 285-byte record holds version (1 byte), receiver (43), page (4), total pages
(4), present flag (1), height (4), block hash (32), txid (32), transaction index
(4), Action index (4), note position (8), Action input nullifier (32), note
commitment (32), ephemeral key (32) and ciphertext prefix (52). Integers are
little endian and hashes use protocol byte order. The Action input nullifier is
decryption context, not the received note's spend nullifier. Encoding and
decoding refuse a note commitment or Action input nullifier that is not a
canonical field element, or an ephemeral key that is not a valid Ironwood
ephemeral public key. A receiver's payments are ordered by note position, one per
page from zero, and every page repeats the total.

Rows are 4096 bytes and hold 14 records plus zero padding. A domain-separated
hash of the salt, the receiver's tag and the page selects a row. Publications
start at 8192 rows, the fewest that PIR clients and servers accept. A crowded
bucket retries up to 16 salts derived from the terminal hash, the first being
the hash itself, and only then doubles the table, up to 65536 rows. Overflow at
the maximum fails the candidate instead of dropping records. So do more records
than the table has slots, caught before placement; a supplied manifest claiming
more records than its slots is malformed. The manifest (profile
`ironwood-zero-ovk-receiver-v1`) binds the network, inclusive block coverage,
boundary hashes, tree positions, geometry, salt, record count, the filter sets
and the SHA-256 of the rows and of the filter file. The immutable revision is a
domain-separated SHA-256 of every field at fixed width, as Enhance and Status
hash their manifests, and a manifest with a field this version does not know is
refused.

`Snapshot::validate` checks a supplied publication whole, before a server prepares
it: the row and filter digests, the declared filter sets and a paid set of exactly
the records' receivers, every slot and row padding, each record's coverage and
bucket, the exact record count, every receiver's pages, each continuing the last
in chain order as `snapshot::check_next` requires, and that no two records share an
output or note position. No chain trust is implied. A record stored outside its
bucket would otherwise make lookups of that receiver find nothing.

## Filters

Each publication carries an `IWFLT1` filter file of labeled BIP 158 Golomb-coded
sets of receivers, keyed by the salt, so a wallet can test its receivers before looking
any up. The manifest declares every set's label and size:

- `paid`: every receiver with a payment in the publication.
- `<provider>/recent`: every Orchard receiver a swap provider was given within the
  window the manifest declares, as a payout or refund address, in any status.
- `<provider>/seen`: every Orchard payout address the provider was given since its
  feed started, which the manifest also declares.

For each provider set the manifest also declares when the feed's last complete read
began, so a wallet can tell how current the set is. A recent window reaches back
from that read.

The indexer publishes `near-intents/recent` (24 hours) and `near-intents/seen`
from the NEAR Intents explorer, and adds a provider's sets only once its feed
has completed a read. Wallets use the sets they recognize and ignore the rest,
so a new provider needs no format change.

Sets are built with `bitcoin::bip158`, as Transparent's filters are, with P = 10
and M = 1,533 and SipHash keys from a domain-separated SHA-256 of the salt, so a
receiver outside a set matches it about once in 1,533 tests. The file holds the set
count, then for each set in label order its label, byte length (little endian) and
BIP 158 encoding (a CompactSize count and the coded deltas), about 1.6 bytes per
receiver, and is at most 8 MiB. A reader decodes every value and checks the
padding before matching. Every wallet downloads the same file, so testing it
reveals nothing. The publisher is trusted for the sets' completeness, as for the
rows.

## Witnesses

The `IWPROOF1` witness file has a 152-byte header binding the genesis, revision,
terminal height and hash, tree size and root. Sorted 37-byte nodes (level, index,
hash) follow, with every sibling of each payment position. It is capped at 64 MiB.

Building the file reads every commitment since the empty tree, so the store
refuses a history longer than the caller's commitment limit before reading it.

## Tests

`cargo test -p receiver-directory --features store` covers recovery of a public
mainnet refund, records, publications, store restart and rollback, and witnesses
against an independent tree. Recovery follows
`zcash/zips@afa086bd976e316612a5c06fb139429958d07d84`, NU6.3 proposal, section
4.19.3 (`decryptovk`).
