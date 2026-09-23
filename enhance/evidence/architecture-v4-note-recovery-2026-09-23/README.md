# Authenticated synthetic Ironwood record — September 23, 2026

A new integration test uses pinned `zakura-orchard` 1.0.1 and
`zcash_note_encryption` 0.4.2 APIs to construct a synthetic V3 Ironwood note.
These crates were already in the lockfile; they are now direct test dependencies.
Fixed inputs are public test material, not wallet credentials. No cryptographic
primitive or production key-handling API was introduced.

The 737-byte Enhance record is published through a real coordinator and two
worker HTTP listeners, retrieved via the encrypted v4 client at position 32,
and checked byte-for-byte. With the fixture's compact-action commitment/context,
incoming decryption and outgoing recovery both reproduce the expected note,
recipient and 512-byte memo.

Negative checks reject the Orchard V2 domain, wrong commitment, wrong incoming
and outgoing viewing keys, altered encrypted-note bytes, and altered outgoing
ciphertext. Altering outgoing ciphertext does not break incoming decryption.
An explicit metadata-boundary check demonstrates that changing expiry height
does not invalidate note authentication: indexer metadata is not authenticated
by successful note decryption.

[Integration test](tests.log): passed in 7.32 seconds.
[Clippy](clippy.log): passed with warnings denied. Formatting and diff checks passed.

This is a constructed cryptographic fixture using the pinned library, not an
independent canonical-chain oracle, trusted-chain proof, downstream wallet scan,
known-answer vector from another implementation, or full wallet conformance
certificate. Those requirements and hardware/deployment gates remain open.
