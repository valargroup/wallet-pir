# Recovery vectors

`recovery-vectors.json` is a byte-for-byte copy of wallet-libraries'
`zakura/receiver-directory/tests/fixtures/recovery-vectors.json` at the pinned
revision 3c57bbb2b9c6a92eba8da48846d88d555646125d: the pinned encodings and digests
of a paid bucket, an empty bucket, a cumulative paid set, a seen set, a bucket index
and a catalog head. `src/recovery.rs` checks that the indexer's grouping of payments
reproduces them. Replace it with the library's copy when the pin moves.
