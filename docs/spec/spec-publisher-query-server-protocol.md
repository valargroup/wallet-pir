# Publisher–query server protocol

## Transparent artifact transfer and verification

This protocol specifies the protocol between Publisher and Query Server to ingest and refresh the data.

1. The publisher completes preparation and consistency checks, computes each
   immutable artifact's BLAKE3 checksum and exact byte count, and publishes both
   alongside the artifact. Checksums explicitly identify their algorithm and
   digest; a BLAKE3 digest is never encoded in a field named `sha256`.
2. The query server downloads the artifact over authenticated HTTPS, checks the
   received and stored byte counts, and saves the file with its expected hash.
   It does not hash the artifact or repeat preparation consistency checks before
   activation.
3. The query server may activate the publication and serve queries before the
   artifact's hash has been validated locally.
4. After serving starts, one process-wide background thread validates each newly
   received artifact's hash once. Hashing uses at most one CPU core and pauses
   between bounded chunks while queries are pending or executing. Continuous
   query traffic may defer validation indefinitely.
5. Successful validation is recorded and reused for the unchanged file across
   publications and restarts. A newly received replacement requires its own
   validation; copying a file does not inherit the original file's success.
6. A hash failure withdraws every dependent shard revision, including retained
   revisions, while unrelated shards remain available. The query server refetches
   the artifact and validates the replacement before restoring affected service;
   derived runtimes from the failed artifact must be discarded first.

Small metadata is verified before use. Publication authority, canonicality and
revocation checks remain required regardless of artifact validation status.

This artifact checksum policy also applies to Enhance and Status prepared
distribution objects and metadata. It does not change wallet-protocol source
commitments, public-parameter bindings, chain hashes or upstream-defined hashes.
Those retain their specified algorithms even when they cover the same bytes.
The publisher-query-server discovery contract is major version 2; predecessor
distribution contracts are accepted only by explicit offline migration tools.
