# Publisher–query server protocol

## Transparent artifact transfer and verification

This protocol specifies the protocol between Publisher and Query Server to ingest and refresh the data.

1. The publisher completes preparation and consistency checks, computes each
   immutable artifact's SHA-256 hash and exact byte count, and publishes both
   alongside the artifact.
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
