# Txid display genesis journal census, 2026-10-07

Raw output of the read-only journal census (`txid-sizing-export --journal-census`,
wallet-pir PR #129 head `452f50d4`) over a genesis-to-3,508,673 txid display journal
ingested on the coordinator on 2026-10-07. Retained raw input for the layout analysis;
no conclusions are recorded here yet.

- `census.json.gz`: the census JSON, gzip -9 -n. The uncompressed JSON's SHA-256 is
  `ce0f3fff30d963b99cd6431634a66f50e781c63c4675e4c8c0dc7fef9b29c86b` (as in the receipt).
- `receipt.json`: written by the run. Its `compiler` field is wrong: it records the
  host's default `rustc` (1.91.0); the binary (`f1ce7749…`) was built with the
  repository-pinned 1.97.1 (build log, unit `txid-census-build-452f50d4`).
- Ingest history: `transparent-event-ingest` (release `d191f86b`) with `--txid-display`
  from height 0, stop 3,508,673. Heights 0–~414k ran on `/srv/zakura` with 4 workers;
  it was stopped when `/srv/zakura` crossed its 20% headroom floor and the journal was
  moved to a dedicated volume (`/srv/txid-display-genesis`, Terraform `983cbedb`).
  The rest ran under `eatmydata` with 6 workers; one stop at 1,866,000 (the RocksDB
  secondary lost an SST file to node compaction) resumed from its checkpoint. Complete at
  22:56:45 UTC: 3,508,674 blocks, 354,009,971 events, 17,024,724 display records.
- Memory figures in the JSON are the source reservation formula, not native RSS, and
  predate the reservation true-up (`3d727bfc`).
