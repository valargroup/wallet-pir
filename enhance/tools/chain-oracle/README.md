# Chain-derived public query oracle

`enhance-chain-oracle` constructs exact-answer records directly from canonical
Zakura blocks. It does not read the Enhance journal, coordinator records, or
worker state. It parses raw blocks, independently writes the 653-byte wallet
record format, checks action counts against tree-size growth, and rechecks block
hashes before accepting the output. It also requires the local coordinator to
report two published replicas covering the selected height. The raw chain
transaction parser is the same `zakura-chain` dependency used by the server,
so a separate wallet release test remains necessary.

Build with `cargo build --locked --release -p enhance-chain-oracle` from the
pinned source on a Linux build host and verify the binary checksum when copying
it to the coordinator. Run it there after latency measurements to
avoid making node RPC reads part of the public load experiment. Set `END_HEIGHT`
to a published anchor from `/v1/health`, then use a new output directory:

```sh
/opt/enhance-chain-oracle \
  --cookie /root/.cache/zakura/.cookie \
  --end-height "$END_HEIGHT" --max-blocks 128 --count 16 \
  --out /root/enhance-chain-oracle-1
```

The output contains `oracle.json` for `enhance-pir-load-test --oracle` and a
manifest with source block heights, hashes, tree sizes, and the oracle SHA-256.
Keep the manifest and exact binary checksum with the public query report.
Run the load driver from a separate host through the public HTTPS endpoint.
This checks positional answers; restore and recovery require the consuming
wallet release and independent wallet state assertions.
