# Txid display v2 native demo, 2026-10-08

[Manifest](manifest.json), [native report](native-demo.json) and
[qualification output](qualification.log) for `make transparent-txid-demo` at
clean source `9557d843`. This is local protocol evidence, not a deployment or
capacity run. Scope and the reproducible command: [txid display](../../docs/txid-display.md).

The run covers frozen mainnet blocks 347499–347501 and genesis, with fifteen
complete parent transactions. It goes through the production extractor and
journal (v2x source sidecars), a checkpoint restart, the tiered publisher, an
archive-owner and a recent-replica worker behind an edge, and the wallet's
`transparent-txid-client` over loopback HTTP.

Every one of the 14 eligible entries matched, byte for byte, the entry
`verify_fixture.py` derives with the Python standard library alone from the raw
blocks and parents, both after extraction and through private lookups. The
demo required these cases:
- an ordinary send whose change returns to its source address;
- two source scripts;
- three 300-output transactions, each still one entry;
- unshielding;
- coinbase;
- the genesis P2PK output, which has no address.

Every lookup after warm-up sent two queries of 40,200 B and received two
replies of 5,648 B. An absent txid sent the identical transcript. No txid, tag
or output script appeared in any path, header or query string. Wrong-binding
queries returned 400, an unserved revision 409, and placement and foreign-codec
controls dispatched no query. A separate `--corrupt-oracle` process exited
nonzero and wrote no report.

The publisher is trusted for sources, as for fees. This run does not establish
production capacity, the omission rate on mainnet, cryptographic release
readiness, wallet recovery, or Vizor correctness.
