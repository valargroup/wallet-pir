# Transaction-detail (txid) display

Txid display PIR serves transaction details for history rows. Its serving code
lives in `src/transparent/transparent-shard-server`, and its publisher and
controller live in `src/transparent/transparent-filter-server`. Both history and
display use the journal and extraction code in `src/transparent/transparent-ingest`.
The existing server and controller binaries still run as separate processes.
Scripts live under `tools/txid/` and retained evidence under
[txid evidence](../../evidence/txid/README.md).

- [Txid display](txid-display.md): server protocol, native demo, tiered proof of concept and wallet integration stage.
- [Shipped runtimes plan](txid-display-shipped-runtimes-plan.md): coordinator-built recent display runtimes loaded on workers.
- [Sizing and anonymity findings](transparent-txid-sizing-and-anonymity-findings.md): whole-range sizing estimates and routing limits.
- [Changelog](CHANGELOG.md)
