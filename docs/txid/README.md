# Transaction-detail (txid) display

Txid display PIR serves transaction details for history rows. Its serving code
lives in `src/transparent/transparent-query-server`, and its publisher and
controller live in `src/shared/pir-publisher/transparent`, beside the extraction
code both history and display use. The journal, shared display verification and
runtime preparation live in `src/transparent/transparent-shared`, so the
publisher does not depend on the worker application. The existing server and controller binaries still
run as separate processes.
Operations (the `txid-display` deploy driver, its fleet and observation scripts,
unit templates, route snippet and the `transparent-display` service descriptor)
live under `tools/txid/`; scripts live under `tools/txid/` and retained evidence under
[txid evidence](../../evidence/txid/README.md).

- [Txid display](txid-display.md): server protocol, native demo, tiered proof of concept and wallet integration stage.
- [Shipped runtimes plan](txid-display-shipped-runtimes-plan.md): coordinator-built recent display runtimes loaded on workers.
- [Sizing and anonymity findings](transparent-txid-sizing-and-anonymity-findings.md): whole-range sizing estimates and routing limits.
- [Changelog](CHANGELOG.md)
