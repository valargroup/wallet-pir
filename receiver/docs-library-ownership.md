# Receiver protocol library ownership

Receiver records, cryptographic checks, witnesses, PIR client/evaluator code,
publication state and wallet-facing HTTP handlers are released by wallet-libraries.
This repository consumes exact library revisions while their initial registry
releases are pending. Each dependency also declares the intended release version.

The existing service package/import names remain as adapters:

- `receiver-directory` re-exports the protocol and owns the indexer's SQLite
  store. Database schema, atomic coverage, provider reads and rewinds are unchanged.
  Storage errors now live at `receiver_directory::store::Error`; protocol errors
  remain at `receiver_directory::Error`.
- `receiver-pir` re-exports the protocol client and evaluator.
- `receiver-pir-server` adds process identity and aggregate HTTP metrics to the
  same handlers used by wallet recovery and scenario tests.
- `pir-native` re-exports the shared native profiles. Golden tests retain their
  fixed protocol bytes, including P8, P16 and dithered query coverage.
- `pir-control::admission` re-exports shared admission primitives. Service refusal
  responses, queue limits and cancellation semantics stay unchanged.

Node ingestion, RocksDB access, provider polling, startup, monitoring and
production configuration remain here. No deployment or node pin change is part
of this extraction. A service can keep a tested library release while a newer
wallet release advances Common independently.

The draft depends on the wallet-libraries receiver extraction. After separate
publication authorization, publish native and admission libraries first, then
receiver directory, PIR and HTTP support. Replace these Git pins with their
exact registry versions and regenerate Cargo.lock before a service release.
No wallet-pir tag needs to move for a wallet-library release.
