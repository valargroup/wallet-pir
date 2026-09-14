# M0 commands

All wallet commands ran against the reconstructed source, never the user's working
source. Build artifacts reused `CARGO_TARGET_DIR=/Users/roman/projects/wallet-libraries/target`.
The build profile is Cargo's configured test profile (optimized with debuginfo),
not release. Tool versions and hardware are recorded in `environment.json`.

After reconstruction, append `coverage-probe.rs` to
`zakura/wallet-store/tests/transparent.rs` and copy `status-probe.dart` to
`zakura/wallet-app/client/test/m0_status_probe_test.dart`, only in the isolated copy.

```sh
cargo test --locked --offline -p zakura-wallet-store --test transparent m0_measure_checkpoint_writes -- --nocapture
cargo test --locked --offline -p zakura-wallet-transparent --test store --test recover -p zakura-wallet-facade --test wallet
cargo test --locked --offline -p zakura-wallet-store --test transparent
```

From each isolated package directory:

```sh
# zakura/wallet-app/client
fvm dart pub get --offline
fvm dart test
# zakura/wallet-app/state
fvm flutter pub get --offline
fvm flutter test
# zakura/wallet-app/example
fvm flutter pub get --offline
fvm flutter test
```

Public read-only observations:

```sh
curl -fsS --max-time 15 https://transparent-pir.valargroup.dev/v1/filters/shards
curl -fsS --max-time 15 https://enhance-pir.valargroup.dev/v1/filters/shards
grpcurl -max-time 15 -import-path /Users/roman/projects/wallet-libraries/zakura/wallet-lwd/proto -proto service.proto -d '{"height":"3477098"}' us.zec.stardust.rest:443 cash.z.wallet.sdk.rpc.CompactTxStreamer/GetTreeState
grpcurl -max-time 15 -import-path /Users/roman/projects/wallet-libraries/zakura/wallet-lwd/proto -proto service.proto -d '{}' us.zec.stardust.rest:443 cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock
```

Private read attempts and exact errors are in `ssh-observations.json`. The requested
coordinator path was the existing rollout's status file. No restart was attempted.
