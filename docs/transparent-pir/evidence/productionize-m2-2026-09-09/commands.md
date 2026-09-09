# M2 commands

All wallet commands ran in the dedicated worktree
`/Users/roman/projects/wallet-libraries-m2-recovery` on branch
`m2/macos-recovery-beta`, created from wallet HEAD `b6aa1f97f`. Cargo reused
`CARGO_TARGET_DIR=/Users/roman/projects/wallet-libraries/target` (the primary
checkout's build cache, not its sources or wallet files). The primary checkout
was not modified, reset or cleaned; its untracked `.vscode/` was left alone.
No existing application wallet was opened. Tool versions are in
`environment.json`.

## Checkpoint-write comparison

Baseline, in a detached scratch worktree at `b6aa1f97f` with the M0 probe
appended:

```sh
git worktree add --detach <scratch>/wallet-baseline b6aa1f97fd48744f31f129c59c7d287a7c96d5aa
cat docs/transparent-pir/evidence/productionize-m0-2026-09-09/coverage-probe.rs \
  >> <scratch>/wallet-baseline/zakura/wallet-store/tests/transparent.rs
cargo test --locked --offline -p zakura-wallet-store --test transparent m0_measure_checkpoint_writes -- --nocapture
```

Corrected, from the M2 worktree (the probe is now a committed test that also
asserts the bound):

```sh
cargo test --locked --offline -p zakura-wallet-store --test transparent appending_a_checkpoint_writes_only_its_own_row -- --nocapture
```

## Native, database and adapter checks

```sh
cargo test --locked --offline -p zakura-wallet-store --test transparent
cargo test --locked --offline -p zakura-wallet-transparent --test recover --test store
cargo test --locked --offline -p zakura-wallet-facade
cargo test --locked --offline -p zakura_wallet_bridge
# the repository's required suite (.github/workflows/verify.yml, `tests` job)
cargo test --workspace --exclude zakura-wallet-lib --exclude zakura-pir-enhance --locked --offline
```

## Generated bindings

```sh
cd zakura/wallet-app/bindings && flutter_rust_bridge_codegen generate   # 2.11.1
```

## Dart and Flutter checks

From each package directory under `zakura/wallet-app`:

```sh
# client
fvm dart pub get && fvm dart analyze --fatal-infos && fvm dart test
# state, ui, example
fvm flutter pub get && fvm dart analyze --fatal-infos && fvm flutter test
# bindings (native tests need the release library)
cargo build --locked --offline -p zakura_wallet_bridge --release
fvm flutter test --tags native test/native_test.dart
```

## Live lifecycle through the real native library

```sh
ZAKURA_LIVE=1 \
ZAKURA_TRANSPARENT_FILTERS=https://enhance-pir.valargroup.dev \
ZAKURA_TRANSPARENT_SHARDS=https://transparent-pir.valargroup.dev \
ZAKURA_LIVE_REPORT=<evidence>/live-recovery-report.txt \
fvm flutter test --tags live test/live_recovery_test.dart
```

## macOS release build

```sh
cd zakura/wallet-app/example
fvm flutter build macos --release \
  --dart-define=ZAKURA_MODE=recovery \
  --dart-define=ZAKURA_TRANSPARENT_FILTERS=https://enhance-pir.valargroup.dev \
  --dart-define=ZAKURA_TRANSPARENT_SHARDS=https://transparent-pir.valargroup.dev
shasum -a 256 build/macos/Build/Products/Release/ZakuraRecoveryBeta.app/Contents/MacOS/ZakuraRecoveryBeta
```

## Documentation

```sh
make check-docs
```
