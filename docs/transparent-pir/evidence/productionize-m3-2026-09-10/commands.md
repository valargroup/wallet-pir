# M3 commands

All wallet commands ran in the dedicated worktree
`/Users/roman/projects/wallet-libraries-m3-correctness` on branch
`m3/wallet-correctness`, created from wallet HEAD `0ce6d158f`. Cargo reused
`CARGO_TARGET_DIR=/Users/roman/projects/wallet-libraries/target` (the primary
checkout's build cache, not its sources or wallet files). The primary checkout
was not modified, reset or cleaned. No existing application wallet was opened.
Tool versions are in `environment.json`. Every command below exited 0 unless
its section says otherwise.

## Correctness fixtures, reorgs, interruption, kill, shadow

```sh
cargo test --locked --offline -p zakura-wallet-transparent \
  --test recover --test store --test blocks_model --test correctness \
  --test reorg --test interrupt --test shadow --test kill
cargo test --locked --offline --release -p zakura-wallet-transparent --test kill -- --nocapture --test-threads=1
ZAKURA_TRANSPARENT_BLOCKS_JSONL=<pir>/data/transparent-pir-evaluation/mainnet-3470268-3471419.jsonl:<pir>/data/transparent-pir-evaluation/mainnet-3470268-3471419-resumed.jsonl \
  cargo test --locked --offline --release -p zakura-wallet-transparent --test real_chain -- --nocapture
```

## Store, facade, bridge and the repository's required suite

```sh
cargo test --locked --offline -p zakura-wallet-store --test transparent
cargo test --locked --offline -p zakura-wallet-facade
cargo test --locked --offline -p zakura_wallet_bridge
# .github/workflows/verify.yml, `tests` job
cargo test --workspace --exclude zakura-wallet-lib --exclude zakura-pir-enhance --locked --offline
cargo clippy --offline --tests -p zakura-wallet-transparent -p zakura-wallet-store \
  -p zakura-wallet-facade -p zakura_wallet_bridge -p zakura-wallet-sync -p zakura-wallet-lwd
cargo fmt --all --check   # fails at baseline on zakura/wallet-core/src/detected.rs; unchanged by M3
```

## Generated bindings

```sh
cd zakura/wallet-app/bindings && flutter_rust_bridge_codegen generate   # 2.11.1; `git status` clean afterwards
```

## Dart and Flutter checks

```sh
cd zakura/wallet-app/client   && fvm dart pub get && fvm dart analyze --fatal-infos && fvm dart test
cd zakura/wallet-app/state    && fvm flutter pub get && fvm dart analyze --fatal-infos && fvm flutter test
cd zakura/wallet-app/ui       && fvm flutter pub get && fvm dart analyze --fatal-infos && fvm flutter test
cd zakura/wallet-app/example  && fvm flutter pub get && fvm dart analyze --fatal-infos && fvm flutter test
cd zakura/wallet-app/bindings && fvm flutter pub get && fvm dart analyze --fatal-infos lib test
```

## Bridge libraries

```sh
# The beta library, as the release build carries it.
cargo build --locked --offline -p zakura_wallet_bridge --release
shasum -a 256 target/release/libzakura_wallet_bridge.dylib
cd zakura/wallet-app/bindings && ZAKURA_BRIDGE_LIBRARY=<beta dylib> fvm flutter test --tags native test/native_test.dart
# The rehearsal library: the same source with the loopback feature, never shipped.
cargo build --locked --offline -p zakura_wallet_bridge --release --features fixture-loopback
shasum -a 256 target/release/libzakura_wallet_bridge.dylib
```

## Kill through the real bridge against fixture services on the loopback

```sh
cd zakura/wallet-app/bindings
ZAKURA_FIXTURE=1 ZAKURA_FIXTURE_LOOPBACK=1 \
ZAKURA_BRIDGE_LIBRARY=<fixture-loopback dylib> \
CARGO_TARGET_DIR=/Users/roman/projects/wallet-libraries/target \
ZAKURA_FIXTURE_REPORT=<evidence>/kill-fixture-report.txt \
fvm flutter test -r expanded --tags fixture test/kill_fixture_test.dart
```

The test starts `ZAKURA_FIXTURE_SERVE=1 cargo test --offline --release -p zakura-wallet-transparent --test fixture_services -- --nocapture --test-threads=1`
itself and runs the wallet under test as `fvm flutter test --tags fixture-child test/kill_fixture_child_test.dart`.

## macOS release builds

```sh
cd zakura/wallet-app/example
fvm flutter build macos --release --dart-define=ZAKURA_MODE=recovery \
  --dart-define=ZAKURA_TRANSPARENT_FILTERS=https://enhance-pir.valargroup.dev \
  --dart-define=ZAKURA_TRANSPARENT_SHARDS=https://transparent-pir.valargroup.dev
fvm flutter build macos --release --dart-define=ZAKURA_MODE=shadow \
  --dart-define=ZAKURA_TRANSPARENT_FILTERS=https://enhance-pir.valargroup.dev \
  --dart-define=ZAKURA_TRANSPARENT_SHARDS=https://transparent-pir.valargroup.dev
shasum -a 256 build/macos/Build/Products/Release/ZakuraRecoveryBeta.app/Contents/MacOS/ZakuraRecoveryBeta
```

## Not run

```sh
# The deployed accepted-anchor regression suite, after M1 acceptance, from the PIR checkout at the accepted fleet revision:
TRANSPARENT_REGRESSION_OUT=<evidence>/transparent-regression make transparent-regression
# defaults: TRANSPARENT_LOAD_URL=https://transparent-pir.valargroup.dev,
#           TRANSPARENT_REGRESSION_FILTER_URL=https://enhance-pir.valargroup.dev,
#           TRANSPARENT_REGRESSION_FIXTURE=server/transparent-regression/fixtures/mainnet.json

# The kill through the release library against the public services, in a window agreed with the M1 operator:
ZAKURA_LIVE=1 ZAKURA_KILL=1 ZAKURA_TRANSPARENT_FILTERS=https://enhance-pir.valargroup.dev \
ZAKURA_TRANSPARENT_SHARDS=https://transparent-pir.valargroup.dev \
ZAKURA_PHRASE_FILE=<outside the repository> ZAKURA_BIRTHDAY=<height> \
ZAKURA_LIVE_REPORT=<evidence>/kill-live-report.txt \
fvm flutter test --tags kill test/kill_recovery_test.dart

# The application-level shadow comparison on public paths, after the release app has synced a shadow profile with real history:
cargo run -p zakura-wallet-transparent --release --example shadow_compare -- \
  --profile "$HOME/Library/Containers/org.valargroup.zakura-recovery-beta/Data/Library/Application Support/org.valargroup.zakura-recovery-beta/shadow" \
  --untouched "$HOME/Library/Containers/org.valargroup.zakura-recovery-beta/Data/Library/Application Support/org.valargroup.zakura-recovery-beta/recovery" \
  --expected <reducer snapshot at the same accepted anchor> --report <evidence>/shadow-comparison.json
```

## Documentation

```sh
make check-docs
```
