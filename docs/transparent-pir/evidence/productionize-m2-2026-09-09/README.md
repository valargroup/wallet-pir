# M2 — Bounded native macOS recovery application

Captured 2026-09-09. Responsible execution role: Claude (Fable 5.1), acting as
M2 wallet engineer for Roman under the [handoff](handoff.md). This record closes
the M2 milestone of the [execution plan](../../productionize-plan.md) with the
limits stated at the end. It does not close M1, does not make privately
recovered transparent balances authoritative (M3), and does not authorize
distribution (M6).

## Source, preservation and ownership

| Identity | Value |
|---|---|
| Wallet repository | `zakura-core/wallet-libraries`, primary checkout `/Users/roman/projects/wallet-libraries` |
| Baseline at start | `b6aa1f97fd48744f31f129c59c7d287a7c96d5aa`, clean apart from an untracked `.vscode/` |
| Working branch and worktree | `m2/macos-recovery-beta` at `/Users/roman/projects/wallet-libraries-m2-recovery` |
| Final wallet source | `0ce6d158f` on `m2/macos-recovery-beta` (commits listed below) |
| PIR client pin | unchanged, `22e6bec` |
| PIR documentation source | this repository at `3258c22`, plus the documentation commit that adds this bundle |

The primary checkout was not reset, cleaned or switched; its untracked editor
configuration is untouched. No existing wallet database was opened: the
example's existing wallet lives in the `com.example.zakuraExample` sandbox
container and the beta uses a different bundle identifier, so it has a
different container. `docs/roman_notes.md` was not touched. The M1 operator's
fleet, coordinator, generator, canary and services were not changed.

M0's findings were reproduced against `b6aa1f97f` before anything was changed:
the checkpoint rewrite (this bundle's `baseline-coverage-probe.log`, 2N + 2
exactly), the near-tip suppression (`statusAgainst` still returned the bare
height for `query-budget`, `chain-unknown` and `sync-in-progress` within ten
blocks), and the beta boundaries (native-load failure fell back to the demo,
`$HOME/.zakura-example` was the only profile, the send screen confirmed with a
freshly generated phrase). None had been addressed by the two commits that
landed after M0's capture.

## Commits

Seven commits on `m2/macos-recovery-beta`, each reviewable alone:

- `608732d62` Append a transparent coverage checkpoint without rewriting prior rows
- `d47efe660` Refuse another build's wallet layout before opening it for writing
- `62eed6e99` Let a transparent run be stopped, and check the map is the wallet's chain
- `046e1f4ee` Add a recovery-only wallet configuration and check the server's chain
- `bb30a9128` Build the bridge without sending unless asked, and say so
- `58123c6a4` Never silence why transparent coverage stopped short, and show the details
- `0ce6d158f` Make the example a recovery-only beta with explicit modes and its own profile

The primary checkout's branch is untouched; nothing is pushed.

## Requirement-by-requirement record

The M2 definition of done, verbatim from the plan, and what establishes each
part. "Native test" means a Rust test against the real store, adapter, facade
or in-process shard service; "bridge test" means Dart driving the release
`libzakura_wallet_bridge.dylib`; "live" means the release library against the
public services.

| Requirement | Implementation | Evidence |
|---|---|---|
| A macOS release build uses real native bindings | `fvm flutter build macos --release` with `ZAKURA_MODE=recovery` and the two public transparent endpoints; Cargokit builds `zakura_wallet_bridge` without its `send` feature | `release-build.log`; artifact `ZakuraRecoveryBeta.app`, executable SHA-256 `851df2642730f402c3bfc088dbd5189313c2cdea3713ecd889da893f89c2d4c1`, bundled bridge framework `360b6a5393deb1dfbaa6731a92e89084d99400414dd0238ee7cd9e8d3cfc7fc1`, ad-hoc signed, sandboxed with network client (`release-artifact-hashes.txt`); the separately built `libzakura_wallet_bridge.dylib` the bridge and live tests load is `d04ff35cf751ca93571a074ce40d1a7c4faf8c4c805affc9571864b4e87cf5eb` (`bridge-dylib-hash.txt`); launched and screenshotted (`release-app-recovery-mode.png`) |
| ...and identifies its recovery mode | `ZAKURA_MODE` is required; a banner on every screen names the mode; `build_info()` reports `send_enabled` from the native build and the startup gate refuses a native library that can send | `mode_test.dart` (14), `app_test.dart` (18), bridge test "the native build identifies itself as recovery-only", screenshot |
| Restore/import works end to end | Unchanged facade import path; onboarding in a beta mode offers restore only | Live: fresh phrase restored at birthday tip−30 (`live-recovery-report.txt`); bridge import tests |
| Sync works end to end | Shielded scan plus one transparent run per pass; the run now checks the map describes the wallet's chain, and can be stopped between requests | Live report; 27 adapter recovery tests including stop and failure; 9 facade lifecycle tests |
| Balances and history work end to end | `balance_with_coverage` in one SQLite snapshot; history unchanged | Live report (balance and history read after sync and after reopen); facade and bridge tests |
| Resume works end to end | Store keeps commits; stop raises the transparent stop signal, then the engine token; reopen finds the same anchor and coverage; the next run continues | Live report (stop → close → reopen → resume, coverage compared); adapter tests `a_stopped_run_keeps_its_commits_and_the_next_run_finishes`, `a_restarted_wallet_continues_from_its_commits_without_refetching` |
| Partial or unsupported coverage never appears as a synchronized zero balance | Nothing read → `covered_through: None` (was birthday−1); `statusAgainst` silences only `scan-ahead`/`publication-behind` within ten blocks; unresolved spends and owed pages are counted; `CoverageDetails` shows target, covered, settled, reason, pending, unresolved beside the balance and history | `coverage_test.dart` (6), `coverage_details_test.dart` (4), store test `a_script_never_read_reports_coverage_below_the_birthday`, adapter test `a_wallet_that_has_scanned_nothing_reads_nothing_and_says_so` |
| Direct API calls cannot send | Bridge built without `send`: `send` returns `SendDisabled` before the phrase is read; facade `recovery_only` refuses `send` and `quote` before parsing; `open` sets `recovery_only = !SEND_ENABLED`, not an argument | Bridge test "a direct send is refused before the phrase is read"; facade test `a_recovery_only_wallet_cannot_send_or_quote`; live report (send refused with code 20) |
| Existing databases are unchanged | Separate bundle identifier and container; profile under `Application Support/org.valargroup.zakura-recovery-beta/<mode>` with a marker; a directory made for another mode, or with files and no marker, is refused and left as found; `WalletDb::open` reads versions read-only before any DDL or WAL switch | Store test `an_unsupported_layout_is_refused_before_the_files_are_touched` (file bytes compared); facade test `an_unsupported_layout_is_refused_and_left_as_found`; `mode_test.dart` profile cases; `stat` of the existing container before and after (`existing-wallet-untouched.txt`) |
| Ordinary telemetry contains no seeds, keys, addresses or scripts | Store and adapter refusal messages no longer carry script hex; the diagnostics screen and its copy text carry heights, counts, reasons, hosts and versions only; `debugPrint` of native errors removed from startup | Store test `a_refused_script_is_not_named_in_the_error`; `app_test.dart` "Diagnostics opens and names nothing about the wallet"; live test prints no phrase, key, address or script by construction |

Handoff items beyond the definition of done:

| Item | Disposition |
|---|---|
| Explicit development-demo, validation/shadow and opt-in recovery modes | `ZakuraMode.{demo,shadow,recovery}`; no default; each beta mode has its own profile; shadow figures are labelled validation output and the recovery notice names the M3 gate |
| Native-load failure fails visibly | `startUp` returns `StartupFailed` with the step and message; the gate shows `FailureScreen`; the demo is reachable only by asking for it |
| Endpoint pairing and network identity | Dart: both services, TLS, distinct hosts; native: the same at `Wallet::open` (`Configuration`), the light server's `GetLightdInfo.chainName` checked before open and on every connection, the shard map's `network`/`genesis_hash` checked against the wallet's chain before any read |
| PIR opt-in authority gated on M3 | No authority flag exists to flip: transparent figures in both beta modes carry the unverified notice and the suffix "recovered privately, unverified" / "shadow validation figure, not a balance" |
| Cancellation | `StopSignal` refuses the next transport request; latency bounded by one request; committed shards kept; completion recorded as `stopped`, and a failed run as `failed`, never left at `sync-in-progress` |
| Mnemonic material | Not persisted anywhere; the create path is absent from beta modes; import derives keys and drops the phrase as before |

## Checkpoint-write comparison

Row changes for one appended checkpoint after N existing ones, same probe
shape as M0, same machine, matched source pairs (`coverage-comparison.json`):

| Prior checkpoints | Baseline `b6aa1f97f` | Corrected |
|---:|---:|---:|
| 0 | 2 | 2 |
| 10 | 22 | 2 |
| 100 | 202 | 20 |
| 1,000 | 2,002 | 200 |

The corrected commit reads only the row keyed by `(script, start_height)`.
An identical retry writes only the commit record (1 change); extending the
same shard to a later accepted target rewrites that one row (2 changes); a
rollback inside the tail followed by the re-read at the lower target likewise.
The historical read-delete-reinsert path remains for the one case it was
written for (a second range at the same start) and is asserted not to be
reached by an ordinary append. Replay, replacement, reorg and the library's
store contract suite are unchanged and pass.

## Validation results

All commands and exit statuses are in [commands.md](commands.md); logs are
beside this file. Every command below passed.

| Check | Scope | Result |
|---|---|---|
| `cargo test --workspace --exclude zakura-wallet-lib --exclude zakura-pir-enhance --locked` | The repository's required suite, 51 test binaries | 1,076 passed, 0 failed, 15 ignored (`workspace-tests.log.gz`) |
| `zakura-wallet-store` `tests/transparent.rs` | Store: coverage, rollback, layout preflight, append cost, message hygiene | 19 passed (was 14 at baseline; 5 new) |
| `zakura-wallet-transparent` `tests/recover.rs`, `tests/store.rs` | Adapter against the in-process shard service; library store contract | 27 + 3 passed (25 + 3 at baseline; stop and failure cases new) |
| `zakura-wallet-facade` | Unit, import, lifecycle and wallet suites | 26 + 11 + 9 + 25 passed (25 wallet was 18; recovery-only cases new) |
| `cargo clippy --tests` on the five changed crates | Lints | No warnings in changed code; one pre-existing `large_enum_variant` in `facade/src/send.rs` and one in vendored `zcash_client_sqlite` remain (`clippy.log`) |
| `flutter_rust_bridge_codegen generate` | Regenerated glue, both halves | Clean; `dart analyze --fatal-infos` clean in every package |
| `zakura_client` `dart test` | Models, wallet handle, new coverage presentation | 53 passed (47 at baseline) |
| `zakura_state` `flutter test` | Providers | 38 passed |
| `zakura_ui` `flutter test` | Widgets, new coverage details | 53 passed (49 at baseline) |
| `zakura_example` `flutter test` | App, modes, startup, profile | 32 passed (12 at baseline) (`example-tests.log.gz`) |
| `zakura_bindings` `flutter test --tags native` | The release bridge library through the generated glue | 21 passed (`native-tests.log`) |
| `zakura_bindings` `flutter test --tags live` | Release library against the public services | 1 passed (`live-test.log`, `live-recovery-report.txt`) |
| `make check-docs` in this repository | Links | No broken links |


## Live lifecycle and release build

**Live lifecycle** (`live-recovery-report.txt`, `live-test.log`), through
`libzakura_wallet_bridge.dylib` built from `0ce6d158f` without the `send`
feature, against `https://us.zec.stardust.rest:443`,
`https://enhance-pir.valargroup.dev` (filters) and
`https://transparent-pir.valargroup.dev` (shards). The wallet is a phrase
generated in the test process and never written anywhere; nothing on the
chain is its money, so it matched no filter and sent no private query.

| Step | Observed |
|---|---|
| Native build | `send_enabled=false`, schema `transparent-shard-v7`, layout 4 |
| Server identity | chain `main`, height 3,477,838, ECC LightWalletD v0.5.4 |
| Restore | fresh account, birthday 3,477,808 (tip − 30) |
| Direct send | refused with `SendDisabled` before the phrase was read |
| First sync | 5.037 s to `idle` at scanned 3,477,838 = tip; not failed |
| Transparent coverage | `complete`, accepted target 3,477,838, covered 3,477,838, settled 3,477,807, 0 pending, 0 unresolved, synchronized |
| Balance and history | 0 and empty, presented as "Transparent coverage through block 3477838" |
| Stop, close, reopen | account and birthday found again; completion, target and coverage identical |
| Resume | second run 3.523 s, still complete at 3,477,838, coverage did not go backwards |

An earlier run of the same test against the pre-commit build (formatting-only
differences) gave the same shape at tip 3,477,834 in 5.572 s and 3.537 s.

**Release build.** `ZakuraRecoveryBeta.app` was built twice: once during
development and again from the committed `0ce6d158f`; the hashes recorded in
`release-artifact-hashes.txt` are the second build's; the Dart layer,
`App.framework/App`, hashes identically in both
(`297d9026dd2af82a43bcf1f578d8d37350dcf17160eae3c3de58d002625af003`), and
only the Rust halves differ. The first build was launched from the build
directory: it loaded the native library, read
`build_info`, confirmed the light server's chain, created its profile marker
under its own sandbox container, opened an empty wallet and showed the
recovery banner with restore-only onboarding (`release-app-recovery-mode.png`).
Driving the restore form in the release binary by injected input was
abandoned: the app launched from a terminal could not be made frontmost, and
the two synthetic clicks issued before that was noticed landed in the user's
browser window instead (a page footer; nothing was submitted). The restore,
sync, stop, reopen and resume flows are therefore evidenced through the
release library above rather than through the release binary's screens; the
Dart layers between them are covered by the 32 example tests. The existing
example wallet's files were hashed before and after every launch and are
identical (`existing-wallet-untouched-before.txt`, `-after.txt`).


## Limits and what remains

- **M3 is not closed by anything here.** No independent ledger or history
  comparison was run; the live wallet is a fresh synthetic one with no history,
  so it exercised the map, geometry, filters, target, store and persistence
  paths and sent no private query. Opt-in PIR balances remain non-authoritative
  and are labelled so in both beta modes.
- **Live coordination.** No M1 operator session was reachable from this session.
  The live check was limited to one fresh wallet: a map read, filters from its
  birthday forward, and one `init`; no `POST` query. It is recorded here so the
  operator can account for it; it was not cleared in advance.
- **Release artifact.** Ad-hoc signed (`CODE_SIGN_IDENTITY = "-"`), not
  notarized, not distributed. Signing and channel belong to M6.
- **Unsupported scripts.** A wallet-derived script is always P2PKH, so
  `outside_coverage` is zero in practice; the state carries no persisted count of
  it. Such a script would hold `covered_through` below the birthday and keep the
  balance unsynchronized, but it would not be named. Recorded for M3's
  "unsupported scripts are explicit" gate.
- **Shadow comparison store.** Shadow mode isolates its profile and labels its
  output; the independent reconstruction it is compared against is M3's fixture,
  not part of this application.
- **macOS only**, one account per store, no receive screen in beta modes, no
  send anywhere: all deliberate for a recovery-only beta and reversible.
- **Product assumptions made here**, to be confirmed: beta modes offer restore
  only (no create, no receive); the demo keeps receive and has no send at all;
  the light server default is the public `us.zec.stardust.rest:443` and is the
  one default the beta carries.

## M3 fixtures this leaves for the next milestone

- Independent block/journal reconstruction at the same accepted anchor for a
  wallet with transparent history (receives, spends, coinbase, self-transfer,
  multiple scripts, old receive spent recently, gap-limit discovery).
- Interruption around persistence boundaries with real events (the stop test
  here covers the adapter with a synthetic set; a process kill during a private
  query on the release build is not yet exercised).
- Same-height forks and cross-tier reorgs on controlled infrastructure.
- A shadow profile compared against that reconstruction, with neither path
  repairing the other.
