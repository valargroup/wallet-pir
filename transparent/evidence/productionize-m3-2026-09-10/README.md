# M3 — Wallet correctness before authoritative PIR use

Captured 2026-09-10. Responsible execution roles, as the
[execution plan](../../docs/remaining-work.md) requires: **M3 wallet engineer**
Claude (Fable 5.1), acting for Roman under the [handoff](handoff.md);
**independent correctness reviewer** Claude (Fable 5.1) in a separate,
fresh-context session that read the [adapter contract](../../docs/wallet-adapter.md),
the definition of done and the diff, re-ran the fixtures and wrote
[review.md](review.md) without editing code. The M1 operator was not engaged:
nothing here touched the fleet, coordinator, generator, canary or public
services, and no private query was sent to them.

This record establishes the fixture-local scope of M3. **It does not close
M3.** The deployed accepted-anchor regression suite and the application-level
comparison on public paths remain unexecuted because M1 is open (see
"Unexecuted gates"), and the handoff forbids marking M3 complete while they
are missing. Opt-in PIR balances remain non-authoritative and labelled so.

## Source, preservation and ownership

| Identity | Value |
|---|---|
| Wallet repository | `zakura-core/wallet-libraries`, primary checkout `/Users/roman/projects/wallet-libraries` |
| Baseline at start | `0ce6d158f020dcace36e6727fe02a88d47c66c9e` (M2's final source), clean apart from an untracked `.vscode/` |
| Working branch and worktree | `m3/wallet-correctness` at `/Users/roman/projects/wallet-libraries-m3-correctness` |
| Final wallet source | `7937d48df` (`7937d48dfd03f905f2029318d0820db46867d8cf`); every Rust artifact and test result below was produced at `27b42e260`, which the final commit follows with a change to one Dart test file only (commits listed below) |
| PIR client pin | unchanged, `22e6bec` |
| PIR documentation source | this repository, `main` at `d345503` when the docs worktree was made, plus the commit that adds this bundle |
| Real-chain sample | `data/transparent-pir-evaluation/mainnet-3470268-3471419{,-resumed}.jsonl`, SHA-256 in `SHA256SUMS` |

The primary checkout was not reset, cleaned or switched. No existing wallet
database was opened: the example wallet's `com.example.zakuraExample`
container was hashed before and after every build (`existing-wallet-untouched-before.txt`,
`-after.txt`) and is identical; the release application was built but not
launched. `docs/roman_notes.md` was not touched. Every wallet the fixtures use
is synthetic and lives in a temporary directory; the one phrase generated
during the loopback rehearsal existed only in that run's process and pipe and
was never written to disk or to any record here.

## Commits

- `dcd11948c` Let the recovery harness observe, fault and hold the service it runs
- `9dd1994fc` Model the chain as blocks, and read it twice by code that shares nothing
- `345160af9` Watch imported scripts in their own scope, and count what the tables cannot index
- `b0d52b272` Show how many addresses the private path cannot recover
- `db02334f4` Keep discovering while coverage is complete but a spend is unresolved
- `dca16b437` Recover every M3 wallet history from blocks and hold it to the reducer
- `c1a1d4fcc` Fork the chain at one height, across the tiers and inside the tail, and stay exact
- `fc46b45da` Say interrupted, not in progress, when a wallet reopens mid-run
- `22502b199` Interrupt the private ledger at every boundary and prove it resumes exactly
- `b8580df81` Read a shadow profile without touching it, kill the process mid-query, and recover real blocks
- `ee68d6d25` Kill the real bridge mid-query against fixture services on the loopback
- `ca0f68ee6` Name the shadow comparison command, and keep a kill test for the public services
- `7c11ecbce` Keep the generated bridge glue as the generator writes it
- `313c3094d` Let the shadow comparison command read its arguments
- `27b42e260` Compare a shadow profile as closely as the fixtures do, and look harder for plaintext (the reviewer's F1, F2, F5, F9, F10)
- `7937d48df` Show the child's own output when the loopback rehearsal times out

Each is reviewable alone and each builds and passes on its own.

## What was built

- **A block model and two independent readings of it** (`tests/common/blocks.rs`).
  A fixture is blocks — transactions with inputs and outputs, a coinbase as the
  first inputless transaction — rather than events already sorted into shards.
  `extract` turns blocks into the events a publisher indexes, by the
  publisher's rules, and that is what gets published to the real shard service
  and recovered privately. `reduce` walks the same blocks with its own
  outpoint bookkeeping and states what a wallet holding some scripts must end
  up with: every event, unspent output, spend, transaction effect, the balance
  and the unresolved count. It never consults an event, the library's ledger
  or the store. `compare_blocks` holds the store to it event by event
  (transaction index, coinbase flag and values included), through the ledger
  the library replays, and through the balance, outputs and history the wallet
  shows. A loader reads the real-chain JSON-lines sample into the same model.
- **The catalogue** (`tests/common/catalogue.rs`): one chain carrying every
  history the milestone names, over a two-tier set, paid to scripts derived
  from the wallet's own seed and one imported script.
- **A harness that can observe, fault, hold and stop the real service**
  (`tests/common/mod.rs`), count queries per shard and revision, share and
  swap filters mid-run, lose a connection, move to another service, and check
  a request log for anything that names the wallet.
- **Product changes**, each with its tests: imported scripts in their own
  address scope; the unsupported-script count persisted and presented; a
  wallet reopened mid-run reads `interrupted`; discovery continues while a
  spend is unresolved; a shadow-profile reader and comparison command; a
  fixture light server and a loopback feature that no shipped build carries.

## Requirement-by-requirement record

The M3 definition of done, verbatim from the plan, and what establishes each
part. "Adapter test" means a Rust test against the real store, adapter and the
in-process shard service; "bridge rehearsal" means Dart driving the real
`libzakura_wallet_bridge.dylib` in a separate process.

| Requirement | Established by |
|---|---|
| Every mandatory fixture passes without unexplained event, UTXO or history differences | `correctness.rs`: `the_catalogue_is_recovered_exactly_at_the_accepted_anchor` compares events, ledger, balance, outputs and history with the block reducer for receives, spends, a coinbase, a self-transfer, a paged multi-script history, an old receive spent across the tier boundary, an offline receive and spend, a zero-balance history, unused scripts, an imported script and gap-limit discovery; `..._in_two_runs_equals_one_run`, `coinbase_receives_keep_their_flag_through_the_store`, `a_zero_balance_history_is_retained_not_erased`, `an_imported_script_is_read_from_the_set_start_without_raising_a_derived_birthday`. `real_chain.rs`: six scripts chosen from 1,152 real mainnet blocks (three P2PKH with receive and spend, one P2SH, one coinbase recipient, one receive-only) recovered exactly. The catalogue's expected balance includes a 312,500,000-zatoshi coinbase output; the ledger counts it and the wallet's spendable view holds it back. |
| Restart/resume is idempotent | `correctness.rs::a_restart_between_shards_replays_nothing_and_changes_nothing` (close and reopen on disk; a second full run asks nothing and commits only the anchor); `interrupt.rs` rows 2, 5, 20 and 21 below; `kill.rs` (three kills, resume exact with zero duplicates); the bridge rehearsal. |
| Reorgs remove orphaned state | `reorg.rs`: seven cases (table below); `interrupt.rs::pending_work_for_a_replaced_revision_is_discarded_not_fetched`; `reorg.rs::pending_work_orphaned_by_a_reorg_is_removed_with_the_range`. |
| Incomplete results preserve progress without committing completion | Every row of the interruption matrix, the kill tests and the bridge rehearsal: after each fault the completion word is never `complete`, the anchor stands where it was, no partial shard is written, every committed event is one the chain produced, and the projection agrees with the ledger. |
| Discovery follows actual wallet derivation rules without silently raising birthday | `correctness.rs::discovery_reads_a_newly_derived_script_over_its_whole_required_range`: a payment at index 9 and one at index 12 leave the external window ending at 22 and the internal at 5, exactly `highest used + limit` per scope; the new script is covered from the set's first height over every shard; every derived script's `required_from` is the birthday after three runs; `an_imported_script_is_read_from_the_set_start...`: the imported script is required from the set's start, the derived ones from the birthday, and neither moves. `recover.rs::a_window_that_keeps_moving_is_reported_unbounded_and_finished_by_the_next_run` (unchanged). |
| Unsupported scripts are explicit | `MAX_INDEXABLE_SCRIPT_BYTES` in the store, asserted equal to the shard layout's limit; `TransparentState.outside_coverage` counted from the address rows; carried through `TransparentCoverage`, `ApiTransparentCoverage` and the Dart model to the balance sentence, the coverage panel and the diagnostics report; `synchronized` is false while it is nonzero. Store test `a_script_too_long_to_index_is_counted_outside_coverage`, adapter test `a_script_the_private_tables_cannot_index_is_reported_not_hidden`, Dart tests in `coverage_test.dart`, `coverage_details_test.dart`, `app_test.dart`. |
| No plaintext address/outpoint fallback occurs | `assert_no_plaintext` over every request the traced service saw, in every catalogue, reorg, real-chain and shadow case: every request is one of the protocol's routes and no path or body carries a wallet script (raw or hex), address, txid (either byte order) or `txid:n`. The adapter constructs only the map, filter, init, manifest, setup and query transports (`source.rs`), and the request log confirms no other route is asked. |
| Only passing shadow validation enables opt-in PIR as the synchronized transparent balance source | There is no flag. The shadow reader and comparison command exist and pass against the reducer on the fixture (`shadow.rs`, five tests); the application-level comparison on public paths is an unexecuted gate. The recovery banner and the "recovered privately, unverified" suffix are unchanged: opt-in PIR figures stay non-authoritative. |
| Review against the adapter contract | [review.md](review.md); findings and dispositions below. |

### The interruption matrix (`tests/interrupt.rs`)

Every row: after the fault, nothing committed is lost, nothing is called
complete, the anchor did not move, no event the chain did not produce was
written, and the projection agrees with the ledger; after the next unfaulted
run, exact against the reducer, no pages owed, and no more queries than a
fresh wallet.

| # | Fault | Mechanism | Test | Specific result |
|---|---|---|---|---|
| 1 | Store fails before a shard commit | `Faulty` store, `ErrBefore` on commit 2 | `a_store_failure_before_a_shard_commit_leaves_the_prior_state_exactly` | exactly one commit landed; nothing from the refused shard |
| 2 | Store fails after a shard commit landed | `ErrAfter` on commit 1 | `a_landed_commit_the_caller_never_heard_of_is_not_repeated` | commit counted; an identical retry writes no event and one record; the resume issues no directory query to that shard |
| 3 | Panic mid-run | `PanicBefore` on commit 2, on-disk files | `a_panic_in_a_run_leaves_no_completion_and_no_partial_shard` | both databases pass `integrity_check`; completion left `sync-in-progress` (the facade turns it into `interrupted` on open) |
| 4 | Reopen after a dead run | facade `Wallet::open` | facade `a_wallet_left_mid_sync_opens_as_interrupted_not_in_progress` | `sync-in-progress` becomes `interrupted`; any other word stays |
| 5 | Store fails at the anchor commit | `ErrBefore` on the second pass's anchor | `a_failure_at_the_anchor_commit_keeps_every_shard_and_no_anchor` | every event held; resume asks nothing and writes no event |
| 6 | Store fails after a filter is kept | `ErrAfter` on `put_filter` 1 | `a_filter_kept_before_a_failure_is_not_read_again` | the kept filter is served from the store; the resume reads one filter fewer |
| 7 | Store fails after a setup is kept | `ErrAfter` on `put_setup` 1 | `setup_kept_before_a_failure_is_not_fetched_again` | one setup segment fewer than a fresh wallet fetches |
| 8 | Tampered setup segment | service flips a byte | `a_tampered_setup_segment_is_refused_before_it_is_kept` | `failed`; nothing of the shard kept or read |
| 9 | Tampered manifest | | `a_tampered_manifest_is_refused_and_nothing_from_the_shard_is_read` | no private query to the shard |
| 10 | Tampered page answer | | `a_tampered_page_answer_is_never_committed` | not complete; only true events held |
| 11 | Tampered directory answer | | `a_tampered_directory_answer_is_never_committed` | as 10 |
| 12 | Connection lost between queries | `Cut` transport | `a_connection_lost_between_queries_fails_the_run_and_resumes_exactly` | `failed`; commits kept |
| 13 | Service vanishes | its runtime is dropped mid-run | `a_service_that_vanishes_mid_run_leaves_the_wallet_resumable` | `failed` at the next request |
| 14 | Overload, brief, names a delay | two 503s with `Retry-After: 0` | `a_brief_overload_is_waited_out_and_the_run_completes_exactly` | complete; two queries re-asked; no long wait |
| 15 | Overload, unrelenting | every query refused | `an_unrelenting_overload_is_named_and_keeps_pending_work_durable` | `overloaded:<shard>` after four attempts; anchor absent; resume exact |
| 16 | 503 that names no delay | | `a_503_without_a_delay_fails_the_run_rather_than_spinning` | `failed` without backoff |
| 17 | Tail replaced between queries | `SwitchAfter` to a superseding revision | `a_tail_replaced_between_queries_is_refreshed_and_re_derived_in_the_same_run` | one run completes against the new tail; old coverage truncated; map refreshed within the library's bound |
| 18 | Tail replaced while pages owed | | `pending_work_for_a_replaced_revision_is_discarded_not_fetched` | no query names the old revision; nothing owed |
| 19 | Reorg beneath pages owed | `reorg.rs` | `pending_work_orphaned_by_a_reorg_is_removed_with_the_range` | pending gone with the range; every terminal on the new chain |
| 20 | Stop at every request boundary | `StopAfter` for every n | `a_stop_at_every_request_boundary_never_commits_completion` | `stopped` each time; an anchor survives only from a pass that completed to the target before the stop |
| 21 | Partial runs on budgets of 1–5 queries | repeated `recover` | `repeated_partial_runs_converge_to_one_ledger_without_duplicates` | commits monotone; each run re-reads at most a directory row per matched shard; every event once |
| 22 | The library's own commit contract | `tests/store.rs` (unchanged) | `the_wallet_store_meets_the_contract` | idempotent retry; differing retry refused |

### Forks and reorgs (`tests/reorg.rs`)

| Case | Test | Result |
|---|---|---|
| Same height, another hash, inside a sealed shard, learned from the wallet's chain | `a_same_height_fork_inside_a_sealed_shard_is_rolled_back_to_the_last_accepted_terminal` | rollback to the last accepted shard terminal below the fork; the losing spend gone; shards below never re-read; coverage below keeps its hashes; exact |
| The same, after the wallet's own rewind | `a_same_height_fork_the_wallet_rewound_first_is_re_read_whole` | nothing left to roll back; the forked shard read again whole |
| Every transaction mined one block later | `a_reconverging_fork_reappears_at_new_heights_and_hashes` | no contradiction; every shifted event one block higher; balance unchanged |
| Fork on the first recent block | `a_cross_tier_reorg_just_above_the_archive_boundary` | archive tier untouched and not re-read |
| Fork inside the last archive shard | `a_cross_tier_reorg_that_reaches_into_the_archive_tier` | rollback to the shard before it; shards 1–3 read again, geometry kept |
| Fork inside the provisional tail, republished as revision 1 | `a_fork_inside_the_provisional_tail_is_a_replaced_revision` | sealed shards kept; provisional coverage on the new revision only |
| Reorg beneath pages owed | `pending_work_orphaned_by_a_reorg_is_removed_with_the_range` | see row 19 |

The library rolls back to the highest accepted shard terminal below a
changed sealed shard, not to the block before the fork, because coverage
rests on terminals. The tests state this where they assert it.

### Process kills

`tests/kill.rs` re-executes the test binary as a child with the store, adapter,
HTTP transport and service all real and the wallet's files on disk; the
service holds a chosen query and the parent sends SIGKILL while it is in
flight. Three variants: during the first query (nothing committed), during a
directory query (only prior shards), during a page query (pages still owed on
disk, the fetched page not owed again). Both databases pass `integrity_check`
afterwards, the completion word is `sync-in-progress` (no dead run can write
its reason) and the resume converges exactly with zero duplicates. Log:
`kill-tests.log`.

**Through the real bridge** (`kill-fixture-report.txt`, `kill-fixture-run.log`):
fixture services on the loopback — a fixture light server serving the
catalogue's blocks, the real shard service holding the second page query of
shard 1 — and the wallet under test in a second process through the
`fixture-loopback` build of `libzakura_wallet_bridge.dylib`, restored from a
phrase made for the run. It was killed with SIGKILL while that query was in
flight; reopened through the bridge it read `interrupted` with one page owed
and no anchor; it resumed in about 1.8 s to `complete`, synchronized, at the
tip, with the spendable balance and the 58 history entries the reducer
predicted. The beta library cannot reach loopback services: the feature is
off and the process must also carry `ZAKURA_FIXTURE_LOOPBACK=1`; both library
hashes are recorded in `bridge-dylib-hashes.txt`.

### Shadow comparison (`tests/shadow.rs`, `examples/shadow_compare.rs`)

`read_shadow_snapshot` opens a profile's cache database read-only and
immutable through SQLite directly, never through the wallet's own database
type; it refuses anything but a marker naming this application's namespace
and `mode: shadow`, and refuses a non-empty write-ahead log. `compare` names
missing, extra and differing receives and spends and the anchor; `sanitized`
carries counts and digests only. The command digests every file under the
profile and under each `--untouched` directory before and after and fails if
any changed, takes no network address, and writes the full detail only to a
file the caller names. Five tests: equal to the reducer on the fixture;
reading writes nothing; a mismatch reported by count and digest with no
outpoint, txid or script in the report; a recovery-mode profile and a
foreign marker refused before any file is opened; the recovery profile beside
a shadow profile untouched by a comparison, and a busy profile refused.

## Validation results

All commands are in [commands.md](commands.md); logs are beside this file.
Every command passed.

| Check | Scope | Result |
|---|---|---|
| `cargo test --workspace --exclude zakura-wallet-lib --exclude zakura-pir-enhance --locked --offline` | The repository's required suite at `27b42e260` | 1,133 passed, 0 failed, 15 ignored (`workspace-tests.log.gz`) |
| `zakura-wallet-transparent` `tests/correctness.rs` | The catalogue, two runs, coinbase, zero balance, imported script, restart, discovery, two lower-target refusals | 9 passed (in the workspace log; re-run by the reviewer) |
| `tests/reorg.rs` | Seven forks and reorgs | 7 passed |
| `tests/interrupt.rs` | The interruption matrix, rows 1–3, 5–18, 20–21 | 19 passed (`interrupt-tests.log.gz`) |
| `tests/kill.rs`, release profile | Three process kills and the child entry | 4 passed (`kill-tests.log`) |
| `tests/shadow.rs` | Shadow reader, comparison and command | 5 passed |
| `tests/blocks_model.rs` | Extractor and reducer self-checks, the legacy fixture re-extracted, the sample loaded | 7 passed (`real-chain.log`) |
| `tests/real_chain.rs`, release profile, both sample files | Six imported real-chain scripts over 1,152 mainnet blocks: 25 events, 12 spends, 1 unspent output, 0 unresolved, 25 private queries over 6 shards | 1 passed (`real-chain.log`) |
| `tests/recover.rs`, `tests/store.rs` | The M2 adapter suite, unchanged in behaviour, with the outside-coverage assertion extended | 27 + 3 passed |
| `zakura-wallet-store` `tests/transparent.rs` | Store: imported scope, spendable exclusion, outside-coverage count (long, at the limit, OP_RETURN) | 22 passed (was 19 at M2) |
| `zakura-wallet-facade` | Unit, import, lifecycle and wallet suites, with `interrupted` on open | 26 + 11 + 9 + 26 passed |
| `cargo clippy --tests` on the changed crates | Lints | Clean in changed code; the two pre-existing `large_enum_variant` allowances remain (`clippy.log`) |
| `cargo fmt --all --check` | Formatting | Fails at the baseline on `zakura/wallet-core/src/detected.rs`, untouched by M3; every changed file is formatted |
| `flutter_rust_bridge_codegen generate` | Regenerated glue, both halves | `git status` clean afterwards; `dart analyze --fatal-infos` clean in every package's own code |
| `zakura_client` `dart test` | Models, coverage sentence with `outsideCoverage` and `interrupted` | 54 passed (`dart-client.log`) |
| `zakura_state` `flutter test` | Providers | 38 passed |
| `zakura_ui` `flutter test` | Widgets, coverage panel row | 54 passed |
| `zakura_example` `flutter test` | App, modes, diagnostics line | 32 passed |
| `zakura_bindings` `flutter test --tags native` | The beta bridge library through the generated glue | 21 passed (`native-tests.log`) |
| `zakura_bindings` `flutter test --tags fixture` | The loopback kill rehearsal through the real bridge | 1 passed (`kill-fixture-run.log`, `kill-fixture-report.txt`) |
| `make check-docs` in this repository | Links | No broken links |

**Bridge libraries** (`bridge-dylib-hashes.txt`): the beta library built without
features is `52a51b5797916b05595cb5e82e1162b1689a6a8282b2b3d64c6126efdff5d802`;
the `fixture-loopback` build the rehearsal loaded is
`816ca88554268e29812b5cab2f8673907c45879a94ca5c9b7ef32d75890fe869`. Both from
`27b42e260`, ad-hoc signed by the linker.

**Release builds** (`release-artifact-hashes.txt`): `ZakuraRecoveryBeta.app` was
built from `27b42e260` in recovery mode (executable
`31e2323a893ef2abfa5e5b9ba119cf2749cca2ff08f78239e3f73cf3bc055b18`) and in shadow
mode (`e20def80dc92fc6100ca51f9c0101c9391e5ecf540083e49c26049195818f683`); the
bundled bridge framework hashes identically in both
(`d8c4599458bcac999e83e42d11733320c7f730447b255f2eb9a5785e79d582bc`). Ad-hoc
signed, not notarized, not distributed, not launched. The existing example
wallet's container was hashed before and after every build and is identical.

## Reviewer findings and dispositions

[review.md](review.md) is the reviewer's own record: a verdict per clause of
the adapter contract and of the definition of done, twelve findings, its own
re-execution of the fixture binaries (32 passed, 0 failed, exit 0), and a
second look at the fix commit. No finding was blocking. Dispositions:

| Finding | Severity | Disposition |
|---|---|---|
| F1 The shadow comparison compared less than the in-process one: no transaction index, no script on spends, no completion, duplicate outpoints overwritten | major | Fixed in `27b42e260`: both facts carry the index and the script digest, a duplicate outpoint is refused, `equal` requires the profile's run to have completed and the unresolved counts to agree; the reviewer's reproduction is now `the_recovery_profile_is_untouched_by_a_shadow_comparison`'s stopped-short case. Reviewer: resolved. |
| F2 `assert_no_plaintext` looked only at the path and body; query string, headers, bare hash160, base64 not covered | major | Fixed in `27b42e260`: request line, headers and body; a query string is refused; hash160 windows, base64 standard and URL forms, display-order txid bytes. Reviewer: resolved, with the residual that base64 is matched at 3-byte alignment and percent-encoding is not modelled; the raw-byte window covers binary bodies. |
| F3 The extractor and reducer share the chain model, the transaction iterator and the coinbase rule; the reducer ignores `TxIn.prev` | minor | Accepted as a stated limit: they share the fixture's representation of blocks and nothing about events, ledgers or stores, which is the independence the milestone asks for. A real-chain script whose prior output lies before the sample would fail loudly, not pass; none of the six chosen did. Recorded under Limits. |
| F4 `Fact::without_index` is a standing invitation; `chain_from_events` pads with an `OP_RETURN` output | minor | Accepted: used once, in the self-check that ties the new fixture form to the M2 one, and every M3 comparison keeps the index. Inherited by M4 as a cleanup. |
| F5 Empty or `OP_RETURN` script rows were neither watched nor counted | minor | Fixed in `27b42e260`: counted as outside coverage in the run and in the store, with a store test. Reviewer: resolved. |
| F6 `interrupted` is written by any opener; two processes on one profile | minor | Documented in `Wallet::open`: one process per profile is assumed, as it was before M3, and nothing holds a lock. A profile lock is inherited by M6 with the release review. |
| F7 No non-test producer of the shadow command's expected snapshot | minor | Accepted for the fixture scope; the public-path comparison needs a producer from the reducer over real blocks at an accepted anchor, which is part of that unexecuted gate and inherited with it. |
| F8 Completion vocabulary drift between the contract, the library and the wallet | minor | The [adapter contract](../../docs/wallet-adapter.md) now lists every word the library returns and the words the reference integration adds, and states that `unresolved-spends` commits no anchor. |
| F9 Discovery continuation rested on an unasserted library invariant | minor | Fixed in `27b42e260`: continuation past unresolved spends requires the report's `covered_through` to have reached the target. Reviewer: resolved. |
| F10 A vacuous assertion in the kill harness | minor | Fixed in `27b42e260`. Reviewer: resolved. |
| F11 The fixture phrase crosses a pipe in clear text | minor | Accepted by design for a phrase made for one run of a synthetic chain; no evidence file carries it (checked by search), and the rehearsal's report carries counts and completion words only. |
| F12 `compare_blocks` scopes events to one account | observation | Accepted: the catalogue has one account, and `PirStore::add_scripts` refuses any script the wallet did not derive. |

The reviewer also observed that `cargo test --test` never built the
`shadow_compare` example, which the workspace suite then failed on; fixed in
`313c3094d`, and the workspace suite, which builds examples, passes.

## Unexecuted gates

These are required for M3 and were not run. Each has its command in
[commands.md](commands.md).

- **The deployed accepted-anchor regression suite** (`make transparent-regression`,
  [testing](../../docs/testing.md)) against the accepted fleet at the pinned
  publication. Blocked: M1 is open; the `f2f351c` canary failed on HTTP 503 at
  2026-09-09 22:03 UTC and a correction with a fresh acceptance run is in
  progress ([M1 control failure](https://github.com/valargroup/enhance-pir/blob/42b5f9c145cc3f2a400c428938c566d2cc9699db/docs/transparent-pir/evidence/productionize-m1-publication-503-2026-09-10/README.md)).
  Run it after M1 acceptance from the PIR checkout at the accepted fleet
  revision and add `report.json` and `junit.xml` here.
- **The application-level comparison on public paths**: a shadow profile with
  real transparent history synced by the release application, compared with
  the reducer's reconstruction at the same accepted anchor through
  `shadow_compare`. Needs a window agreed with the M1 operator, because it
  sends private queries, and a wallet with real history.
- **The kill through the release library against the public services**
  (`kill_recovery_test.dart`, skipped unless `ZAKURA_KILL=1`). Same window.

## Limits

- The independent reconstruction reads blocks the fixture itself wrote, and
  real blocks from a captured sample; it does not replay the chain from a
  node. The real-chain sample is 1,152 blocks far above the archive cutoff,
  with imported rather than derived scripts, so it exercises extraction,
  publication, retrieval and the ledger on real data, not the frozen
  regression fixture's profiles.
- The catalogue's tiers differ in table geometry (`recent-4k` and
  `recent-8k`), as the deployed set's do, not in shard span; the block model's
  layout is fixed per set.
- A kill lands between two requests of the wallet's own process; the store's
  transaction is what makes the boundary, and SQLite's integrity is what the
  kill tests check. Power loss below SQLite is not rehearsed.
- The trusted-indexer completeness model of the [contract](../../docs/contract.md)
  is not strengthened: everything here checks the wallet against what was
  published, and the publisher against the blocks it was given.
- `cargo fmt --all --check` fails at the baseline on `zakura/wallet-core/src/detected.rs`,
  which M3 does not touch; the changed crates are formatted.
- The fixture light server needs tonic's server side, which adds axum 0.8 to
  `Cargo.lock` under a feature no shipped build enables.

## What M4 and M5 inherit

M4 inherits the catalogue and the block model as the frozen wallet and chain
inputs its matched workflows need; the fault-injecting service for a
controlled comparison; `ShadowSnapshot` and `compare` as the equality check
between the combined-compact and PIR workflows; per-shard and per-revision
query accounting for traffic totals; and the fixture light server, which lets
a whole-wallet workflow run against a synthetic chain on one machine.

M5 inherits rows 12–16 of the interruption matrix as the wallet-side
expectation of the capacity protocol (temporary overload is explicit and
resumable; a 503 that names no delay is a failure, not a wait); the kill
tests as the client half of the outage rehearsals; and the `interrupted`
completion word for the runbook's account of what a wallet shows after a
crash.

All three gates above are required for M3, including the kill against the
public services: the loopback kill recorded here establishes the library's
behaviour, not the deployed services'. M6 inherits the banner and label change
once M3 through M5 pass, and any non-blocking reviewer finding deferred there.
