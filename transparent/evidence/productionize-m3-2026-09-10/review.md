# M3 wallet-correctness review

Independent correctness review of milestone M3 (transparent-PIR wallet
integration), read-only. Nothing outside this file was written.

## 1. Scope

- Worktree: `/Users/roman/projects/wallet-libraries-m3-correctness`, branch
  `m3/wallet-correctness`.
- Baseline `0ce6d158f`, final HEAD `ca0f68ee6f2983402440f50a1b3cf149965fc9ad` at first pass; `27b42e26080f1d196577912e2a9a38dbea7df95f` after the fix commit (section 7)
  (12 commits, `dcd11948c` .. `ca0f68ee6`). Diff over `zakura/`: 71 files,
  +7686/-243.
- Library pinned by the worktree: `enhance-pir` rev `22e6bec`
  (`~/.cargo/git/checkouts/enhance-pir-45703bb4e00f7aca/22e6bec`), consulted
  for `pir/transparent-wallet/src/{sync.rs,ledger.rs}`,
  `pir/transparent-filter/src/script.rs`, `pir/transparent-shard/src/{records.rs,build.rs}`.
- Contract documents read in full: `docs/transparent-pir/wallet-adapter.md`;
  `docs/transparent-pir/contract.md` lines 15-85;
  `docs/transparent-pir/productionize-plan.md` lines 130-153.
- Files read in full (worktree paths under `zakura/`):
  `wallet-transparent/src/{source.rs,store.rs,scripts.rs,shadow.rs,chain.rs}`,
  `wallet-transparent/tests/common/{mod.rs,blocks.rs,catalogue.rs}`,
  `wallet-transparent/tests/{correctness.rs,reorg.rs,shadow.rs,kill.rs,blocks_model.rs,real_chain.rs,fixture_services.rs}`,
  `wallet-transparent/examples/shadow_compare.rs`,
  `wallet-store/src/gap.rs`, `wallet-sync/src/transparent.rs`,
  `wallet-app/bindings/test/{kill_fixture_test.dart,kill_fixture_child_test.dart,kill_recovery_test.dart}`,
  `wallet-app/client/lib/src/models.dart` (the `synchronized` getter).
- Read as the diff plus targeted excerpts: `wallet-store/src/{lib.rs,transparent.rs,report.rs}`,
  `wallet-store/tests/transparent.rs`, `wallet-app/facade/src/{lib.rs,config.rs,import.rs,query.rs}`,
  `wallet-app/facade/tests/wallet.rs`, `wallet-app/bridge/Cargo.toml`,
  `wallet-lwd/src/fixture.rs` (first 120 lines), `wallet-transparent/tests/interrupt.rs`
  (module header, helpers 236-450, store-fault tests 450-545 and 604-665,
  stop/repetition tests 1200-1336).

## 2. Verdicts per wallet-adapter.md clause

Line references are `file:line` in the worktree unless prefixed `lib:` (the
enhance-pir checkout above).

| Clause | Verdict | Where it is established |
|---|---|---|
| Target: the wallet accepts the anchor block independently; publication coverage must reach it | holds | `wallet-transparent/src/source.rs:146-167` takes the target from `block_height_extrema`/`accepted_block_hash`, before the map is consulted; `chain.rs:82-119` pins `tip()` to that target. `lib:sync.rs:513-523` returns `publication-behind` when the map ends below it; `tests/recover.rs:641-651` (pre-existing) covers `chain-unknown`. |
| One atomic mutating boundary `commit_shard`; committed retry idempotent; differing retry refused | holds | Store: `wallet-store/src/transparent.rs:966-1130` checks contradictions before any write inside one `transactionally`, then `INSERT OR IGNORE`. Tests: `wallet-store/tests/transparent.rs:306-377` (identical retry writes only a commit record; a differing receive is refused whole, including the innocent sibling; a second spend of the same outpoint is refused); `tests/interrupt.rs:479-542` (a landed commit the caller never heard of is retried through `PirStore::commit_shard` and writes no event, and the resumed run does not re-query that directory). |
| `required_from` never raised for a known script | holds | `wallet-store/tests/transparent.rs:466` (`a_required_height_is_never_raised`, pre-existing); `tests/correctness.rs:248-253` (second run leaves derived at birthday, imported at set start); `tests/correctness.rs:374-400` (three runs later every required height is where the first run put it). |
| Chain view: a height listed with another hash is a reorg, coverage above it rolled back; a height not listed is unknown and the sync stops short | holds (reorg branch exercised through shard terminals only) | `chain.rs:132-139` maps listed/other-hash/unlisted to Accepted/Rejected/Unknown; `lib:sync.rs:630-676` rolls back to the highest accepted terminal on Rejected and returns `chain-unknown` on Unknown. Reorg: `tests/reorg.rs:135-199, 295-336, 339-383, 386-420, 423-510` (rollback heights asserted, losing-branch events absent, terminals re-hashed on the winning branch). Unknown: only `tests/recover.rs:641` (pre-M3). Note that every M3 reorg fixture accepts the new chain only at shard boundaries (`common/mod.rs:655-662`), so the "another hash" signal is always a terminal; a hash disagreement at a non-terminal height the wallet also holds is not exercised, which is consistent with the library's rule (coverage rests on terminals, `reorg.rs:7-11`). |
| Anchor commits only on a complete sync to the explicit accepted target | holds | `lib:sync.rs:1029-1046`: anchor committed only when completion is Complete, coverage reaches the target, pending is empty and unresolved is empty. `tests/correctness.rs:46-47` (anchor at `chain.last()` with `complete`); `tests/kill.rs:165` and `tests/interrupt.rs:1247-1250` (no anchor / anchor only from a completed pass); `tests/correctness.rs:194-197` (an unresolved-spend wallet completes with no anchor: `state.anchor` is not asserted there, but `lib:sync.rs:1040-1043` skips the commit). |
| Events above the target never enter the ledger | holds | `tests/correctness.rs:54-87` (target at end of shard 2, `compare_blocks` against `reduce(.., FIRST, mid)` which excludes everything above); `lib:sync.rs:2101` `commit_bounded` trims a shard read past the target. |
| Synchronized only when `complete` and `unresolved == 0` | holds (and stricter) | `wallet-sync/src/transparent.rs:109-114` and `client/lib/src/models.dart:115-122` also require `pending == 0` and `outside_coverage == 0`. |
| Call `sync_once` again with the scripts after gap-limit advance; `discovery-unbounded` is the wallet's rule | holds | `source.rs:183-247` widens the window and re-syncs per pass, bounded by `MAX_PASSES`; `tests/correctness.rs:308-407` (index 12 derived by widening and read over the whole set from `FIRST`; window ends at highest used + limit in both scopes; nothing re-derived afterwards). |
| Pass every accepted header the wallet has for covered heights | holds by construction | `chain.rs:55-79,83-118` loads every shard boundary, every coverage terminal, the anchor, every source anchor and pending target anchor. |
| Never share a store between two set identities; refuse another chain/profile/genesis and a lower map anchor | holds | `store.rs:158-177` (`bind_set` refuses a non-continuing identity); `source.rs:320-343, 355-382` (map/init chain, network, profile, genesis checked); `tests/recover.rs:685` (pre-existing). The `SetMismatch` path itself is not exercised by an M3 test. |
| Must not substitute a raised birthday for retained history | holds | `scripts.rs:97-101` (derived: `max(birthday, set_start)`; imported: set start, never the birthday); `tests/correctness.rs:175-254` (imported script read from `FIRST` while derived stay at the birthday; two spends of below-birthday receives reported unresolved, not absorbed). |
| Must not present an empty UTXO set as evidence of no history | holds | `tests/correctness.rs:139-172` (zero-balance history retained in the ledger and the shown history); `outside_coverage` non-zero forbids "synchronized" (`wallet-store/src/transparent.rs:1556-1565`, `wallet-sync/src/transparent.rs:113`). See F5 for the ≤40-byte non-filter-element gap. |
| Must not look up an address or outpoint in plaintext | holds for the routes and encodings checked; the check is weaker than its name (F2) | `common/mod.rs:1216-1258` checks every request path and body against script hex, address text, txid hex (both orders) and raw script/txid bytes; every M3 test that serves the catalogue ends with `check_requests` (`catalogue.rs:153-163`). |
| A lower target requires an explicit accepted-anchor rollback | holds | `lib:sync.rs:531-546` (`AnchorRegressed` when the held anchor is still accepted; "target below retained events" when no anchor); `tests/correctness.rs:412-455, 458-516` (both refusals leave events, anchor and commit count untouched, zero queries; the explicit `rollback_transparent_to` then yields exactly the prefix). |
| Persistence compatibility (schema 2 migration) | not exercised by M3 | Pre-existing store tests (`wallet-store/tests/transparent.rs:717`) only. |

## 3. Verdicts per definition-of-done clause (productionize-plan.md 147-153)

| Clause | Verdict | Where |
|---|---|---|
| Every mandatory fixture passes without unexplained event, UTXO or history differences | holds for the fixture-local scope | One catalogue chain carries every listed case (`catalogue.rs:39-121`: receive/spend across tiers, coinbase, self-transfer, paged history over two tiers, old receive spent recently into the provisional tail, offline receive+spend, zero-balance history, never-paid scripts, imported script below birthday, window-edge payment, payment to an index only the widened window derives). `compare_blocks` (`blocks.rs:790-905`) compares the exact event set including transaction index, coinbase flag and value; the UTXO map with script/value/height/coinbase; the resolved-spend map; per-transaction history sums; the unresolved count; and the wallet-shown balance, outputs and history. The expectation comes from `reduce` (`blocks.rs:700-786`), which reads blocks with its own outpoint bookkeeping and never touches an event or the ledger. See F3 for what the reducer shares with the extractor. |
| Restart/resume is idempotent | holds | `tests/correctness.rs:257-305` (close/reopen changes nothing on disk; a third run asks zero queries and adds only an anchor commit); `tests/kill.rs:218-272` (SIGKILL during first directory query, a later directory query, and the second page query; reopen is intact, holds only true events, no anchor, resumes to the exact ledger at no more cost than a fresh wallet); `tests/interrupt.rs:1204-1281` (stop after every request boundary 1..=N, each resumes exactly); `interrupt.rs:1284-1336` (repeated budgeted runs converge without duplicates). |
| Reorgs remove orphaned state | holds | `tests/reorg.rs` all six cases end in `compare_blocks` against the winning branch; `reorg.rs:179-183` (losing spend gone), `reorg.rs:423-510` (pending pages of an orphaned revision removed and never queried). Store side: `wallet-store/src/transparent.rs:593-670` deletes events, coverage, pending pages and the projection above the cut in one transaction. |
| Incomplete results preserve progress without committing completion | holds | `common/mod.rs:1541-1562` (`assert_progress_kept`) used by every interrupt case; `interrupt.rs:450-476` (store error before a commit leaves the prior state exactly); `interrupt.rs:604-663` (anchor-commit failure keeps every shard and no second anchor); `kill.rs:160-165`. |
| Discovery follows actual wallet derivation rules without silently raising birthday | holds | `source.rs:305-311` uses the wallet's own `maintain_transparent_addresses` with `GapLimits::default()`; `tests/correctness.rs:326-356` asserts the window against the rule, not against what the run did; `correctness.rs:381-387` required heights stay at the birthday. Imported scripts sit in scope 3 and never move a window (`wallet-store/tests/transparent.rs:1018-1079`). |
| Unsupported scripts are explicit; no plaintext address/outpoint fallback | holds for >40-byte scripts; a gap for empty/OP_RETURN rows (F5); the plaintext tripwire has blind spots (F2) | `wallet-store/src/transparent.rs:1556-1565`, `scripts.rs:80-83`, `wallet-store/tests/transparent.rs:1144-1178` (41 bytes counted, exactly 40 not), UI: `coverage_details.dart` names the count. No code path in `zakura/` fetches an address or outpoint from a server: the transport surface is `FilterSource`/`ShardTransport` only (`source.rs:282-296`). |
| Only passing shadow validation enables opt-in PIR as the synchronized balance source | partly established | The shadow reader/comparator exists and is proven not to touch either profile (`tests/shadow.rs:136-152, 233-293`); the app keeps shadow and recovery profiles as separate stores (`example/lib/mode.dart:175`). What does not exist in this range is any gate that turns a passing comparison into authority: authority remains a build-mode decision (`ZAKURA_MODE`), which is the M2 position. This clause is therefore procedural evidence, not enforced behaviour. |

## 4. Findings

Severity: blocking / major / minor. None blocking.

### F1 (major) — the shadow comparison is weaker than the in-process comparison it stands in for

Bears on: "every mandatory fixture passes without unexplained event ... differences"; "only passing shadow validation enables opt-in PIR".

`wallet-transparent/src/shadow.rs:67-89, 151-181`: `ReceiveFact` carries script digest, value, height, coinbase; `SpendFact` carries spending txid, input index, height. Neither carries `transaction_index`, and `SpendFact` carries no script at all, so a spend indexed under the wrong script, or any event with a wrong transaction index, compares `equal`. Both maps are keyed by outpoint, so a second event for the same outpoint silently overwrites the first in the snapshot (the store's own uniqueness makes this unreachable today for a single profile, but the expected-side JSON has no such guard). `compare` at `shadow.rs:338-379` never looks at `completion`: a profile whose last run stopped short (`query-budget`, `interrupted`) but whose retained events and stale anchor match still reports `result: equal`. The fixture comparison in `tests/shadow.rs:104-133` is against a complete profile so this is not caught. Compare with `blocks.rs:790-854`, which compares `Fact` (with transaction index and script on both event kinds) and the unresolved count. Reproduction: take the `synced_shadow_profile()` of `tests/shadow.rs:89`, `UPDATE transparent_set SET completion='query-budget'`, run `compare` — `equal` is still true. Recommendation: add `transaction_index` and script digest to both facts, add `unresolved` and `completion == "complete"` to the equality (or to a separate `synchronized` field in `SanitizedReport`), and refuse duplicate keys when building the snapshot.

### F2 (major) — `assert_no_plaintext` asserts less than its name and its comment claim

Bears on: "no plaintext address/outpoint fallback".

`common/mod.rs:1002-1004` records only `req.uri().path()`; the query string and all headers are dropped before the check, so `?script=<hex>` or a header carrying an address would pass. `common/mod.rs:1149-1155` says outpoints `txid:n` are covered; they are only covered through the txid needle (fine), but the 20-byte hash160 that P2PKH/P2SH scripts wrap (`76a914 <20> 88ac`) is not a needle, so a request carrying the bare hash would pass; base64/base58 of any needle also passes. This is a tripwire against a broken client, and the real transport (`HttpShardTransport`, library code) sends PIR ciphertext, so there is no evidence of a leak; but the property "none of them carried a script, an address, a txid or an outpoint" (`mod.rs:1214-1215`) is not what is checked. Recommendation: record `uri()` in full and the headers; add the hash160 window and a base64 needle; cheap to add.

### F3 (minor) — the two block readings are not as independent as `blocks.rs:1-20` states

Bears on: fixture expectations derived independently.

`extract` (`blocks.rs:457-519`) and `reduce` (`blocks.rs:700-786`) share the `Chain`/`Tx` model, the `transactions()` iterator and the coinbase rule `index == 0 && tx.vin.is_empty()` (lines 476 and 710). A bug in `transactions()` ordering or in the coinbase rule would affect both sides identically. `reduce` does not consult `TxIn.prev` (`blocks.rs:51`), while `extract` does (`blocks.rs:478-481`): for a real-chain sample an input whose consumed output was created before the sample and whose `prev.script` is a chosen script is extracted as a Spend event but the reducer neither emits the event nor counts it unresolved. That would surface as a loud failure in `real_chain.rs` ("events differ" / "unresolved spends differ"), not a silent pass, but it is a modelling gap: `candidate_scripts()` (`blocks.rs:271-297`) also ignores `prev`, so the selection can pick such a script. Not run here (needs `ZAKURA_TRANSPARENT_BLOCKS_JSONL`).

### F4 (minor) — `Fact::without_index` exists and is used once, correctly, but is a standing invitation

`blocks.rs:651-658`; the only use is `tests/blocks_model.rs:117` comparing the legacy event fixture (which assigns arbitrary transaction indices, `common/mod.rs:153,183,207,222`) with its re-extraction. Every M3 correctness comparison goes through `compare_blocks`, which keeps the index. The `chain_from_events` builder pads gaps with an `OP_RETURN` output (`blocks.rs:559-564`) that `extract` would emit as a receive; equality at `blocks_model.rs:120` holds only because every legacy event has `output_index == 0`. Fragile, not wrong.

### F5 (minor) — an empty or `OP_RETURN` script row is neither watched nor counted

Bears on: "unsupported script classes must be reported as outside coverage, never as absent" (contract.md).

`scripts.rs:84-88` skips a row whose script is not a filter element (empty or OP_RETURN, `lib:transparent-filter/src/script.rs:38-40`) without incrementing `outside_coverage`; `wallet-store/src/transparent.rs:1556-1565` counts only `length > 40`. `import_transparent_script` (`wallet-store/src/lib.rs:527-536`) validates nothing, so such a row can be created. Today no facade path imports scripts, so this is latent; the fix is either to refuse the import or to count it. The "exactly at the limit" case is covered (`wallet-store/tests/transparent.rs:1166-1178`, and `lib:records.rs:113` agrees that 40 is indexable).

### F6 (minor) — `interrupted`-on-open is written by any opener, including a second process

Bears on: restart/resume idempotence; two processes on one profile.

`wallet-app/facade/src/lib.rs:119-121` rewrites `sync-in-progress` to `interrupted` on every `Wallet::open`. There is no profile lock (no `flock`/lock file anywhere in `facade/src`, `bridge/src`, `bindings/lib`; the only guard is the in-process `AlreadySyncing`, `facade/src/error.rs:59`). A second process opening the same profile while the first is mid-run will show `interrupted` for a sync that is running, and its own `start_sync` would then run a second engine over the same SQLite files. Commit idempotence (`wallet-store/src/transparent.rs:966-1130`) prevents duplicate events, but the completion word and anchor become last-writer-wins. This predates M3 (nothing prevented two processes before either); M3 makes the misreport concrete. Recommendation: a lock file in the profile directory, or at least document single-process use beside `shadow.rs`'s `Busy` check, which already treats a non-empty WAL as "in use".

### F7 (minor) — the shadow-comparison tool has no way to produce its `--expected` input outside the test suite

Bears on: "only passing shadow validation enables opt-in PIR".

`shadow_compare.rs:3-7` takes `--expected <snapshot.json>`; the only producer of a `ShadowSnapshot` from blocks is `tests/shadow.rs:30-42` (`snapshot_of` over the test reducer), which is test-only code and reads the test `Chain` model, not mainnet blocks. `real_chain.rs` can load a JSONL block sample but does not emit a snapshot file. For the deployed public-path comparison (out of scope, blocked on M1) an independent producer will be needed; as it stands the tool is proven only against its own fixture.

### F8 (minor) — completion vocabulary drift between the library, the wallet and the contract

`wallet-adapter.md` lists `query-budget, byte-budget, pending-limit, overloaded:<shard>, chain-unknown:<height>, discovery-unbounded`; the library also returns `unresolved-spends` and `publication-behind:<height>` (`lib:sync.rs:335-356`, `source.rs:385-401`); the wallet adds `sync-in-progress`, `stopped`, `failed`, `interrupted`, `chain-rewound` (`source.rs:120-125,136`, `facade/src/lib.rs:120`, `wallet-store/src/transparent.rs:630`). `bindings/lib/src/rust/api/types.dart:500-504` repeats the short list. `client/lib/src/models.dart:159-164` handles the wallet words. The contract should be updated; this is documentation, not behaviour. Related: `an_imported_script_is_read_from_the_set_start...` (`correctness.rs:193-197`) relies on `unresolved-spends` being reported *instead of* `complete`, and the anchor therefore never commits for a wallet with a genuinely unresolvable old spend; every later run re-verifies against the target. Intended by the library (`lib:sync.rs:1040-1043`), worth stating in the contract.

### F9 (minor) — the discovery-continuation change is sound; the reasoning depends on a library invariant that is not asserted locally

`source.rs:235-246` continues widening after `UnresolvedSpends`. This is safe only because the library reports `UnresolvedSpends` solely after `covered_through >= target` and `pending` is empty (`lib:sync.rs:1029-1043`), so every script in scope has been read to the target before the next pass adds scripts; new scripts have no coverage and are read from their own `required_from` regardless. No test in the range pins that invariant on the wallet side (a future library change returning `UnresolvedSpends` early would make a widened pass proceed on short coverage). `correctness.rs:308-407` shows discovery over full range for the complete case only. Recommendation: assert in `source.rs` (debug or error) that `report.covered_through >= target_height` before continuing on `UnresolvedSpends`.

### F10 (minor) — vacuous assertion in the kill harness

`tests/kill.rs:139` sets `output_before_kill: true` unconditionally and `kill.rs:215` asserts it. The real checks are at `kill.rs:123-130` (child printed `phase=recovering` and not `phase=done`). Harmless; remove the field.

### F11 (minor) — the fixture phrase crosses a pipe in clear text, by design

`tests/fixture_services.rs:174` prints `PHRASE=...` to stdout; `kill_fixture_test.dart:104,123` consumes it and passes it to the child through the environment. The phrase is freshly generated for a synthetic chain (`fixture_services.rs:96-98`) and the parent's own `note()` lines (`kill_fixture_test.dart:50-54`, the `ZAKURA_FIXTURE_REPORT` content) never include it, nor any address or txid. A raw capture of the services' stdout would contain it. Acceptable for a throwaway fixture wallet; the evidence procedure should keep only the `FIXTURE`/`ZAKURA_FIXTURE_REPORT` lines.

### F12 (observation) — `compare_blocks` scopes stored events to `my_scripts(db, account)`

`blocks.rs:791-801, 811-813, 828-830` filter stored events, UTXOs and spends to the account's scripts before comparing, but `history` (`blocks.rs:844-849`) is whole-ledger. For the single-account catalogue this is exact; a multi-account wallet would need per-account reduction. `PirStore::add_scripts` (`store.rs:213-219`) refuses any script the wallet did not derive, so nothing outside `mine` can be stored today.

### Things looked for and not found

- The reducer does not skip transaction index, coinbase, history sums or unresolved counting (`blocks.rs:604-625, 733-741, 765-773, 777-781, 742-745`); `compare_blocks` compares all of them.
- The shadow tool opens the profile with `mode=ro&immutable=1` through `rusqlite` directly, never through `WalletDb` (`shadow.rs:221-229`), refuses a non-empty `-wal`/`-shm` (`shadow.rs:215-220`), and `run_compare` digests every file before and after (`shadow.rs:468-483`); `tests/shadow.rs:136-152, 233-282` prove both profiles unchanged.
- The kill tests do kill mid-query: the service marks `in_flight` when the held request arrives (`common/mod.rs:1083-1086`) and the parent SIGKILLs while the client is blocked on that response (`kill.rs:107-119`; `kill_fixture_test.dart:157-163` on the `HELD` line). A kill during the SQLite commit itself is not exercised; that property rests on SQLite's own atomicity.
- Imported scope code 3 collides with nothing: `KeyScope::from_code` knows 0 and 1 (`wallet-core/src/account.rs:36-42`), 2 is reserved (`wallet-store/src/schema.rs:397`), the gap-limit queries select by `scope.code()` (`gap.rs:96-114`), `transparent_watch` admits 3 explicitly (`wallet-store/src/lib.rs:473-475`), and `spendable_utxos` restricts to `key_scope IN (0, 1)` (`report.rs:447`), the only spend selector in `zakura/` (`wallet-store/src/lib.rs:591-602`). `wallet-store/tests/transparent.rs:1081-1142` proves the imported output is counted and never offered.
- `fixture-loopback` is off by default in both `facade/Cargo.toml:9-14` and `bridge/Cargo.toml` (`default = []`), requires `ZAKURA_FIXTURE_LOOPBACK=1` at runtime and a loopback URL (`config.rs:148-152`); no build script, cargokit config or podspec in `zakura/wallet-app` enables it. Not reachable in a shipped build.
- Evidence-facing prints: `real_chain.rs:73-79,121-130` and `kill.rs:42-54` print counts, heights and completion words only; the Dart notes print `hold`, `tip`, `birthday`, counts and completion words.

## 5. Re-execution

Command, run from the worktree:

```
CARGO_TARGET_DIR=/Users/roman/projects/wallet-libraries/target cargo test --locked --offline \
  -p zakura-wallet-transparent --test correctness --test reorg --test shadow --test kill --test blocks_model
```

Run in the background at the start of the review (it waited on the shared
build-directory lock for some time, then compiled and ran; debug profile, as
the command was given). Exact result lines from the log
(`scratchpad/testrun.log`):

```
     Running tests/blocks_model.rs (/Users/roman/projects/wallet-libraries/target/debug/deps/blocks_model-7796e1206e464c16)
test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s
     Running tests/correctness.rs (/Users/roman/projects/wallet-libraries/target/debug/deps/correctness-eb6fcf982d705a72)
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 46.29s
     Running tests/kill.rs (/Users/roman/projects/wallet-libraries/target/debug/deps/kill-8dbb8b9970a7b9ad)
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 80.21s
     Running tests/reorg.rs (/Users/roman/projects/wallet-libraries/target/debug/deps/reorg-0eb9ba5432bf0adc)
test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 75.72s
     Running tests/shadow.rs (/Users/roman/projects/wallet-libraries/target/debug/deps/shadow-36d143bbcd945051)
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 33.71s
EXIT=0
```

Exit status 0; 32 tests passed, 0 failed, 0 ignored. Notes: `kill.rs`'s
count of 4 includes `kill_child_entry`, which is a no-op in the parent
process (`kill.rs:30-33`) and only does work when re-executed as the child;
the three real kill cases each spawned, killed and resumed a child.
`blocks_model.rs`'s `the_jsonl_sample_loads_and_chains` returned early
(`ZAKURA_TRANSPARENT_BLOCKS_JSONL` unset) and counts as a pass.

Not run, per instructions: `interrupt` (run by the workspace suite concurrently), `real_chain` and `the_jsonl_sample_loads_and_chains` (need the block sample), `fixture_services` and the Dart fixture/kill rehearsals (need a built bridge and services), `live`.

## 6. Overall verdict

For the fixture-local scope, the M3 definition of done is established. Every
mandatory case named in the plan is carried by one catalogue chain and held,
event by event, to a block reducer that never sees an event or the ledger;
restart, SIGKILL mid-query, stop at every request boundary, budgeted
repetition and six fork/reorg shapes all end exact against that reducer with
no anchor committed short of a complete sync; discovery follows the wallet's
own gap-limit rule and no required height moves; over-long scripts are
counted and shown rather than hidden; imported scripts are watched in their
own scope and never offered as spendable; and the re-executed suite passes
(32/32, exit 0). No finding is blocking. Two are major and should be fixed
before the shadow comparison is relied on as evidence: the shadow snapshot
compares less than `compare_blocks` does (no transaction index, no script on
spends, no completion) and so can call an incomplete or mis-indexed profile
`equal` (F1); and the plaintext tripwire checks only the URL path and body,
not the query string, headers, hash160 or non-hex encodings (F2). The
remaining findings are minor: an empty/OP_RETURN row would be neither watched
nor counted (F5), a safe discovery-continuation that leans on an unasserted
library invariant (F9), a vacuous kill-harness assertion (F10), the
interrupted-on-open rewrite with no profile lock (F6), a shadow tool with no
non-test producer of its expected input (F7), and completion-word drift
between contract, library and wallet (F8). The "only passing shadow
validation enables opt-in PIR" clause is met procedurally (the comparator
exists, cannot touch either profile, and reports sanitized counts) but is not
enforced by any gate in the code; authority is still a build-mode decision.
What remains, and is out of this review's scope, is the deployed
accepted-anchor regression suite and the public-path comparison, both blocked
on M1, plus a real-chain run of `real_chain.rs` and the Dart loopback kill
rehearsal, which need the block sample and a `fixture-loopback` bridge build
respectively and were not executed here.

## 7. Second look: fix commit 27b42e260

Revisions reviewed: `313c3094d` ("Let the shadow comparison command read its
arguments", one line in `examples/shadow_compare.rs`), `7c11ecbce`
(regenerated bridge glue, `bridge/src/frb_generated.rs` only, not reviewed),
and `27b42e260` ("Compare a shadow profile as closely as the fixtures do, and
look harder for plaintext"; 9 files, +251/-77). HEAD after these:
`27b42e26080f1d196577912e2a9a38dbea7df95f`. Read as `git show` of each.

| Finding | Resolved? | What the fix does, and what it introduces |
|---|---|---|
| F1 (major) shadow comparison weaker than `compare_blocks` | **Yes** | `shadow.rs`: `ReceiveFact.transaction_index`, `SpendFact.{script_sha256, transaction_index}`; `add_event` now returns `Err(Corrupt)` on a second event for an outpoint it holds (both maps); `ShadowSnapshot::{unresolved, is_complete}`; `compare` requires `complete && anchor_equal && actual.unresolved() == expected.unresolved()` on top of the event sets; `SanitizedReport` gains `complete`, `actual_unresolved`, `expected_unresolved`; digest domain bumped to `shadow-snapshot-v2`. Test `tests/shadow.rs:278-297` copies a complete profile, sets `completion='query-budget'`, and asserts exit 1 with `complete: false` and `missing_receives: 0` — the reproduction I gave. Introduced: (a) the `--expected` JSON format changed (new required fields), which only matters once a producer exists (F7); (b) a wallet with a genuinely unresolvable old spend can never compare `equal`, because `complete` is required and the library reports `unresolved-spends` instead of `complete` — consistent with the "synchronized" rule, worth stating in the evidence procedure; (c) `expected.unresolved()` is derived from the reducer's Spend facts lacking a Receive fact, which matches `reduce`'s own unresolved rule (`blocks.rs:742-745`) but is a second derivation of it; a divergence would show as `actual_unresolved != expected_unresolved`, i.e. loudly. |
| F2 (major) plaintext tripwire checked path and body only | **Yes, with one residual gap** | `common/mod.rs`: `Request` now carries the full request target (`uri`) and every header; `assert_no_plaintext` refuses any query string outright, then checks request line, headers and body for every text needle (exact and case-folded) and every byte needle. Needles add base64 (standard and URL alphabets, unpadded so a prefix match is enough), the bare hash160 for P2PKH/P2SH (hex, base64, raw), and display-order txid bytes. Residual: a base64 match is only found when the needle begins at a 3-byte boundary of the encoded stream (a script or hash embedded mid-stream encodes differently); percent-encoding is not covered. Both are beyond what a broken client would plausibly do, and the raw-byte window catches the pre-encoding form in any binary body. The hand-rolled `base64_with` is correct for the unpadded case (checked by hand for 1-, 2- and 3-byte tails). |
| F5 (minor) empty/OP_RETURN rows neither watched nor counted | **Yes** | `scripts.rs:84-90` increments `outside_coverage` for a non-filter-element before skipping it; `wallet-store/src/transparent.rs:1558-1564` counts `length = 0 OR substr(script,1,1) = X'6a'` beside `length > 40`. The two predicates agree with the library's `is_filter_element` (`lib:transparent-filter/src/script.rs:24-40`: empty, or first byte `0x6a`). Store test added (`wallet-store/tests/transparent.rs:1178-1191`). Not added: a run-level test that an OP_RETURN address row raises `TransparentProgress.outside_coverage`; the store-level count is what the UI shows, so this is a minor gap in coverage of the fix, not in the fix. `import_transparent_script` still validates nothing, which is now harmless because such a row is counted. |
| F9 (minor) continuation on `unresolved-spends` relied on an unasserted invariant | **Yes** | `source.rs:239-247`: the `UnresolvedSpends` arm continues only when `report.covered_through >= target_height`; any other incomplete reason stops. Types line up (`u64` both sides). No test pins the arm (it cannot be reached with the pinned library, which never reports `UnresolvedSpends` early), which is acceptable for a guard. |
| F10 (minor) vacuous kill-harness assertion | **Yes** | `output_before_kill` field and its assertion removed from `tests/kill.rs`; the real checks at `kill.rs:121-128` remain. |
| F6 (minor, dispositioned) | Documented | `facade/src/lib.rs:118-121` states the one-process-per-profile assumption. Behaviour unchanged. |

Other observations on the fix commit:

- `313c3094d` shows that `examples/shadow_compare.rs` did not compile at
  `ca0f68ee6` (`let value = || args.next()` needs `let mut`): `cargo test
  --test ...` does not build examples, so the example was never compiled in
  the M3 range. The command in `example/README.md` would have failed. Result
  of building it now (`cargo build --locked --offline -p zakura-wallet-transparent
  --example shadow_compare`, same target dir): `Finished dev profile ... in
  2m 49s`, `EXIT=0`, no warnings from the example — it compiles at
  `27b42e260`. Recommendation: include
  `--examples` (or `cargo build --examples`) in whatever `make check`
  equivalent gates this workspace.
- The sanitized report now carries `complete`, `actual_unresolved` and
  `expected_unresolved`; none of them name a script, outpoint or value, so
  `a_mismatch_is_reported_by_count_and_digest_only` (`tests/shadow.rs:155-195`)
  still holds (its "only two long hex runs" assertion is unaffected because
  `script_sha256` lives in the detail, not the report).

Re-execution after the fix, from the worktree:

```
CARGO_TARGET_DIR=/Users/roman/projects/wallet-libraries/target cargo test --locked --offline \
  -p zakura-wallet-transparent --test shadow --test correctness
```

Exact result lines (`scratchpad/testrun2.log`, debug profile; the build
waited on the shared target-directory lock first):

```
     Running tests/correctness.rs (/Users/roman/projects/wallet-libraries/target/debug/deps/correctness-eb6fcf982d705a72)
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 51.30s
     Running tests/shadow.rs (/Users/roman/projects/wallet-libraries/target/debug/deps/shadow-36d143bbcd945051)
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 21.81s
EXIT=0
```

Exit status 0; 14 passed, 0 failed. The new "stopped-short profile is not
equal" assertions live inside the existing
`the_recovery_profile_is_untouched_by_a_shadow_comparison` test, so the
count of 5 is unchanged. `reorg`, `kill`, `blocks_model` and `interrupt`
were not re-run after the fix; the fix touches `common/mod.rs` (the
plaintext check every one of them calls through `check_requests`) and
`source.rs`, so the coordinator should re-run those four before closing.

Verdict after the second look: F1 and F2 are resolved; F5, F9 and F10 are
resolved; F6 is documented rather than fixed, which is adequate for M3. The
overall verdict in section 6 stands, now without the two major findings; the
scope that remains (deployed accepted-anchor suite, public-path comparison, a
non-test producer of the shadow tool's expected snapshot, the real-chain and
loopback rehearsals) is unchanged.
