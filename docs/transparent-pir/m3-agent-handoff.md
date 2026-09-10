# M3 wallet-correctness agent handoff

Prepared 2026-09-09 after M2 closed. Assign the receiving agent as M3 wallet
engineer, name the independent correctness reviewer the plan requires, and
record both roles with the working branch, baseline and final revisions in
the milestone evidence.

## Objective and ownership

Implement M3: establish wallet correctness before any privately recovered
transparent balance becomes authoritative. Compare what the application
recovers with an independent reconstruction at the same independently
accepted anchor, exercise the lifecycle under interruption and reorg, and
keep shadow validation state separate from PIR state. Only a passing M3
enables opt-in PIR as the synchronized transparent balance source; nothing
in M2 flipped that, and there is no flag to flip. Read the repository's
applicable instructions before editing.

Own wallet, fixture and comparison changes. The M1 operator owns the live
fleet, coordinator, Amsterdam generator, canary and acceptance evidence; do
not change their binaries, configuration, services or load, and do not run
private queries against the public services without agreeing a window with
them first. Report server or protocol blockers to that operator.

## Dependency gate

M3 requires M1 and M2. M2 is closed
([evidence](evidence/productionize-m2-2026-09-09/README.md)). M1 was still open
at this handoff: check [status](status.md) and
[remaining work](remaining-work.md#macos-recovery-beta-milestones) before
starting, and read the M1 evidence directory that closes it when it does.

Before M1 closes, the following can proceed against local fixtures:

- The independent block or journal reconstruction and the exact comparison of
  events, UTXOs and history at an accepted anchor.
- The interruption matrix around persistence boundaries, on the adapter and on
  the release build.
- Same-height fork and cross-tier reorg fixtures with the in-process shard
  service.
- The shadow-profile comparison harness and its isolation checks.

After M1 closes: the deployed accepted-anchor regression suite
([testing](testing.md)) against the accepted fleet, and the application-level
comparison on public paths. Do not substitute the 20-wallet benchmark or the
M2 live check for either.

## Read first

- [Execution plan, M3 section](productionize-plan.md#m3--establish-wallet-correctness-before-authoritative-pir-use).
- [Wallet adapter contract](wallet-adapter.md), which the reviewer reviews against.
- [M2 evidence](evidence/productionize-m2-2026-09-09/README.md), especially
  "Limits and what remains" and "M3 fixtures this leaves for the next milestone".
- [Contract](contract.md) for the trusted-indexer completeness model M3 does not
  strengthen.
- [Testing](testing.md) for the accepted-anchor regression suite and its fixture.
- The wallet adapter's recovery suite,
  `zakura/wallet-transparent/tests/recover.rs` and `tests/common/mod.rs`: the
  in-process shard service, publisher, synthetic chain and exact comparison the
  M3 fixtures should extend rather than replace.

## Repositories, conventions and preservation

Wallet repository: `/Users/roman/projects/wallet-libraries`, branch
`feature/private-ironwood-enhancement`, at `0ce6d158f` after M2 (pushed). The
M2 worktree `/Users/roman/projects/wallet-libraries-m2-recovery` on
`m2/macos-recovery-beta` holds the built release application; it may be
removed once the artifact is no longer needed. Create a new M3 branch and
worktree from the current branch head; recheck for user changes first, and
never reset, clean or switch the primary checkout. Its untracked `.vscode/`
is unrelated.

Build and test with `CARGO_TARGET_DIR=/Users/roman/projects/wallet-libraries/target`
(the primary checkout's cache, not its sources). Cargokit uses its own target
directory. The bridge is now recovery-only by default: any harness that opens
a wallet through the facade or bridge with `recovery_only` set, which the
bridge always does, must name two `https` transparent services on distinct
hosts or `open` refuses with `Configuration` (code 21); the native tests use
unreachable `.invalid` hosts for this. Native bridge tests take the library
from `ZAKURA_BRIDGE_LIBRARY`; the live test runs only with `ZAKURA_LIVE=1`.
The example needs `--dart-define=ZAKURA_MODE=demo|shadow|recovery`.

PIR repository: `/Users/roman/projects/spendability-pir`, `main` at `a99e09e`
(pushed). Another session commits in this checkout; make changes in a
worktree, never `git add -A`, and leave `docs/roman_notes.md` and the untracked
`m2-agent-handoff.md` alone. Documentation changes run `make check-docs`.

Use synthetic wallets and disposable profiles only. Do not open or migrate
the existing example wallet (`com.example.zakuraExample` container) or any
personal wallet. Never persist mnemonic material or include seeds, keys,
addresses, scripts or transaction identifiers in evidence; the M2 diagnostics
screen and live test show the sanitized shape to keep.

## What M2 leaves open for M3

- **No independent comparison exists yet.** The M2 live wallet was fresh and
  matched nothing; it exercised map, geometry, filters, target, store and
  persistence and sent no private query. M3 needs wallets with real
  transparent history: receives, spends, coinbase, self-transfer, multiple
  scripts, an old receive spent recently, offline receive and spend, imported
  scripts and gap-limit discovery, and unused or zero-balance histories.
- **Interruption on the release build.** The adapter's stop test uses a
  synthetic set; a process kill during a private query on the release build,
  and interruption around each persistence boundary with real events, are not
  yet exercised. The store commits per shard and the stop lands within one
  request; verify idempotent resume and no committed completion after each.
- **Unsupported scripts are not named.** A wallet-derived script is always
  P2PKH, so `outside_coverage` is zero in practice and is not persisted; such
  a script would hold coverage below the birthday and keep the balance
  unsynchronized, but nothing says why. M3's "unsupported scripts are
  explicit" gate decides whether to persist and present the count.
- **Shadow mode isolates but does not compare.** The shadow profile has its
  own container directory and labels its output; the independent state it is
  compared against is M3's fixture. Neither path may repair or contaminate the
  other, and the comparison must run only on designated validation profiles.
- **Discovery rules.** `discovery-unbounded` after sixteen passes is reported,
  not resolved; M3 confirms discovery follows the wallet's derivation rules
  without silently raising a birthday, and that a lower target requires an
  explicit accepted-anchor rollback.

## Decisions still awaiting the user

Made autonomously in M2 and reversible; confirm or change before the beta
cohort sees them: beta modes offer restore only (no create, no receive), no
mode has a send screen, and the public `https://us.zec.stardust.rest:443`
light server is the one default the beta carries. Also pending: the M1
operator's acknowledgement of M2's uncoordinated live check (one map read,
filters from a near-tip birthday, one `init`, no private query).

## Definition of done and handoff

Use the M3 definition of done in the execution plan without weakening it:
every mandatory fixture passes without unexplained event, UTXO or history
differences; restart and resume are idempotent; reorgs remove orphaned state;
incomplete results preserve progress without committing completion;
discovery follows actual derivation rules; unsupported scripts are explicit;
no plaintext address or outpoint fallback occurs; and only passing shadow
validation enables opt-in PIR. The reviewer's independent finding against the
adapter contract is part of the record, not a formality.

Deliver reviewable commits, an immutable evidence bundle with source
revisions, commands, raw failures and a pass/fail result, and a
requirement-by-requirement record. Identify anything that could not be
validated and why. List what M4 and M5 inherit separately; do not mark M3
complete while any mandatory fixture or the deployed regression suite is
missing.
