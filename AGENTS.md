# Wallet PIR development

Keep product code, operations, docs and evidence under `enhance/`,
`transparent/` or `receiver/`; shared code lives under `shared/`. The legacy
demo workspace is independent. Preserve existing Cargo package and binary names.

## Iteration and validation

- Batch edits, then use `make check-fast BASE=<comparison-sha>` for affected
  checks. This includes committed, staged, unstaged and untracked changes.
  `make check-package PACKAGE=<name> TEST=<filter> FEATURES=<features>` selects
  a package. Omit optional variables when unused. `OFFLINE=1` uses fetched deps.
- `make doctor` checks local prerequisites; `NETWORK=1` also checks authenticated
  access. Run `make prepare-dev` once when pinned dependencies need fetching.
- When a PR is requested, publish the reviewable change before long validation;
  report pending/skipped checks accurately and immediately arm the installed
  `misc-create-pr` skill's durable CI monitor for the head SHA. Continue other
  authorized work. Do not duplicate full CI locally or poll it in the foreground.
- Keep one owner/result per snapshot/check. Use the installed `roman-dev-ux`
  durable local-check helper for checks that must survive a turn. Verify the
  checkout and head before accepting results; a background process alone does
  not establish automatic wakeup. Record slow commands/blockers with that skill.
- `make check` / `make check-full` explicitly run comprehensive local checks.
  Full CI on main, exact-SHA artifact verification and hardware qualification
  remain release gates. Fast CI is feedback, not release qualification.
- Keep focused evidence before merging/deploying cryptographic, protocol,
  privacy and migration changes. Preserve independent oracles, negative cases,
  restart/recovery coverage and real HTTP boundaries. Test with `release-fast`;
  deploy binaries use the separate fat-LTO `release` profile.
- Keep Cargo writers sequential within one target directory. For independent
  concurrent work use separate worktrees/target lanes; reuse each lane across
  edits. Do not create a new cold target per command or weaken trust boundaries.

## Branches and production changes

- Commit and push directly to `main` unless Roman asks for a PR; when he does,
  open one against `main`. Work in the primary checkout or a visible worktree,
  not a hidden per-task isolation copy.
- Main CI runs after the push. Before pushing, run only the focused
  `make check-fast` scope for the change; watch main CI in the background and
  fix or revert promptly if the push turns it red.
- Production changes go through `ops/scripts/wallet-pir-deploy.py`
  (`plan`/`preflight` first), which holds the production lock. Do not mutate
  production hosts by hand (`systemctl`, signals, `scp` of binaries) or while
  another session holds the lock, and never during a monitoring-only task.
  Report `status` and the rollback command after each deploy.

## Documentation and operations

Start with `docs/README.md` and the product index. Never infer live deployment
from source. Keep evidence provenance and immutable retained raw inputs; repair
links when removing superseded documents. Production protocol defaults, rollout
floors, qualification durations, and rollback gates are not iteration shortcuts.
