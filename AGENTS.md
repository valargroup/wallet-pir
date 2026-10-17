# Wallet PIR development

Keep product code, operations, docs and evidence under `enhance/`,
`transparent/` or `receiver/`; shared code lives under `shared/`. The legacy
demo workspace is independent. Preserve existing Cargo package and binary names.

## Toolchain, build and run

- Use the Rust version pinned in `rust-toolchain.toml` (currently 1.97.1),
  with rustfmt and clippy. Linux prerequisites are C/C++ build tools,
  Clang/libclang, pkg-config, OpenSSL development headers, protoc, Python
  3.11+, Git, jq, shellcheck, Node and make. See
  [the development workflow](docs/development.md) for validation details.
- On the exe.dev devbox, the persistent primary checkout is
  `/home/exedev/workspace/wallet-pir`. Rust and Node are installed under the
  development user's home; open a new login shell after toolchain setup.
  Git uses the attached GitHub integration through `github.int.exe.xyz`;
  organization-scoped Git URL rewrites also cover pinned Git dependencies.
  Cargo uses Git CLI fetching so those rewrites apply. Never store integration
  credentials in the repository.
- Fetch pinned dependencies and install the local push guard with
  `make prepare-dev`; verify prerequisites with `make doctor`.
- Compile all CPU workspace binaries, including the Enhance CLI, for local use:
  `cargo build --locked --workspace --bins --profile release-fast --features enhance-pir/cli`.
  Use `make build` for deploy-profile binaries (fat-LTO `release`, slower
  and more memory-intensive). Do not enable CUDA on a CPU-only devbox.
- Inspect entrypoints without starting a fleet:
  `cargo run --locked --profile release-fast -p enhance-pir-server --bin enhance-pir-server -- --help`
  and
  `cargo run --locked --profile release-fast -p enhance-pir --features cli --bin enhance-pir-cli -- --help`.
  `make run-server` and `make run-worker` also show help, using `release`.
- To start an isolated local worker after building:
  `target/release-fast/enhance-pir-server worker --listen 127.0.0.1:8091 --data-dir .local-pir/worker`.
  A worker alone is not an end-to-end retrieval deployment; coordinator,
  chain inputs and role configuration are described in the product runbooks.
  Keep private/control listeners on loopback and never reuse production data
  or invoke load-test targets against their default production origins casually.
- For focused Rust tests use `make check-package PACKAGE=pir-control` (or
  the package being changed); add `TEST=<filter>` when appropriate.
  `cargo fmt --all --check` checks formatting. `make test` runs the full
  optimized workspace tests; `make check-full` additionally runs lint,
  documentation, tooling and operations checks. Neither replaces hardware
  or release qualification.

## Iteration and validation

- Keep routine PR and main fast feedback under three minutes; retain full service qualification separately.
- Keep every automated CPU correctness test below 30 seconds and the complete
  warm correctness run below three minutes, including incremental compilation,
  setup, teardown, tooling and lint. `make check-correctness` enforces the budget;
  `python3 tools/ci/correctness.py --record-only` diagnoses overruns without
  omitting coverage. Report cold preparation separately. Hardware and explicit
  soak qualification retain their existing release gates.
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
  Full affected-service qualification, exact-SHA artifact verification and hardware
  qualification remain release gates; unrelated regression CI runs asynchronously.
  Fast CI is feedback, not release qualification.
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
- `make prepare-dev` installs a pre-push guard that refuses a push to `main`
  while main's last finished fast CI run failed. Fix or revert that
  failure first; push the fix itself with `WALLET_PIR_PUSH_ON_RED=1`.
- Production changes go through `ops/scripts/wallet-pir-deploy.py`
  (`plan`/`preflight` first), which holds the production lock. Do not mutate
  production hosts by hand (`systemctl`, signals, `scp` of binaries) or while
  another session holds the lock, and never during a monitoring-only task.
  Report `status` and the rollback command after each deploy.
- The fleet runs but has no users yet: change schemes, formats and protocols with a direct cutover, and never plan dual runtimes, compatibility windows or transitions for older wallets or servers.

## Documentation and operations

Start with `docs/README.md` and the product index. Never infer live deployment
from source. Keep evidence provenance and immutable retained raw inputs; repair
links when removing superseded documents. Production protocol defaults, rollout
floors, qualification durations, and rollback gates are not iteration shortcuts.

- Commit only code, tests, configuration and concise documentation needed for the requested change; keep generated run output, AI task reports and exploratory scripts out of Git, and promote only essential evidence or tested reusable tools.

## Dependency and release boundary

- Keep release dependencies one-way: wallet-pir may consume wallet-libraries, but wallet-libraries must not depend on packages owned or released by wallet-pir, including normal, optional, build, target-specific and development dependencies.

## Focused changes

- Give each change one outcome; split independently useful refactors, operations features, benchmarks and unrelated fixes.
- Run `make check-hygiene BASE=<comparison-sha>` before publishing; the same rules apply to direct pushes and PRs.
- Store local run output and exploratory scripts under ignored `.local-pir/`; add data allowances only with their consumer or essential evidence claim.
- Update existing documentation; keep self-review verdicts, task summaries and handoff reports in chat or PR descriptions.
