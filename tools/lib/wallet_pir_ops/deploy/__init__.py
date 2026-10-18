"""Transactional deploys of Enhance, Status and Receiver PIR.

`tools/scripts/wallet-pir-deploy.py` is the entry point; see `cli.USAGE`.

- `descriptors`: roles and readiness from `tools/deploy/services.toml`,
  hosts from an operator inventory outside the repository.
- `units`: systemd unit parsing, effective configuration and rendering.
- `remote`: the executor interface every host action goes through, over SSH.
- `host_helper`: the host side of each remote action.
- `transaction`: the runner-side journal.
- `engine`: plan, preflight, deploy, rollback, status and baselines.
- `package`: the operations package and operator inventory on the coordinator.
"""
