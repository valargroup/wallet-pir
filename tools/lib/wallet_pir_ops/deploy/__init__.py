"""Fleet data operations over SSH that are not binary rollouts.

`tools/scripts/wallet-pir-deploy.py` is the entry point; see `cli.USAGE`.
Binary rollouts use `tools/deploy/pir-deploy` and `tools/deploy/pirctl`.

- `descriptors`: roles and readiness from `tools/deploy/services.toml`,
  hosts from an operator inventory outside the repository.
- `units`: systemd unit parsing, effective configuration and rendering.
- `remote`: the executor interface every host action goes through, over SSH.
- `host_helper`: the host side of each remote action.
- `transaction`: the runner-side journal.
- `engine`: the per-service binary engine behind `transparent-binary-*` and
  `tools/enhance/scripts/deploy-cuda.py`, and the shared mutation fence.
- `package`: the operations package and operator inventory on the coordinator.
"""
