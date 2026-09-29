# Control sessions and restricted forwarding accounts

The shared control transport for Enhance, Status and Transparent: supervised
SSH masters that carry restricted forwards. This page describes source. It is
not deployed for any product; each product's deployment and status documents
record what runs.

## Supervisor

- Library: `ops/lib/wallet_pir_ops/control_sessions.py` (standard library only)
- Entry point: `ops/scripts/wallet-pir-control-sessions.py`
- Unit: `ops/deploy/wallet-pir-control-sessions.service`
- Configuration: `/etc/wallet-pir/control-sessions.json`
- Status example: `ops/deploy/control-sessions.status.example.json`

Each configured session is one foreground `ssh -N` master (`ControlMaster=yes`).
It carries the session's TCP local forwards (`-L`), TCP reverse forwards (`-R`)
and Unix-socket local forwards, with `ExitOnForwardFailure=yes`. The master runs
without user or system SSH configuration, without an agent and in batch mode.
It accepts only the host keys in the session's known-hosts file. When the
session sets `known_hosts_sha256`, the file must still match that digest before
every start.

The supervisor restarts an exited master after a bounded exponential backoff:
`restart_initial_seconds`, doubling up to `restart_max_seconds`. The delay goes
back to the initial value after a master stays up for `stable_seconds`.

The supervisor stops only processes it started. It removes a dead socket at a
control or forward path. It never removes a regular file, a symlink, or a socket
that still accepts connections. Such a path keeps its session `blocked` until an
operator clears it. A lock in the runtime directory allows one supervisor per
directory. The unit's runtime directory is removed on stop, and its
control-group kill mode stops every master with it.

`status.json` in the runtime directory is replaced atomically on every change.
It holds each session's `state` (`connecting`, `connected`, `backoff`, `blocked`
or `stopped`), plus `pid`, `started_at`, `connected_since`, `restarts`,
`last_exit_code`, `last_error` and `next_start_in_seconds`. A session counts as
connected once its control socket exists, which OpenSSH creates after
authentication and after binding the local forwards.

`check` asks each master over its control socket (`ssh -O check`) and connects
to each local forward. It exits 1 if any of them is down. A reverse forward
listens on the remote host, so it is reported with its master:
`ExitOnForwardFailure` ends the master if the host refuses that forward.
`check` contacts no remote host. `validate` prints each master command.

Clients that multiplex over a master use `client_args`. `ControlMaster=no` never
starts a master, and `ProxyCommand=false` makes the fallback OpenSSH would
otherwise take when the master is gone (a fresh login) fail. Artifact transfers
must not use a control master: they open their own connection. On Transparent,
an rsync over the shared connection delayed status reads past their budget.

Transparent's own supervisor (`transparent-live-fleet.py --control-sessions`)
keeps its standalone copy of the socket naming and SSH arguments, because the
live reconciler is installed as a single script; a test holds its arguments
equal to this library's (see
[Transparent deployment](../transparent/docs/deployment.md#hardening-rollout-gate)).

## Restricted forwarding account

Each host gets a forwarding-only account whose key can reach exactly the
configured endpoints. Both halves are generated from the supervisor
configuration:

```sh
wallet-pir-control-sessions.py --config C authorized-keys --user status-control --public-key key.pub
wallet-pir-control-sessions.py --config C sshd-match --user status-control \
    --authorized-keys-file /etc/ssh/status-control/authorized_keys
```

The key line is
`restrict,port-forwarding,command="/bin/false",permitopen="…",permitlisten="…" <key>`:

- `restrict` disables all forwarding, the terminal and `~/.ssh/rc`.
- `port-forwarding` re-enables TCP forwarding.
- `permitopen` confines `-L` destinations and `permitlisten` confines `-R`
  listeners to the configured literal `IPv4:port` endpoints.

For the Status example the generator reproduces the key line in
`ops/infra/digitalocean/production/status/cloud-init.yaml.tftpl` byte for byte.
An empty `permitopen` list would allow every destination, so a key needs at
least one local forward.

The Match block sets the same limits server side:

- `AllowTcpForwarding` (`local`, `remote` or `yes`)
- `PermitOpen` and `PermitListen` (`none` when empty)
- `AllowStreamLocalForwarding no`
- no agent forwarding, X11, TTY or tunnel devices
- `ForceCommand /bin/false`
- `ClientAliveInterval 2` and `ClientAliveCountMax 3`

## Unix-socket forwarding: open check

**Question:** does `restrict` block Unix-socket (streamlocal) forwarding, and
can the account be limited to one socket?

**Answer:** `restrict` blocks it, and it cannot be limited to one socket. A
restricted account must forward loopback TCP only, so both generators refuse
sessions with Unix-socket forwards.

Checked on 2026-09-29 against the OpenSSH 10.0p2 manual pages (`sshd(8)`
AUTHORIZED_KEYS FILE FORMAT, `sshd_config(5)`) and a private `sshd` on
127.0.0.1:

- `restrict` alone: every forward is refused, TCP and Unix.
- `restrict,port-forwarding` plus `permitopen`:
  - TCP `-L` works only to the listed destinations.
  - Every Unix-socket `-L` is refused ("request to connect to path … denied").
    When a TCP destination list is in force, OpenSSH checks Unix-socket opens
    against it too, and no `permitopen` entry can name a path.
- `AllowTcpForwarding no` also refuses Unix-socket `-L`.
- With forwarding re-enabled and no destination list, every TCP destination and
  every socket the account can open is reachable. That is not a restriction.
- `permitlisten` does not govern Unix-socket `-R`: under the Status key line
  alone, the key could create a listening socket anywhere the `status-control`
  account can write. Only `AllowStreamLocalForwarding no` (or `local`) in the
  Match block stops it. It was added to the live Status host's
  `60-status-control.conf` on 2026-09-30 (the previous file is kept as
  `/root/60-status-control.conf.before-streamlocal-20260930`) and to the
  cloud-init template, whose user data Terraform ignores on existing droplets.
  After the reload both tunnels stayed active, a fresh TCP forward answered and a
  Unix-socket `-R` was refused.

`RealSshdTest` in `ops/tests/control_sessions/test_control_sessions.py` repeats
this with the generated key line and Match block. Run it with
`WALLET_PIR_SSHD_PROBE=1 make check-ops-control-sessions`. It checks that:

- the supervised master's permitted forwards work;
- a different TCP destination, a Unix-socket `-L`, a Unix-socket `-R` and an
  unlisted `-R` port are all refused.

Removing `AllowStreamLocalForwarding no` makes it fail. Re-run it on the
hosts' OpenSSH version before relying on the result there.

**Consequence for Transparent:** today status reads use Unix forwards to
`/run/transparent-pir/control.sock` over root SSH. A restricted account needs
the worker's control socket on a loopback TCP port instead, listed in
`permitopen`. Two ways to get there:

- a listener in `transparent-shard-server`;
- `systemd-socket-proxyd` in front of the existing socket.

The trade-off: any local user on the worker can connect to a loopback TCP port,
whereas the socket is root-only. The port therefore needs its own guard, either
a firewall rule matching the forwarding account's uid or authentication in the
control protocol. That decision precedes the Transparent step below.

## Rollout order

Only the Status sshd change in step 1 has been done (2026-09-30). Each other
step needs Roman's approval, the production lock, armed APM alerts and the
product's own gates. Replacing the two Status tunnels briefly closes the public
Status query path, so it needs a window.

1. **Status** (canary). On the Status host, `AllowStreamLocalForwarding no` is
   in the `status-control` Match block (done).

   On the coordinator:
   - create the `wallet-pir-control` system user;
   - make `/etc/status-pir-control/id_ed25519` group-readable by it (`0640`,
     group `wallet-pir-control`);
   - install the script, its library and the unit;
   - render the example with the Status host address and the known-hosts
     SHA-256 into `/etc/wallet-pir/control-sessions.json`, then run `validate`.

   Then swap the transport:
   - stop and disable `status-control-tunnel` and `status-query-tunnel`;
   - start `wallet-pir-control-sessions`;
   - require `check` to pass;
   - point `status-controller-qualification.service`'s `After=`/`Wants=` at the
     new unit.

   Run the fault drill:
   - kill a master;
   - rotate the key, appending the new key and verifying before removing the
     old;
   - restart the Status host.

   Rollback is starting the two tunnel units again after stopping the
   supervisor. Their templates stay in `enhance/ops/deploy/`.
2. **Transparent.**
   - Decide on and build the loopback TCP control endpoint.
   - Add a `pir-control` account through Terraform cloud-init, with its key in
     Infisical.
   - Move status reads, then `prepare`, `activate` and `invalidate`, from root
     SSH to the forward.
   - Root SSH remains only for deploys and artifact transfers.
3. **Enhance** (last, gated).
   - Split control routes onto loopback.
   - Reach them through forwards, with a reverse forward for the prepared-packing
     download.
   - Make `ENHANCE_INTERNAL_TOKEN` mandatory.
   - Before cutover, measure refresh round-trip time and master reconnect time
     against the 5 s watchdog.
