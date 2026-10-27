---
name: wallet-pir-deploy
description: Deploy, dry-run, roll back and check Wallet PIR releases across the production fleet with pir-deploy and pirctl. Use when asked to deploy or redeploy a main commit, ship the publisher or a query role, roll back, check what the fleet runs, time a deploy, or add a host to the deploy inventory.
---

# Deploying Wallet PIR

Every main commit becomes a GitHub release `build-<sha12>` about five minutes after
the push. The `pirctl` script on each host installs a release there.
[`pir-deploy`](../../../tools/deploy/pir-deploy) runs `pirctl` over SSH on every
selected host, wave by wave. [Operations](../../../docs/operations.md#deploying-a-build)
owns the contract; this skill is the procedure.

A production restart needs the user's go for that mode and build. A request to
check, dry-run or time something does not authorize a restart.

## Before any deploy

1. Use the release of the exact main commit that was asked for. Find the
   release-build run, wait for it with `ci-run-watch`, and confirm the release
   exists:

   ```bash
   gh run list -R valargroup/wallet-pir --workflow release.yml --limit 3 --json databaseId,headSha,status
   gh release view build-<sha12> -R valargroup/wallet-pir --json assets -q '.assets[].name'
   ```

2. Set the SSH config the inventory's names resolve through. The inventory and SSH
   config stay outside the repository, in `~/.config/wallet-pir/`:

   ```bash
   export PIR_SSH_CONFIG=~/.config/wallet-pir/ssh_config
   ```

3. Bring `pirctl` on every host to the same build, so hosts and driver agree. This
   takes a few seconds:

   ```bash
   for h in $(awk '!/^#/ && NF {print $1}' ~/.config/wallet-pir/inventory | sort -u); do
     (ssh -F "$PIR_SSH_CONFIG" -o BatchMode=yes "$h" sudo pirctl self-update <sha12> </dev/null 2>&1 | tail -1 | sed "s/^/$h: /") &
   done; wait
   ```

4. Choose the mode (below). Read the changelogs under `docs/<product>/CHANGELOG.md`
   for the commits since the running build. The driver refuses a partial mode when
   the release's contract major differs from the live publisher's. It cannot see a
   change to what the publisher sends query servers that leaves the contract major
   alone. When a changelog says query servers must be deployed with the publisher,
   use `all` or `--breaking`.

5. Dry-run the mode, then run it. A dry run takes no lease and restarts nothing:
   each selected host unpacks the build, its binary accepts the host's config, and
   the build's probe passes against the service running now.

   ```bash
   tools/deploy/pir-deploy --dry-run <mode> ...
   tools/deploy/pir-deploy <mode> ...
   ```

Run a real deploy in the background (`run_in_background`) and read its output when
it finishes. In zsh, a mode kept in a variable must be split (`${=args}`), or the
driver prints its usage.

## Modes

| Mode | Command | Restarts | Typical |
|---|---|---|---|
| Publisher only | `pir-deploy publisher <sha>` | The shared publisher; query roles keep serving | 35 s, up to 80 s |
| Unified only | `pir-deploy unified <sha>` | The unified query nodes | 30 s |
| One product | `pir-deploy product <group> <sha>` | That product's roles in wave order (workers, router, ingress) | 12–25 s |
| Breaking product change | `pir-deploy product <group> <sha> --breaking` | Publisher, then unified nodes and that product | 45–55 s, up to 110 s |
| Everything | `pir-deploy all <sha>` | Publisher, then every query role in waves | 50–60 s |
| Roll back | `pir-deploy rollback <group\|destination>` | Each selected component returns to its previous release | 8–20 s |
| Status | `pir-deploy status` | Nothing; one line per component: build, active, ready | instant |

Groups are `publisher`, `unified`, `enhance`, `status`, `transparent`, `txid` and
`receiver`. A product's group covers only its own roles; the unified nodes serve
every product and move with `unified`, `all` or `--breaking`.

The typical times come from three cycles of every mode in October 2026. The
publisher's upper end comes from a restart that lands while Receiver is processing a
new block: the old process finishes that work before it exits, and the new one does
it again.

Pass `--cold` before the mode when a component starts with no prepared state, such
as a new host. It allows up to an hour to become ready instead of 120 s.

## What a deploy does

1. Takes a lease on the publisher's host so two deploys never overlap. An expired
   lease is taken over, so a crashed deploy never needs manual cleanup.
2. Opens the APM deployment window, so alerts stay quiet during the deploy.
3. Prepares every selected host before anything restarts.
4. Restarts each wave's hosts in parallel. Components that share a host restart one
   after another.
5. Verifies each component. Its process must run the new file and report ready
   within 120 s. Then `pir-probe` must pass within 15 s: one encrypted query per
   product the component serves, checked against pinned answers.
6. Closes the alert window and drops the lease, and prints one timing line per
   wave.

When a component fails verification, its host restores the previous release on its
own. The driver then rolls back the rest of that wave and starts no later wave.
Earlier waves keep the new build. Report the failing component and the build each
host ended on (`pir-deploy status`).

## Rolling back and redeploying

`pir-deploy rollback <group|destination>` swaps the selected components back to the
release each one ran before, then restarts and verifies them. Redeploy afterwards
with the matching mode and the newer sha.

Deploying a build a component already runs changes nothing. To time a mode again on
the same build, roll that group back first, then deploy it. To restart everything,
alternate between two compatible builds.

A component's previous release must still accept what the publisher now sends. Query
servers built before `e9848f1e4157` refuse the Receiver data later publishers send,
so do not roll the unified nodes back past it while the publisher runs a later
build. When rolling back across a change that a changelog marks as breaking, roll
back the publisher together with the query roles.

## On a host

`/etc/wallet-pir/pirctl.conf` lists the host's components, one per line:

```
<component> unit=<systemd unit> asset=<pir-publisher|pir-query-server> config=<file> health=<management /health URL> [probe=<public URL> products=<p1,p2,...>]
```

Releases are unpacked into `/opt/wallet-pir/<asset>/releases/<sha12>/`. Each
component's unit runs `/opt/wallet-pir/<component>/current/<asset>`, and
`previous` keeps the release it replaced. `/var/log/pirctl.log` records every
prepare, restart, readiness time, lease and alert window with a timestamp.

Use `ssh-check` for a quick look at one host, for example
`sudo pirctl status` or `sudo tail -n 20 /var/log/pirctl.log`. The single-host
commands are `pirctl prepare|check|deploy|verify|rollback <component> ...`. Prefer
the driver, which holds the lease and the alert window.

## Adding a host or component

1. Install `pirctl` from a release and check it against that release's
   `SHA256SUMS`:

   ```bash
   curl -fsSLO https://github.com/valargroup/wallet-pir/releases/download/build-<sha12>/pirctl
   curl -fsSLO https://github.com/valargroup/wallet-pir/releases/download/build-<sha12>/SHA256SUMS
   grep ' pirctl$' SHA256SUMS | sha256sum -c - && sudo install -m 0755 pirctl /usr/local/sbin/pirctl
   ```

2. Add the component's line to `/etc/wallet-pir/pirctl.conf`. Give `probe=` and
   `products=` to every component that answers wallets.
3. Run `sudo pirctl adopt <component>`. It moves a running unit onto the `current`
   link without restarting it.
4. Add `<ssh destination> <component> <group> [wave]` to the inventory. Within a
   group, lower waves restart first: workers wave 1, routers 2, ingress 3.
5. Dry-run the component's group.

## Report

For each mode run, give the build, the exit status, the total time and each wave's
time, and the readiness and probe time of each component, taken from the driver's
output. End with `pir-deploy status`. If anything failed, say which component failed
and what each host now runs. Keep raw logs in the ignored `.local-pir/` directory.
