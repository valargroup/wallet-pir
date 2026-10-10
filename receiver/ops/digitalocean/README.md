# Receiver PIR on DigitalOcean

One Droplet, `receiver-pir-poc-01`, runs `receiver-pir.service`, which starts
`receiver-directory --serve` against mainnet fleet nodes on the private network,
polls every 10 seconds and publishes two blocks below the tip. The service listens
on the Droplet's private address, `10.70.0.11:18380`, and nothing listens on its
public interface. Caddy terminates public TLS for `receiver-pir.valargroup.dev` and
proxies only the wallet routes (`init`, `public`, `query`, `rows`, `witness` and
`filters` under `/v1/receiver/`). `/v1/receiver/health` and `/metrics` stay off
the edge, as Transparent keeps its operator routes: the PIR monitor's probe reads
health over the private network. `/metrics` is private and nothing collects it
yet; collection belongs in pir-apm when something consumes it.
[`ops/tests/test_receiver_ops_config.py`](../../../ops/tests/test_receiver_ops_config.py)
pins this edge, the unit and cloud-init. The public-chain index lives in
`/srv/receiver-pir/index` and holds no wallet data. No Enhance service runs on this Droplet.

## Infrastructure

`ops/infra/digitalocean/production/receiver.tf` manages the Droplet (`nyc3`,
`s-4vcpu-8gb-amd`, Ubuntu 24.04, with this directory's `cloud-init.yaml`), its project
membership, its firewall and the unproxied A record `receiver-pir.valargroup.dev`
(TTL 300), behind `receiver_pir_enabled`. The Droplet has `prevent_destroy` and
ignores `user_data` and `ssh_keys` changes. The firewall allows SSH from `allowed_ssh_cidrs`
and from the coordinator's public address (`wallet_pir_coordinator_dns_ipv4`, as
`monitor.tf` does), HTTP and HTTPS from anywhere, and port 18380 only from the PIR
monitor Droplet; the host's `ufw` also limits 18380 to `10.70.0.0/16`. The
coordinator, in `ams3`, is not on this Droplet's private network, so the locked
operations below reach it only over that public SSH rule.

The Droplet (ID 604069093) and its DNS record already exist, as the
[2026-10-08 deployment record](../../evidence/deployment-2026-10-08/README.md)
shows. No DigitalOcean firewall is attached to it yet; only the host's `ufw`
limits port 18380. Import the Droplet and the record before the first plan that
enables the root's receiver resources, on the coordinator and under its lock like
every production operation. Set `receiver_pir_enabled = true` in
`/etc/enhance-pir/production.tfvars`, find the record ID, then import:

```bash
curl -sS -H "Authorization: Bearer $CF_API_TOKEN" \
  'https://api.cloudflare.com/client/v4/zones/d3ac9657be6101818fed439c62fdcadf/dns_records?type=A&name=receiver-pir.valargroup.dev' \
  | jq -r '.result[0].id'

import() {
  ops/scripts/wallet-pir-terraform.sh import -var-file=/etc/enhance-pir/production.tfvars \
    -var="enhance_group_count=<current group count>" "$@"
}
import 'digitalocean_droplet.receiver_pir[0]' 604069093
import 'cloudflare_dns_record.receiver_pir[0]' 'd3ac9657be6101818fed439c62fdcadf/<record-id>'
```

The saved plan that follows must show no Droplet replacement or resize (if the
live size slug differs from `s-4vcpu-8gb-amd`, set it first). It creates the
firewall, which closes every port it does not list, so confirm SSH from
`allowed_ssh_cidrs` and from the coordinator's `/32`, without which every later
locked operation loses the Droplet, and the monitor's 18380 rule. Do not apply a
plan that destroys anything.

## Release and deploy

CI full on `main` builds `receiver-directory` and `receiver-probe` for `x86-64-v3`
and publishes them, with this directory's unit template, `Caddyfile` and
`cloud-init.yaml`, as the `receiver-pir-<sha>` artifact
holding `receiver-pir.tar.gz` (`tools/ci/release.py`). To build outside CI, use `--locked`,
`-p receiver-indexer`, `--release` and `RUSTFLAGS='-C target-cpu=x86-64-v3'`; from
an arm64 host, cross-compile with `--target x86_64-unknown-linux-gnu` in
`rust:1.98.0-bookworm`, linking with `x86_64-linux-gnu-gcc`.

`receiver-directory` is the `receiver` service of
[`ops/scripts/wallet-pir-deploy.py`](../../../ops/scripts/wallet-pir-deploy.py),
described in [`deploy.toml`](../../../enhance/ops/deploy/deploy.toml) with one role,
`server`, whose `receiver-pir.service` the tool renders whole from
[`receiver-pir.service.in`](receiver-pir.service.in) (`template` mode). Install,
upgrade and roll back the binary, and change the unit, only through the tool, which
holds the production lock on the coordinator, so two operators cannot interleave on
the Droplet. Do not copy binaries, repoint `current`, edit the unit or restart it
by hand.

```sh
ops/scripts/wallet-pir-deploy.py plan receiver --archive receiver-pir.tar.gz --sha <rev>
ops/scripts/wallet-pir-deploy.py preflight receiver --archive receiver-pir.tar.gz --sha <rev> --stage
ops/scripts/wallet-pir-deploy.py deploy receiver --archive receiver-pir.tar.gz --sha <rev>
ops/scripts/wallet-pir-deploy.py status receiver
ops/scripts/wallet-pir-deploy.py rollback receiver [--transaction ID]
```

The tool installs the binary as
`/opt/receiver-pir/releases/<sha256>/receiver-directory` and writes
`/etc/systemd/system/receiver-pir.service` from the template with `@RELEASE@` set
to that directory and `@NEAR_KEY@` to the inventory's key id (see
[the NEAR key](#installing-or-rotating-the-near-key)). After the restart it waits
up to 300 seconds for `http://10.70.0.11:18380/v1/receiver/health` to report the
running digest and a set `serving`, which stays null until the first publication.
Then, still under the lock and before it commits, it runs the inventory's
`exact_check` on the Droplet, since the coordinator cannot reach the private
health route: `{release_dir}/receiver-directory probe`, the probe built into the
deployed binary with its pinned fixture. It first waits up to 300 seconds
(`--await-feed-reads`) for the restarted process to read both NEAR feeds, which
proves the key file the unit names, and for the served publication to hold those
reads, then looks up the pinned payment with one live encrypted query over the
public origin and checks the filter file and private health. Its chain checks use
the fleet nodes the service reads, so it gates the deploy but is not an independent
oracle; the monitor's probe is. A failed check, one still running at its
480-second `timeout`, or `rollback` restores the previous unit file and binary.
Report `status receiver` and the rollback command after each deploy.

A deploy whose binary and effective unit already run is a no-op: it commits no
transaction and runs no check, so it is not a fresh qualification. A change to
only the inventory's `exact_check` deploys nothing either: run its `argv` by hand
on the Droplet, with `{release_dir}` replaced by the running release directory,
and confirm it prints `"passed":true`.

To change the unit's arguments or settings, change `receiver-pir.service.in` in a
reviewed commit and deploy from that checkout. `plan` prints each changed setting
as a `drift` line, and `deploy` refuses it until `--allow-unit-drift` accepts the
reviewed drift. With the binary unchanged, the same bundle redeploys the staged
release with the new unit; a commit refreshes the baseline.

Before the first tool deploy, once:

1. Add the Droplet to the coordinator's real inventory as in
   [`deploy-inventory.example.json`](../../../enhance/ops/deploy/deploy-inventory.example.json):
   a host with its SSH address, `services.receiver.template_vars.NEAR_KEY` set to
   a new key id `<id>`, such as the date, `services.receiver.roles.server` on the host with
   `vars.listen` set to the `--bind` address, `10.70.0.11:18380`, and
   `services.receiver.exact_check` on the same host, as there. Add its host key
   to the inventory's pinned `known_hosts` and update `known_hosts_sha256`.
2. Authorize the deploy key (`ssh.key`, `~/.ssh/wallet-pir-deploy` in the example)
   for root on the Droplet.
3. Install the key the Droplet runs with as key file `<id>`, from Infisical as
   [below](#installing-or-rotating-the-near-key).
4. Run `ops/scripts/wallet-pir-deploy.py capture-baseline receiver` and review it.
   `preflight` and `deploy` refuse any unit change made after it.
5. Delete and stop nothing: the new release keeps its provider store in
   `providers.sqlite`, which it builds from `--near-since` on first start, and leaves
   the earlier build's `provider.sqlite` untouched for rollback. Delete
   `/srv/receiver-pir/index/provider.sqlite*` only once the rollback window has passed.
   That build reads every swap since `--near-since`, one feed after the other: in
   production on 2026-10-10, on a fresh `providers.sqlite`, the payouts feed's first
   read took about 14 minutes and the refunds feed's about 18. So for the first
   deploy raise the inventory's `--await-feed-reads` to 3600 and the exact check's
   `timeout` to 3900. A deploy that rolls back keeps the reads it completed in
   `providers.sqlite`, so a retry waits only for what remains. During a rollback the
   old and new processes briefly share the partner key's rate limit, so the explorer
   may answer one request with HTTP 429, which the next poll retries.

The live Droplet still runs the unit installed by hand, which starts
`/opt/receiver-pir/current/receiver-directory`, reads the key from the optional
`/etc/receiver-pir/near.env` and has no drop-ins. The first deploy adopts it: the
tool replaces the unit at `/etc/systemd/system`, the only fragment path a template
role accepts, and keeps the old text in the transaction for rollback. Besides the
binary path only the key file should differ, so `plan` and `preflight` should show
a `write` of the unit and one `drift` line, `Service.EnvironmentFile`, from
`-/etc/receiver-pir/near.env` to the versioned file; review it and deploy with
`--allow-unit-drift`, even for the binary that already runs. Any other drift line
means the live unit differs from the template; reconcile the template in a reviewed
commit rather than accept it unread. The role owns its whole unit (`owns_unit` in
`deploy.toml`), so a drop-in beside it is refused, except an `ExecStart`-only
managed drop-in from an earlier `exec-drop-in` deploy, which `--retire-historical`
moves into the transaction directory. That first transaction's rollback returns to
the hand-installed unit, so leave `current` and `near.env` alone.

### Installing or rotating the NEAR key

The unit reads the NEAR Intents explorer partner key, as `NEAR_INTENTS_EXPLORER`,
from `/etc/receiver-pir/near-<id>.env`, where `<id>` is the inventory's
`services.receiver.template_vars.NEAR_KEY`, and does not start without that file.
A key change installs a new file and deploys a unit that names it, on the
coordinator:

1. Install the key under a new id, such as the date, with
   [`near-key.py`](near-key.py), which reaches the Droplet over the pinned SSH of
   the inventory `WALLET_PIR_DEPLOY_INVENTORY` names, here from Infisical:

   ```sh
   infisical run --projectId=<project> --env=prod --path=/valargroup --silent -- \
     flock -n /run/lock/wallet-pir-production.lock receiver/ops/digitalocean/near-key.py 2026-10-09
   ```

   It writes `near-2026-10-09.env` (root, mode 0600) and changes nothing else; the
   service reads it only once a deploy names it. Key files are never replaced or
   deleted, since rolling back through several transactions needs the older ones,
   so a retry uses a new id. The legacy `near.env` stays too.
2. Set `services.receiver.template_vars.NEAR_KEY` to the new id in the
   coordinator's inventory.
3. Deploy the running release again with the reviewed drift. `plan` should show one
   `drift` line, `Service.EnvironmentFile`, from the old key file to the new one:

   ```sh
   ops/scripts/wallet-pir-deploy.py plan receiver --archive receiver-pir.tar.gz --sha <rev>
   ops/scripts/wallet-pir-deploy.py deploy receiver --archive receiver-pir.tar.gz --sha <rev> --allow-unit-drift
   ```

The exact check's `--await-feed-reads` passes only once the restarted process has
read both feeds, so with the new key. On a failure the tool rolls back to the
previous unit, which names the previous key file. Rollback restores the unit, not
the inventory, so then set `NEAR_KEY` back to the previous id. A rolled-back
receiver passed readiness again, which does not show that the explorer still
accepts the previous key: check on the Droplet that health's `near.reads` fill in.

After `preflight --stage` of a new release, run the check's `argv` once by hand on
the Droplet, with `{release_dir}` replaced by the staged release directory, and
confirm it prints `"passed":true`, which also shows that the Droplet reaches the
public origin and both nodes.

## Host provisioning

These steps prepare a new Droplet before it serves anything; no deploy reaches it
before its baseline is captured.

1. `cloud-init.yaml` installs Caddy and `ufw`, creates the `receiver-pir` user,
   `/opt/receiver-pir/releases` and `/srv/receiver-pir`, and opens SSH, HTTP, HTTPS
   and port 18380 from the private network.
2. Install the first `receiver-directory` from a verified bundle (`tools/ci/release.py
   extract`, as for [the monitor](#monitoring)) as
   `/opt/receiver-pir/releases/<sha256>/receiver-directory`, where `<sha256>` is
   its `sha256sum`, the tool's release layout.
3. Check the nodes' private RPC URLs and the `--bind` address (the Droplet's private
   IPv4) in `receiver-pir.service.in`, and the same address in `Caddyfile`. Install
   the `Caddyfile` in `/etc/caddy`, and the unit rendered as the tool renders it
   except for a ` (bootstrap)` suffix on its `Description`, so that the first
   deploy of this same binary restarts it and runs the exact check instead of
   being a no-op:

   ```sh
   sed -e 's|@RELEASE@|/opt/receiver-pir/releases/<sha256>|' -e 's|@NEAR_KEY@|<id>|' \
     -e 's|^Description=.*|& (bootstrap)|' receiver-pir.service.in >/etc/systemd/system/receiver-pir.service
   ```

   `--near-since` sets where a new provider database's first read starts; keep it
   at the feed's original start.
4. Install the NEAR Intents explorer partner key, which the recent and seen filters
   need, as key file `<id>` with steps 1 to 3 [before the first tool
   deploy](#release-and-deploy). Without it the service does not start.
5. Enable and start `receiver-pir` and reload `caddy`. Check that
   `https://receiver-pir.valargroup.dev/v1/receiver/health` returns 404 and that
   `http://10.70.0.11:18380/v1/receiver/health` answers from the monitor host.
6. Capture the baseline (step 4 [before the first tool deploy](#release-and-deploy))
   with the bootstrap unit running.
7. Deploy that bundle. `plan` must show one `restart` and exactly one `drift` line,
   `Unit.Description`, from the bootstrap description to the template's; anything
   else, a no-op included, means the unit differs from step 3, so fix it first.
   Deploy with `--allow-unit-drift`.

The host is qualified only once that deploy commits its transaction with a passing
exact check, as `status receiver` shows.

After provisioning, every change to the Droplet holds the production lock. Unit
changes, a NEAR key change included, are deploys, as above. The tool writes only
unit files, so the `Caddyfile` changes through [`caddy.py`](caddy.py), which
validates, keeps the predecessor, reloads, verifies and restores on failure (see
its docstring). Run it on the coordinator with `flock -n`, as
`ops/scripts/wallet-pir-terraform.sh` and `near-key.py` run; if the lock is held,
wait and run it again:

```sh
flock -n /run/lock/wallet-pir-production.lock receiver/ops/digitalocean/caddy.py apply receiver/ops/digitalocean/Caddyfile
```

A Caddy change touches neither the unit nor the binary, so the baseline stays valid.

The service publishes from memory and writes no publication files; the index keeps
only `directory.sqlite` and `providers.sqlite`. A `publications/` directory left by
an earlier release can be deleted. Back up `providers.sqlite` with the index:
rebuilding it means one read of every swap since `--near-since`. Logs go to the
journal through `tracing`; `RUST_LOG` in the unit sets the level.

## Monitoring

The PIR monitor runs `receiver-probe` (from the same release, its fixture built
in) every minute as the `receiver` service probe, from its host on the same
private network. `pir-monitor` is not in a release bundle: build it with
`enhance/ops/scripts/build-observability.sh`, as for its other probes. The chain
checks use the monitor host's own node and cookie, as the Status probe and the
Transparent canary do, not the fleet nodes the service reads, so they are an
independent oracle. That node must answer `z_gettreestate` with the Ironwood
root, which `--witnesses` checks the served witness file against. The cookie and
the probe config come from the monitor's host drop-in, [`pir-monitor-service-quality.conf`](../../../enhance/ops/deploy/pir-monitor-service-quality.conf),
installed as `/etc/systemd/system/pir-monitor.service.d/90-service-quality.conf`
(the live monitor already has it). Its `LoadCredential` source,
`/etc/pir-monitor/credentials/chain-rpc-cookie`, must exist before the monitor
starts, or `pir-monitor` does not start: on a new monitor host, copy the cookie of
the node the monitor reaches at `127.0.0.1:18232` there (owned by root, mode 0600),
and copy it again and restart the monitor when that node rotates it.

The drop-in's `/etc/pir-monitor/service-probes.json` is a JSON array of
`{service, command, timeout_seconds}` records, read once at start. A file that is
not an array, or names a service twice, stops `pir-monitor` from starting. The
receiver's record, as a whole file for a new monitor host that runs only this
probe:

```json
[
  {"service": "receiver", "command": ["/opt/pir-monitor/receiver-probe", "--origin",
    "https://receiver-pir.valargroup.dev", "--health-url",
    "http://10.70.0.11:18380/v1/receiver/health",
    "--rpc-url", "http://127.0.0.1:18232", "--cookie",
    "/run/credentials/pir-monitor.service/chain-rpc-cookie", "--witnesses"],
   "timeout_seconds": 30}
]
```

On a host that already has the file, such as the live monitor with its `status`
and `transparent` records, first upgrade `pir-monitor` to a build of this change:
an earlier one accepts only those two services and does not start with a `receiver`
record. Build it with `enhance/ops/scripts/build-observability.sh`, install it as
[the observability deployment](../../../enhance/docs/observability-alerting.md#deployment-and-rollback)
does, keeping the previous binary for rollback, restart it with the unchanged file and
check it is active. Then install the probe as below and merge the record instead of
overwriting the file. Its cloud-init installs no `jq`, so merge on the coordinator: save
the array above as `receiver-probe.json`, then fetch the live file, replace any
`receiver` record while keeping every other one, check the result, and rename it
into place before the restart that reads it:

```sh
ssh <monitor> cat /etc/pir-monitor/service-probes.json >service-probes.json.live &&
jq --slurpfile new receiver-probe.json '
  if type == "array" then . else error("service-probes.json is not an array") end
  | map(select(.service != "receiver")) + $new[0]' service-probes.json.live >service-probes.json.new &&
jq -e 'type == "array"
  and all(.[]; type == "object" and keys == ["command", "service", "timeout_seconds"])
  and ([.[].service] | length == (unique | length))
  and ([.[] | select(.service == "receiver")] | length == 1)' service-probes.json.new >/dev/null &&
scp service-probes.json.new <monitor>:/etc/pir-monitor/service-probes.json.new &&
ssh <monitor> 'cd /etc/pir-monitor && chmod 0644 service-probes.json.new &&
  mv service-probes.json.new service-probes.json && systemctl restart pir-monitor'
```

The fixture pins the public NEAR refund
`2060cf68088b55dcd9e2f91556c72528e1ab8c6834ea3f71b8c80fca9fc51653` (height
3,496,114, Action 0, note position 610503), which every publication holds, and
its receiver as decoded independently from NEAR's refund address (see
`receiver/crates/receiver-directory/tests/fixtures/zero-ovk-action-oracle`); the
probe refuses a recovery that does not reproduce it. It alerts when the served
anchor leaves the chain, the lookup misses or misreports the pinned payment, the
filter file is wrong or a completed NEAR payout is missing from the index
(correctness, `answer_mismatch`), when the built-in fixture is malformed, recovers
another receiver, is off the node's chain or outside the publication
(`oracle_invalid`), and after three failed probes when the publication or the
recent set goes stale, a recent completed payout to an Orchard receiver has no
parsable transaction (`payouts_uncheckable`) or a request fails (availability).

`pir-monitor` is not a deploy-tool service for any product, so install
`receiver-probe` on the monitor host from the bundle, checked against its
`SHA256SUMS` on the coordinator and again on the monitor host:

```sh
tools/ci/release.py extract --sha <rev> --kind receiver-pir --archive receiver-pir.tar.gz --output receiver-pir-<rev>
scp -r receiver-pir-<rev> <monitor>:/root/receiver-pir-tools.new
ssh <monitor> sh -s <<'EOF'
set -eu
cd /root/receiver-pir-tools.new
sha256sum -c SHA256SUMS
install -D -m 0755 -o root -g root receiver-probe /opt/pir-monitor/receiver-probe
cmp receiver-probe /opt/pir-monitor/receiver-probe
cd /
rm -r /root/receiver-pir-tools.new
EOF
```
