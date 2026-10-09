# Receiver PIR on DigitalOcean

One Droplet, `receiver-pir-poc-01`, runs `receiver-pir.service`, which starts
`receiver-directory --serve` against mainnet fleet nodes on the private network,
polls every 10 seconds and publishes two blocks below the tip. The service listens
on the Droplet's private address, `10.70.0.11:18380`, and nothing listens on its
public interface. Caddy terminates public TLS for `receiver-pir.valargroup.dev` and
proxies only the wallet routes (`init`, `public`, `query`, `rows`, `witness` and
`filters` under `/v1/receiver/`). `/v1/receiver/health` and `/metrics` stay off
the edge, as Transparent keeps its operator routes: the PIR monitor reads health
(through its probe) and `/metrics` over the private network.
[`ops/tests/test_receiver_ops_config.py`](../../../ops/tests/test_receiver_ops_config.py)
pins this edge, the unit's private listener and hardening, and cloud-init's account,
directories and firewall rule. The public-chain index lives in `/srv/receiver-pir/index` and
holds no wallet data. No Enhance service runs on this Droplet.

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

An imported Droplet has no SSH keys in state, and the provider replaces a Droplet
whose keys change, so `receiver.tf` ignores them; keep it that way. The saved plan
that follows must show no Droplet replacement or resize; the
recorded size slug is `s-4vcpu-8gb-amd`, and if the live one differs, set it first.
The plan creates the firewall, which then closes every port it does not list, so
confirm in it SSH from `allowed_ssh_cidrs` and from the coordinator's `/32`, without
which every later locked operation loses the Droplet, and the monitor's 18380
rule. It may also add the Droplet to the project. Do not apply a plan that destroys anything.

## Release and deploy

CI full on `main` builds `receiver-directory` and `receiver-probe` for `x86-64-v3`
and publishes them, with this directory's unit template, `Caddyfile`,
`cloud-init.yaml` and `probe-fixture.json`, as the `receiver-pir-<sha>` artifact
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
`/opt/receiver-pir/releases/<sha256>/receiver-directory`, with the bundle's
`receiver-probe` and `probe-fixture.json` beside it, each checked against the
bundle's `SHA256SUMS`, runs its `--help` there, and writes `/etc/systemd/system/receiver-pir.service` from the template with
`@RELEASE@` set to that directory. After the restart it requires the running
executable's digest and a health answer from
`http://10.70.0.11:18380/v1/receiver/health` whose `serving` is set, within 300
seconds: health answers at once, but `serving` stays null until the first
publication from the index. Then, still under the lock and before it commits, it
runs the inventory's `exact_check`: the [deploy probe](#deploy-probe) on the
Droplet itself, which looks up the pinned payment with one live encrypted query
over the public origin and checks the filter file and private health, as the
monitor's probe below does. It runs on the Droplet because the coordinator, in
`ams3` on `10.142.0.0/16`, cannot reach the private health route. Its chain checks
use the fleet nodes the service reads, the only ones the Droplet reaches, so it
gates the deploy but is not an independent oracle; the monitor's probe remains
that. A failed check, or one still running at its 120-second `timeout`, fails the
deploy. A failure, or `rollback`, restores the previous unit file and binary.
Report `status receiver` and the rollback command after each deploy.

To change the unit's arguments or settings, change `receiver-pir.service.in` in a
reviewed commit and deploy from that checkout. `plan` prints each changed setting
as a `drift` line, and `deploy` refuses it until `--allow-unit-drift` accepts the
reviewed drift. The receiver deploys only from a bundle (`--archive`); with the
binary unchanged, the same bundle redeploys the staged release with the new unit. The restart, readiness check and rollback
are those of any deploy, and a commit refreshes the baseline.

Before the first tool deploy, once:

1. Add the Droplet to the coordinator's real inventory as in
   [`deploy-inventory.example.json`](../../../enhance/ops/deploy/deploy-inventory.example.json):
   a host with its SSH address, `services.receiver.roles.server` on it with
   `vars.listen` set to the `--bind` address, `10.70.0.11:18380`, and
   `services.receiver.exact_check` on the same host, as there. Add its host key
   to the inventory's pinned `known_hosts` and update `known_hosts_sha256`.
2. Authorize the deploy key (`ssh.key`, `~/.ssh/wallet-pir-deploy` in the example)
   for root on the Droplet.
3. Run `ops/scripts/wallet-pir-deploy.py capture-baseline receiver` and review it.
   `preflight` and `deploy` refuse any unit change made after it.

The live Droplet still runs the unit installed by hand, which starts
`/opt/receiver-pir/current/receiver-directory` and has no drop-ins. The first
deploy adopts it: the tool replaces the unit at `/etc/systemd/system`, the only
fragment path a template role accepts, and keeps the old text in the transaction
for rollback. Only the binary path should differ, so its `plan` should show a
`write` of the unit and no `drift` lines. A drift line means the live unit differs
from the template; reconcile the template in a reviewed commit rather than accept
it unread. A drop-in that does not set `ExecStart` would stay and still apply; one
that does is refused, except an `ExecStart`-only managed drop-in from an earlier
`exec-drop-in` deploy, which `--retire-historical` moves into the transaction
directory. That first transaction's rollback returns to `current`, so leave
`current` alone. Deploying the binary that already runs is a no-op that leaves the
hand-installed unit in place until the template changes.

### Deploy probe

The inventory's `exact_check` runs `{release_dir}/receiver-probe` with
`{release_dir}/probe-fixture.json`: the probe and fixture the deploy staged in the
release directory from the bundle it deploys, so nothing is installed for it by
hand. A new fixture also needs its digest in the check's `--fixture-sha256`. The
release directory is named by the server binary's digest alone, so a bundle with
the same `receiver-directory` but another probe or fixture is refused, before any
change, wherever that release is already staged; it is never reused or
overwritten. After `preflight --stage`, run the check's `argv` once by hand on the
Droplet, with `{release_dir}` replaced by the staged release directory, and confirm
it prints `"passed":true`, which also shows that the Droplet reaches the public
origin and both nodes.

## Host provisioning

These steps prepare a new Droplet before it serves anything. It is not yet in the
deploy inventory, so no deploy can reach it.

1. `cloud-init.yaml` installs Caddy and `ufw`, creates the `receiver-pir` user,
   `/opt/receiver-pir/releases` and `/srv/receiver-pir`, and opens SSH, HTTP, HTTPS
   and port 18380 from the private network.
2. Install the first `receiver-directory` as
   `/opt/receiver-pir/releases/<sha256>/receiver-directory`, where `<sha256>` is
   its `sha256sum`, the tool's release layout.
3. Check the nodes' private RPC URLs and the `--bind` address (the Droplet's private
   IPv4) in `receiver-pir.service.in`, and the same address in `Caddyfile`. Install
   the `Caddyfile` in `/etc/caddy`, and the unit rendered as the tool renders it:
   `sed 's|@RELEASE@|/opt/receiver-pir/releases/<sha256>|' receiver-pir.service.in >/etc/systemd/system/receiver-pir.service`.
   `--near-since` sets where a new provider database's first read starts; keep it
   at the feed's original start.
4. For the recent and seen filters, put the NEAR Intents explorer partner key in
   `/etc/receiver-pir/near.env` as `NEAR_INTENTS_EXPLORER=<key>`, owned by root with
   mode 0600. Without it the service publishes no provider sets.
5. Enable and start `receiver-pir` and reload `caddy`. Check that
   `https://receiver-pir.valargroup.dev/v1/receiver/health` returns 404 and that
   `http://10.70.0.11:18380/v1/receiver/health` answers from the monitor host.

After provisioning, every change to the Droplet holds the production lock. Unit
changes are deploys, as above. The tool cannot change the `Caddyfile` or `near.env`:
it writes only unit files and has no command that runs other work under its lock.
Make those changes under the same lock, held for the whole change with `flock -n`
on the coordinator, as `ops/scripts/wallet-pir-terraform.sh` holds it. `flock -n`
fails at once while a deploy, rollback or Terraform run holds the lock; then wait
and run it again, never without the lock. Neither change touches the unit or the
binary, so the baseline stays valid. Copy the new file to the Droplet first, which
changes nothing, then run the change as root on the coordinator, where `<droplet>`
is SSH to root on the Droplet with an identity it accepts:

```sh
# Caddyfile, reviewed in this directory first.
flock -n /run/lock/wallet-pir-production.lock ssh <droplet> sh -s <<'EOF'
set -eu
caddy validate --adapter caddyfile --config /root/Caddyfile.new
install -m 0644 /root/Caddyfile.new /etc/caddy/Caddyfile
systemctl reload caddy
EOF

# near.env, which the service reads only at start; the restart interrupts serving.
flock -n /run/lock/wallet-pir-production.lock ssh <droplet> sh -s <<'EOF'
set -eu
install -m 0600 -o root -g root /root/near.env.new /etc/receiver-pir/near.env
rm /root/near.env.new
systemctl restart receiver-pir
timeout 300 sh -c 'until curl -fsS http://10.70.0.11:18380/v1/receiver/health | grep -q "\"serving\":\""; do sleep 5; done'
EOF
```

The service publishes from memory and writes no publication files; the index keeps
only `directory.sqlite` and `provider.sqlite`. A `publications/` directory left by
an earlier release can be deleted. Back up `provider.sqlite` with the index:
rebuilding it means one read of every swap since `--near-since`. Logs go to the
journal through `tracing`; `RUST_LOG` in the unit sets the level.

## Monitoring

The PIR monitor runs `receiver-probe` (from the same release) every minute as the
`receiver` service probe, from its host on the same private network, with the
release's `probe-fixture.json` installed beside it as
`receiver-probe-fixture.json`. `pir-monitor` accepts the `receiver` service only
from this commit on and is not in a release bundle: build it with
`enhance/ops/scripts/build-observability.sh`, as for its other probes. The chain
checks use the monitor host's own node and cookie, as the Status probe and the
Transparent canary do, not the fleet nodes the service reads, so they are an
independent oracle. The cookie and the probe config come from the monitor's host
drop-in, [`pir-monitor-service-quality.conf`](../../../enhance/ops/deploy/pir-monitor-service-quality.conf),
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
    "http://10.70.0.11:18380/v1/receiver/health", "--fixture",
    "/opt/pir-monitor/receiver-probe-fixture.json", "--fixture-sha256",
    "b54f89f7346022918a0f137a2d1ea139fb5853238d695f9c8b98972c9f2a7591",
    "--rpc-url", "http://127.0.0.1:18232", "--cookie",
    "/run/credentials/pir-monitor.service/chain-rpc-cookie"], "timeout_seconds": 30}
]
```

On a host that already has the file, such as the live monitor with its `status`
and `transparent` records, merge instead of overwriting, after installing the probe
and fixture as below. Its cloud-init installs no `jq`, so merge on the coordinator: save
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
3,496,114, Action 0, note position 610503), which every publication holds. Each
run looks up its receiver with one live encrypted query over the public origin and
checks the filter file, so it moves about 140 KB: the session manifest, the
14,848-byte public setup, a 77,876-byte query, a 5,684-byte response and the
filter file (about 37 KB in October 2026), plus health. It alerts when the
served anchor leaves the chain, the lookup misses or misreports the pinned payment,
the filter file is wrong or a completed NEAR payout is missing from the index
(correctness), when the
fixture fails its pin (oracle), and after three failed probes when the publication
or the recent set goes stale or a request fails (availability).

From this commit, the same drop-in sets `PIR_MONITOR_METRICS_TARGETS` to scrape
`http://10.70.0.11:18380/metrics` once a minute (body capped at 256 KiB, 5-second
deadline); it takes effect once the monitor runs a build from this commit with the
reinstalled drop-in. `/monitor-status` reports each target under `metrics`: collection health
(`stale` after 180 seconds without a successful scrape, and the last failure
`category`), per-endpoint request, status-class, cancellation and incomplete-response
increases with p50/p99 latency bucket bounds, and the per-minute samples, with
`reset` marking a restart. All of these cover only samples whose whole interval
lies in the last hour; `window_seconds` is the span they cover, shorter after a
start or an outage, since a sample spanning the hour's start is dropped, not
split. Status classes are as counted: `4xx` includes 429 overload but is not
overload. It adds no alert rules.

`pir-monitor` is not a deploy-tool service, so install `receiver-probe` and its
fixture on the monitor host from the bundle. On the coordinator, `release.py
extract` checks every file against the bundle's `SHA256SUMS`; copy the result to
the monitor host and install it there, checking the digests again:

```sh
tools/ci/release.py extract --sha <rev> --kind receiver-pir --archive receiver-pir.tar.gz --output receiver-pir-<rev>
scp -r receiver-pir-<rev> <monitor>:/root/receiver-pir-tools.new
ssh <monitor> sh -s <<'EOF'
set -eu
cd /root/receiver-pir-tools.new
sha256sum -c SHA256SUMS
install -D -m 0755 -o root -g root receiver-probe /opt/pir-monitor/receiver-probe
install -m 0644 -o root -g root probe-fixture.json /opt/pir-monitor/receiver-probe-fixture.json
cmp receiver-probe /opt/pir-monitor/receiver-probe
cmp probe-fixture.json /opt/pir-monitor/receiver-probe-fixture.json
cd /
rm -r /root/receiver-pir-tools.new
EOF
```
