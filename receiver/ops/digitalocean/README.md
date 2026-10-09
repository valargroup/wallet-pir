# Receiver PIR on DigitalOcean

One Droplet, `receiver-pir-poc-01`, runs `receiver-pir.service`, which starts
`receiver-directory --serve` against mainnet fleet nodes on the private network,
polls every 10 seconds and publishes two blocks below the tip. The service listens
on the Droplet's private address, `10.70.0.11:18380`, and nothing listens on its
public interface. Caddy terminates public TLS for `receiver-pir.valargroup.dev` and
proxies only the wallet routes (`init`, `public`, `query`, `rows`, `witness` and
`filters` under `/v1/receiver/`). `/v1/receiver/health` and `/metrics` stay off
the edge, as Transparent keeps its operator routes: the PIR monitor's probe reads
health over the private network. Nothing collects `/metrics` yet.
[`ops/tests/test_receiver_ops_config.py`](../../../ops/tests/test_receiver_ops_config.py)
pins this edge, the unit's private listener and hardening, and cloud-init's account,
directories and firewall rule. The public-chain index lives in `/srv/receiver-pir/index` and
holds no wallet data. No Enhance service runs on this Droplet.

## Infrastructure

`ops/infra/digitalocean/production/receiver.tf` manages the Droplet (`nyc3`,
`s-4vcpu-8gb-amd`, Ubuntu 24.04, with this directory's `cloud-init.yaml`), its project
membership, its firewall and the unproxied A record `receiver-pir.valargroup.dev`
(TTL 300), behind `receiver_pir_enabled`. The Droplet has `prevent_destroy` and
ignores `user_data` and `ssh_keys` changes. The firewall allows SSH from `allowed_ssh_cidrs`, HTTP
and HTTPS from anywhere, and port 18380 only from the PIR monitor Droplet; the
host's `ufw` also limits 18380 to `10.70.0.0/16`.

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
confirm SSH from `allowed_ssh_cidrs` and the monitor's 18380 rule in it, and may add
the Droplet to the project. Do not apply a plan that destroys anything.

## Release and deploy

CI full on `main` builds `receiver-directory` and `receiver-probe` for `x86-64-v3`
and publishes them, with this directory's unit, `Caddyfile`, `cloud-init.yaml` and
`probe-fixture.json`, as the `receiver-pir-<sha>` artifact holding
`receiver-pir.tar.gz` (`tools/ci/release.py`). To build outside CI, use `--locked`,
`-p receiver-indexer`, `--release` and `RUSTFLAGS='-C target-cpu=x86-64-v3'`; from
an arm64 host, cross-compile with `--target x86_64-unknown-linux-gnu` in
`rust:1.98.0-bookworm`, linking with `x86_64-linux-gnu-gcc`.

`receiver-directory` is the `receiver` service of
[`ops/scripts/wallet-pir-deploy.py`](../../../ops/scripts/wallet-pir-deploy.py),
described in [`deploy.toml`](../../../enhance/ops/deploy/deploy.toml) with one role,
`server`, on `receiver-pir.service`. Install, upgrade and roll it back only through
the tool, which holds the production lock on the coordinator, so two operators
cannot interleave on the Droplet. Do not copy binaries, repoint `current` or restart
the unit by hand.

```sh
ops/scripts/wallet-pir-deploy.py plan receiver --archive receiver-pir.tar.gz --sha <rev>
ops/scripts/wallet-pir-deploy.py preflight receiver --archive receiver-pir.tar.gz --sha <rev> --stage
ops/scripts/wallet-pir-deploy.py deploy receiver --archive receiver-pir.tar.gz --sha <rev> --skip-exact-check
ops/scripts/wallet-pir-deploy.py status receiver
ops/scripts/wallet-pir-deploy.py rollback receiver [--transaction ID]
```

The tool installs the binary as
`/opt/receiver-pir/releases/<sha256>/receiver-directory`, runs its `--help` there,
and writes the managed drop-in that replaces only the binary path in the unit's
`ExecStart`, keeping its arguments. After the restart it requires the running
executable's digest and a health answer from
`http://10.70.0.11:18380/v1/receiver/health` whose `serving` is set, within 300
seconds: health answers at once, but `serving` stays null until the first
publication from the index. A failure, or `rollback`, restores the previous drop-ins
and binary. `current` keeps pointing at the last by-hand install, which the first
transaction's rollback returns to, so leave it alone. The inventory has no
`exact_check` for the receiver; the monitor's receiver probe below is that check, so
confirm its next run passes after a deploy and roll back if it does not. Report
`status receiver` and the rollback command after each deploy.

Before the first tool deploy, once:

1. Add the Droplet to the coordinator's real inventory as in
   [`deploy-inventory.example.json`](../../../enhance/ops/deploy/deploy-inventory.example.json):
   a host with its SSH address, and `services.receiver.roles.server` on it with
   `vars.listen` set to the `--bind` address, `10.70.0.11:18380`. Add its host key
   to the inventory's pinned `known_hosts` and update `known_hosts_sha256`.
2. Authorize the deploy key (`ssh.key`, `~/.ssh/wallet-pir-deploy` in the example)
   for root on the Droplet.
3. Run `ops/scripts/wallet-pir-deploy.py capture-baseline receiver` and review it.
   `preflight` and `deploy` refuse any unit change made after it.

## Host provisioning

These steps prepare a new Droplet before it serves anything.

1. `cloud-init.yaml` installs Caddy and `ufw`, creates the `receiver-pir` user,
   `/opt/receiver-pir/releases` and `/srv/receiver-pir`, and opens SSH, HTTP, HTTPS
   and port 18380 from the private network.
2. Install the first `receiver-directory` in a directory under
   `/opt/receiver-pir/releases` and point `/opt/receiver-pir/current` at it; the
   unit starts it from there.
3. Check the nodes' private RPC URLs and the `--bind` address (the Droplet's private
   IPv4) in `receiver-pir.service`, and the same address in `Caddyfile`, then
   install them in `/etc/systemd/system` and `/etc/caddy`. `--near-since` sets where
   a new provider database's first read starts; keep it at the feed's original
   start.
4. For the recent and seen filters, put the NEAR Intents explorer partner key in
   `/etc/receiver-pir/near.env` as `NEAR_INTENTS_EXPLORER=<key>`, owned by root with
   mode 0600. Without it the service publishes no provider sets.
5. Enable and start `receiver-pir` and reload `caddy`. Check that
   `https://receiver-pir.valargroup.dev/v1/receiver/health` returns 404 and that
   `http://10.70.0.11:18380/v1/receiver/health` answers from the monitor host.

The tool changes only the binary. A later change to the unit's arguments, the
`Caddyfile` or `near.env` is not a deploy-tool operation: make it only when
`status receiver` shows no open transaction, then run `capture-baseline receiver`
again; until then `preflight` and `deploy` refuse the changed unit.

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
and copy it again and restart the monitor when that node rotates it. Add this entry
to its
`/etc/pir-monitor/service-probes.json`:

```json
{"service": "receiver", "command": ["/opt/pir-monitor/receiver-probe", "--origin",
  "https://receiver-pir.valargroup.dev", "--health-url",
  "http://10.70.0.11:18380/v1/receiver/health", "--fixture",
  "/opt/pir-monitor/receiver-probe-fixture.json", "--fixture-sha256",
  "b54f89f7346022918a0f137a2d1ea139fb5853238d695f9c8b98972c9f2a7591",
  "--rpc-url", "http://127.0.0.1:18232", "--cookie",
  "/run/credentials/pir-monitor.service/chain-rpc-cookie"], "timeout_seconds": 30}
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

`pir-monitor` is not a deploy-tool service, so `receiver-probe` and its fixture are
installed on the monitor host as above.
