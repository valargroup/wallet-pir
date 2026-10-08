# Receiver PIR on DigitalOcean

One Droplet, `receiver-pir-poc-01`, runs `receiver-pir.service`, which starts
`receiver-directory --serve` against mainnet fleet nodes on the private network,
polls every 10 seconds and publishes two blocks below the tip. The service listens
on the Droplet's private address, `10.70.0.11:18380`, and nothing listens on its
public interface. Caddy terminates public TLS for `receiver-pir.valargroup.dev` and
proxies only the wallet routes (`init`, `public`, `query`, `rows`, `witness` and
`filters` under `/v1/receiver/`). `/v1/receiver/health` and `/metrics` stay off
the edge, as Transparent keeps its operator routes: the PIR monitor reads them over
the private network. The public-chain index lives in `/srv/receiver-pir/index` and
holds no wallet data. No Enhance service runs on this Droplet.

## Infrastructure

`ops/infra/digitalocean/production/receiver.tf` manages the Droplet (`nyc3`,
`s-4vcpu-8gb`, Ubuntu 24.04, with this directory's `cloud-init.yaml`), its project
membership, its firewall and the unproxied A record `receiver-pir.valargroup.dev`
(TTL 300), behind `receiver_pir_enabled`. The Droplet has `prevent_destroy` and
ignores `user_data` changes. The firewall allows SSH from `allowed_ssh_cidrs`, HTTP
and HTTPS from anywhere, and port 18380 only from the PIR monitor Droplet; the
host's `ufw` also limits 18380 to `10.70.0.0/16`.

The Droplet (ID 604069093), its firewall and its DNS record already exist. Import
them before the first plan that enables the root's receiver resources, on the
coordinator and under its lock like every production operation. Set
`receiver_pir_enabled = true` in `/etc/enhance-pir/production.tfvars`, find the
firewall and record IDs, then import:

```bash
doctl compute firewall list --format ID,Name,DropletIDs | grep 604069093
curl -sS -H "Authorization: Bearer $CF_API_TOKEN" \
  'https://api.cloudflare.com/client/v4/zones/d3ac9657be6101818fed439c62fdcadf/dns_records?type=A&name=receiver-pir.valargroup.dev' \
  | jq -r '.result[0].id'

import() {
  ops/scripts/wallet-pir-terraform.sh import -var-file=/etc/enhance-pir/production.tfvars \
    -var="enhance_group_count=<current group count>" "$@"
}
import 'digitalocean_droplet.receiver_pir[0]' 604069093
import 'digitalocean_firewall.receiver_pir[0]' <firewall-id>
import 'cloudflare_dns_record.receiver_pir[0]' 'd3ac9657be6101818fed439c62fdcadf/<record-id>'
```

The saved plan that follows must show no Droplet replacement or resize; if the
Droplet's size slug differs from `s-4vcpu-8gb`, set it to the actual slug first. It
may update the firewall in place (its name and the monitor's 18380 rule) and add
the Droplet to the project. Do not apply a plan that destroys anything.

## Release and install

1. CI full on `main` builds `receiver-directory` and `receiver-probe` for
   `x86-64-v3` and publishes them, with this directory's unit, `Caddyfile`,
   `cloud-init.yaml` and `probe-fixture.json`, as the `receiver-pir-<sha>` artifact
   (`tools/ci/release.py`). To build outside CI, use `--locked`,
   `-p receiver-indexer`, `--release` and `RUSTFLAGS='-C target-cpu=x86-64-v3'`;
   from an arm64 host, cross-compile with `--target x86_64-unknown-linux-gnu` in
   `rust:1.98.0-bookworm`, linking with `x86_64-linux-gnu-gcc`.
2. A new Droplet's `cloud-init.yaml` installs Caddy and `ufw`, creates the
   `receiver-pir` user, `/opt/receiver-pir/releases` and `/srv/receiver-pir`, and
   opens SSH, HTTP, HTTPS and port 18380 from the private network.
3. Install `receiver-directory` in a versioned directory under
   `/opt/receiver-pir/releases` and point `/opt/receiver-pir/current` at it.
4. Check the nodes' private RPC URLs and the `--bind` address (the Droplet's
   private IPv4) in `receiver-pir.service`, and the same address in `Caddyfile`,
   then install them in `/etc/systemd/system` and `/etc/caddy`. `--near-since` sets
   where a new provider database's first read starts; keep it at the feed's
   original start.
5. For the recent and seen filters, put the NEAR Intents explorer partner key in
   `/etc/receiver-pir/near.env` as `NEAR_INTENTS_EXPLORER=<key>`, owned by root
   with mode 0600. Without it the service publishes no provider sets.
6. Enable and start `receiver-pir` and reload `caddy`. Check that
   `https://receiver-pir.valargroup.dev/v1/receiver/health` returns 404 and that
   `http://10.70.0.11:18380/v1/receiver/health` answers from the monitor host.

The service publishes from memory and writes no publication files; the index keeps
only `directory.sqlite` and `provider.sqlite`. A `publications/` directory left by
an earlier release can be deleted. Back up `provider.sqlite` with the index:
rebuilding it means one read of every swap since `--near-since`. Logs go to the
journal through `tracing`; `RUST_LOG` in the unit sets the level.

## Monitoring

The PIR monitor runs `receiver-probe` (from the same release) every minute as the
`receiver` service probe, from its host on the same private network, with the
release's `probe-fixture.json` installed beside it as
`receiver-probe-fixture.json`:

```json
{"service": "receiver", "command": ["/opt/pir-monitor/receiver-probe", "--origin",
  "https://receiver-pir.valargroup.dev", "--health-url",
  "http://10.70.0.11:18380/v1/receiver/health", "--fixture",
  "/opt/pir-monitor/receiver-probe-fixture.json", "--fixture-sha256",
  "b54f89f7346022918a0f137a2d1ea139fb5853238d695f9c8b98972c9f2a7591",
  "--rpc-url", "http://10.70.0.19:8232", "--rpc-url", "http://10.70.0.6:8232",
  "--no-auth"], "timeout_seconds": 30}
```

The fixture pins the public NEAR refund
`2060cf68088b55dcd9e2f91556c72528e1ab8c6834ea3f71b8c80fca9fc51653` (height
3,496,114, Action 0, note position 610503), which every publication holds. Each
run looks up its receiver with one live encrypted query over the public origin, so
it moves about 100 KB: the session manifest, the 14,848-byte public setup, a
77,876-byte query and a 5,684-byte response, plus health. It alerts when the
served anchor leaves the chain, the lookup misses or misreports the pinned payment
or a completed NEAR payout is missing from the index (correctness), when the
fixture fails its pin (oracle), and after three failed probes when the publication
or the recent set goes stale or a request fails (availability). To
upgrade, install a new release directory, repoint `current` and restart the
service. It republishes from the index before answering requests.
