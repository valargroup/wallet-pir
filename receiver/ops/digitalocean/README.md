# Receiver PIR on DigitalOcean

One Droplet runs `receiver-pir.service`, which starts `receiver-directory --serve`
against a mainnet RPC node and polls every 10 seconds with no confirmation delay.
Caddy terminates public TLS and proxies only `/v1/receiver/*` to loopback port
18380. The public-chain index lives in `/srv/receiver-pir/index` and holds no
wallet data. No Enhance service runs on this Droplet.

## Provisioning

1. Create an `s-4vcpu-8gb-amd` or larger Droplet with `cloud-init.yaml`. It
   installs build tools, Caddy and Rust 1.98.0, allows only SSH, HTTP and HTTPS,
   and creates `/opt/receiver-pir/releases` and `/srv/receiver-pir`.
2. Create a `receiver-pir` system user that owns `/srv/receiver-pir`.
3. Build `receiver-directory` from a pinned wallet-pir revision with `--locked`,
   profile `release-fast` and `RUSTFLAGS='-C target-cpu=x86-64-v3'`, which
   overrides the repository's `target-cpu=native`. From an arm64 host,
   cross-compile with `--target x86_64-unknown-linux-gnu` in
   `rust:1.98.0-bookworm`, linking with `x86_64-linux-gnu-gcc`.
4. Install the binary in a versioned directory under `/opt/receiver-pir/releases`
   and point `/opt/receiver-pir/current` at it.
5. Set the node's RPC URL in `receiver-pir.service` and the public host name in
   `Caddyfile`, then install them in `/etc/systemd/system` and `/etc/caddy`.
6. For the recent and seen filters, put the NEAR Intents explorer partner key in
   `/etc/receiver-pir/near.env` as `NEAR_INTENTS_EXPLORER=<key>`, owned by root
   with mode 0600. Without it the service publishes those sets empty.
7. Enable and start `receiver-pir` and `caddy`. Keep port 18380 and the RPC node
   private.

The service prunes obsolete publication files, keeping the SQLite index and the
current and previous publications. To upgrade, install a new release directory,
repoint `current` and restart the service. It republishes from the index before
answering requests.
