# Receiver deployment record

Captured 2026-10-08 at 23:18:07 UTC ([`captured_at.txt`](captured_at.txt)) from
the DigitalOcean and Cloudflare APIs, the public origin and the Droplet itself.
It records what was live, not a qualification run. The capture commands were not
retained; each file below says what it holds. `SHA256SUMS.original` is the
capture's own checksum file, and `SHA256SUMS` hashes every file here, including
this note and [`manifest.json`](manifest.json).

- [`droplet.json`](droplet.json): Droplet 604069093, `receiver-pir-poc-01`, in
  `nyc3` with size slug `s-4vcpu-8gb-amd`, created 2026-09-27, public IPv4
  161.35.182.172 and private IPv4 10.70.0.11.
- [`cloud-firewalls.json`](cloud-firewalls.json): `[]`, no DigitalOcean firewall
  attached to the Droplet.
- [`dns-a.txt`](dns-a.txt): `receiver-pir.valargroup.dev` resolves to
  161.35.182.172 with TTL 300.
- [`host.txt`](host.txt): on the Droplet, `receiver-pir` is active from release
  directory `c7f6d296-20261008` (the receiver deployment change before its review
  fixes), the binary's SHA-256, the `ufw` rule limiting 18380 to `10.70.0.0/16`,
  the listener on 10.70.0.11:18380 only, and the private health answer: the same
  binary hash, the served session and the indexer report (81 payouts checked, none
  missing).
- [`public-health-status.txt`](public-health-status.txt): the public origin
  answers `/v1/receiver/health` with 404.
- [`public-init.json`](public-init.json): the public session manifest, at height
  3,511,128 with 8192 rows, 24,596 records and the `paid`, `near-intents/recent`
  and `near-intents/seen` sets.
