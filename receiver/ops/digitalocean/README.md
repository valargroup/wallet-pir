# Receiver PIR POC on DigitalOcean

Deployed 2026-09-27 in project wallet-pir, NYC3. Droplet receiver-pir-poc-01
(ID 604069093), IP 161.35.182.172, size s-4vcpu-8gb-amd ($56/month).
Public receiver origin is https://161-35-182-172.sslip.io. Caddy provides
public TLS and proxies only /v1/receiver/* to loopback port 18380.

The receiver runs independently of the Mac Studio. receiver-pir.service refreshes
from the configured mainnet RPC every 10 seconds, with no confirmation delay.
The integrated daemon follows Enhance's canonical publication lifecycle, swaps
prepared revisions without restarting HTTP, and revokes sessions on reorg. Its public-chain index lives
at /srv/receiver-pir/index. The initial SQLite database was copied using SQLite
backup from the Studio index; it contains no wallet database or seed.

Provision using cloud-init.yaml; install receiver-pir.service under
/etc/systemd/system and Caddyfile under /etc/caddy. Build receiver-directory
from the pinned wallet-pir source with locked dependencies, Rust 1.98.0, profile
release-fast, and target-cpu=x86-64-v3 (RUSTFLAGS overrides the repository's
target-cpu=native). From an arm64 host, cross-compile with
`--target x86_64-unknown-linux-gnu` in rust:1.98.0-bookworm, linking with
x86_64-linux-gnu-gcc. The current source baseline is
48a6c2a55603fdc5eeb1823cc8102588fc561d0f.
Install the integrated binary into a versioned directory
under /opt/receiver-pir/releases and point /opt/receiver-pir/current at it.
Enable receiver-pir and caddy. Keep ports 18380 and RPC internals private;
only SSH, HTTP certificate issuance and HTTPS are exposed.

Active release: `/opt/receiver-pir/releases/48a6c2a5-20260930`, deployed
2026-09-30 at 14:04:09 UTC from the committed baseline above, with no local
overlay. It adds the whole-file rows endpoint, PIR serving from 8192 to 65536
rows, and the in-memory witness cache. `BUILD-INFO` in the release directory
records the build.

- Binary SHA256: `a541d982a0893ba96ff5ad519ee4ca36a6fd63390007c5880c4652535b57fd54`
- The first publication after restart took 28.7 seconds, 26.6 of them building
  the witness cache. The next took 2.05 seconds.
- External checks at heights 3501525 and 3501526: the manifest, the 33,554,432
  byte row file matching the manifest digest, the 14,336 byte PIR setup matching
  its digest, the 4,294,224 byte common witness file, and 409 for an unknown
  session on the rows route.

Rollback assets are `previous.service` in the new release directory, the prior
release `/opt/receiver-pir/releases/follow-enhance-20260927`, and
`/srv/receiver-pir/rollback-before-48a6c2a5.sqlite`. Stop the service, restore
the public-chain database with SQLite's backup API, restore the prior unit and
symlink, reload systemd and start.

The prior release ran commit 207053dc exactly: its archived source overlay
matched that commit apart from this deployment note. It could serve only 8192
rows, so an overflowing directory deferred publication, and it had no rows
endpoint.
The live daemon prunes obsolete publication files; it retains the canonical
SQLite journal and the current and previous publication files.

Original release (retained for rollback): /opt/receiver-pir/releases/e9e48d2-20260927.
Binary SHA256:
- receiver-directory: e24c2d187b5227c1b80b6ed72cc2fbee2b77879412f93c7db171e287d8a9aef5
- receiver-pir-server: 8191d759b2427bb133f10039a3f47afb471babcddded63e0da45395a0f7e672b

Enhance uses the existing public https://enhance-pir.valargroup.dev service,
protocol ironwood-enhance-pir-v9-native-two-mask-m29, via the native-reinspiring
client feature. No Enhance service is hosted on this Droplet. The duplicate
swap-pir-integration-01 (603863517) was deleted. Studio launch jobs
com.valargroup.codex.swap-receiver-poc and com.valargroup.codex.swap-pir-tunnel
were unloaded; archived chain data remains for rollback/evidence.

Validation passed after retiring the old services: receiver manifest and common
witness roots matched mainnet RPC, and the Vizor dependency-graph real-refund test
retrieved the receiver record, fetched ciphertext through public Enhance, and
verified inclusion at position 610503. Manual wallet restore remains a separate
test. Wallets reach the service through Vizor's route-aware transport, including
Tor.

SSH as root works with the account's DigitalOcean deploy key (verified
2026-09-30). No authentication or SSH configuration was changed by either
deployment.
