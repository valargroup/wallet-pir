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
from the pinned wallet-pir source with locked, vendored
dependencies, Rust 1.98.0, profile release-fast, and target-cpu=x86-64-v3. The
current source baseline is e9e48d2b3e04818ba049697020a5cc93e216e463.
Install the integrated binary into a versioned directory
under /opt/receiver-pir/releases and point /opt/receiver-pir/current at it.
Enable receiver-pir and caddy. Keep ports 18380 and RPC internals private;
only SSH, HTTP certificate issuance and HTTPS are exposed.

Active release: `/opt/receiver-pir/releases/follow-enhance-20260927`, deployed
2026-09-27 at 19:03:37 UTC. This is the source baseline above plus the local
receiver lifecycle changes; these changes have not yet been committed or pushed.
The deployed source overlay is archived in the release directory.

- Source overlay SHA256: `ce30e00e7b407d9d08fd353872b5bcbc08b17fcb8276cca9cd3b9f86b36352f5`
- Binary SHA256: `cf8c846cecdf4a0d6e452a26b8e5ad41f377f15bcfd6e7ec2528f7052ccaf73a`
- Verified heights 3498323 and 3498324 through the external HTTPS endpoint,
  matching canonical RPC over continuous HTTPS serving. The process check during
  the second preparation reported PID 32450 and zero service restarts.
- The first publication took 27.55 seconds; unchanged-tip checks took 10–11 ms.
  Most publication time remains common-witness reconstruction. A 10-second poll
  is not a promise of sub-20-second publication latency.
- The Vizor dependency-graph mainnet test retrieved refund position 610503,
  verified its witness against the independently queried node root, and checked
  the ciphertext returned by public Enhance.

Rollback assets are `previous.service` and `previous-source.tar.gz` in the new
release directory, plus `/srv/receiver-pir/rollback-before-follow-enhance.sqlite`.
Stop the service, restore the public-chain database with SQLite's backup API,
restore the prior unit and symlink, reload systemd and start. Restore the database
as well as the binary because the old indexer applies its ten-block delay.
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
test. The current POC wallet transport is direct HTTPS and still rejects Tor mode.

Final external observation reached height 3498325. Subsequent SSH reads failed
with public-key authentication errors, so the final journal export and copying
this updated deployment note onto the host were not completed. No authentication
or SSH configuration was changed by this deployment.
