# Transparent block benchmark host

`transparent-block-bench.json` records the separately provisioned benchmark host.
It is not a PIR replica, router, chain node, or Terraform-managed production host.
The service unit is `../systemd/transparent-block-server.service`.

The public origin uses Caddy with an automatically issued TLS certificate and
proxies to loopback port 8096. Preserve the pre-encoded artifact bytes:

```caddyfile
transparent-sync-bench.valargroup.dev {
    reverse_proxy 127.0.0.1:8096 {
        transport http {
            compression off
        }
    }
}
```

Disable the proxy transport's automatic decompression as shown above; do not add
an `encode` middleware. The wallet explicitly negotiates gzip and decodes it after
checking the encoded payload's length and checksum. Its cloud firewall allows HTTPS from the recorded
measurement client and the recorded operator address, SSH from the operator, measurement client and export host, and HTTP for
ACME certificate validation. If the measurement client's IP changes, update that
specific firewall rule and the recorded address before testing. Never broaden
PIR-worker firewall rules to make a benchmark work.

Metrics remain private. From the measurement client:

```sh
ssh -N -L 19097:127.0.0.1:8097 root@10.142.0.14
```

Use `BLOCK_METRICS_URL=http://127.0.0.1:19097/metrics` in the comparison. Record
metrics from every PIR worker independently as described in the load-test README.

Copy a complete, verified export into `/srv/transparent-sync-bench/data` before
starting `transparent-block-server`. Check exported size against available storage
with at least 25% free headroom; add a dedicated data volume if necessary. Dataset
files must be readable by the service's dynamic user. Export and serving are
separate steps so node reads and dataset preparation are excluded from timing.

Keep the host, dataset, and reports after a run for reproducibility. Deleting the
host or volume requires a separate explicit request. No private SSH key or agent
environment file is installed on this host.

The full comparison uses the existing `transparent-pir-loadgen-01` host
(`152.42.137.247`, ams3, 16 vCPUs, 32 GiB). Both recovery methods use their public
TLS origins from that host. These are regional backend-load measurements, not
Canadian WAN latency measurements. A separate Mac transport diagnostic is kept
outside the paired results. No SSH keys are copied to the load generator.

The run files are staged in `/opt/transparent-sync-bench-client`. Its `compare.py`
uses the same CLI as the local Make target. The six PIR worker metrics endpoints
are reachable over the private VPC. Filter and block metrics remain on loopback
and are forwarded over SSH; note this transport in the conditions metadata and
retain scrape failures in the report. Do not expose private metrics publicly.

The September 9 comparison uses SSH tunnels entirely within Amsterdam: the
load generator connects to the baseline over the private network, and the
coordinator reverse-forwards its filter metrics to load-generator loopback port
19100. Authentication used a temporary forwarded agent; private keys were not
copied. These established connections do not depend on the operator Mac staying
online. Recreate them after a host restart before starting an APM comparison.
