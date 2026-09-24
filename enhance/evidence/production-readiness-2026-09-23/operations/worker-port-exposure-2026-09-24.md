# Worker port exposure check

At 2026-09-24 07:26 UTC, the external `roman-ipir-bench-8vcpu` host attempted
TCP connections to ports 8091 and 8291 on each serving worker's current public
IPv4 address. The addresses were confirmed by `hostname -I` on the workers:
`206.189.104.99` for private `10.142.0.15` and `206.189.14.84` for private
`10.142.0.16`. Each connection used a three-second timeout.

| Public worker IPv4 | 8091 | 8291 |
| --- | --- | --- |
| `206.189.104.99` | timeout | timeout |
| `206.189.14.84` | timeout | timeout |

This establishes that the two worker ports were unreachable from that external
host during this check. Concurrent `ss -lntp` output showed the serving workers
listening only on `10.142.0.15:8091` and `10.142.0.16:8091`, respectively.
On the coordinator, canonical port 8080, isolated campaign port 8280, and node
RPC port 8232 listened on `127.0.0.1`; Caddy listened on public port 443.
These are point-in-time socket observations. They do not by themselves prove
the firewall policy is correct for every source; retain the firewall inventory
and keep probing after any network configuration change.
