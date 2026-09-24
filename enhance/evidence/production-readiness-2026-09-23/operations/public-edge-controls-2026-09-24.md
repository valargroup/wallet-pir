# Public edge and query bounds

At 2026-09-24 07:58 UTC, `caddy validate --config /etc/caddy/Caddyfile`
reported `Valid configuration` on the serving coordinator. Caddy was active
with zero service restarts. An HTTPS `HEAD /v1/health` request to
`https://enhance-pir.valargroup.dev` returned HTTP 200 with a valid TLS
connection from the external operator host. The live Caddyfile routes public
Enhance queries to `127.0.0.1:8080` and denies public `/metrics` and `/ready`.
The [worker socket check](worker-port-exposure-2026-09-24.md) records the
private serving bindings separately.

The pinned server source limits a query body to 512 KiB before admission, with
a 30-second body-read deadline. It allows four active queries and 16 waiters,
with a two-second admission deadline and a ten-second timeout for each worker
route. The client and load reports measure successful end-to-end
requests. These source and configuration checks do not prove that oversized or
slow uploads return the intended status through the live edge; exercise those
failure paths after the measured public load.
