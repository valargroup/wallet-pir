# node-rpc

The JSON-RPC transport Enhance, Transparent and Receiver use to read a Zakura node.
It owns only transport: the cookie, deadlines, the response bound, single calls and
batches. Requests, block and transaction parsing, chain identity, canonical checks and
error mapping stay in each product's `zakura.rs`.

| | Enhance and Transparent | Receiver |
|---|---|---|
| Cookie | read once, when the client is built | re-read before every request (the node rotates it on restart) |
| Whole request | 120 s | 120 s |
| Connect | no separate deadline | 20 s |
| Response bound | 64 MiB, whole response | derived from the chain's block limits (`RPC_RESPONSE_BYTES`) |
| 401 | `InvalidCookie` | the HTTP status error |

The 64 MiB Enhance and Transparent bound is deliberately a whole-response bound: far
above any block or transaction batch they request, it only stops a misbehaving endpoint
from making them buffer without limit. Bodies are streamed within the request deadline
and the bound whether or not the response declares its length; an oversized,
truncated or malformed body is refused.

Single calls are JSON-RPC 1.0 under each product's fixed id (`enhance-pir`,
`transparent-filter`, `receiver-indexer`). Batches are JSON-RPC 2.0 with integer ids
`0..n`, since the node refuses 1.0 batches. A batch's answer must be an array holding
exactly one response per issued id; a missing, repeated, unexpected, negative or
non-integer id fails the whole batch, and valid responses are returned in request order
with each entry's own result or RPC error. A node refusing a batch as a whole answers
with one error object, reported as that RPC error. There are no retries, no splitting
and no fallback endpoint.

Tests (`tests/http.rs`) drive real HTTP servers that write every response byte:
batch validation and reordering, authentication and cookie rotation or removal,
connect and body deadlines (injected short), the exact bound with and without a
declared length, truncation and malformed bodies.
