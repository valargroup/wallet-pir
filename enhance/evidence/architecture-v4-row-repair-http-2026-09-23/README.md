# HTTP row-loss recovery campaign — September 23, 2026

The actual coordinator, two worker HTTP listeners, encrypted Rust client and
`enhance-pir-v4 repair-rows` CLI execute this local campaign:

1. Publish 67 records, keep a client on that retained generation, then publish
   100 records and keep the newer generation available.
2. Stop the first worker and delete its compiled cache and durable rows.
   Startup fails; exact current and retained queries succeed through its peer.
3. Invoke the actual repair binary using the peer's local row archive. The first
   invocation restores two units; the second restores zero. Both report restart
   required and unqualified. The target journal remains byte-identical.
4. Restart the repaired worker at its original address and stop its peer.
5. Require exact encrypted responses for retained positions 0, 32, 33 and 66,
   and current positions 67 and 99. Verify the retained client did not refresh
   away from its original generation.

[The HTTP test](http-tests.log) passed in 18.15 seconds.
[Clippy](clippy.log) passed for the integration-test target with warnings denied.
Formatting and diff checks passed. Test directories and owned listeners are
removed on completion.

This proves the local HTTP/CLI recovery path, including sole-replica service after
rebuild. It is not a deployed Linux recovery campaign, network file-transfer test,
load benchmark or hardware qualification. Automatic remote orchestration,
lost-journal recovery and sources unavailable on every peer remain outstanding.
