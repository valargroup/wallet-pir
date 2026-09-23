# Actual coordinator crash after durable publication

The canonical CLI test now exercises a second actual process-crash boundary.
After the preceding publication's commit outbox drains, a new same-height reorg
is requested and worker commit requests are held. The test requires two durable
pending commit notifications and verifies that the served manifest exactly
matches the journal's published manifest before killing the coordinator process.

Restart must recover that same manifest, increase the fencing epoch, and drain
both acknowledgements without increasing the attempt counter. Exact current and
old-generation PIR queries must still match the frozen record oracles. Pending
commit delivery is tested separately from the existing prepare-phase crash, which
must instead advance the attempt counter when rebuilding an uncommitted candidate.

The initial version passed in 50.46 seconds. The final version, with an explicit
preceding-outbox drain before arming the barrier, passed in 50.04 seconds. Test
Clippy passed with warnings denied. No
production code or services changed. Full CI already selects this integration
suite; remote CI has not been run.

These are local subprocess/HTTP tests with synthetic block envelopes, not a
physical host crash, storage fault, live-chain inclusion or hardware qualification.
The default retained client still uses generation 1 after generation 3 recovers.
