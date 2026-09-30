# Activity metadata implementation evidence

This directory retains observations from the v3/v11 activity metadata implementation
starting from wallet-pir `22fe04c5f5e6f781dbb697bea24850f2632907b5` and
zakura-core/wallet-libraries `908913c7c8ed69ee1a35cc001172a0800d236d3a`.

[Storage observation](storage.json) records the new 250 GiB coordinator volume and
unchanged live service state. The live node uses database format 29; the previous
reader used format 28. The transparent publisher dependency pin must therefore
track the observed running node before secondary ingestion.

Initial iteration found and corrected the obsolete maximum-entry-size assertion,
a missing schema destructuring field and a missing store codec dependency. These
failed checks do not establish passing validation. Final check outputs and exact
candidate identities are retained separately when available.

The frozen [e47bdf79 prototype](prototype-e47bdf79/publication.json) publishes
1,000 real blocks (3499739–3500738), 26,868 events, and both selected geometries.
The [independent RPC oracle](prototype-e47bdf79/oracle-all.json) matched every
block's events and transaction metadata. [Artifact verification](prototype-e47bdf79/verify.json)
reproduced both shards' filters, directories and pages exactly.

The [5 QPS gate](prototype-e47bdf79/query-5-result.json) completed 596 exact
queries in 120 seconds; the [20 QPS gate](prototype-e47bdf79/query-20-result.json)
completed 11,965 in 600 seconds across four processes. Both had zero failed
attempts, logical failures and missed slots. These are loopback HTTP candidate
measurements on the coordinator with frozen revisions, including the frozen tail;
they do not qualify canonical HTTPS or whole-wallet sustained capacity.

The first [SQLite run](prototype-e47bdf79/wallet-one.json) retained six failures
caused by a missing store directory. Its [corrected run](prototype-e47bdf79/wallet-one-retry.json)
completed 17 exact recoveries with no failures. That tool deleted each completed
database. The [retained 1/4/8-concurrency run](prototype-e47bdf79/wallet-retained-48d08a73.json)
completed 61 recoveries without failures. [Reopen comparison](prototype-e47bdf79/reopen-check-03c3c754.json)
independently decoded every retained event, matched fixture digests, and recomputed
transaction metadata, account movement, unresolved inputs and aggregate payments.
Older receives outside the bounded publication remain unresolved and partial.
No production cutover or sustained qualification is claimed.
