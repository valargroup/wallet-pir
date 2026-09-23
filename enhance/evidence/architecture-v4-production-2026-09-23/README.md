# Direct production deployment and validation

The user explicitly authorized direct SSH deployment on existing production
hosts, stopping legacy Enhance services, bypassing CI where possible, and no
new hosts. This supersedes the earlier isolated-host deployment gates; it does
not establish hardware qualification.

The clean candidate `9718a6dcf9801385f69f31bb71f02efd261914f0` was staged and
checksum-verified on the coordinator and both c-4 production workers. Legacy
binaries, configuration and data remain available for rollback. Legacy Enhance
services, autoscaling and APM are disabled. V4 uses worker private port 8091 and
the existing Caddy origin on coordinator port 8080.

The initial journal copy was rejected because production used 29 records per row.
The explicit offline migration preserved all 565,944 records and block entries,
changing packing metadata to 33 records per row. Both records files had SHA-256
`5ab51089db27257ed24175a7e117889456b6ca55f95e40151b5b9b7370863a8f`.
V4 then reconciled live RPC data and published canonical generations on both peers.

Four native load cases passed with 1,870 correct measured answers and no wrong
answers/request errors: public HTTPS, private concurrency 1 and 2, and open-loop
2 QPS. The public test ran from the coordinator through the public HTTPS origin;
a separate external IPv4 health request also succeeded. Oracle records came from
the preserved legacy canonical journal, independent of v4 evaluation but not an
independent chain extractor. Reports retain all latency and warmup counts.

One-replica-offline and coordinator-restart checks added 329 correct measured
answers with zero errors. Independent worker sampling recorded no swap and no
collection errors at the captured points. These short checks are not full-size
hardware qualification.

A six-hour active-profile campaign is now running on the production c-4 pair,
with one-second host/cgroup/process/runtime samples. The public canonical service
is temporarily stopped; synthetic fixtures listen only on loopback port 8280.
Canonical worker directories are preserved as `worker.canonical` for restoration.
See `active-campaign.json` for the live systemd unit and data paths. No passing
sustained-test claim is made here. Sealed-role testing and final restoration remain.
