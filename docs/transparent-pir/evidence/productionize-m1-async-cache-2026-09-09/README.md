# Optional snapshot persistence qualification — 2026-09-09

Verified runtimes become servable before the optional snapshot writer finishes.
The writer retains its runtime reference, cache pin and memory reservation until
persistence completes. The benchmark drains all writers before initial readiness
and after clients finish, and samples memory through the final drain.

The existing Amsterdam generator used the chunked-split backend, four worker CPUs
and separate client CPUs. This is a screening experiment, not live acceptance.

| Build slots | Three repeated visibility times | Retries | Combined screen |
|---|---|---|---|
| 1 | 12.444, 12.420, 12.416 s | 0, 0, 0 | All pass |
| 2 | 8.876, 8.916, 7.858 s | 1, 6, 5 | Only first passes |

The second two-slot run misses the reference retry fraction (8.0% versus 7.79%).
The third also has a 2.179 s maximum completion gap versus the 2.089 s reference.
No threshold was relaxed. The focused two-slot run likewise narrowly misses the
retry screen. See `comparison.json` and run `python3 summarize.py` to reproduce.

The test executable SHA-256 is
`599fbd27e2f06966493a2dd0f85370e5f175694739b499d080ba3753ba538bbc`.
Raw worker reports, complete external-client records and manifests are retained
under `focused/` and `qualification/`. This candidate was not deployed.
