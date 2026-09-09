# Parent-filter evaluation

This implements an opt-in experiment. The user subsequently approved [production artifact hosting](evidence/parent-filters-production-2026-09-08/README.md); the full paired performance benchmark remains incomplete. Recent catch-up bandwidth is the primary objective. The existing mixed-20 synthetic weights are secondary; these are not wallet population frequencies.

## Reproduce

Build `hierarchical-filter-eval` from `transparent-regression` and `transparent-loadtest` with the `release-fast` profile. The evaluator uses the regression crate's read-only committed-journal reader, without starting ingest or opening RocksDB. Use a fresh output directory for every extraction and sweep.

```sh
cargo build --profile release-fast -p transparent-regression --bin hierarchical-filter-eval
cargo build --profile release-fast -p transparent-loadtest
cargo build --profile release-fast -p transparent-shard-server --bin parent-evaluation-server

target/release-fast/hierarchical-filter-eval extract \
  --data-dir /path/to/journal --publication /path/to/frozen/publication \
  --sample /path/to/sample.json --out /path/to/evaluation/dataset

target/release-fast/hierarchical-filter-eval sweep \
  --dataset /path/to/evaluation/dataset --out /path/to/evaluation/sweep

python3 tools/transparent_parent_evaluate.py select /path/to/evaluation/sweep
```

Extraction verifies the sample's chain anchor, every shard endpoint, and every reconstructed child filter against both the map digest and published bytes. Complete filter element sets include scripts excluded from private directory rows. It preserves raw per-shard sets for exact cross-shard deduplication. A failed extraction does not write the complete provenance marker.

The sweep fixes groups at consecutive shard boundaries, never crosses geometries, and excludes the provisional tail. Recent group sizes are 1, 2, 4, 8 and 16; archive sizes additionally include 32, 50, 80 and 160. The parent M values are 100, 1,000 and 10,000. P minimizes expected geometric-gap code length independently of the workload (6, 9 and 13 respectively). Child filter parameters are unchanged. One-child parents isolate the precision effect from cross-shard deduplication.

Each parent is built from the raw union, strictly decoded, and checked against every mapped union entry. Each group is tested with 100,000 deterministic, exactly absent scripts. Per-wallet counters include metadata, parent bodies, child bodies, requests, true/false descents and skipped children. Separate stress rows cover 1/48-block windows, 100/1,000-script sets, and synthetic concentrated/dispersed archive selections. Those selections describe workload construction, not a guarantee about the observed activity distribution; inspect their actual descents.

The fixed sample split is SHA-256 ordered by original wallet index and seed 1, within each profile: first 80 tune, remaining 40 validate. Stress rows receive no product weight. `split.json` and `evaluation-workload.json` preserve exact identities. Candidate selection combines separate recent/archive manifests, charging discovery only to wallets needing uncached sealed children in that tier. The smaller-of-parent-or-needed-children decision uses public sizes and the recovery interval.

## HTTP prototype

The default wallet/filter behavior is unchanged. Explicit scenario configuration enables the experiment:

```json
{
  "experimental_parent_manifests": {
    "recent-8k": "http://127.0.0.1:18095/recent-8k-k2-m1000.json",
    "archive-wide": "http://127.0.0.1:18095/archive-wide-k8-m1000.json"
  }
}
```

These are example matrix entries, not selected production parameters. Serve the sweep directory from an isolated static HTTP origin; manifests refer to digest-addressed `artifacts/<hash>.bin` relative to their directory. Run `parent-evaluation-server --shard-dir /path/to/frozen/publication` on the frozen publication. This loopback-only server omits whole-set background prewarming; the driver runs a baseline warm-up for each seed before the measured variants. Bind experimental services to loopback and access them through SSH forwarding if needed.

```sh
python3 tools/transparent_parent_evaluate.py waves \
  --evaluation /path/to/evaluation/sweep \
  --scenario server/transparent-loadtest/scenarios/mixed-20-wave.json \
  --sample /path/to/sample.json \
  --shard-url http://127.0.0.1:18092 \
  --filter-url http://127.0.0.1:18092 \
  --parent-origin http://127.0.0.1:18095 \
  --out /path/to/evaluation/http

python3 tools/transparent_parent_evaluate.py summarize-http \
  /path/to/evaluation/http /path/to/evaluation/sweep
```

The runner uses held-out wallets only, seeds 1–10, and a saved shuffled variant order.

For repeated experiments, first run `python3 tools/transparent_parent_evaluate.py held-out /path/to/sample.json /path/to/evaluation/sweep /path/to/held-out-sample.json`. Then `hierarchical-filter-eval seeds --data-dir /path/to/journal --map /path/to/dataset/map.json --sample /path/to/held-out-sample.json --out /path/to/journal-seeds` exports prior ledgers in one journal pass. It checks every wallet's measured-window digest/count and runs the independent prior-ledger reducer. The driver generates the same held-out bytes. Pass `--journal-seeds /path/to/journal-seeds` to `waves`. The explicit `experimental_journal_seeds` scenario field requires a frozen publication. File hashes, map/sample/anchor identity, script scope and prior-ledger consistency are rechecked before import. Reports label this `journal-verified`, not an HTTP preparation recovery. Filters, setup caches and measured coverage remain cold. The complete manifest is written only after all wallets pass verification.

Each recovery has an independent cold client cache; preparation traffic is excluded and baseline preparation uses no parent experiment. Use identical server hardware, cache limits and warm-up procedures across variants, recording these and source/binary hashes beside results. A failed wave stops the run and preserves its report and logs. Do not treat missing waves as successful observations.

Parent bodies and manifests use the existing SQLite filter cache with a unique revision key for each artifact. Manifest cache identity also includes its URL and the current map digest. Before accepting a negative, the wallet checks the exact ordered child revisions, chain, geometry, interval and parameters; it strictly validates body length, digest, element count, encoding and padding. Missing/corrupt/stale parent evidence falls back to child retrieval. Existing bounded coverage commits preserve accepted-anchor clipping and script identity; new scripts are tested again. The provisional tail always uses direct retrieval.

## Decision gates and interpretation

Rank tuning configurations by equally weighted mean downloads for catch-up 1d, 7d and 30d. Within 1% of the best recent score, rank by the mixed-20 weighted mean, then requests. Freeze up to three finalists before inspecting held-out results. Direct download remains a candidate for either tier.

Require exact recovery and no increase in mean bytes for any primary recent profile. HTTP median latency for each must remain within 10% of baseline. Do not infer p99 from these samples. Adoption requires at least 5% recent savings, or recent performance within 1% and at least 10% mixed savings. Inspect per-profile and stress regressions even if aggregate gates pass. Retained-cache, revision, corruption, import, accepted-prefix and rollback checks are separate correctness gates. A latency result with unresolved noise needs another paired set before a recommendation; the tool does not automatically relabel it as passing.

Offline tables account for filter/manifest payloads, not socket/TLS bytes or complete PIR recovery cost. Parent negatives can suppress child false positives and thereby change private traffic, so final decisions use HTTP recovery totals. Parent union/build costs and false-positive counts are measured; general fleet capacity is not established by this experiment.

## Privacy and trust

Opting in accepts disclosure of probable activity in coarse chain intervals through selective child downloads. This differs from the default script-independent public filter fetch pattern. No script or private row locator is sent in plaintext. Digests bind publication identity and detect corruption; they do not prove completeness against an actively malicious indexer. A negative retains the existing trusted-indexer completeness assumption. See the [contract](contract.md).

## Recorded experiment

The [2026-09-08 evidence](evidence/parent-filters-2026-09-08/README.md) contains the complete full-journal offline sweep, frozen finalists, stress results and HTTP validation outcome.
