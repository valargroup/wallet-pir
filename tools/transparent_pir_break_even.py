"""Where private transparent-history retrieval stops beating ordinary download.

The measured workloads bracket the answer rather than give it: a two-event
address costs 0.10x ordinary retrieval and a 9,152-event address costs 1.42x,
and the crossover is between them. This reconstructs the cost of an arbitrary
sync from the same per-unit constants the client charges, then checks that
reconstruction against the preserved runs before reporting anything from it.

The check is the point. A cost model that does not reproduce the measurements
it claims to extend is a source of confident wrong numbers, so `--check` fails
rather than prints if it drifts.

Per-table constants are asserted in `pir/transparent-history/src/client.rs`
(`the_tables_differ_only_in_what_a_published_set_costs`), so a geometry change
breaks that test rather than silently invalidating this file.
"""

import argparse
import json
import math
from pathlib import Path

#: Bytes per public matrix set, packing keys, selector and response body.
DIRECTORY = {"set": 14_336, "key": 86_016, "selector": 20_480, "response": 5_120}
PAGES = {"set": 71_680, "key": 86_016, "selector": 20_480, "response": 25_600}

#: Wire framing, matching the constants in the client.
BATCH_HEADER, QUERY_HEADER, RESPONSE_HEADER = 13, 1, 17

#: Public matrix sets a table publishes.
PUBLIC_SETS = 4

#: Generation geometry: events carried in the directory row, then per page.
INLINE_EVENTS, EVENTS_PER_PAGE = 2, 186

#: Ordinary retrieval and BIP 158 filter delivery at three measured intervals,
#: from docs/transparent_pir_http.md. Not linear in blocks, so nothing here
#: extrapolates outside the outermost pair.
INTERVALS = [(12, 9_492, 1_077), (288, 446_142, 28_655), (1_152, 2_888_097, 138_716)]

BLOCK_SECONDS = 75


def plan_bytes(table, queries, sets):
    """Total bytes for `queries` selections sent in batches of `sets`."""
    batches = math.ceil(queries / sets)
    padded = batches * sets
    return (
        sets * table["set"]
        + batches * (BATCH_HEADER + table["key"] + RESPONSE_HEADER)
        + padded * (QUERY_HEADER + table["selector"] + table["response"])
    )


def best_plan(table, queries):
    """The plan the client's policy would choose: cheapest, ties to smaller."""
    if queries == 0:
        return 0, 0
    return min((plan_bytes(table, queries, s), s) for s in range(1, PUBLIC_SETS + 1))


def pages_for(events):
    if events <= INLINE_EVENTS:
        return 0
    return math.ceil((events - INLINE_EVENTS) / EVENTS_PER_PAGE)


def sync_bytes(events, public_bytes):
    """One address, one sync: filters and manifest, one index lookup, its pages."""
    return (
        public_bytes
        + best_plan(DIRECTORY, 1)[0]
        + best_plan(PAGES, pages_for(events))[0]
    )


def validate(results):
    """Reproduce the single-address runs, or refuse to report anything."""
    runs = {r["workload"]: r for r in results["runs"]}
    public = runs["unused_100"]["public_download_bytes"]
    failures = []
    for workload in ("sparse_1", "large_1"):
        run = runs[workload]
        modelled = sync_bytes(run["verified_events"], public)
        if modelled != run["total_bytes"]:
            failures.append(
                f"{workload}: model {modelled} != measured {run['total_bytes']}"
            )
    return public, failures


def report(public, ordinary):
    print(f"public bytes per sync (filters and manifest): {public}")
    print(f"ordinary retrieval for the same interval:     {ordinary}\n")

    print("-- history size: where one address stops being worth retrieving privately --")
    previous = None
    for events in range(1, 20_000):
        total = sync_bytes(events, public)
        if total > ordinary:
            print(
                f"   last win  {previous[0]:>6} events, {pages_for(previous[0]):>3} pages: "
                f"{previous[1]:>9} ({previous[1] / ordinary:.2f}x)"
            )
            print(
                f"   crossover {events:>6} events, {pages_for(events):>3} pages: "
                f"{total:>9} ({total / ordinary:.2f}x)"
            )
            break
        previous = (events, total)

    print("\n-- sync frequency: what each measured interval's budget buys --")
    index_only = best_plan(DIRECTORY, 1)[0]
    for blocks, retrieval, filters in INTERVALS:
        budget = retrieval - filters
        lookups = max(
            (n for n in range(0, 200) if best_plan(DIRECTORY, n)[0] <= budget), default=0
        )
        pages = max(
            (n for n in range(0, 200) if index_only + best_plan(PAGES, n)[0] <= budget),
            default=None,
        )
        hours = blocks * BLOCK_SECONDS / 3600
        print(
            f"   {blocks:>5} blocks ({hours:>4.1f} h): budget {budget:>9} -> "
            f"{lookups:>3} index lookups, or 1 index + "
            f"{'no' if pages is None else pages} pages"
        )

    print("\n-- shortest interval that affords one lookup --")
    # Interpolated between bracketing measured points only; the three intervals
    # are far apart and retrieval is not linear in blocks, so these are
    # approximate and cannot be quoted as thresholds.
    for label, cost in (
        ("index lookup only", index_only),
        ("index lookup plus one page", index_only + best_plan(PAGES, 1)[0]),
    ):
        for (b0, r0, f0), (b1, r1, f1) in zip(INTERVALS, INTERVALS[1:]):
            low, high = r0 - f0, r1 - f1
            if low <= cost <= high:
                blocks = b0 + (cost - low) * (b1 - b0) / (high - low)
                print(
                    f"   {label:<28} {cost:>9} B -> ~{blocks:>4.0f} blocks "
                    f"(~{blocks * BLOCK_SECONDS / 3600:.1f} h), interpolated "
                    f"between {b0} and {b1}"
                )
                break
        else:
            print(f"   {label:<28} {cost:>9} B -> outside the measured intervals")

    print("\n-- marginal cost of a page lookup, as sharing saturates --")
    floor = sync_bytes(INLINE_EVENTS, public)
    for pages in (1, 2, 4, 8, 16, 32, 50):
        total = public + best_plan(DIRECTORY, 1)[0] + best_plan(PAGES, pages)[0]
        print(
            f"   {pages:>3} pages (batch of {best_plan(PAGES, pages)[1]}): "
            f"{total:>9} total, {(total - floor) / pages:>8.0f} per page"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results",
        type=Path,
        default=Path("docs/transparent-pir-evaluation/per-table-reuse/policy.json"),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the model reproduces the preserved runs and print nothing else",
    )
    args = parser.parse_args()

    results = json.loads(args.results.read_text())
    public, failures = validate(results)
    if failures:
        raise SystemExit("cost model does not reproduce the measurements:\n  " + "\n  ".join(failures))
    if args.check:
        print("PASS: cost model reproduces the preserved single-address runs")
        return
    report(public, results["ordinary_retrieval_bytes_same_interval"])


if __name__ == "__main__":
    main()
