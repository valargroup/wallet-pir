#!/usr/bin/env python3
"""Illustrative independent table placement, with explicit workload assumptions.

No row is a measured full-chain population. No CPU/latency qualification follows
from byte scans or reservation upper bounds.
"""
import json
import math
from pathlib import Path

ROW = 4096
FIXED = 14848 + 16 + 32 + 2 * 2048 * 8 + 8 + 2048 * 2048 * 2 * 8


def reservation(rows, segments):
    return segments * (rows * ROW + FIXED)


def scenario(population, coverage, mean_inline_payload, mean_overflow_payload,
             lookup_buckets, overflow_buckets, lookup_rows, overflow_rows,
             density=.75, mean_overflow_requests=3, cover=0):
    # Envelope/framing 48 bytes implemented, including entry length.
    directory_bytes = population * (48 + coverage * mean_inline_payload)
    # 42-byte fragment+length overhead; illustrative mean 3 fragments.
    overflow_bytes = population * (1 - coverage) * (mean_overflow_payload + 42 * mean_overflow_requests)
    ds = max(1, math.ceil(directory_bytes / lookup_buckets / density / (lookup_rows * ROW)))
    ps = max(1, math.ceil(overflow_bytes / overflow_buckets / density / (overflow_rows * ROW)))
    requests = 2 + (max(cover, mean_overflow_requests) * (1-coverage) + cover * coverage)
    evals = 2 * ds + (requests - 2) * ps
    return dict(population=population,coverage=coverage,mean_inline_payload=mean_inline_payload,
        mean_overflow_payload=mean_overflow_payload,lookup_buckets=lookup_buckets,overflow_buckets=overflow_buckets,
        lookup_rows=lookup_rows,overflow_rows=overflow_rows,target_density=density,
        mean_overflow_requests=mean_overflow_requests,cover=cover,
        projected_directory_occupied_bytes=directory_bytes,projected_overflow_occupied_bytes=overflow_bytes,
        directory_segments_per_bucket=ds,overflow_segments_per_bucket=ps,
        native_reservation_bytes=lookup_buckets*reservation(lookup_rows,ds)+overflow_buckets*reservation(overflow_rows,ps),
        encrypted_requests_per_tx=requests,segment_evaluations_per_tx=evals,
        scan_bytes_proxy_per_tx=2*ds*lookup_rows*ROW+(requests-2)*ps*overflow_rows*ROW,
        average_lookup_population=population/lookup_buckets,
        average_overflow_intersection_population=population*(1-coverage)/lookup_buckets/overflow_buckets,
        # Segment counts/refresh/timing can lower these averages arbitrarily.
        qualified_minimum_population=None,qualified_latency_seconds=None)


def report():
    return {"schema":"txid-sizing-geometry-projections-v1","qualification":"UNQUALIFIED",
        "assumptions":{"population":17000000,"population_note":"illustrative scale only; history event-bearing count is not display census",
        "transaction_workload":"uniform distinct txid opens; no cache hits, no setup downloads, no retries",
        "mean_inline_payload":80,"mean_overflow_payload":8500,"mean_overflow_requests":3,
        "density":.75,"bucket_load":"balanced means; hash concentration/tail not qualified",
        "query_cost":"full encoded-database scan bytes proxy, not CPU/latency; segment responses 5632+16 bytes each"},
        "scenarios":[scenario(17000000,c,80,8500,l,o,dr,pr,cover=cover)
        for c in (.85,.90,.95,.99) for l in (1,4,16,64) for o in (1,4,16)
        for dr,pr in ((4096,4096),(8192,32768),(32768,65536)) for cover in (0,3)]}


if __name__ == "__main__":
    import argparse
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument("output",type=Path)
    args=parser.parse_args(); args.output.write_text(json.dumps(report(),sort_keys=True,separators=(",",":"))+"\n")
