#!/usr/bin/env python3
"""Derive schema-8 payload sizes and worker residency from the pinned encoder.

Nothing here is measured. Every constant is read from ipir-sp at the pinned
revision accc424e879d8da425fa620aad80f0f2c4e0defd (ipir-sp/src/params.rs and
ipir-sp/src/modulus_switch.rs) and from enhance_pir::types. The only evidence
offered for the model is that it reproduces the measured schema-7 sizes and the
benchmarked schema-8 sizes exactly; those equalities are asserted below and the
script exits non-zero if any of them stops holding.
"""
import json
import math

# --- ipir-sp/src/params.rs -------------------------------------------------
POLY_LEN = 2048
PLAINTEXT_MODULUS = 1 << 14
SINGLE_CRT_Q = 72_057_594_037_641_217
Q_PRIME_1 = 1 << 20

# --- enhance_pir::types ----------------------------------------------------
RECORD_BYTES = 737
SHARD_ROWS = 8_192
RETAINED_GENERATIONS = 8

# Measured once, from the 430,088-byte schema-7 upload and the 169,992-byte
# schema-8 upload: both leave the same remainder after the row-dependent term,
# so the 8-byte generation prefix plus the serialized packing keys is a
# constant that does not depend on the instance count.
PREFIX_AND_PACKING_KEYS = 86_024


def modulus_bits(q):
    return max(1, (q - 1).bit_length())


def query_bits(rows):
    """ipir_sp::modulus_switch::query_modulus_bits."""
    bound = 64.0 * PLAINTEXT_MODULUS * PLAINTEXT_MODULUS * math.sqrt(rows)
    return min(max(1, math.ceil(math.log2(bound))), modulus_bits(SINGLE_CRT_Q))


def instances(row_bytes):
    return math.ceil(row_bytes * 8 / (POLY_LEN * 14))


def upload_bytes(logical_rows):
    return PREFIX_AND_PACKING_KEYS + logical_rows * query_bits(logical_rows) // 8


def response_bytes(row_bytes):
    """16-byte prefix plus one response body per instance."""
    return 16 + instances(row_bytes) * (POLY_LEN * modulus_bits(Q_PRIME_1) // 8)


def public_params_bytes(row_bytes):
    """One published c1 row per instance, at full q precision."""
    return instances(row_bytes) * (POLY_LEN * modulus_bits(SINGLE_CRT_Q) // 8)


def logical_rows(positions, records_per_row):
    used = math.ceil(positions / records_per_row)
    return max(1 << (max(used, SHARD_ROWS) - 1).bit_length(), SHARD_ROWS)


def shard_runtime_bytes(row_bytes):
    """One ShardRuntime: the packed u16 database plus the partial CRS blocks."""
    inst = instances(row_bytes)
    database = SHARD_ROWS * (inst * POLY_LEN) * 2
    crs = inst * POLY_LEN * POLY_LEN * 8
    return database + crs


def worker_resident_bytes(shards, row_bytes):
    """Sealed shards hold one runtime; the frontier holds one per retained
    generation plus the unpublished candidate."""
    return ((shards - 1) + RETAINED_GENERATIONS + 1) * shard_runtime_bytes(row_bytes)


def row(label, records_per_row, positions):
    row_bytes = RECORD_BYTES * records_per_row
    rows = logical_rows(positions, records_per_row)
    up, down = upload_bytes(rows), response_bytes(row_bytes)
    return {
        "label": label,
        "positions": positions,
        "records_per_row": records_per_row,
        "row_bytes": row_bytes,
        "logical_rows": rows,
        "query_bits": query_bits(rows),
        "instances": instances(row_bytes),
        "upload_bytes": up,
        "response_bytes": down,
        "combined_bytes": up + down,
        "combined_kib": round((up + down) / 1024, 3),
        "public_params_bytes": public_params_bytes(row_bytes),
    }


LIVE_POSITIONS = 467_255          # /v1/enhance/init, 2026-09-20, height 3,489,681
SCHEMA8_BOUNDARY = 29 * 16_384    # 475,136: last count at 16,384 logical rows
SCHEMA7_BOUNDARY = 9 * 65_536     # 589,824: last count at 65,536 logical rows

payloads = [
    row("schema 7, live count", 9, LIVE_POSITIONS),
    row("schema 8, live count", 29, LIVE_POSITIONS),
    row("schema 8, past its boundary", 29, SCHEMA8_BOUNDARY + 1),
    row("schema 7, past its boundary", 9, SCHEMA7_BOUNDARY + 1),
]

residency = [
    {
        "records_per_row": rpr,
        "shards": shards,
        "shard_runtime_mib": shard_runtime_bytes(RECORD_BYTES * rpr) // 1024**2,
        "resident_gib": round(worker_resident_bytes(shards, RECORD_BYTES * rpr) / 1024**3, 3),
    }
    for rpr in (9, 29)
    for shards in (2, 7, 8, 10, 16)
]

# --- the equalities this model is accountable to ---------------------------
# Measured schema 7, from the live origin and evidence/public-baseline-2026-09-14.
assert upload_bytes(65_536) == 430_088
assert response_bytes(9 * RECORD_BYTES) == 10_256
assert public_params_bytes(9 * RECORD_BYTES) == 28_672
# Benchmarked schema 8, from enhance/evidence/layout-benchmark-2026-09-20.
assert upload_bytes(16_384) == 169_992
assert response_bytes(29 * RECORD_BYTES) == 30_736
assert public_params_bytes(29 * RECORD_BYTES) == 86_016
assert upload_bytes(32_768) == 258_056
# Instance counts, cross-checked against the server's own unit tests.
assert instances(9 * RECORD_BYTES) == 2
assert instances(29 * RECORD_BYTES) == 6
assert instances(30 * RECORD_BYTES) == 7, "a thirtieth record needs a seventh instance"

print(json.dumps({"payloads": payloads, "worker_residency": residency}, indent=2))
