"""Restrict the pinned workload sample to what the recent journal slice covers.

Keeps every client whose required range starts at or after the slice start,
unchanged (scripts, required_from, expected_digest), and declares the slice's
start height so the load tool accepts the slice-published set.
"""
import json
import sys

START = 3262749
source, target = sys.argv[1:3]
sample = json.load(open(source))
sample["clients"] = [c for c in sample["clients"] if c["required_from"] >= START]
kept = sorted({c["class"] for c in sample["clients"]})
sample["start_height"] = START
if isinstance(sample.get("classes"), list):
    sample["classes"] = [c for c in sample["classes"] if (c if isinstance(c, str) else c.get("name")) in kept]
if isinstance(sample.get("per_class"), dict):
    sample["per_class"] = {k: v for k, v in sample["per_class"].items() if k in kept}
json.dump(sample, open(target, "w"))
print(len(sample["clients"]), "clients in", kept)
