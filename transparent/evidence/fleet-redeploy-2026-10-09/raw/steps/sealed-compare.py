import json, sys
a, b = (json.load(open(p)) for p in sys.argv[1:3])
ident = lambda m: {k: v for k, v in m.items() if k != "shards"}
sa = [s for s in a["shards"] if s["sealed"]]; sb = [s for s in b["shards"] if s["sealed"]]
assert ident(a) == ident(b), "set identity changed"
assert sb[:len(sa)] == sa, "a sealed entry changed"
assert "recuts" not in b, "unexpected declaration"
print("sealed prefix identical:", len(sa), "sealed; tail", b["shards"][-1]["shard_id"], b["shards"][-1]["end_height"])
