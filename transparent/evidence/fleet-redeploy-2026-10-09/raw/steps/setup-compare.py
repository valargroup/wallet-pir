import hashlib, json, sys, urllib.request
hosts = sys.argv[1:]
get = lambda u: urllib.request.urlopen(u, timeout=20).read()
m = json.loads(get("http://%s:8093/v1/shards" % hosts[0]))
bad = 0
for sh in m["shards"][-3:]:
    for table in ("directory", "pages"):
        d = {h: hashlib.sha256(get("http://%s:8093/v1/shards/%d/revisions/%s/setup/%s/0" % (h, sh["shard_id"], sh["manifest_digest"], table))).hexdigest() for h in hosts}
        same = len(set(d.values())) == 1; bad += not same
        print(sh["shard_id"], sh["sealed"], table, "same" if same else d)
sys.exit(1 if bad else 0)
