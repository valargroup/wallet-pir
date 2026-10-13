import json, urllib.request
out = open("inputs/served-setup.tsv", "w"); ok = err = 0
for line in open("inputs/plan.tsv"):
    f = line.rstrip("\n").split("\t"); sid, tier, dig = f[0], f[1], f[2]
    kind = "archive" if tier == "sealed" else "recent"
    url = f"http://10.142.0.18:8095/v2/txid/{kind}/shards/{sid}/revisions/{dig}/setup/directory-0/0"
    try:
        j = json.load(urllib.request.urlopen(url, timeout=60))
        out.write("\t".join([sid, dig, j["public_params_sha256"], j["geometry"], j["public_params_epoch"]]) + "\n"); ok += 1
    except Exception as e:
        out.write(f"{sid}\t{dig}\tERROR {e}\n"); err += 1
print("ok", ok, "err", err)
