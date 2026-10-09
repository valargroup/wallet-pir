import json, os, shutil, sys
p = "/srv/transparent-activity/canonical-load/v11/pins.json"; new, *ids = sys.argv[1:]
d = json.load(open(p)); d.update({i: new for i in ids})
t = p + ".tmp"; open(t, "w").write(json.dumps(d, indent=2) + "\n"); shutil.copymode(p, t); os.replace(t, p); print(d)
