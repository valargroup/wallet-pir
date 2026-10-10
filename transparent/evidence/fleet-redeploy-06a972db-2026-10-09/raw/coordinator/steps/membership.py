import json, time
m = json.load(open("/opt/transparent-publisher/v11/state/membership.json"))
print("membership age", round(time.time()-m["updated_unix"],1), "routed_recent", m["routed_recent"],
      {k:(v["state"],v.get("rendered"),v["warm"],v["map_sha256"][:12]) for k,v in m["members"].items()})
