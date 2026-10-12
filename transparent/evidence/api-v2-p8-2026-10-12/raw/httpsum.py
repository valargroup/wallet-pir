import json, glob, collections, sys
print("%-28s %5s %7s %9s %7s %9s %8s %10s %6s %6s" % ("case","reqs","setup_n","setup_KB","query_n","upload_KB","resp_KB","all_down_KB","p50_s","max_s"))
stages=set()
for f in sorted(glob.glob("case-*.http.ndjson")):
    rows=[json.loads(l) for l in open(f)]
    st=collections.defaultdict(lambda:[0,0,0,[]])
    for r in rows:
        s=r["stage"]; stages.add(s)
        k="setup" if "setup" in s else ("query" if "query" in s else s)
        st[k][0]+=1; st[k][1]+=r["submitted_payload_bytes"]; st[k][2]+=r["completed_body_bytes"]; st[k][3].append(r["seconds"])
    q=st["query"]; se=st["setup"]; secs=sorted(q[3]) or [0]
    tot=sum(v[2] for v in st.values())
    print("%-28s %5d %7d %9.1f %7d %9.1f %8.1f %10.1f %6.2f %6.2f" % (f[5:-11],len(rows),se[0],se[2]/1e3,q[0],q[1]/1e3,q[2]/1e3,tot/1e3,secs[len(secs)//2],secs[-1]))
print(sorted(stages))
