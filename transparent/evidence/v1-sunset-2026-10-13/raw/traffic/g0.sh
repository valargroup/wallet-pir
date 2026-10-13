snap() { ssh -o BatchMode=yes root@167.99.42.60 'ssh -o BatchMode=yes -o ConnectTimeout=10 -o UserKnownHostsFile=/opt/transparent-publisher/credentials/known_hosts -i /opt/transparent-publisher/credentials/deploy-ssh root@10.142.0.11 "date -u +%s; for h in 10.142.0.10 10.142.0.8 10.142.0.7; do curl -s -m5 http://\$h:8093/metrics | grep ^pir_http_arrivals_total | sed \"s/^/\$h /\"; done"' </dev/null; }
snap > $1/g0-start.txt; sleep 1800; snap > $1/g0-end.txt
T0=$(head -1 $1/g0-start.txt); T1=$(head -1 $1/g0-end.txt)
ssh -o BatchMode=yes root@167.99.42.60 "python3 - $T0 $T1" </dev/null > $1/g0-load.txt <<'PY'
import gzip,json,glob,sys,collections
t0,t1=map(float,sys.argv[1:]);c=collections.Counter()
for f in sorted(glob.glob('/srv/transparent-activity/canonical-load/v11/queries-*.jsonl.gz'))[-3:]:
  for line in gzip.open(f,'rt'):
    try:v=json.loads(line)
    except: continue
    u=v.get('unix') or 0
    if t0<=u<=t1:c[str(v.get('event'))+':'+str(v.get('table'))]+=1
print(dict(c))
PY
paste -d' ' <(tail -n +2 $1/g0-start.txt) <(tail -n +2 $1/g0-end.txt) | awk '{print $1,$2,$6-$3}' > $1/g0-delta.txt
