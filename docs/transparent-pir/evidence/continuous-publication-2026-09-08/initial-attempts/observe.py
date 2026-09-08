import base64, datetime, hashlib, json, pathlib, time, urllib.request, threading, math, statistics
cookie=pathlib.Path('/root/.cache/zakura/.cookie').read_text().strip()
auth='Basic '+base64.b64encode(cookie.encode()).decode()
def get(url,data=None):
    req=urllib.request.Request(url,data,{'Authorization':auth,'Content-Type':'application/json'} if data else {})
    with urllib.request.urlopen(req,timeout=8) as r: return r.read()
def rpc(method,params=[]):
    return json.loads(get('http://127.0.0.1:8232',json.dumps(dict(jsonrpc='2.0',id=1,method=method,params=params)).encode()))['result']
output_lock=threading.Lock()
def emit(**kw):
    with output_lock:
        print(json.dumps(dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),**kw)),flush=True)
stop=threading.Event()
def sample_metrics():
    while not stop.is_set():
        workers={}
        for last in [10,8,7,12,6,9]:
            try:
                raw=get(f'http://10.142.0.{last}:8093/metrics').decode()
                workers[str(last)]=[line for line in raw.splitlines() if not line.startswith('#') and any(name in line for name in ['rss_bytes','resident_bytes','builds_total','build_failures_total','restore','overload'])]
            except Exception as error:
                workers[str(last)]={'error':str(error)}
        emit(event='worker_metrics',workers=workers)
        stop.wait(30)
first=rpc('getblockcount'); seen={}; finished={}; last_digest=None; began=time.monotonic(); last_heartbeat=0
emit(event='start',node_height=first,target_blocks=20)
threading.Thread(target=sample_metrics,daemon=True).start()
while len(finished)<20 and time.monotonic()-began<3600:
    try:
        height=rpc('getblockcount'); now=time.monotonic()
        for h in range(first+1,height+1):
            if h not in seen: seen[h]=now; emit(event='node_observed',height=h)
        status=json.loads(get('http://127.0.0.1:8094/v1/status'))
        # Public fetches, including both TLS origins, determine visibility.
        if any(h not in finished for h in seen):
            a=get('https://transparent-pir.valargroup.dev/v1/shards')
            b=get('https://enhance-pir.valargroup.dev/v1/filters/shards')
            if a==b:
                m=json.loads(a); tail=m['shards'][-1]; digest=hashlib.sha256(a).hexdigest()
                if rpc('getblockhash',[tail['end_height']])!=tail['terminal_block_hash']:
                    emit(event='orphan_advertised',tail=tail); raise RuntimeError('public endpoint is not canonical')
                for h in seen:
                    if h<=tail['end_height'] and h not in finished:
                        latency=time.monotonic()-seen[h]; finished[h]=latency
                        emit(event='block_visible',height=h,seconds=latency,tail=tail,map_sha256=digest,status=status)
                last_digest=digest
            else: emit(event='origins_differ_during_sample')
        if now-last_heartbeat>=30:
            emit(event='heartbeat',node_height=height,completed=len(finished),status=status); last_heartbeat=now
    except Exception as e:
        emit(event='error',error=str(e))
    time.sleep(1)
values=sorted(finished.values())
emit(event='result',blocks=len(values),passed=len(values)>=20 and max(values)<=30 if values else False,
     p50=statistics.median(values) if values else None,p95=values[max(0,math.ceil(len(values)*.95)-1)] if values else None,
     maximum=max(values) if values else None,duration=time.monotonic()-began)

stop.set()
