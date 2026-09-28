"""Poll Status role loopback endpoints every 100 ms from the droplet; log slow responses.

Distinguishes a frozen worker from a frozen router during query stalls.
"""
import concurrent.futures
import sys
import time
import urllib.request

TARGETS = {
    'worker_health': 'http://127.0.0.1:8481/control/health',
    'router_health': 'http://127.0.0.1:8482/control/health',
    'router_query_listener': 'http://127.0.0.1:8484/v1/status/query',
    # Telemetry does not take the role state lock: slow here means the process
    # or runtime froze, fast here while health is slow means lock contention.
    'worker_metrics_nolock': 'http://127.0.0.1:8481/internal/metrics',
    'router_metrics_nolock': 'http://127.0.0.1:8482/internal/metrics',
}


def probe(url):
    t = time.perf_counter()
    try:
        req = urllib.request.Request(url, data=b'' if url.endswith('/query') else None, method='POST' if url.endswith('/query') else 'GET')
        urllib.request.urlopen(req, timeout=5).read()
        status = 'ok'
    except urllib.error.HTTPError as e:
        status = str(e.code)
    except Exception as e:
        status = type(e).__name__
    return (time.perf_counter() - t) * 1000, status


end = time.time() + float(sys.argv[1])
pool = concurrent.futures.ThreadPoolExecutor(len(TARGETS))
while time.time() < end:
    started = time.time()
    futures = {n: pool.submit(probe, u) for n, u in TARGETS.items()}
    slow = {n: f.result() for n, f in futures.items()}
    slow = {n: v for n, v in slow.items() if v[0] > 150}
    if slow:
        stamp = time.strftime('%H:%M:%S', time.gmtime(started)) + f'.{int(started % 1 * 1000):03d}'
        print(stamp, ' '.join(f'{n}={ms:.0f}ms/{st}' for n, (ms, st) in slow.items()), flush=True)
    time.sleep(max(0, 0.1 - (time.time() - started)))
