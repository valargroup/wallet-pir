"""Six-hour joint Enhance + Status production soak with alert sampling.

Adapted from /root/prod-soak-20260927.py: Status queries go to the router query
tunnel (8492), and each batch records p50/p99 latency gates for both products.
"""
import datetime
import json
import pathlib
import subprocess
import time
import urllib.request

root = pathlib.Path('/root/prod-soak-20260927g')
root.mkdir(exist_ok=False)
sha = 'c5a6486476d91c88b692838c3449f4ddb917201c'
STATUS_BIN = '/opt/wallet-pir/releases/' + sha + '/native/status-pir'
ENHANCE_BIN = '/root/full-stack-stage/cpu/enhance-pir-load-test'
BATCHES = 36
BATCH_S = 600
QPS = 20
P99_MS = 2000
P50_MS = 700
urls = {
    'apm': 'https://enhance-pir.valargroup.dev/apm/monitor-status',
    'monitor': 'https://monitor-pir.valargroup.dev/monitor-status',
}


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def keys(d, field):
    out = []
    for i in d.get('incidents', []):
        on = i.get('active') if field == 'active' else i['condition'].get('firing')
        if on:
            out.append(i['condition']['key'] + ':' + i['condition'].get('resource', ''))
    return out


def sample(batch):
    row = {'at': stamp(), 'batch': batch}
    for n, u in urls.items():
        try:
            with urllib.request.urlopen(u, timeout=8) as r:
                row[n] = json.load(r)
        except Exception as e:
            row[n] = {'unavailable': type(e).__name__}
    with (root / 'samples.jsonl').open('a') as f:
        f.write(json.dumps(row) + '\n')
    pub = row['apm'].get('publication') or {}
    ch = row['apm'].get('chain') or {}
    summary = {'at': row['at'], 'batch': batch,
               'behind': (ch.get('height') or 0) - (pub.get('anchor_height') or 0) if pub.get('anchor_height') else None}
    for n in urls:
        d = row[n]
        if 'unavailable' in d:
            summary[n] = {'unavailable': d['unavailable']}
        else:
            summary[n] = {'progress_ok': d.get('progress_ok'), 'canary_ok': d.get('canary_ok'), 'shadow': d.get('shadow'),
                          'active': keys(d, 'active'), 'firing': keys(d, 'firing'),
                          'pending': (d.get('delivery') or {}).get('pending')}
    print(json.dumps(summary), flush=True)
    return row


def status_p50(b):
    rows = [json.loads(l) for l in (root / f'status-{b:02}/requests.jsonl').read_text().splitlines() if l.endswith('}')]
    d = sorted(r.get('duration_ms') if r.get('duration_ms') is not None else 1e9 for r in rows)
    return d[len(d) // 2] if d else None


started = time.time()
base = sample(-1)
assert base['monitor'].get('canary_ok') is True, 'external canary not healthy at start'
(root / 'manifest.json').write_text(json.dumps({
    'commit': sha, 'started_at': stamp(), 'batches': BATCHES, 'batch_seconds': BATCH_S,
    'enhance_qps': QPS, 'status_qps': 20, 'alert_mode': 'active',
    'status_query_origin': 'http://127.0.0.1:8492 (router query tunnel to status-pir-01)',
    'status_window_blocks': 4096,
    'latency_gate': {'p99_ms': P99_MS, 'p50_ms': P50_MS},
    'abort_on': ['two consecutive external canary failures', 'incorrect answer'],
    'record_only': ['HTTP errors', 'unstarted arrivals', 'latency gate'],
}, indent=2) + '\n')
stop = None
procs = []
canaries = {}
try:
    for b in range(BATCHES):
        lg = [(root / f'enhance-{b:02}.log').open('w'), (root / f'status-{b:02}.log').open('w')]
        e = subprocess.Popen([ENHANCE_BIN, '--server', 'https://enhance-pir.valargroup.dev',
                              '--oracle', '/root/architecture2-deploy/public-oracle.json',
                              '--rate', str(QPS), '--parallelism', '10', '--duration', f'{BATCH_S}s',
                              '--warmup', '0s', '--max-error-rate', '0.001',
                              '--json-out', str(root / f'enhance-{b:02}.json')],
                             stdout=lg[0], stderr=subprocess.STDOUT)
        s = subprocess.Popen([STATUS_BIN, 'probe-live-load', '--origin', 'http://127.0.0.1:8480',
                              '--query-origin', 'http://127.0.0.1:8492',
                              '--cookie', '/root/.cache/zakura/.cookie', '--seconds', str(BATCH_S),
                              '--report-dir', str(root / f'status-{b:02}')],
                             stdout=lg[1], stderr=subprocess.STDOUT)
        procs = [e, s]
        while e.poll() is None or s.poll() is None:
            time.sleep(30)
            row = sample(b)
            # Record every failed canary; stop only when two consecutive canaries fail.
            at = row['monitor'].get('canary_at', 0)
            if at >= started and at not in canaries:
                canaries[at] = row['monitor'].get('canary_ok')
                if canaries[at] is False:
                    with (root / 'canary-failures.jsonl').open('a') as f:
                        f.write(json.dumps({'canary_at': at, 'failure': row['monitor'].get('failure_category')}) + '\n')
                    print('CANARY_FAILED ' + json.dumps({'canary_at': at, 'failure': row['monitor'].get('failure_category')}), flush=True)
                recent = [canaries[k] for k in sorted(canaries)[-2:]]
                if len(recent) == 2 and all(v is False for v in recent):
                    stop = 'external_canary_failed_twice'
                    break
        if stop:
            break
        rep = json.loads((root / f'enhance-{b:02}.json').read_text())
        summ = [json.loads(l) for l in (root / f'status-{b:02}/summary.jsonl').read_text().splitlines() if l.strip()][-1]
        sp50 = status_p50(b)
        gate = ((rep.get('p99_ms') or 0) < P99_MS and (rep.get('p50_ms') or 0) < P50_MS
                and (summ.get('p99_ms') or 0) < P99_MS and (sp50 or 0) < P50_MS)
        line = {'batch': b, 'ended': stamp(),
                'enhance': {k: rep.get(k) for k in ('completed', 'succeeded', 'incorrect_answers', 'errors',
                                                     'unstarted_arrivals', 'p50_ms', 'p99_ms')},
                'status': {**{k: summ.get(k) for k in ('offered', 'correct', 'failed', 'unstarted', 'p99_ms')},
                           'p50_ms': sp50},
                'latency_gate': gate, 'exit': [e.returncode, s.returncode]}
        # Record transport/admission errors and keep going; stop only on wrong answers.
        rows = [json.loads(l) for l in (root / f'status-{b:02}/requests.jsonl').read_text().splitlines() if l.endswith('}')]
        wrong = sum(1 for r in rows if r['result'] not in ('correct', 'error', 'unstarted') or 'mismatch' in (r.get('error') or ''))
        line['status']['non_error_mismatches'] = wrong
        with (root / 'batches.jsonl').open('a') as f:
            f.write(json.dumps(line) + '\n')
        print('BATCH ' + json.dumps(line), flush=True)
        if rep.get('incorrect_answers') or wrong:
            stop = f'batch_{b}_failed'
            break
finally:
    for p in procs:
        if p.poll() is None:
            p.terminate()
            p.wait()
    res = {'ended_at': stamp(), 'stop_reason': stop}
    (root / 'result.json').write_text(json.dumps(res) + '\n')
    print('RESULT ' + json.dumps(res), flush=True)
