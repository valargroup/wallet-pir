#!/usr/bin/env python3
"""Read-only production sampler and hard-stop supervisor for the txid display load.

Every 15 s: history 5 QPS load status, history /v1/status, display controller
status and the latched.json check on the coordinator (read-only). Every 30 s:
recent-01 host CPU and unit CPU counters (read-only). Each healthy iteration
refreshes the bench permit; txid-rate pauses by itself when the permit is older
than 45 s. On any stop rule the permit is set to deny, every txid tool on the
bench is killed, TRIPPED is written and never cleared by this script.
"""
import json, os, subprocess, sys, time

OUT = sys.argv[1]
SSHP = sys.argv[2]
BENCH = 'roman-ipir-bench-8vcpu'
PERMIT = '/root/txid-measure-d191f86b/permit'
LOAD = '/srv/transparent-activity/canonical-load/v11'
SEP = '@@SEP@@'
COORD_CMD = (f"cat {LOAD}/status.json; echo {SEP}; curl -s -m 5 127.0.0.1:8094/v1/status; echo {SEP}; "
             f"curl -s -m 5 127.0.0.1:8099/v1/status; echo {SEP}; "
             f"if [ -e {LOAD}/latched.json ]; then echo LATCHED; else echo none; fi; echo {SEP}; date +%s.%N")
RECENT_CMD = ("head -1 /proc/stat; cat /proc/loadavg; "
              "systemctl show -p CPUUsageNSec --value transparent-txid-display-worker.service transparent-shard-server.service; date +%s.%N")


def run(cmd, timeout=25):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout
    except subprocess.TimeoutExpired:
        return 124, ''


def write(name, obj):
    with open(os.path.join(OUT, name), 'a') as f:
        f.write(json.dumps(obj, separators=(',', ':')) + '\n')


def tripped():
    return os.path.exists(os.path.join(OUT, 'TRIPPED'))


def trip(reasons, sample):
    with open(os.path.join(OUT, 'TRIPPED'), 'a') as f:
        f.write(json.dumps({'unix': time.time(), 'reasons': reasons, 'sample': sample}) + '\n')
    run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', '-o', 'ControlMaster=auto', '-o', 'ControlPersist=900', '-o', 'ControlPath=' + os.path.expanduser('~/.ssh/cm-txid/%C'), BENCH,
         f"echo deny > {PERMIT}; pkill -f 'txid-measure-d191f86b/target/release/examples/txid-' ; echo killed"], 20)
    write('events.jsonl', {'unix': time.time(), 'event': 'TRIPPED', 'reasons': reasons})
    print('TRIPPED', reasons, flush=True)


# Amended rule (2026-10-07 rerun): a history error stops the run only when unrecovered.
ERR = {'base': None, 'seen': {}, 'pending_since': None, 'start': time.time()}
QLOG_CMD = ("cd " + LOAD + "; for h in $(date -u -d '-90 sec' +%Y%m%dT%H) $(date -u +%Y%m%dT%H); do "
            "zcat queries-$h.jsonl.gz 2>/dev/null; done | grep -E '\"event\":\"error\"|\"recovered\":true' | sort -u")


def check_errors(count, now):
    if ERR['base'] is None:
        ERR['base'] = count
    unresolved = [e for e in ERR['seen'].values() if not e['recovered']]
    if count <= ERR['base'] and not unresolved:
        return []
    rc, out = run([SSHP, 'coordinator', QLOG_CMD], 40)
    for line in out.splitlines():
        try:
            r = json.loads(line)
        except ValueError:
            continue
        if r.get('unix', 0) < ERR['start'] - 5:
            continue
        lid = r.get('logical_id')
        if r.get('event') == 'error':
            if lid not in ERR['seen']:
                ERR['seen'][lid] = {'error': r, 'recovered': False, 'first_seen': now}
                write('history-errors.jsonl', {'unix': now, 'kind': 'error', 'record': r})
            ERR['seen'][lid]['retry_scheduled'] = r.get('retry_scheduled')
        elif r.get('recovered') and lid in ERR['seen'] and not ERR['seen'][lid]['recovered']:
            ERR['seen'][lid]['recovered'] = True
            write('history-errors.jsonl', {'unix': now, 'kind': 'recovered', 'record': r})
    reasons = []
    for lid, e in ERR['seen'].items():
        if e['recovered']:
            continue
        if e.get('retry_scheduled') is False:
            reasons.append('unrecovered history error %s (no retry scheduled)' % lid)
        elif now - e['first_seen'] > 30:
            reasons.append('unrecovered history error %s (no recovered retry within 30 s)' % lid)
    new = count - ERR['base']
    if new > len(ERR['seen']):
        ERR['pending_since'] = ERR['pending_since'] or now
        if now - ERR['pending_since'] > 60:
            reasons.append('history error count +%d but only %d error records found after 60 s' % (new, len(ERR['seen'])))
    else:
        ERR['pending_since'] = None
    return reasons


def main():
    base_failures = None
    fails = 0
    i = 0
    while True:
        t0 = time.time()
        rc, out = run([SSHP, 'coordinator', COORD_CMD])
        parts = out.split(SEP)
        sample = {'unix': t0, 'ok': False}
        reasons = []
        try:
            load = json.loads(parts[0]); hist = json.loads(parts[1]); ctl = json.loads(parts[2])
            latched = parts[3].strip(); remote_unix = float(parts[4].strip())
            t = load['trailing_60s']; lc = ctl.get('last_cycle') or {}
            sample = {
                'unix': t0, 'ok': True, 'remote_unix': remote_unix, 'load_utc': load.get('utc'),
                'load_mode': load.get('mode'), 'load_reasons': load.get('reasons'),
                'exact': t.get('exact'), 'errors': t.get('errors'), 'missed': t.get('missed_slots'),
                'p50': t.get('http_p50_seconds'), 'p95': t.get('http_p95_seconds'), 'p99': t.get('http_p99_seconds'),
                'freshness': hist.get('freshness_seconds'), 'ready_replicas': hist.get('ready_replicas'),
                'cycle_seconds': hist.get('cycle_seconds'), 'public_height': hist.get('public_height'),
                'latched': latched,
                'ctl_failures': ctl.get('failures'), 'ctl_halted': ctl.get('halted'), 'ctl_stalled': ctl.get('stalled'),
                'ctl_mode': ctl.get('mode'), 'ctl_phase': ctl.get('phase'),
                'cycle': lc.get('cycle'), 'cycle_ms': lc.get('cycle_ms'), 'freshness_ms': lc.get('freshness_ms'),
                'prepare_ms': lc.get('prepare_ms'), 'ship_ms': lc.get('ship_ms'), 'activate_ms': lc.get('activate_ms'),
                'recent_records': (lc.get('recent') or {}).get('records'), 'archives': lc.get('archives'),
                'sealed_published': lc.get('sealed_published'), 'dropped': lc.get('dropped'),
            }
            if base_failures is None:
                base_failures = ctl.get('failures') or 0
            fails = 0
            age = remote_unix - _utc(load.get('utc'))
            sample['load_status_age_s'] = age
            sample['count_error'] = load['counts'].get('error'); sample['count_recovered'] = load['counts'].get('recovered')
            reasons += check_errors(load['counts'].get('error') or 0, t0)
            if (t.get('exact') or 0) < 290: reasons.append('history exact %s < 290' % t.get('exact'))
            if (t.get('http_p99_seconds') or 0) > 0.5: reasons.append('history p99 %s > 0.5' % t.get('http_p99_seconds'))
            if (hist.get('freshness_seconds') or 0) > 30: reasons.append('freshness %s > 30' % hist.get('freshness_seconds'))
            if (hist.get('ready_replicas') or 0) < 2: reasons.append('ready_replicas %s' % hist.get('ready_replicas'))
            if (ctl.get('failures') or 0) > base_failures: reasons.append('controller failures %s > %s' % (ctl.get('failures'), base_failures))
            if ctl.get('halted') is not None: reasons.append('controller halted %s' % ctl.get('halted'))
            if ctl.get('stalled'): reasons.append('controller stalled')
            if latched != 'none': reasons.append('latched.json present')
            if age > 90: reasons.append('history load status stale %.0f s' % age)
        except Exception as e:  # unreadable status
            fails += 1
            sample['error'] = '%s rc=%s' % (e, rc)
            if fails >= 2:
                reasons.append('status unreadable twice: %s' % e)
        sample['reasons'] = reasons
        write('history-samples.jsonl', sample)
        if reasons and not tripped():
            trip(reasons, sample)
        if not tripped():
            run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', '-o', 'ControlMaster=auto', '-o', 'ControlPersist=900', '-o', 'ControlPath=' + os.path.expanduser('~/.ssh/cm-txid/%C'), BENCH, f'echo allow > {PERMIT}'], 12)
        if i % 2 == 0:
            rc2, out2 = run([SSHP, 'recent', RECENT_CMD])
            lines = [l for l in out2.split('\n') if l.strip()]
            try:
                cpu = [int(x) for x in lines[0].split()[1:]]
                write('recent-cpu.jsonl', {'unix': time.time(), 'proc_stat_cpu': cpu, 'loadavg': lines[1],
                                           'txid_worker_cpu_ns': int(lines[2]), 'history_shard_cpu_ns': int(lines[3]),
                                           'remote_unix': float(lines[4])})
            except Exception as e:
                write('recent-cpu.jsonl', {'unix': time.time(), 'error': str(e), 'rc': rc2})
        print(time.strftime('%H:%M:%S', time.gmtime()), 'p99=%s exact=%s err=%s fresh=%s cyc_fresh=%s prep=%s %s' % (
            sample.get('p99'), sample.get('exact'), sample.get('errors'), sample.get('freshness'),
            sample.get('freshness_ms'), sample.get('prepare_ms'), 'TRIPPED' if tripped() else ''), flush=True)
        i += 1
        time.sleep(max(0.0, 15 - (time.time() - t0)))


def _utc(s):
    from datetime import datetime
    return datetime.fromisoformat(s).timestamp()


if __name__ == '__main__':
    main()
