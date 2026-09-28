"""Sample coordinator CPU by process group every 5 s (percent of one core) plus load and memory."""
import json
import os
import sys
import time

GROUPS = {
    'zakurad': lambda c: 'zakurad' in c,
    'enhance_coordinator': lambda c: 'enhance-pir-server' in c and ' coordinator' in c,
    'enhance_ingress': lambda c: 'query-ingress' in c,
    'enhance_router_or_other_enhance': lambda c: 'enhance-pir-server' in c and ' coordinator' not in c and 'query-ingress' not in c,
    'status_controller': lambda c: 'serve-distributed' in c,
    'status_query_tunnel': lambda c: c.startswith('/usr/bin/ssh') and '8492:' in c,
    'status_control_tunnel': lambda c: c.startswith('/usr/bin/ssh') and '8481:' in c,
    'status_load_client': lambda c: 'probe-live-load' in c,
    'enhance_load_client': lambda c: 'enhance-pir-load-test' in c,
    'transparent': lambda c: 'transparent' in c,
    'pir_apm': lambda c: 'pir-apm' in c and 'credential' not in c,
    'caddy': lambda c: 'caddy' in c,
}
HZ = os.sysconf('SC_CLK_TCK')
out = open(sys.argv[1], 'a')


def snapshot():
    ticks = {}
    for pid in os.listdir('/proc'):
        if not pid.isdigit():
            continue
        try:
            cmd = open(f'/proc/{pid}/cmdline', 'rb').read().replace(b'\0', b' ').decode(errors='replace').strip()
            st = open(f'/proc/{pid}/stat').read().rsplit(')', 1)[1].split()
            t = int(st[11]) + int(st[12])
        except (OSError, IndexError, ValueError):
            continue
        g = next((k for k, f in GROUPS.items() if f(cmd)), 'other')
        ticks[(pid, g)] = t
    cpu = open('/proc/stat').readline().split()[1:]
    return ticks, [int(x) for x in cpu]


prev, prev_cpu = snapshot()
while True:
    time.sleep(5)
    cur, cur_cpu = snapshot()
    per = {}
    for (pid, g), t in cur.items():
        d = t - prev.get((pid, g), t)
        per[g] = per.get(g, 0) + d
    tot = sum(cur_cpu) - sum(prev_cpu)
    idle = (cur_cpu[3] + cur_cpu[4]) - (prev_cpu[3] + prev_cpu[4])
    row = {'t': int(time.time()), 'host_busy_pct': round(100 * (1 - idle / tot), 1) if tot else None,
           'iowait_pct': round(100 * (cur_cpu[4] - prev_cpu[4]) / tot, 1) if tot else None,
           'load1': float(open('/proc/loadavg').read().split()[0]),
           'mem_avail_kb': int([l for l in open('/proc/meminfo') if l.startswith('MemAvailable')][0].split()[1]),
           'cores_used': {k: round(v / HZ / 5, 2) for k, v in sorted(per.items(), key=lambda x: -x[1]) if v}}
    out.write(json.dumps(row) + '\n')
    out.flush()
    prev, prev_cpu = cur, cur_cpu
