"""Per-second coordinator NIC throughput plus Enhance publication phase, to correlate with Status latency."""
import sys
import time
import urllib.request


def nic():
    out = {}
    for line in open('/proc/net/dev').read().splitlines()[2:]:
        name, data = line.split(':', 1)
        f = data.split()
        out[name.strip()] = (int(f[0]), int(f[8]))
    return out


def phase():
    try:
        text = urllib.request.urlopen('http://127.0.0.1:8080/metrics', timeout=1).read().decode()
    except Exception:
        return '?'
    for line in text.splitlines():
        if line.startswith('enhance_operation_phase') and line.endswith(' 1'):
            return line.split('"')[1]
    return 'idle'


end = time.time() + float(sys.argv[1])
prev = nic()
while time.time() < end:
    time.sleep(1)
    cur = nic()
    parts = []
    for n in ('eth0', 'eth1', 'wg-enhance-gpu'):
        if n in cur:
            rx = (cur[n][0] - prev[n][0]) / 1e6
            tx = (cur[n][1] - prev[n][1]) / 1e6
            parts.append(f'{n} rx {rx:6.1f} tx {tx:6.1f} MB/s')
    print(time.strftime('%H:%M:%S'), phase(), ' | '.join(parts), flush=True)
    prev = cur
