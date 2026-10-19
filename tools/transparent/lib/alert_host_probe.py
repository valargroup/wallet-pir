"""On-host, read-only observations for `alert_activation`, printed as one small JSON line.

The runner sends this file's source to `python3 -c` on the host (standard library
only), so secrets in a process environment, incident bodies or delivery errors
never leave the host: every command prints only whitelisted fields.

- `modes <database>`: each family's last evaluated mode (`metadata` rows `mode*`).
- `status <url>`: a summary of `/monitor-status`, read with a hard byte bound;
  a larger or invalid body is an error, never a truncated parse.
- `environ <pid>`: the whitelisted path, listen and mode settings of a running
  process.
- `shadow <pid> <binary> <config> <port> <seconds>`: run the staged binary as an
  isolated shadow pir-apm beside the running one: the running process's nonsecret
  `PIR_APM_*` settings (so the real configured sources), a temporary state,
  history and configuration copy with every mode `shadow`, its own loopback port,
  no webhook and no scaler. It runs as the unit's user, is stopped and removed
  within the bound, and reports whether it evaluated with a valid configuration.
"""
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request

STATUS_LIMIT = 16 << 20
ENV_KEYS = ('PIR_APM_STATE_PATH', 'PIR_APM_HISTORY_PATH', 'PIR_APM_LISTEN', 'PIR_APM_TRANSPARENT_CONFIG',
            'PIR_APM_ALERT_MODE', 'PIR_APM_QUALITY_ALERT_MODE', 'PIR_MONITOR_STATE_PATH', 'PIR_MONITOR_LISTEN',
            'PIR_MONITOR_ALERT_MODE', 'PIR_MONITOR_SERVICE_ALERT_MODE')
SECRET_WORDS = ('SLACK', 'WEBHOOK', 'TOKEN', 'SECRET', 'PASSWORD', 'KEY', 'CREDENTIAL')


def modes(database):
    db = sqlite3.connect('file:%s?mode=ro' % database, uri=True)
    try:
        return dict(db.execute("SELECT key, value FROM metadata WHERE key LIKE 'mode%'"))
    finally:
        db.close()


def status(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        body = response.read(STATUS_LIMIT + 1)
    if len(body) > STATUS_LIMIT:
        raise ValueError('monitor-status exceeds %d bytes' % STATUS_LIMIT)
    document = json.loads(body)
    delivery = document.get('delivery') or {}
    quality = document.get('quality') or {}
    return {'progress_ok': document.get('progress_ok'), 'evaluated_at': document.get('evaluated_at'),
            'storage_error': document.get('storage_error'),
            'quality_enabled': quality.get('enabled'), 'quality_storage_error': quality.get('storage_error'),
            'delivery': {'configured': delivery.get('configured'), 'pending': delivery.get('pending'),
                         'last_success': delivery.get('last_success'), 'failure': bool(delivery.get('failure'))}}


def process_environ(pid):
    with open('/proc/%d/environ' % int(pid), 'rb') as handle:
        items = handle.read().split(b'\0')
    return dict(item.decode(errors='replace').split('=', 1) for item in items if b'=' in item)


def environ(pid):
    values = process_environ(pid)
    return {key: values.get(key) for key in ENV_KEYS}


def process_user(pid):
    with open('/proc/%d/status' % int(pid)) as handle:
        fields = dict(line.split(':', 1) for line in handle if ':' in line)
    return int(fields['Uid'].split()[1]), int(fields['Gid'].split()[1])


def shadow(pid, binary, config, port, seconds):
    running = process_environ(pid)
    uid, gid = process_user(pid)
    env = {k: v for k, v in running.items()
           if k.startswith('PIR_APM_') and not any(word in k for word in SECRET_WORDS)}
    env.pop('PIR_APM_SCALER_STATUS', None)
    directory = tempfile.mkdtemp(prefix='pir-apm-shadow-')
    child = None
    result = {'evaluated': False}
    try:
        with open(config) as handle:
            document = json.load(handle)
        document['capacity_alert_mode'] = 'shadow'
        for service in document.get('services', []):
            service['alert_mode'] = 'shadow'
        shadow_config = os.path.join(directory, 'transparent.json')
        with open(shadow_config, 'w') as handle:
            json.dump(document, handle)
        os.chown(directory, uid, gid)
        os.chown(shadow_config, uid, gid)
        env.update({'PATH': '/usr/bin:/bin', 'PIR_APM_STATE_PATH': os.path.join(directory, 'incidents.sqlite'),
                    'PIR_APM_HISTORY_PATH': os.path.join(directory, 'service-quality.sqlite'),
                    'PIR_APM_TRANSPARENT_CONFIG': shadow_config, 'PIR_APM_LISTEN': '127.0.0.1:%d' % int(port),
                    'PIR_APM_ALERT_MODE': 'shadow', 'PIR_APM_QUALITY_ALERT_MODE': 'shadow',
                    'PIR_APM_SCALING_ALERT_MODE': 'shadow', 'PIR_APM_CAPACITY_ALERT_MODE': 'shadow'})
        # As the unit's user and group only; root also drops its supplementary groups.
        identity = {'user': uid, 'group': gid, **({'extra_groups': []} if os.geteuid() == 0 else {})}
        child = subprocess.Popen([binary], env=env, cwd=directory, **identity,
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + float(seconds)
        while time.monotonic() < deadline and child.poll() is None:
            time.sleep(2)
            try:
                summary = status('http://127.0.0.1:%d/monitor-status' % int(port))
            except Exception:
                continue
            recorded = modes(env['PIR_APM_STATE_PATH'])
            result = {'evaluated': bool(summary['evaluated_at']), 'summary': summary,
                      'modes': {k: v for k, v in recorded.items() if k.startswith('mode')}}
            if result['evaluated'] and summary['quality_enabled'] and any(k.startswith('mode:quality-') for k in recorded):
                break
        result['exited'] = child.poll()
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(10)
        shutil.rmtree(directory, ignore_errors=True)
    return result


def main(argv):
    command, args = argv[0], argv[1:]
    try:
        result = {'modes': modes, 'status': status, 'environ': environ, 'shadow': shadow}[command](*args)
        print(json.dumps({'ok': True, 'result': result}, sort_keys=True))
        return 0
    except Exception as error:  # the error type only: messages can carry paths or URLs
        print(json.dumps({'ok': False, 'error': type(error).__name__}))
        return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
