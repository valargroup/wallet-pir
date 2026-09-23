"""Task-specific supervisor: retain evidence and restore canonical serving.

Run once under caffeinate. Does not certify qualification. Stops the campaign if sampling is unhealthy.
"""
import fcntl
import json
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
COORDINATOR = 'root@167.99.42.60'
WORKERS = ['root@10.142.0.15', 'root@10.142.0.16']
SSH = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
       '-o', 'ConnectTimeout=15', '-o', 'ServerAliveInterval=15',
       '-o', 'ServerAliveCountMax=3', '-i', str(Path.home() / '.ssh/id_ed25519')]


def remote(host, script, timeout=180):
    command = SSH + ([] if host == COORDINATOR else ['-J', COORDINATOR])
    result = subprocess.run(command + [host, 'sh', '-se'], input=script,
                            text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f'{host}: {result.stderr} {result.stdout}')
    return result.stdout


def save(name, value):
    temporary = ROOT / (name + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(ROOT / name)


def restore_remote(host, script):
    for attempt in range(60):
        try:
            return remote(host, script, timeout=600)
        except Exception as error:
            print('Restoration retry:', host, attempt, error, flush=True)
            if attempt == 59:
                raise
            time.sleep(30)


def finish():
    deadline = time.monotonic() + 8 * 3600
    began = time.monotonic()
    while time.monotonic() < deadline:
        try:
            state = remote(COORDINATOR, 'systemctl show enhance-pir-v4-sealed-campaign.service -p ActiveState --value').strip()
            save('sealed-supervisor-status.json', {'phase': 'monitoring', 'service_state': state, 'time': time.time()})
            if state in ('inactive', 'failed'):
                break
            if state != 'active':
                print('Transitional campaign state:', state, flush=True)
            if state == 'active' and time.monotonic() - began > 120:
                for worker in WORKERS:
                    probe = remote(worker, "systemctl is-active enhance-pir-v4-sealed-sampling.service || true; tail -1 /srv/enhance-pir-v4/validation/sealed-samples/samples.jsonl || true")
                    lines = probe.splitlines()
                    sample = json.loads(lines[-1])['sample'] if lines and lines[-1].startswith('{') else {'wall_time_ns': 0, 'error': 'missing sample'}
                    if lines[0] != 'active' or sample.get('error') or time.time_ns() - sample['wall_time_ns'] > 60_000_000_000:
                        save('sealed-sampling-failure.json', {'worker': worker, 'time': time.time(), 'reason': 'sampler failed or stale'})
                        restore_remote(COORDINATOR, 'systemctl stop enhance-pir-v4-sealed-campaign.service')
                        break

        except Exception as error:
            print('Observation failed; campaign not restarted:', error, flush=True)
        time.sleep(30)

    # Confirm termination before changing worker state, including on timeout.
    restore_remote(COORDINATOR, '''if test "$(systemctl show enhance-pir-v4-sealed-campaign.service -p LoadState --value)" != not-found; then
  systemctl stop enhance-pir-v4-sealed-campaign.service
fi
test "$(systemctl show enhance-pir-v4-sealed-campaign.service -p MainPID --value)" = 0
systemctl stop enhance-pir-v4-coordinator.service
for unit in enhance-pir-v4-sealed-helper-1.service enhance-pir-v4-sealed-helper-2.service; do
  if test "$(systemctl show "$unit" -p LoadState --value)" != not-found; then systemctl stop "$unit"; fi
done
''')
    try:
        report = remote(COORDINATOR, 'cat /srv/enhance-pir-v4/validation/sealed/exercise.json')
        (ROOT / 'sealed-final.json').write_text(report)
    except Exception as error:
        print('Final report collection failed:', error, flush=True)

    for worker in WORKERS:
        restore_remote(worker, '''for unit in enhance-pir-v4-sealed-sampling.service enhance-pir-v4-sealed-sampling-resumed.service; do
  if test "$(systemctl show "$unit" -p LoadState --value)" != not-found; then
    systemctl stop "$unit"
  fi
  test "$(systemctl show "$unit" -p MainPID --value)" = 0
done
systemctl stop enhance-pir-v4-worker.service
if test -d /srv/enhance-pir-v4/worker.canonical; then
  if test -e /srv/enhance-pir-v4/worker; then
    test ! -e /srv/enhance-pir-v4/worker.sealed-completed
    mv /srv/enhance-pir-v4/worker /srv/enhance-pir-v4/worker.sealed-completed
  fi
  test -d /srv/enhance-pir-v4/worker.sealed-completed
  mv /srv/enhance-pir-v4/worker.canonical /srv/enhance-pir-v4/worker
else
  test -d /srv/enhance-pir-v4/worker.sealed-completed
  test -d /srv/enhance-pir-v4/worker
fi
systemctl start enhance-pir-v4-worker.service
systemctl is-active enhance-pir-v4-worker.service
''')
    restore_remote(COORDINATOR, 'systemctl start enhance-pir-v4-coordinator.service\nsystemctl is-active enhance-pir-v4-coordinator.service')
    save('sealed-supervisor-status.json', {'phase': 'canonical_restored_validation_pending', 'time': time.time()})
    benchmark = (ROOT / 'canonical-load.py').read_text().replace(
        "Path('/srv/enhance-pir-v4/validation/canonical')",
        "Path('/srv/enhance-pir-v4/validation/canonical-after-sealed')")
    command = SSH + [COORDINATOR, 'python3', '-']
    result = subprocess.run(command, input=benchmark, text=True, capture_output=True, timeout=1200)
    (ROOT / 'canonical-after-sealed.log').write_text(result.stdout + result.stderr)
    if result.returncode:
        raise RuntimeError('Canonical post-campaign validation failed; service remains restored; inspect remote reports')
    health = remote(COORDINATOR, 'curl --fail --silent --max-time 15 http://127.0.0.1:8080/v1/health')
    (ROOT / 'canonical-after-sealed-health.json').write_text(health)
    save('sealed-supervisor-status.json', {'phase': 'canonical_restored_validation_passed', 'time': time.time(),
                                  'qualification': 'unqualified', 'sealed_campaign': 'completed; assessment pending'})


if __name__ == '__main__':
    with (ROOT / 'finish-sealed.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            finish()
        except Exception as error:
            save('sealed-supervisor-error.json', {'error': str(error), 'time': time.time()})
            raise
