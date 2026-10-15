#!/usr/bin/env python3
"""Paired benchmark of prepared history runtimes: shipping off against on.

    bench.py fixture --bin BIN --source SLICE --work WORK
    bench.py arm     --bin BIN --work WORK --name NAME --ship on|off [...]
    bench.py pairs   --bin BIN --work WORK

`fixture` builds the immutable inputs every arm starts from: a journal of
real mainnet blocks rebased to height 0 (`history_bench_chain fixture`), the
bootstrap publication through the base height, and the probe publication of
the whole fixture whose shard boundaries chose that base height so that a
recent directory shard seals inside the measured blocks.

`arm` runs one arm on fixed CPU groups: the mock node and journal writer,
the worker (a recent replica under a one-replica assignment), the
publication controller with `ship_runtimes` on or off and this directory's
fleet adapter, then the open-loop query load. The cold first preparation and
one warm-up block precede the measured window; the measured window is the
steady blocks appended at a fixed interval under the load; a worker restart
follows it. Everything an arm observes is written under its directory:
process CPU and memory samples, per-thread worker CPU, the CPU each measured
group spent on processes outside the arm, readiness probes, every query,
every appended block, every publication cycle, every staging, and the
workers' and controller's metrics.

`pairs` runs the six arms in the prescribed order: off/on, on/off, off/on.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request

HERE = Path(__file__).resolve().parent
GROUPS = {'publisher': '0,1', 'worker': '2,3', 'load': '4,5', 'aux': '6,7'}
RECENT = 'decoupled-2k-8k'
ARCHIVE = 'decoupled-32k-32k'
PAIRS = [('pair1-off', 'off'), ('pair1-on', 'on'), ('pair2-on', 'on'), ('pair2-off', 'off'),
         ('pair3-off', 'off'), ('pair3-on', 'on')]


def sh(*args, **kwargs):
    return subprocess.run([str(a) for a in args], check=True, **kwargs)


def http_json(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.status, json.loads(response.read())


def http_status(url, timeout=2):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code
    except Exception:
        return None


def wait_for(predicate, timeout, what, interval=0.25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise TimeoutError(what)


def sha256_tree(root, out):
    files = sorted(p for p in Path(root).rglob('*') if p.is_file())
    with open(out, 'w') as stream:
        for path in files:
            digest = subprocess.run(['sha256sum', str(path)], check=True, capture_output=True,
                                    text=True).stdout.split()[0]
            stream.write(f'{digest}  {path.relative_to(root)}\n')


# Fixture ----------------------------------------------------------------

def fixture(args):
    work = Path(args.work)
    out = work / 'fixture'
    out.mkdir(parents=True)
    journal = out / 'journal-full'
    report = sh(Path(args.bin) / 'history_bench_chain', 'fixture', '--source', args.source,
                '--first', args.first, '--blocks', args.blocks, '--out', journal,
                capture_output=True, text=True).stdout
    chain = json.loads(report)
    cookie = out / 'cookie'
    cookie.write_text('bench:bench\n')

    def publish(output, through):
        started = time.monotonic()
        sh(Path(args.bin) / 'shard-publish', '--data-dir', journal, '--output', output,
           '--recent-geometry', RECENT, '--archive-geometry', ARCHIVE,
           '--recent-from', args.recent_from, '--directory-choice', 'all',
           '--through', through, '--zakura-cookie', cookie)
        return time.monotonic() - started

    probe_seconds = publish(out / 'probe', args.blocks - 1)
    probe = json.loads((out / 'probe' / 'shards.json').read_text())
    recent = [e for e in probe['shards'] if e['geometry'] == RECENT]
    seals = [e['end_height'] for e in recent if e['sealed']]
    # The base height: a recent directory shard already sealed below it, and
    # the next one sealing half-way through the steady blocks.
    later = [h for h in seals if h - args.seal_offset > recent[0]['end_height']]
    if not later:
        raise SystemExit(f'no recent seal leaves a sealed shard before it: {seals}')
    base = later[0] - args.seal_offset
    if base + 1 + args.steady_blocks >= args.blocks:
        raise SystemExit('the fixture is too short for the steady blocks')
    initial_seconds = publish(out / 'initial', base)
    initial = json.loads((out / 'initial' / 'shards.json').read_text())
    shutil.rmtree(out / 'probe' / '.staging', ignore_errors=True)
    record = {
        'chain': chain,
        'recent_from': args.recent_from,
        'base': base,
        'warmup_blocks': 1,
        'steady_blocks': args.steady_blocks,
        'steady_heights': [base + 2, base + 1 + args.steady_blocks],
        'seal_in_window': later[0],
        'recent_seals': seals,
        'probe_shards': [{k: e[k] for k in ('shard_id', 'geometry', 'start_height', 'end_height', 'sealed',
                                            'directory_segments', 'page_segments')}
                         for e in probe['shards']],
        'initial_shards': [{k: e[k] for k in ('shard_id', 'geometry', 'start_height', 'end_height',
                                              'sealed', 'manifest_digest')} for e in initial['shards']],
        'publish_seconds': {'probe': probe_seconds, 'initial': initial_seconds},
    }
    (out / 'fixture.json').write_text(json.dumps(record, indent=1) + '\n')
    sha256_tree(out / 'initial', out / 'initial.SHA256SUMS')
    sha256_tree(journal, out / 'journal-full.SHA256SUMS')
    print(json.dumps({k: record[k] for k in ('base', 'seal_in_window', 'steady_heights')}))


# Sampling ---------------------------------------------------------------

def cpus(group):
    return [int(c) for c in group.split(',')]


def read_cpu_jiffies():
    """Busy jiffies per CPU from /proc/stat."""
    busy = {}
    with open('/proc/stat') as stream:
        for line in stream:
            if line.startswith('cpu') and line[3].isdigit():
                parts = line.split()
                values = list(map(int, parts[1:]))
                idle = values[3] + values[4]
                busy[int(parts[0][3:])] = sum(values[:8]) - idle
    return busy


def proc_cpu(pid):
    """utime+stime ticks of a process and every thread's, by thread name."""
    with open(f'/proc/{pid}/stat') as stream:
        fields = stream.read().rsplit(')', 1)[1].split()
    total = int(fields[11]) + int(fields[12])
    threads = {}
    for task in os.listdir(f'/proc/{pid}/task'):
        try:
            with open(f'/proc/{pid}/task/{task}/stat') as stream:
                raw = stream.read()
            name = raw[raw.index('(') + 1:raw.rindex(')')]
            fields = raw.rsplit(')', 1)[1].split()
            threads[name] = threads.get(name, 0) + int(fields[11]) + int(fields[12])
        except (FileNotFoundError, ProcessLookupError, ValueError):
            pass
    return total, threads


def proc_rss(pid):
    with open(f'/proc/{pid}/status') as stream:
        for line in stream:
            if line.startswith('VmRSS:'):
                return int(line.split()[1]) * 1024
    return 0


def children_map():
    children = {}
    for entry in os.listdir('/proc'):
        if entry.isdigit():
            try:
                with open(f'/proc/{entry}/stat') as stream:
                    ppid = int(stream.read().rsplit(')', 1)[1].split()[1])
                children.setdefault(ppid, []).append(int(entry))
            except (FileNotFoundError, ProcessLookupError, ValueError):
                pass
    return children


def descendants(pid, children):
    """A process and every descendant, for roles that spawn helpers."""
    found, frontier = {pid}, [pid]
    while frontier:
        for child in children.get(frontier.pop(), []):
            if child not in found:
                found.add(child)
                frontier.append(child)
    return found


class Sampler(threading.Thread):
    """Samples every 0.25 s: each role's CPU ticks (its process tree), RSS,
    the worker's per-thread CPU, every CPU's busy ticks, host memory and
    worker readiness. Written as one JSON object per line."""

    def __init__(self, path, roles, ready_url):
        super().__init__(daemon=True)
        self.path, self.roles, self.ready_url = path, roles, ready_url
        self.stop_event = threading.Event()

    def run(self):
        with open(self.path, 'a') as stream:
            while not self.stop_event.is_set():
                try:
                    sample = self.sample()
                except Exception as error:  # a gap is recorded, never fatal
                    sample = {'unix': time.time(), 'error': repr(error)}
                stream.write(json.dumps(sample) + '\n')
                stream.flush()
                self.stop_event.wait(0.25)

    def sample(self):
        sample = {'unix': time.time(), 'cpu_busy': read_cpu_jiffies(), 'roles': {}}
        children = children_map()
        for role, pid in list(self.roles.items()):
            try:
                ticks, rss = 0, 0
                for member in descendants(pid, children):
                    try:
                        total, threads = proc_cpu(member)
                        ticks += total
                        rss += proc_rss(member)
                    except (FileNotFoundError, ProcessLookupError):
                        continue
                    if role == 'worker' and member == pid:
                        sample['worker_threads'] = threads
                sample['roles'][role] = {'ticks': ticks, 'rss': rss}
            except (FileNotFoundError, ProcessLookupError):
                sample['roles'][role] = None
        with open('/proc/meminfo') as meminfo:
            for line in meminfo:
                if line.startswith(('MemTotal:', 'MemAvailable:')):
                    sample[line.split(':')[0]] = int(line.split()[1]) * 1024
        sample['disk_free_bytes'] = shutil.disk_usage(self.path).free
        sample['ready'] = http_status(self.ready_url, timeout=0.5)
        return sample

    def stop(self):
        self.stop_event.set()
        self.join()


# Arm --------------------------------------------------------------------

class Arm:
    def __init__(self, args):
        self.args = args
        self.bin = Path(args.bin)
        self.work = Path(args.work)
        self.fixture = self.work / 'fixture'
        self.meta = json.loads((self.fixture / 'fixture.json').read_text())
        self.dir = self.work / 'arms' / args.name
        self.raw = self.dir / 'raw'
        self.processes = {}
        self.events = []

    def event(self, name, **fields):
        fields.update(event=name, unix=time.time())
        self.events.append(fields)
        with open(self.raw / 'arm-events.jsonl', 'a') as stream:
            stream.write(json.dumps(fields) + '\n')
        print(json.dumps(fields), flush=True)

    def spawn(self, role, group, args, env=None, stdin=None):
        log = open(self.raw / f'{role}.log', 'ab')
        process = subprocess.Popen(['taskset', '-c', GROUPS[group]] + [str(a) for a in args],
                                   stdout=subprocess.PIPE if stdin else log, stderr=log,
                                   stdin=stdin, env={**os.environ, 'NO_COLOR': '1', **(env or {})},
                                   text=bool(stdin))
        self.processes[role] = process
        return process

    def stop(self, role, sig=signal.SIGTERM, timeout=30):
        process = self.processes.pop(role, None)
        if process and process.poll() is None:
            process.send_signal(sig)
            try:
                process.wait(timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

    def driver(self, command, timeout):
        driver = self.processes['chain']
        driver.stdin.write(command + '\n')
        driver.stdin.flush()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = driver.stdout.readline()
            if not line:
                raise RuntimeError('the chain driver exited')
            if line.startswith('done') or line.startswith('error'):
                return line.strip()
        raise TimeoutError(command)

    def controller_height(self):
        try:
            return http_json(f'http://127.0.0.1:{self.ports["controller"]}/v1/status', 2)[1].get('public_height')
        except Exception:
            return None

    def worker_args(self):
        a = self.args
        return [self.bin / 'transparent-shard-server', '--listen', f'127.0.0.1:{self.ports["worker"]}',
                '--shard-dir', self.worker_initial, '--assignment', self.worker_initial / 'assignment.json',
                '--worker-id', 'recent-01', '--cache-bytes', a.cache_bytes,
                '--runtime-cache-dir', self.dir / 'worker' / 'runtime-cache',
                '--runtime-cache-max-bytes', 2 * a.cache_bytes,
                '--build-slots', 1, '--query-slots', 2, '--prune-excess',
                '--control-socket', self.dir / 'worker' / 'control.sock',
                '--active-record', self.dir / 'worker' / 'active.json']

    def start_worker(self, label):
        started = time.monotonic()
        self.spawn('worker', 'worker', self.worker_args(), env={'TRANSPARENT_BUILD_THREADS': '2'})
        wait_for(lambda: http_status(f'http://127.0.0.1:{self.ports["worker"]}/v1/ready') == 200,
                 1800, 'worker readiness')
        self.event('worker_ready', label=label, seconds=time.monotonic() - started,
                   metrics=self.worker_metrics())

    def worker_metrics(self):
        text = urllib.request.urlopen(f'http://127.0.0.1:{self.ports["worker"]}/metrics', timeout=5).read().decode()
        wanted = ('transparent_shard_builds_total', 'transparent_shard_shipped_loads_total',
                  'transparent_shard_shipped_fallbacks_total', 'transparent_shard_disk_hits_total',
                  'transparent_shard_shipped_check_micros_total', 'transparent_shard_build_failures_total',
                  'transparent_shard_overloads_total')
        values = {}
        for line in text.splitlines():
            name = line.split(' ')[0].split('{')[0]
            if name in wanted:
                values[name] = values.get(name, 0) + float(line.rsplit(' ', 1)[1])
        (self.raw / f'worker-metrics-{int(time.time())}.prom').write_text(text)
        return values

    def run(self):
        a = self.args
        if self.dir.exists():
            raise SystemExit(f'{self.dir} exists: every arm starts from empty state')
        self.raw.mkdir(parents=True)
        base = self.meta['base']
        port = a.port_base
        self.ports = {'rpc': port, 'worker': port + 1, 'controller': port + 2}
        # Identical immutable inputs: copies of the fixture's journal and
        # bootstrap publication, empty runtime caches.
        sh('cp', '-a', self.fixture / 'journal-full', self.dir / 'journal')
        sh('cp', '-a', self.fixture / 'initial', self.dir / 'initial')
        (self.dir / 'publications').mkdir()
        (self.dir / 'worker' / 'publications').mkdir(parents=True)
        (self.dir / 'worker' / 'runtime-cache').mkdir()
        (self.dir / 'fleet').mkdir()
        cookie = self.dir / 'cookie'
        cookie.write_text('bench:bench\n')
        roster = [
            {'id': 'archive-01', 'role': 'archive-owner', 'ssh_host': 'unused',
             'upstream': '127.0.0.1:9', 'cache_bytes': 64 << 30},
            {'id': 'recent-01', 'role': 'recent-replica', 'replica_group': 'recent',
             'ssh_host': 'unused', 'upstream': f'127.0.0.1:{self.ports["worker"]}',
             'cache_bytes': a.cache_bytes},
        ]
        (self.dir / 'roster.json').write_text(json.dumps(roster))
        fleet = {'state_dir': str(self.dir / 'fleet'), 'worker_root': str(self.dir / 'worker' / 'publications'),
                 'worker_socket': str(self.dir / 'worker' / 'control.sock'), 'worker_id': 'recent-01',
                 'worker_upstream': f'127.0.0.1:{self.ports["worker"]}',
                 'assign_binary': str(self.bin / 'shard-assign'), 'roster': str(self.dir / 'roster.json'),
                 'log': str(self.raw / 'fleet.jsonl')}
        (self.dir / 'fleet.json').write_text(json.dumps(fleet))
        # The worker starts on the bootstrap publication, staged as the
        # adapter stages one.
        assignment = self.dir / 'fleet' / 'initial.assignment.json'
        sh(self.bin / 'shard-assign', 'plan', '--shard-dir', self.dir / 'initial', '--roster',
           self.dir / 'roster.json', '--recent-from-height', self.meta['recent_from'], '--headroom', '0.05',
           '--out-assignment', assignment, '--source-sha', 'bench')
        files = sh(self.bin / 'shard-assign', 'files', '--shard-dir', self.dir / 'initial',
                   '--assignment', assignment, '--worker-id', 'recent-01',
                   capture_output=True, text=True).stdout
        self.worker_initial = self.dir / 'worker' / 'publications' / 'initial'
        self.worker_initial.mkdir()
        (self.dir / 'fleet' / 'initial.files').write_text(files)
        sh('rsync', '-a', '--files-from', self.dir / 'fleet' / 'initial.files',
           str(self.dir / 'initial') + '/', str(self.worker_initial) + '/')
        shutil.copyfile(assignment, self.worker_initial / 'assignment.json')

        self.spawn('chain', 'aux', [self.bin / 'history_bench_chain', 'drive', '--full',
                                    self.fixture / 'journal-full', '--journal', self.dir / 'journal',
                                    '--base', base, '--listen', f'127.0.0.1:{self.ports["rpc"]}',
                                    '--log', self.raw / 'blocks.jsonl'], stdin=subprocess.PIPE)
        ready = self.processes['chain'].stdout.readline()
        if not ready.startswith('ready'):
            raise RuntimeError(f'chain driver: {ready!r}')
        self.event('chain_ready', base=base)
        # Cold: the worker builds its assigned runtimes from the bootstrap
        # publication, which carries none, in both arms.
        self.start_worker('cold')
        config = {
            'data_dir': str(self.dir / 'journal'), 'publication_root': str(self.dir / 'publications'),
            'initial_publication': str(self.dir / 'initial'), 'recent_from': self.meta['recent_from'],
            'recent_geometry': RECENT, 'archive_geometry': ARCHIVE,
            'rpc_url': f'http://127.0.0.1:{self.ports["rpc"]}', 'rpc_cookie': str(cookie),
            'fleet_command': str(HERE / 'fleet.py'), 'fleet_config': str(self.dir / 'fleet.json'),
            'listen': f'127.0.0.1:{self.ports["controller"]}', 'source_sha': 'bench',
            'directory_choice': 'all', 'follow_journal': True, 'ship_runtimes': a.ship == 'on',
        }
        (self.dir / 'controller.json').write_text(json.dumps(config, indent=1))
        self.spawn('controller', 'publisher', [self.bin / 'transparent-publish-controller', '--config',
                                               self.dir / 'controller.json'],
                   env={'TRANSPARENT_BUILD_THREADS': '2', 'RUST_LOG': 'info'})
        # The controller's first cycle re-offers the bootstrap publication:
        # with shipping on, the publisher's cold preparation of every runtime.
        wait_for(lambda: self.controller_height() == base, 1800, 'bootstrap activation')
        self.event('bootstrap_active')
        # One warm-up block, outside the window.
        self.driver('advance 1', 60)
        wait_for(lambda: self.controller_height() == base + 1, 600, 'warm-up activation')
        self.event('warmup_active', metrics=self.worker_metrics())

        sampler = Sampler(self.raw / 'samples.jsonl', {
            'publisher': self.processes['controller'].pid, 'worker': self.processes['worker'].pid,
            'chain': self.processes['chain'].pid}, f'http://127.0.0.1:{self.ports["worker"]}/v1/ready')
        duration = a.steady_blocks * a.interval_ms // 1000
        load = self.spawn('load', 'load', [
            self.bin / 'history_bench_load', '--worker', f'http://127.0.0.1:{self.ports["worker"]}',
            '--oracle', self.dir / 'publications', '--oracle', self.dir / 'initial',
            '--oracle', self.dir / 'worker' / 'publications',
            '--geometry', RECENT, '--rate', a.rate, '--duration-seconds', duration,
            '--seed', a.seed, '--out', self.raw / 'queries.jsonl'])
        sampler.roles['load'] = load.pid
        sampler.start()
        self.event('steady_start', blocks=a.steady_blocks, interval_ms=a.interval_ms, rate=a.rate)
        self.driver(f'run {a.steady_blocks} {a.interval_ms}', duration + 120)
        self.event('steady_blocks_appended')
        last = base + 1 + a.steady_blocks
        try:
            wait_for(lambda: (self.controller_height() or 0) >= last, 300, 'final activation')
            self.event('steady_final_active', height=last)
        except TimeoutError:
            self.event('steady_final_not_active', height=self.controller_height())
        load.wait(duration + 600)
        self.event('steady_end', load_exit=load.returncode, metrics=self.worker_metrics())
        sampler.stop()
        status = http_json(f'http://127.0.0.1:{self.ports["controller"]}/v1/status')[1]
        (self.raw / 'controller-status.json').write_text(json.dumps(status, indent=1))
        (self.raw / 'controller-metrics.prom').write_bytes(
            urllib.request.urlopen(f'http://127.0.0.1:{self.ports["controller"]}/metrics', timeout=5).read())
        # Restart outside the window: the worker reloads its active
        # publication, offered prepared runtimes when it was staged with them.
        self.stop('worker')
        restart_sampler = Sampler(self.raw / 'restart-samples.jsonl', {}, f'http://127.0.0.1:{self.ports["worker"]}/v1/ready')
        restart_sampler.start()
        self.start_worker('restart')
        restart_sampler.stop()
        self.event('arm_done')

    def close(self):
        for role in list(self.processes):
            if role == 'chain' and self.processes[role].poll() is None:
                try:
                    self.processes[role].stdin.write('quit\n')
                    self.processes[role].stdin.flush()
                except BrokenPipeError:
                    pass
            self.stop(role)


def arm(args):
    # The orchestrator, its sampler and the adapter it starts stay on the
    # auxiliary CPUs, off every measured group.
    os.sched_setaffinity(0, cpus(GROUPS['aux']))
    runner = Arm(args)
    try:
        runner.run()
    except BaseException as error:
        runner.event('arm_failed', error=repr(error))
        raise
    finally:
        runner.close()


def progress(summary):
    """Hub job progress, when run as one."""
    path = os.environ.get('AI_DEV_JOB_STATUS')
    if path:
        Path(path).write_text(json.dumps({'summary': summary, 'at': time.time()}))


def pairs(args):
    work = Path(args.work)
    budget = work / 'budget.json'
    if not budget.exists():
        # Fixed before the first pair and held for all six arms.
        memory = {line.split(':')[0]: int(line.split()[1]) * 1024
                  for line in open('/proc/meminfo') if line.startswith(('MemTotal:', 'MemAvailable:'))}
        disk = shutil.disk_usage(work)
        record = {'cache_bytes': args.cache_bytes, 'runtime_disk_cache_bytes': 2 * args.cache_bytes,
                  'host_memory_total': memory['MemTotal'], 'host_memory_available': memory['MemAvailable'],
                  'disk_total': disk.total, 'disk_free': disk.free, 'groups': GROUPS,
                  'steady_blocks': args.steady_blocks, 'interval_ms': args.interval_ms, 'rate': args.rate,
                  'seed': args.seed, 'unix': time.time()}
        if memory['MemAvailable'] - 3 * args.cache_bytes < 0.2 * memory['MemTotal']:
            raise SystemExit(f'the cache budget leaves under 20% host memory: {record}')
        if disk.free < 0.2 * disk.total:
            raise SystemExit(f'under 20% disk headroom: {record}')
        budget.write_text(json.dumps(record, indent=1) + '\n')
    for index, (name, ship) in enumerate(PAIRS):
        if (work / 'arms' / name).exists():
            print(json.dumps({'event': 'arm_exists', 'name': name}), flush=True)
            continue
        progress(f'arm {index + 1}/6 {name} running')
        try:
            arm(argparse.Namespace(**{**vars(args), 'name': name, 'ship': ship}))
        except Exception as error:
            # A failed arm is retained as recorded; the sequence continues.
            print(json.dumps({'event': 'arm_error', 'name': name, 'error': repr(error)}), flush=True)
        progress(f'arm {index + 1}/6 {name} finished')
    progress('all six arms finished')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    f = sub.add_parser('fixture')
    f.add_argument('--bin', required=True)
    f.add_argument('--source', required=True)
    f.add_argument('--work', required=True)
    f.add_argument('--first', type=int, default=3262749)
    f.add_argument('--blocks', type=int, default=60000)
    f.add_argument('--recent-from', type=int, default=2000)
    f.add_argument('--steady-blocks', type=int, default=300)
    f.add_argument('--seal-offset', type=int, default=152)
    for name in ('arm', 'pairs'):
        p = sub.add_parser(name)
        p.add_argument('--bin', required=True)
        p.add_argument('--work', required=True)
        if name == 'arm':
            p.add_argument('--name', required=True)
            p.add_argument('--ship', choices=['on', 'off'], required=True)
        p.add_argument('--steady-blocks', type=int, default=300)
        p.add_argument('--interval-ms', type=int, default=4000)
        p.add_argument('--rate', type=int, default=20)
        p.add_argument('--seed', type=int, default=20261014)
        p.add_argument('--cache-bytes', type=int, required=True)
        p.add_argument('--port-base', type=int, default=18400)
    args = parser.parse_args()
    {'fixture': fixture, 'arm': arm, 'pairs': pairs}[args.command](args)


if __name__ == '__main__':
    main()
