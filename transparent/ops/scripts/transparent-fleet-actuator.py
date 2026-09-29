#!/usr/bin/env python3
"""Adds and removes elastic recent replicas, one journaled operation at a time.

The only process that creates or destroys transparent droplets. It acts on
the scaler's request (`scaler/request.json`) in `act` mode, plans without
applying in `act-dry`, and on operator commands. Every phase is written to
`scaler/journal/operation.json` before its side effect; an interrupted apply
proceeds only once Terraform state and the DigitalOcean API agree.
Archive owners and static hosts are never touched. See
transparent/docs/elastic-recent.md.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid

SCRIPTS = Path(__file__).resolve().parent
NAME = re.compile(r'^transparent-pir-recent-(\d{2,3})$')
SERVE_DEADLINE = 25 * 60
DRAIN_SECONDS = 120
QUIET_SECONDS = 30
MAX_APPLY_ATTEMPTS = 2
DEFAULTS = dict(cache_bytes=5368709120, memory_max='7G', build_slots=1, bwlimit_kbps=60000,
                freshness_pause_seconds=20, publication_root='/srv/transparent-pir/publications',
                release_store='/opt/transparent-publisher/releases')


def load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS/filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


INVENTORY = load_module('fleet_inventory', 'transparent-fleet-inventory.py')


class ActuatorError(RuntimeError):
    pass


def atomic_json(path, value):
    INVENTORY.atomic_json(path, value)


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return default


def http_json(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def log(event, **fields):
    print(json.dumps({'event': event, 'unix': round(time.time(), 3), **fields}), flush=True)


class Actuator:
    """One step of the state machine per call to `step`; `run_once` loops
    until the open operation waits on something external."""

    def __init__(self, config, fleet_config, elastic, do, remote=None, now=time.time):
        self.c = {**DEFAULTS, **config}
        self.fleet = fleet_config
        self.scaler = Path(self.c['scaler_dir'])
        self.journal = self.scaler/'journal'
        self.state = Path(self.fleet['state_dir'])
        self.inventory = INVENTORY.Inventory(self.state, self.fleet['roster'], self.fleet.get('known_hosts'))
        self.elastic = elastic
        self.do = do
        self.remote = remote or Remote(self.fleet)
        self.now = now

    # -- journal ---------------------------------------------------------
    def operation(self):
        return read_json(self.journal/'operation.json')

    def save(self, op, **changes):
        op.update(changes, phase_unix=self.now())
        atomic_json(self.journal/'operation.json', op)
        log('operation_phase', id=op['id'], kind=op['kind'], phase=op['phase'])
        return op

    def finish(self, op, outcome):
        # Injected host keys are no longer needed once the operation ends.
        for key in (op.get('host_keys') or {}).values():
            for path in (key.get('private_path'), str(key.get('private_path', '')) + '.pub'):
                if path:
                    Path(path).unlink(missing_ok=True)
        op.update(outcome=outcome, finished_unix=self.now())
        # Numbered, so the history reads in completion order.
        sequence = len(list((self.journal/'done').glob('*.json')))
        atomic_json(self.journal/'done'/f"{sequence:06d}-{op['id']}.json", op)
        (self.journal/'operation.json').unlink()
        log('operation_finished', id=op['id'], kind=op['kind'], outcome=outcome)

    def disabled(self):
        return (self.scaler/'disabled').exists()

    def policy(self):
        return read_json(self.scaler/'policy.json', {}) or {}

    def history(self, seconds=86400):
        cutoff = self.now() - seconds
        done = []
        for path in sorted((self.journal/'done').glob('*.json')):
            value = read_json(path)
            if value and value.get('finished_unix', 0) >= cutoff:
                done.append(value)
        return done

    # -- entry points ----------------------------------------------------
    def start(self, kind, **fields):
        if self.disabled():
            raise ActuatorError('the actuator is disabled (scaler/disabled exists)')
        if self.operation():
            raise ActuatorError('an operation is already open: ' + self.operation()['id'])
        op = dict(id=uuid.uuid4().hex[:12], kind=kind, phase='requested', created_unix=self.now(),
                  dry=False, apply_attempts=0, **fields)
        self.check_start(op)
        self.journal.mkdir(parents=True, exist_ok=True)
        (self.journal/'done').mkdir(exist_ok=True)
        return self.save(op)

    def check_start(self, op):
        inv = self.inventory.load()
        members = {m['id']: m for m in inv['members']}
        live_recent = [m for m in inv['members'] if m['role'] == 'recent-replica' and m['intent'] in ('enrolled', 'draining')]
        policy = self.policy()
        if op['kind'] in ('scale_out', 'replace'):
            count = op.get('count', 1)
            if not 1 <= count <= policy.get('max_step', 3):
                raise ActuatorError('scale-out count outside 1..max_step')
            if len(live_recent) + count > policy.get('max_recent', 6):
                raise ActuatorError('scale-out would exceed max_recent')
            if sum(1 for m in inv['members'] if m['intent'] != 'retired') + count > 32:
                raise ActuatorError('the fleet is capped at 32 members')
        if op['kind'] in ('scale_in', 'replace'):
            member = members.get(op.get('member'))
            if member is None or member['role'] != 'recent-replica':
                raise ActuatorError('only recent replicas can be removed or replaced')
            if op['kind'] == 'scale_in' and member.get('origin') != 'elastic':
                raise ActuatorError('static hosts are removed by an operator, never by the actuator')
            if op['kind'] == 'scale_in' and len(self.history_destroys()) >= policy.get('daily_destroys', 2):
                raise ActuatorError('daily destroy budget spent')

    def history_destroys(self):
        return [op for op in self.history() if op.get('destroyed')]

    def consume_request(self):
        """Start the scaler's pending request, once, in act or act-dry mode."""
        mode = self.policy().get('mode', 'observe')
        request = read_json(self.scaler/'request.json')
        if mode not in ('act', 'act-dry') or not request or self.operation():
            return None
        consumed = self.journal/'consumed.jsonl'
        seen = set(consumed.read_text().split()) if consumed.exists() else set()
        if request['decision_id'] in seen:
            return None
        self.journal.mkdir(parents=True, exist_ok=True)
        with consumed.open('a') as f:
            f.write(request['decision_id'] + '\n')
        fields = dict(decision_id=request['decision_id'], reason=request.get('reason'))
        if request['action'] == 'scale_out':
            fields['count'] = request.get('count', 1)
        else:
            fields['member'] = request['member']
        try:
            op = self.start(request['action'], **fields)
        except ActuatorError as error:
            log('request_refused', decision_id=request['decision_id'], error=str(error))
            return None
        if mode == 'act-dry':
            op = self.save(op, dry=True)
        return op

    def run_once(self, max_steps=50):
        if self.disabled():
            log('actuator_disabled')
            return None
        op = self.operation() or self.consume_request()
        for _ in range(max_steps):
            if op is None or op.get('fenced'):
                return op
            if not self.step(op):
                return self.operation()
            op = self.operation()
        return op

    # -- state machine ---------------------------------------------------
    def step(self, op):
        handler = getattr(self, f"phase_{op['kind']}_{op['phase']}", None) or getattr(self, f"phase_{op['phase']}", None)
        if handler is None:
            raise ActuatorError(f"no handler for {op['kind']} in phase {op['phase']}")
        return handler(op)

    def fence(self, op, reason):
        self.save(op, fenced=True, fence_reason=reason)
        log('operation_fenced', id=op['id'], reason=reason)
        return False

    # scale_out and replace share provisioning.
    def phase_requested(self, op):
        if op['kind'] == 'scale_in':
            return self.phase_scale_in_requested(op)
        names = self.allocate_names(op.get('count', 1))
        keys = {name: self.generate_host_key(op, name) for name in names}
        self.save(op, phase='keys_ready', names=names, host_keys=keys)
        return True

    def phase_keys_ready(self, op):
        members = self.elastic_members(extra=op['names'])
        plan, digest = self.elastic.plan(self.member_specs(members), self.host_keys(op), self.terraform_vars())
        summary = self.validate_plan(plan, create=op['names'])
        if op.get('dry'):
            self.save(op, phase='dry_planned', plan_sha256=digest, plan_summary=summary)
            self.finish(op, 'dry-run planned')
            return False
        self.save(op, phase='planned', plan=str(plan), plan_sha256=digest, plan_summary=summary)
        return True

    def phase_planned(self, op):
        if self.disabled():
            return False
        op = self.save(op, phase='applying', apply_attempts=op.get('apply_attempts', 0) + 1)
        self.elastic.apply(Path(op['plan']), op['plan_sha256'])
        return self.resolve_created(op)

    def phase_applying(self, op):
        # Found only after a crash during apply: resolve against the provider.
        return self.resolve_created(op)

    def resolve_created(self, op):
        state = self.elastic.state_members()
        droplets = {d['name']: d for d in self.do.droplets(tag=self.c['worker_tag'])}
        names = op['names']
        in_state = [n for n in names if n in state]
        at_provider = [n for n in names if n in droplets]
        if len(in_state) == len(names) and all(str(state[n]['id']) == str(droplets[n]['id']) for n in names if n in droplets) \
                and len(at_provider) == len(names):
            created = {n: {'id': str(state[n]['id']), 'ip': state[n]['ipv4_private']} for n in names}
            self.save(op, phase='provisioned', droplets=created)
            return True
        if not in_state and not at_provider:
            if op.get('apply_attempts', 0) >= MAX_APPLY_ATTEMPTS:
                return self.fence(op, 'apply created nothing after repeated attempts')
            # Nothing exists: plan again (a saved plan is single-use).
            self.save(op, phase='keys_ready')
            return True
        return self.fence(op, 'Terraform state and DigitalOcean disagree about '
                          + ', '.join(sorted(set(names) - set(in_state) | set(names) - set(at_provider))))

    def phase_provisioned(self, op):
        release = self.fleet_release()
        def enroll(inv):
            for name in op['names']:
                if any(m['id'] == name for m in inv['members']):
                    continue
                droplet = op['droplets'][name]
                inv['members'].append(dict(
                    id=name, role='recent-replica', group='recent', origin='elastic', intent='enrolled',
                    intent_unix=self.now(), ssh_host=droplet['ip'], upstream=droplet['ip'] + ':8093',
                    cache_bytes=self.c['cache_bytes'], memory_max=self.c['memory_max'],
                    build_slots=self.c['build_slots'], droplet_id=droplet['id'], size=self.c['size'],
                    ssh_host_key=op['host_keys'][name]['public'], installed_release=release['sha']))
            return inv
        inv = self.inventory.load()
        self.inventory.write(inv['revision'], enroll, 'actuator:' + op['id'])
        self.save(op, phase='enrolled', release=release, deadline_unix=self.now() + SERVE_DEADLINE)
        return True

    def phase_enrolled(self, op):
        installed = set(op.get('installed', []))
        for name in op['names']:
            if name in installed:
                continue
            if self.now() > op['deadline_unix']:
                return self.fail_new(op, name, 'did not install before its deadline')
            if not bootstrap(self, op, name):
                return False
            installed.add(name)
            self.save(op, installed=sorted(installed))
        self.save(op, phase='installed')
        return True

    def phase_installed(self, op):
        record = INVENTORY.membership(self.state)
        serving = INVENTORY.serving_recent(record)
        waiting = [n for n in op['names'] if n not in serving]
        if waiting:
            if self.now() > op['deadline_unix']:
                return self.fail_new(op, waiting[0], 'was not routed before its deadline')
            return False
        if op['kind'] == 'replace':
            self.save(op, phase='replacement_serving')
            return True
        self.finish(op, 'serving')
        return False

    def fail_new(self, op, name, reason):
        """A new member that never serves is quarantined and destroyed."""
        log('member_failed', id=op['id'], member=name, reason=reason)
        self.quarantine(name, op)
        self.save(op, phase='remove_planned_pending', member=name, failure=reason)
        return True

    # replace: the failed member leaves only after its replacement serves.
    def phase_replacement_serving(self, op):
        old = self.member(op['member'])
        if old['intent'] in ('enrolled', 'draining'):
            self.quarantine(op['member'], op)
        if old.get('origin') == 'elastic':
            self.save(op, phase='remove_planned_pending')
            return True
        self.finish(op, 'replaced; static member awaits an operator')
        return False

    # scale_in
    def phase_scale_in_requested(self, op):
        record = INVENTORY.membership(self.state)
        others = INVENTORY.serving_recent(record, exclude={op['member']})
        if len(others) < INVENTORY.MIN_OTHER_SERVING:
            self.finish(op, 'refused: fewer than two other recent replicas serve')
            return False
        member = self.member(op['member'])
        if member['intent'] == 'enrolled':
            inv = self.inventory.load()
            self.inventory.write(inv['revision'], lambda i: INVENTORY.set_intent(i, op['member'], 'draining'),
                                 'actuator:' + op['id'])
        self.save(op, phase='draining')
        return True

    def phase_draining(self, op):
        record = INVENTORY.membership(self.state)
        observed = record['members'].get(op['member'], {})
        since = observed.get('drained_since_unix')
        if observed.get('rendered') or since is None or self.now() - since < DRAIN_SECONDS:
            return False
        if not self.quiet(op):
            return False
        others = INVENTORY.serving_recent(record, exclude={op['member']})
        if len(others) < INVENTORY.MIN_OTHER_SERVING:
            return False
        self.save(op, phase='drained')
        return True

    def quiet(self, op):
        """No request in flight and none admitted for QUIET_SECONDS."""
        member = self.member(op['member'])
        try:
            text = urllib.request.urlopen(f"http://{member['upstream']}/metrics", timeout=5).read().decode()
        except OSError:
            return True  # an unreachable draining member serves nobody
        values = {}
        for line in text.splitlines():
            for name in ('transparent_shard_query_queue_depth', 'transparent_shard_queries_total'):
                if line.startswith(name + '{') or line.startswith(name + ' '):
                    values[name] = float(line.split()[-1])
        queries = values.get('transparent_shard_queries_total')
        last = op.get('quiet_queries')
        if values.get('transparent_shard_query_queue_depth', 1) != 0 or queries != last:
            self.save(op, quiet_queries=queries, quiet_since=self.now())
            return False
        return self.now() - op.get('quiet_since', self.now()) >= QUIET_SECONDS

    def phase_drained(self, op):
        self.remote.run(self.member(op['member'])['ssh_host'], 'systemctl disable --now transparent-shard-server')
        self.save(op, phase='stopped')
        return True

    def phase_stopped(self, op):
        inv = self.inventory.load()
        self.inventory.write(inv['revision'], lambda i: INVENTORY.set_intent(i, op['member'], 'retired'),
                             'actuator:' + op['id'])
        self.save(op, phase='remove_planned_pending')
        return True

    # removal of an elastic droplet (scale_in, failed scale_out, replace)
    def phase_remove_planned_pending(self, op):
        member = self.member(op['member'])
        if member['intent'] == 'quarantined':
            inv = self.inventory.load()
            self.inventory.write(inv['revision'], lambda i: INVENTORY.set_intent(i, op['member'], 'retired'),
                                 'actuator:' + op['id'])
        if len(self.history_destroys()) >= self.policy().get('daily_destroys', 2):
            return self.fence(op, 'daily destroy budget spent; destroy manually or wait')
        keep = [n for n in self.elastic_members() if n != op['member']]
        plan, digest = self.elastic.plan(self.member_specs(keep), {}, self.terraform_vars())
        summary = self.validate_plan(plan, destroy=[op['member']], destroy_ids=[member['droplet_id']])
        self.save(op, phase='remove_planned', plan=str(plan), plan_sha256=digest, plan_summary=summary)
        return True

    def phase_remove_planned(self, op):
        if self.disabled():
            return False
        op = self.save(op, phase='removing')
        self.elastic.apply(Path(op['plan']), op['plan_sha256'])
        return self.resolve_removed(op)

    def phase_removing(self, op):
        return self.resolve_removed(op)

    def resolve_removed(self, op):
        member = self.member(op['member'])
        state = self.elastic.state_members()
        droplets = {str(d['id']) for d in self.do.droplets(tag=self.c['worker_tag'])}
        if op['member'] not in state and str(member['droplet_id']) not in droplets:
            op['destroyed'] = member['droplet_id']
            outcome = {'scale_in': 'removed', 'scale_out': 'failed; new member destroyed',
                       'replace': 'replaced'}[op['kind']]
            self.save(op, phase='removed')
            self.finish(op, outcome)
            return False
        if op['member'] not in state and str(member['droplet_id']) in droplets:
            return self.fence(op, 'droplet still exists after Terraform forgot it')
        # Still in state: the apply did not finish; plan again next run.
        self.save(op, phase='remove_planned_pending')
        return True

    # -- helpers ---------------------------------------------------------
    def member(self, member_id):
        return next(m for m in self.inventory.load()['members'] if m['id'] == member_id)

    def quarantine(self, member_id, op):
        inv = self.inventory.load()
        if self.member(member_id)['intent'] in ('enrolled', 'draining'):
            self.inventory.write(inv['revision'], lambda i: INVENTORY.set_intent(i, member_id, 'quarantined'),
                                 'actuator:' + op['id'])

    def elastic_members(self, extra=()):
        """Names that must exist after the next apply: every elastic member not
        yet destroyed, plus `extra`. Retired members stay until their destroy."""
        state = set(self.elastic.state_members())
        names = {m['id'] for m in self.inventory.load()['members'] if m.get('origin') == 'elastic'
                 and (m['intent'] != 'retired' or m['id'] in state)}
        return sorted(names | set(extra))

    def member_specs(self, names):
        """Terraform's view of each member. Existing droplets keep their recorded
        size and image, so a plan can never resize or rebuild a serving host."""
        state = self.elastic.state_members()
        specs = {}
        for name in names:
            known = state.get(name) or {}
            specs[name] = {'size': known.get('size') or self.c['size'], 'image': known.get('image') or self.c['image']}
        return specs

    def allocate_names(self, count):
        used = {m['id'] for m in self.inventory.load()['members']}
        used |= set(self.elastic.state_members())
        used |= {d['name'] for d in self.do.droplets(tag=self.c['worker_tag'])}
        ordinals = [int(NAME.match(n).group(1)) for n in used if NAME.match(n)]
        first = max(ordinals, default=0) + 1
        return [f'transparent-pir-recent-{i:02d}' for i in range(first, first + count)]

    def generate_host_key(self, op, name):
        directory = self.journal/'keys'/op['id']
        directory.mkdir(parents=True, exist_ok=True)
        os.chmod(directory, 0o700)
        path = directory/name
        if not path.exists():
            subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-C', name, '-f', str(path)], check=True)
        return {'public': ' '.join(Path(str(path) + '.pub').read_text().split()[:2]), 'private_path': str(path)}

    def host_keys(self, op):
        return {name: {'private': Path(key['private_path']).read_text(), 'public': key['public']}
                for name, key in op['host_keys'].items()}

    def terraform_vars(self):
        return {k: self.c[k] for k in ('region', 'vpc_uuid', 'project_id', 'ssh_key_ids', 'deploy_public_key',
                                       'worker_tag') if k in self.c}

    def validate_plan(self, plan, create=(), destroy=(), destroy_ids=()):
        shown = self.elastic.show(plan)
        with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False) as f:
            json.dump(shown, f)
        try:
            # The validator checks every create against the whole pinned profile.
            args = [sys.executable, str(SCRIPTS/'transparent-plan.py'), '--plan-json', f.name,
                    '--size', self.c['size'], '--image', self.c['image'], '--region', self.c['region'],
                    '--vpc-uuid', self.c['vpc_uuid'], '--tag', self.c['worker_tag'],
                    '--project-id', self.c['project_id']]
            for name in create:
                args += ['--allow-create', name]
            for name in destroy:
                args += ['--allow-destroy', name]
            for droplet_id in destroy_ids:
                args += ['--allow-destroy-id', str(droplet_id)]
            result = subprocess.run(args, capture_output=True, text=True)
        finally:
            os.unlink(f.name)
        if result.returncode:
            raise ActuatorError('saved plan refused: ' + (result.stderr or result.stdout).strip()[-500:])
        return json.loads(result.stdout)

    def fleet_release(self):
        """The release most rendered recent replicas run; new hosts get exactly it."""
        record = INVENTORY.membership(self.state)
        roster = {w['id']: w for w in json.loads(Path(self.fleet['roster']).read_text())}
        counts = {}
        for member_id in INVENTORY.serving_recent(record):
            try:
                sha = http_json(f"http://{roster[member_id]['upstream']}/v1/ready")['binary_sha256']
            except (OSError, KeyError, ValueError):
                continue
            counts[sha] = counts.get(sha, 0) + 1
        if not counts:
            raise ActuatorError('no serving recent replica reports its binary')
        sha = max(counts, key=counts.get)
        for sums in sorted(Path(self.c['release_store']).glob('*/SHA256SUMS')):
            for line in sums.read_text().splitlines():
                digest, _, name = line.partition('  ')
                if name.strip() == 'transparent-shard-server' and digest == sha:
                    return {'sha': sums.parent.name, 'binary_sha256': sha, 'dir': str(sums.parent)}
        raise ActuatorError('no release in the store holds the fleet binary ' + sha[:12])


class Remote:
    """SSH to fleet hosts with the fleet key and strictly pinned host keys."""

    def __init__(self, fleet):
        self.args = ['ssh', '-oBatchMode=yes', '-oConnectTimeout=5', '-oStrictHostKeyChecking=yes',
                     '-oUserKnownHostsFile=' + fleet['known_hosts'], '-i', fleet['ssh_key']]

    def run(self, host, command, data=None, timeout=120):
        result = subprocess.run(self.args + ['root@' + host, command], input=data, capture_output=True, timeout=timeout)
        if result.returncode:
            raise ActuatorError(f'{host}: {command.split()[0]} failed: '
                                + result.stderr.decode(errors='replace')[-500:])
        return result.stdout.decode()

    def copy(self, host, sources, destination, bwlimit=None, files_from=None, timeout=3600):
        args = ['rsync', '-a', '-e', shlex.join(self.args)]
        if bwlimit:
            args.append(f'--bwlimit={bwlimit}')
        if files_from:
            args += ['--files-from', files_from]
        result = subprocess.run(args + [str(s) for s in sources] + ['root@' + host + ':' + destination],
                                capture_output=True, timeout=timeout)
        if result.returncode:
            raise ActuatorError(f'{host}: rsync failed: ' + result.stderr.decode(errors='replace')[-500:])


def bootstrap(actuator, op, name):
    """Install a freshly created member. True when its service has started;
    False to retry on the next run. Every step is safe to repeat."""
    remote, droplet = actuator.remote, op['droplets'][name]
    host = droplet['ip']
    try:
        remote.run(host, 'cloud-init status --wait >/dev/null', timeout=900)
    except (ActuatorError, subprocess.TimeoutExpired) as error:
        log('bootstrap_waiting', member=name, reason=str(error)[:200])
        return False
    identity = remote.run(host, 'curl -s --max-time 3 http://169.254.169.254/metadata/v1/id').strip()
    if identity != droplet['id']:
        raise ActuatorError(f'{name}: metadata droplet id {identity!r} is not {droplet["id"]}')
    remote.run(host, "for f in avx2 bmi2 fma; do grep -qw $f /proc/cpuinfo || { echo missing $f >&2; exit 1; }; done")
    release = op['release']
    staging = '/opt/transparent-pir/staged'
    remote.run(host, f'mkdir -p {staging} /opt/transparent-pir/assignments /opt/transparent-publisher '
                     f"{actuator.c['publication_root']}")
    remote.copy(host, [Path(release['dir'])/'transparent-shard-server', Path(release['dir'])/'shard-control'], staging + '/')
    remote.run(host, f"echo '{release['binary_sha256']}  {staging}/transparent-shard-server' | sha256sum -c --quiet && "
                     f'install -m755 {staging}/transparent-shard-server /usr/local/bin/transparent-shard-server && '
                     f'install -m755 {staging}/shard-control /usr/local/bin/shard-control')
    plan = newest_plan_naming(actuator.state, name)
    if plan is None:
        log('bootstrap_waiting', member=name, reason='no publication has been planned with this member yet')
        return False
    digest, request, assignment = plan
    if not publication_fresh(actuator):
        # The bulk copy shares the coordinator's disk and uplink with
        # publication; never add to a publication that is already late.
        log('bootstrap_waiting', member=name, reason='publication freshness above the copy threshold')
        return False
    publication = f"{actuator.c['publication_root']}/{digest}"
    listing = subprocess.run([actuator.fleet['assign_binary'], 'files', '--shard-dir', request['directory'],
                              '--assignment', str(assignment), '--worker-id', name],
                             capture_output=True, check=True, text=True).stdout
    with tempfile.NamedTemporaryFile('w') as files:
        files.write(listing)
        files.flush()
        remote.run(host, f'mkdir -p {publication}')
        remote.copy(host, [str(request['directory']) + '/'], publication + '/', bwlimit=actuator.c['bwlimit_kbps'],
                    files_from=files.name)
    remote.run(host, f'cat > {publication}/assignment.json', data=assignment.read_bytes())
    unit = render_unit(actuator, name, publication)
    remote.run(host, 'cat > /etc/systemd/system/transparent-shard-server.service', data=unit.encode())
    for directory in unit_directories(unit):
        remote.run(host, 'mkdir -p ' + shlex.quote(directory))
    remote.run(host, 'systemctl daemon-reload && systemctl enable --now transparent-shard-server')
    deadline = time.time() + 20 * 60
    while time.time() < deadline:
        try:
            ready = http_json(f'http://{host}:8093/v1/ready')
            if ready.get('ready') and ready.get('binary_sha256') == release['binary_sha256']:
                log('member_installed', member=name, map_sha256=digest)
                return True
        except (OSError, ValueError):
            pass
        time.sleep(5)
    raise ActuatorError(f'{name} did not become ready within 20 minutes')


def publication_fresh(actuator):
    try:
        status = http_json(actuator.c.get('publisher_status', 'http://127.0.0.1:8094/v1/status'))
    except (OSError, ValueError):
        return False
    return status.get('phase') == 'serving' and status.get('freshness_seconds', 1e9) <= actuator.c['freshness_pause_seconds']


def unit_directories(unit):
    """Directories the worker expects to exist before its first start."""
    for line in unit.splitlines():
        if line.startswith('ExecStart='):
            args = shlex.split(line[len('ExecStart='):])
            for flag in ('--runtime-cache-dir', '--active-record'):
                if flag in args:
                    value = args[args.index(flag) + 1]
                    yield value if flag == '--runtime-cache-dir' else str(Path(value).parent)


def newest_plan_naming(state, name):
    """(digest, request, assignment path) of the newest planned publication that
    assigns `name`, or None."""
    plans = sorted(Path(state).glob('*.assignment.json'), key=lambda p: p.stat().st_mtime, reverse=True)
    for path in plans[:20]:
        try:
            assignment = json.loads(path.read_text())
        except ValueError:
            continue
        if any(w['id'] == name for w in assignment.get('workers', [])):
            digest = path.name.split('.')[0]
            request = read_json(Path(state)/f'{digest}.request.json')
            if request and Path(request['directory']).exists():
                return digest, request, path
    return None


def render_unit(actuator, name, publication):
    """A serving recent replica's unit, with this member's identity and first
    publication. Budgets and flags stay exactly those of the running fleet."""
    record = INVENTORY.membership(actuator.state)
    roster = {w['id']: w for w in json.loads(Path(actuator.fleet['roster']).read_text())}
    peer = sorted(m for m in INVENTORY.serving_recent(record) if m in roster)[0]
    unit = actuator.remote.run(roster[peer]['ssh_host'], 'cat /etc/systemd/system/transparent-shard-server.service')
    lines = []
    for line in unit.splitlines():
        if line.startswith('ExecStart='):
            args = shlex.split(line[len('ExecStart='):])
            replace = {'--worker-id': name, '--shard-dir': publication,
                       '--assignment': publication + '/assignment.json'}
            out, skip = [], False
            for index, arg in enumerate(args):
                if skip:
                    skip = False
                    continue
                if arg in replace:
                    out += [arg, replace[arg]]
                    skip = True
                elif arg.split('=', 1)[0] in replace:
                    out.append(arg.split('=', 1)[0] + '=' + replace[arg.split('=', 1)[0]])
                else:
                    out.append(arg)
            if '--worker-id' not in out:
                raise ActuatorError('the peer unit has no --worker-id to replace')
            line = 'ExecStart=' + shlex.join(out)
        lines.append(line)
    return '\n'.join(lines) + '\n'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('/opt/transparent-publisher/scaler/actuator.json'))
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('run', help='advance the open operation or start the pending request')
    out = sub.add_parser('scale-out')
    out.add_argument('--count', type=int, default=1)
    out.add_argument('--dry', action='store_true')
    scale_in = sub.add_parser('scale-in')
    scale_in.add_argument('--member', help='defaults to the highest-numbered elastic replica')
    replace = sub.add_parser('replace')
    replace.add_argument('member')
    sub.add_parser('status')
    sub.add_parser('resolve-apply', help='clear a fence after Terraform state and DigitalOcean were reconciled')
    sub.add_parser('abandon', help='close a fenced operation after manual cleanup')
    args = parser.parse_args(argv)
    config = json.loads(args.config.read_text())
    fleet = json.loads(Path(config['fleet_config']).read_text())
    actuator = build(config, fleet)
    if args.command == 'status':
        print(json.dumps({'operation': actuator.operation(), 'disabled': actuator.disabled(),
                          'recent': [op.get('outcome') for op in actuator.history()]}, indent=1))
        return
    if args.command == 'resolve-apply':
        op = actuator.operation()
        if not op or not op.get('fenced'):
            raise ActuatorError('no fenced operation')
        actuator.save(op, fenced=False, fence_reason=None)
    elif args.command == 'abandon':
        op = actuator.operation()
        if not op or not op.get('fenced'):
            raise ActuatorError('only a fenced operation can be abandoned')
        actuator.finish(op, 'abandoned by operator')
        return
    elif args.command == 'scale-out':
        op = actuator.start('scale_out', count=args.count)
        if args.dry:
            actuator.save(op, dry=True)
    elif args.command == 'scale-in':
        member = args.member or max((m['id'] for m in actuator.inventory.load()['members']
                                     if m.get('origin') == 'elastic' and m['intent'] == 'enrolled'), default=None)
        if member is None:
            raise ActuatorError('no enrolled elastic replica to remove')
        actuator.start('scale_in', member=member)
    elif args.command == 'replace':
        actuator.start('replace', member=args.member, count=1)
    op = actuator.run_once()
    print(json.dumps({'operation': op}, indent=1))


def build(config, fleet):
    """The production wiring: the pinned elastic root and the read-only
    DigitalOcean client, both from the runtime credential in the environment."""
    elastic = load_module('elastic', '../scaler/elastic.py')
    from wallet_pir_ops.digitalocean import DigitalOcean  # on sys.path via elastic.py
    work = Path(config['scaler_dir'])/'journal'/'terraform'
    root = elastic.ElasticRoot(config['elastic_root'], terraform=config.get('terraform', 'terraform'),
                               machine_id=Path('/etc/machine-id').read_text().strip(), work_dir=work)
    return Actuator(config, fleet, root, DigitalOcean(os.environ['DO_TOKEN_NEW_ORG']))


if __name__ == '__main__':
    try:
        main()
    except ActuatorError as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
