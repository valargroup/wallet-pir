"""A deterministic recent-tier fleet for the scaler, on a virtual clock.

One `Scenario` is a seed, a load trace, a capacity error and a fault profile.
`run(scenario)` advances a fake fleet in 15-second ticks and runs the real
`signals.Collector` and `decide.decide` against it exactly as the daemon does
(scrape every tick, decide every fourth), with a fake actuator consuming the
scaler's requests. It returns the actions taken and every invariant
violation observed; the test asserts there are none.

The fleet:

* Recent replicas serve an M/M/1-like queue: capacity `mu` qps each, latency
  15 ms plus an exponential sojourn at rate `mu - lambda`, deadline 10 s;
  overload beyond capacity is refused (queue rejections).
* Blocks arrive as a Poisson process (mean 75 s); each publication activates
  20-40 s later and, for the 9 s before it activates, every serving replica
  rebuilds its tail runtimes: capacity drops to 70% and the ~17% of queries that touch the tail
  wait 0.5-5 s. That is ~2% slow queries and a plain window p99 of ~2.7 s at
  any load, as measured live on 2026-09-29.
* A new replica provisions in ~3 min, installs in ~1 min, warms in 3-5 min
  and attests at the next publication.
* Faults: crashes (restarting or not), replicas that never warm, slow status
  (failed scrapes), correlated crash bursts, reconciler and publisher stalls,
  an unreachable publisher, an archive owner outage (withdrawal), a fenced
  Terraform apply and a maintenance window.

The actuator honours one operation at a time, consumes each decision id once,
journals phases in the contract's order before their side effects, and
guards every stop and destroy itself.
"""
import math
import random

from scaler import decide as D
from scaler import metrics as M
from scaler import signals as S

T0 = 1_790_000_000.0
TICK = 15.0
DECIDE_EVERY = 4
MINUTE = 60.0
HOUR = 3600.0
DAY = 86400.0
BOUNDS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 600.0,
          math.inf)
PRICE = 48.0
ARCHIVE = ('transparent-pir-archive-01', 'transparent-pir-archive-02')
SLOTS = 2
DEADLINE = 10.0
BASE_LATENCY = 0.015
TAIL_FRACTION = 0.167
STALL_SECONDS = 9.0
STALL_CAPACITY = 0.7
# Stalled queries: uniform over 0.5-5 s.
SLOW_SHARE = {0.5: 0.0, 1.0: 1 / 9, 2.0: 3 / 9, 5.0: 1.0}
# Saturated queues: served queries wait 1-10 s.
SATURATED_SHARE = {1.0: 0.0, 2.0: 0.2, 5.0: 0.7, 10.0: 1.0}
STALE = 45.0


def cumulative_share(share, le):
    """Fraction of a piecewise-uniform distribution at or below `le`."""
    keys = sorted(share)
    if le < keys[0]:
        return 0.0
    for low, high in zip(keys, keys[1:]):
        if le < high:
            return share[low] + (share[high] - share[low]) * (le - low) / (high - low)
    return 1.0


SLOW_CDF = [cumulative_share(SLOW_SHARE, le) for le in BOUNDS]
OFFSETS = [le - BASE_LATENCY for le in BOUNDS[:-1]]
SATURATED_CDF = [cumulative_share(SATURATED_SHARE, le) for le in BOUNDS]


class Worker:
    """One recent replica: droplet, process, cache warmth, counters."""

    def __init__(self, member_id, origin, mu):
        self.id, self.origin, self.mu = member_id, origin, mu
        self.intent = 'enrolled'
        self.droplet = True
        self.alive = False
        self.start = None
        self.counters = dict.fromkeys(M.COUNTERS, 0.0)
        self.buckets = [0.0] * len(BOUNDS)
        self.target = 40.0
        self.warm = 0.0
        self.warm_rate = 0.0
        self.ready = False
        self.attesting = False
        self.never_warms = False
        self.dead = False
        self.restart_at = None
        self.faulted = False
        self.metrics_down_until = -math.inf
        self.last_scrape_ok = None
        self.boot_t = None
        self.unrendered_since = None
        self.retire_at = None

    def boot(self, now, warm_seconds):
        self.alive = True
        self.start = now
        self.boot_t = now
        self.counters = dict.fromkeys(M.COUNTERS, 0.0)
        self.buckets = [0.0] * len(BOUNDS)
        self.warm = 0.0
        self.warm_rate = self.target / warm_seconds
        self.ready = self.attesting = False

    def crash(self):
        self.alive = self.ready = self.attesting = False

    def progress(self, dt):
        if not self.alive or self.ready:
            return
        limit = 0.3 * self.target if self.never_warms else self.target
        gain = min(self.warm_rate * dt, max(0.0, limit - self.warm))
        if gain > 0:
            self.warm += gain
            self.counters['prewarm_ops'] += gain
        if self.warm >= self.target:
            self.ready = True

    def serve(self, dt, lam, overlap):
        mu = self.mu * (1 - (1 - STALL_CAPACITY) * overlap / dt)
        n = lam * dt
        buckets = self.buckets
        if lam >= 0.95 * mu:
            served = 0.95 * mu * dt
            deadline = 0.03 * mu * dt
            rejected = max(0.0, n - served - deadline)
            for i, share in enumerate(SATURATED_CDF):
                buckets[i] += served * share
        else:
            gap = mu - lam
            tail = math.exp(-gap * (DEADLINE - BASE_LATENCY))
            deadline = n * tail
            served = n - deadline
            rejected = 0.0
            slow = min(served, TAIL_FRACTION * lam * overlap)
            fast = served - slow
            scale = fast / max(1e-12, 1 - tail)
            exp = math.exp
            for i, offset in enumerate(OFFSETS):
                below = scale * (1 - exp(-gap * offset)) if offset > 0 else 0.0
                buckets[i] += (below if below < fast else fast) + slow * SLOW_CDF[i]
            buckets[-1] += served
        counters = self.counters
        counters['queries'] += served
        counters['queue_rejections'] += rejected
        counters['deadline_exceeded'] += deadline
        counters['slot_busy_us'] += min(lam, mu) / self.mu * SLOTS * dt * 1e6

    def sample(self, now):
        gauges = {'slots': float(SLOTS), 'warm_runtimes': self.warm, 'target_runtimes': self.target,
                  'cache_resident_bytes': 4.0e9 * self.warm / self.target, 'cache_budget_bytes': 5.0e9,
                  'cgroup_memory_bytes': 5.5e9, 'cgroup_memory_max_bytes': 7.5e9}
        # Workers export integer counts; the model accumulates fractions.
        counters = {k: float(int(v)) for k, v in self.counters.items()}
        latency = tuple(zip(BOUNDS, map(float, map(int, self.buckets))))
        return M.Sample(t=now, start=self.start, counters=counters, gauges=gauges, latency=latency, complete=True)


class Actuator:
    """One journaled operation at a time; consumes each decision id once."""

    def __init__(self, fleet):
        self.fleet = fleet
        self.op = None
        self.done = []
        self.consumed = set()
        self.sequence = 0

    def journal(self):
        op = self.op
        if op is None:
            return None
        return {'id': op['id'], 'action': op['action'], 'phase': op['phase'], 'decision_id': op['decision_id'],
                'started_unix': op['started'], 'fenced': op['fenced_until'] > self.fleet.now,
                'member': op.get('member')}

    def tick(self, now):
        fleet = self.fleet
        request = fleet.request
        if self.op is None and request is not None and request['decision_id'] not in self.consumed:
            self.consumed.add(request['decision_id'])
            self.sequence += 1
            self.op = {'id': f'op-{self.sequence}', 'action': request['action'], 'phase': 'requested',
                       'since': now, 'started': now, 'decision_id': request['decision_id'],
                       'count': request.get('count'), 'member': request.get('member'), 'new': [],
                       'fenced_until': -math.inf, 'deadline': now + 30 * MINUTE}
            fleet.log('op-start', self.op['id'], request['action'], request.get('count') or request.get('member'))
        for _ in range(12):  # several instantaneous phases may pass in one tick
            if self.op is None or not self.step(now):
                break

    def finish(self, now, outcome):
        op = self.op
        self.done.append({'id': op['id'], 'decision_id': op['decision_id'], 'action': op['action'],
                          'outcome': outcome, 'completed_unix': now})
        self.fleet.log('op-done', op['id'], op['action'], outcome)
        self.op = None

    def advance(self, phase, now):
        self.op['phase'] = phase
        self.op['since'] = now
        return True

    def step(self, now):
        op, fleet = self.op, self.fleet
        if op['fenced_until'] > now:
            return False
        elapsed = now - op['since']
        action, phase = op['action'], op['phase']
        if action == 'scale_in':
            return self.step_scale_in(op, phase, elapsed, now)
        if action == 'replace' and phase == 'requested':
            target = fleet.workers.get(op['member'])
            if op['member'] in ARCHIVE:
                fleet.violation('actuator asked to replace an archive member')
            if target is None or target.intent != 'enrolled':
                self.finish(now, 'refused')
                return False
            op['count'] = 1
        # scale_out, and the scale_out half of replace.
        if phase == 'requested':
            return self.advance('planned', now) if elapsed >= 10 else False
        if phase == 'planned':
            if fleet.faults.get('fence') and not fleet.fenced_once:
                fleet.fenced_once = True
                op['fenced_until'] = now + fleet.rng.uniform(120, 600)
            return self.advance('applying', now)
        if phase == 'applying':
            if 'provision_delay' not in op:
                op['provision_delay'] = fleet.rng.uniform(150, 210)
            return self.advance('provisioned', now) if elapsed >= op['provision_delay'] else False
        if phase == 'provisioned':
            for _ in range(op['count']):
                op['new'].append(fleet.enroll().id)
            return self.advance('enrolled', now)
        if phase == 'enrolled':
            return self.advance('bootstrapping', now) if elapsed >= 10 else False
        if phase == 'bootstrapping':
            if elapsed < 60:
                return False
            for member_id in op['new']:
                fleet.boot(fleet.workers[member_id], new=True)
            return self.advance('installed', now)
        if phase == 'installed':
            if all(fleet.workers[m].id in fleet.rendered_ids() for m in op['new']):
                return self.advance('serving', now)
            if now >= op['deadline']:
                self.finish(now, 'deadline_exceeded')
            return False
        if phase == 'serving':
            if action == 'scale_out':
                self.finish(now, 'serving')
                return False
            return self.advance('quarantined', now)
        # replace: the failed member leaves only once its replacement serves.
        failed = fleet.workers[op['member']]
        if phase == 'quarantined':
            if not all(m in fleet.rendered_ids() for m in op['new']):
                fleet.violation(f"replace quarantines {failed.id} before its replacement serves")
            failed.intent = 'quarantined'
            fleet.inventory_changed()
            if failed.origin == 'static':
                failed.retire_at = now + 2 * HOUR  # an operator removes it later
                self.finish(now, 'awaiting_operator')
                return False
            return self.advance('stopped', now)
        if phase == 'stopped':
            failed.crash()
            return self.advance('destroy-planned', now)
        if phase == 'destroy-planned':
            return self.advance('destroy-applying', now) if elapsed >= 10 else False
        if phase == 'destroy-applying':
            if elapsed < 60:
                return False
            fleet.destroy(failed)
            self.finish(now, 'destroyed')
            return False
        raise AssertionError(phase)

    def step_scale_in(self, op, phase, elapsed, now):
        fleet = self.fleet
        victim = fleet.workers.get(op['member'])
        if phase == 'requested':
            if op['member'] in ARCHIVE:
                fleet.violation('actuator asked to scale in an archive member')
            others = [w for w in fleet.serving() if w.id != op['member']]
            if victim is None or victim.origin != 'elastic' or victim.intent != 'enrolled' or len(others) < 2:
                self.finish(now, 'refused')
                return False
            victim.intent = 'draining'
            fleet.inventory_changed()
            return self.advance('draining', now)
        if phase == 'draining':
            if victim.id in fleet.rendered_ids():
                victim.unrendered_since = None
                return False
            victim.unrendered_since = victim.unrendered_since or now
            return self.advance('drained', now) if now - victim.unrendered_since >= 120 else False
        if phase == 'drained':
            # A serving replica is stopped only if two others serve.
            others = [w for w in fleet.serving() if w.id != victim.id]
            if len(others) < 2:
                victim.intent = 'enrolled'
                fleet.inventory_changed()
                self.finish(now, 'cancelled')
                return False
            return self.advance('stopped', now)
        if phase == 'stopped':
            victim.crash()
            return self.advance('retired', now)
        if phase == 'retired':
            victim.intent = 'retired'
            fleet.inventory_changed()
            return self.advance('planned', now)
        if phase == 'planned':
            return self.advance('applying', now) if elapsed >= 10 else False
        if phase == 'applying':
            if elapsed < 60:
                return False
            if victim.intent != 'retired':
                fleet.violation(f'destroy of {victim.id} before retire')
            fleet.destroy(victim)
            self.finish(now, 'destroyed')
            return False
        raise AssertionError(phase)


class Scenario:
    def __init__(self, seed, trace, hours, faults=(), capacity=1.0, policy=None, name=None):
        self.seed, self.trace, self.hours = seed, trace, hours
        self.faults = {f: True for f in faults} if not isinstance(faults, dict) else dict(faults)
        self.capacity = capacity
        self.policy = policy
        self.name = name or f'{trace.kind}-{seed}'


class Fleet:
    def __init__(self, scenario, policy):
        self.sc = scenario
        self.rng = random.Random(scenario.seed)
        self.policy = D.validate_policy(policy)
        self.now = T0
        self.mu = 16.0 * scenario.capacity
        self.faults = scenario.faults
        self.workers = {}
        self.ordinal = 0
        self.revision = 1
        self._inventory = None
        self.request = None
        self.actuator = Actuator(self)
        self.events = []
        self.violations = []
        self.fenced_once = False
        self.archive_down_until = {a: -math.inf for a in ARCHIVE}
        self.withdrawn = False
        self.generation = 1
        # Publication.
        self.node = self.public = 1000
        self.next_block = T0 + self.rng.uniform(0, 75)
        self.activations = []
        self.lag_since = None
        self.windows = {k: [] for k in ('reconciler_stall', 'publisher_stall', 'publisher_down', 'maintenance')}
        self.crashes = []  # (time, ids, restart after or None)
        self.slow_status = []  # (time, member index, seconds)
        self.membership_cache = None
        for _ in range(2):
            worker = self.enroll(origin='static')
            worker.boot(T0 - HOUR, 1.0)
            worker.warm, worker.ready, worker.attesting = worker.target, True, True
        self.plan_faults()

    # -- bookkeeping ----------------------------------------------------------------
    def log(self, *event):
        self.events.append((self.now,) + event)

    def violation(self, text):
        self.violations.append(f'{self.now - T0:.0f}s: {text}')

    def inventory_changed(self):
        self.revision += 1
        self._inventory = None

    def enroll(self, origin='elastic'):
        self.ordinal += 1
        worker = Worker(f'transparent-pir-recent-{self.ordinal:02d}', origin, self.mu)
        if origin == 'elastic' and self.faults.get('never_warms') and self.rng.random() < 0.5:
            worker.never_warms = True
        self.workers[worker.id] = worker
        self.inventory_changed()
        return worker

    def boot(self, worker, new=False):
        worker.boot(self.now, self.rng.uniform(180, 300))
        if new and worker.never_warms:
            worker.faulted = True

    def destroy(self, worker):
        worker.crash()
        worker.droplet = False
        if worker.intent != 'retired':
            worker.intent = 'retired'
        self.inventory_changed()

    def inventory(self):
        if self._inventory is None:
            members = [{'id': a, 'role': 'archive-owner', 'group': f'a{i}', 'origin': 'static',
                        'intent': 'enrolled', 'ssh_host': a, 'upstream': a + ':8093'}
                       for i, a in enumerate(ARCHIVE)]
            for w in self.workers.values():
                members.append({'id': w.id, 'role': 'recent-replica', 'group': 'recent', 'origin': w.origin,
                                'intent': w.intent, 'ssh_host': w.id, 'upstream': w.id + ':8093',
                                'size': 's-4vcpu-8gb'})
            self._inventory = {'schema': S.INVENTORY_SCHEMA, 'revision': self.revision, 'members': members}
        return self._inventory

    # -- fleet state -----------------------------------------------------------------
    def roster(self):
        return [w for w in self.workers.values() if w.intent in ('enrolled', 'draining')]

    def archive_up(self):
        return all(self.now >= self.archive_down_until[a] for a in ARCHIVE)

    def rendered(self):
        if self.withdrawn or not self.archive_up():
            return []
        routed = [w for w in self.roster() if w.alive and w.attesting]
        enrolled = [w for w in routed if w.intent == 'enrolled']
        return enrolled or [w for w in routed if w.intent == 'draining']

    def rendered_ids(self):
        return {w.id for w in self.rendered()}

    def serving(self):
        return [w for w in self.rendered() if w.intent == 'enrolled']

    def in_window(self, kind, t=None):
        t = self.now if t is None else t
        return any(start <= t < end for start, end in self.windows[kind])

    def membership(self):
        if self.in_window('reconciler_stall') and self.membership_cache is not None:
            return self.membership_cache
        rendered = self.rendered_ids()
        members = {}
        for a in ARCHIVE:
            up = self.now >= self.archive_down_until[a]
            members[a] = {'role': 'archive-owner', 'intent': 'enrolled', 'state': 'serving' if up else 'unreachable',
                          'routed': up, 'rendered': up and not self.withdrawn, 'observed_unix': self.now}
        for w in self.roster():
            if not w.alive:
                state = 'unreachable' if w.start is not None else 'booting'
            else:
                state = 'serving' if w.attesting else ('prepared' if w.ready else 'warming')
            members[w.id] = {'role': 'recent-replica', 'intent': w.intent, 'state': state,
                             'routed': w.alive and w.attesting, 'rendered': w.id in rendered,
                             'warm': w.ready, 'observed_unix': self.now, 'transport_failures': 0}
        self.membership_cache = {'schema': 1, 'updated_unix': self.now, 'members': members,
                                 'routing_generation': self.generation}
        return self.membership_cache

    def inputs(self):
        return {'errors': {}, 'inventory': self.inventory(), 'membership': self.membership(),
                'maintenance': {'enabled': self.in_window('maintenance')}, 'withdrawn': {'withdrawn': self.withdrawn},
                'operation': self.actuator.journal(), 'operation_mtime': None, 'disabled': False,
                'done': self.actuator.done[-32:]}

    def publisher_status(self):
        return {'phase': 'serving', 'public_height': self.public, 'node_height': self.node,
                'freshness_seconds': 30.0, 'ready_replicas': len(self.serving())}

    # -- faults -----------------------------------------------------------------------
    def plan_faults(self):
        rng, end = self.rng, T0 + self.sc.hours * HOUR
        def when(margin=HOUR):
            return rng.uniform(T0 + 30 * MINUTE, max(T0 + 31 * MINUTE, end - margin))
        f = self.faults
        if f.get('crash'):
            for _ in range(int(f['crash']) if f['crash'] is not True else 1):
                restart = rng.uniform(2, 10) * MINUTE if rng.random() < 0.5 else None
                self.crashes.append((when(), 1, restart))
        if f.get('burst'):
            self.crashes.append((when(), 2, rng.uniform(5, 15) * MINUTE))
        if f.get('slow_status'):
            for _ in range(3):
                self.slow_status.append((when(), rng.randrange(8), rng.uniform(60, 180)))
        for kind, low, high in (('reconciler_stall', 60, 240), ('publisher_stall', 200, 400),
                                ('publisher_down', 60, 120), ('maintenance', 600, 1800)):
            if f.get(kind):
                start = when()
                self.windows[kind].append((start, start + rng.uniform(low, high)))
        if f.get('archive_crash'):
            self.archive_crash = (when(), rng.uniform(3, 8) * MINUTE)
        self.crashes.sort()
        self.slow_status.sort()

    def apply_faults(self):
        now = self.now
        while self.crashes and self.crashes[0][0] <= now:
            _, count, restart = self.crashes.pop(0)
            candidates = [w for w in self.serving()]
            self.rng.shuffle(candidates)
            for w in candidates[:count]:
                w.crash()
                w.faulted = True
                w.restart_at = None if restart is None else now + restart
                w.dead = restart is None
                self.log('crash', w.id, 'restarts' if restart else 'dead')
        while self.slow_status and self.slow_status[0][0] <= now:
            _, index, seconds = self.slow_status.pop(0)
            serving = self.serving()
            if serving:
                w = serving[index % len(serving)]
                w.metrics_down_until = now + seconds
                self.log('slow-status', w.id, seconds)
        for w in self.workers.values():
            if w.restart_at is not None and now >= w.restart_at and w.droplet:
                w.restart_at = None
                self.boot(w)
                self.log('restart', w.id)
        crash = getattr(self, 'archive_crash', None)
        if crash and crash[0] <= now:
            self.archive_down_until[ARCHIVE[0]] = now + crash[1]
            self.archive_crash = None
            self.log('archive-down', ARCHIVE[0])
        was = self.withdrawn
        self.withdrawn = not self.archive_up()
        if was and not self.withdrawn:
            self.generation += 1
        # The operator removes quarantined static members some time later.
        for w in self.workers.values():
            if w.retire_at is not None and now >= w.retire_at:
                w.retire_at = None
                self.destroy(w)
                self.log('operator-retired', w.id)

    # -- one tick ---------------------------------------------------------------------
    def tick(self):
        t0, t = self.now, self.now + TICK
        self.now = t
        self.apply_faults()
        while self.next_block <= t:
            self.node += 1
            self.activations.append([self.next_block + self.rng.uniform(20, 40), self.node])
            self.next_block += max(5.0, self.rng.expovariate(1 / 75.0))
        for activation in self.activations:
            for start, end in self.windows['publisher_stall']:
                if start <= activation[0] < end:
                    activation[0] = end + self.rng.uniform(5, 20)
        overlap = 0.0
        for when, _ in self.activations:
            overlap += max(0.0, min(when, t) - max(when - STALL_SECONDS, t0))
        overlap = min(overlap, TICK)
        lam = self.sc.trace(t0 - T0)
        rendered = self.rendered()
        for w in rendered:
            w.serve(TICK, lam / len(rendered), overlap)
        for w in self.workers.values():
            w.progress(TICK)
        due = [a for a in self.activations if a[0] <= t]
        self.activations = [a for a in self.activations if a[0] > t]
        if due and not self.withdrawn:
            self.public = max(h for _, h in due)
            self.generation += 1
            for w in self.roster():
                if w.alive and w.ready and not w.attesting:
                    w.attesting = True
                    if not w.never_warms:
                        w.faulted = False
        self.lag_since = (self.lag_since or t) if self.node > self.public else None


class SimScaler:
    """The daemon's loop over the fleet, without files."""

    def __init__(self, fleet, policy, decide=None):
        self.fleet = fleet
        self.policy = policy
        self.decide_fn = decide or D.decide
        self.collector = S.Collector(keep_seconds=1800.0)
        self.state = D.initial_state(f'{fleet.sc.seed:032x}')
        self.decisions = 0
        self.flags = set()

    def scrape(self):
        fleet, now = self.fleet, self.fleet.now
        for w in fleet.roster():
            if w.alive and now >= w.metrics_down_until:
                self.collector.record_sample(w.id, w.sample(now))
                w.last_scrape_ok = now
            else:
                self.collector.record_failure(w.id, now, 'unreachable')
        if fleet.in_window('publisher_down'):
            self.collector.record_publisher(None, now, 'publisher unreachable')
        else:
            self.collector.record_publisher(fleet.publisher_status(), now)
            fleet.publisher_seen = now

    def decide(self):
        fleet, now = self.fleet, self.fleet.now
        snapshot = self.collector.snapshot(fleet.inputs(), self.policy, now)
        decision, self.state = self.decide_fn(snapshot, self.policy, self.state, now)
        self.decisions += 1
        self.flags.update(f.split(':')[0] for f in decision['flags'])
        if decision['action'] != 'hold' and 'decision_id' in decision:
            request = {'schema': 1, 'decision_id': decision['decision_id'], 'created_unix': now,
                       'action': decision['action'], 'reason': decision['reason']}
            if decision['action'] == 'scale_out':
                request['count'] = decision['count']
            else:
                request['member'] = decision['member']
            fleet.check_request(request, self.state)
            fleet.request = request
            fleet.log('request', decision['action'], decision.get('count') or decision.get('member'),
                      decision['reason'])
        return decision
