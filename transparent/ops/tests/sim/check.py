"""Run one simulated scenario and check the scaler's invariants.

Checked on every tick:
* serving recent replicas never fall below two once reached, except through
  injected faults, withdrawal or maintenance;
* the fleet never costs more than the monthly cap;
* at most one actuator operation (the actuator is structurally single).

Checked on every request the scaler writes:
* no open operation and no earlier request still waiting;
* every signal fresh by the fleet's ground truth: membership, publisher
  status, public publication within 180 s of the node, the metrics of every
  rendered replica, no maintenance or withdrawal;
* never an archive member; within the rolling daily action and destroy
  budgets and the cost cap; no scale-in within the scale-in cooldown of a
  scale-out.

Checked after the run, for fault-free scenarios with an accurate capacity
model: a step up that needs at most `max_step` more replicas is served by
enough replicas within 20 minutes; after a step down the tier scales in.
"""
import math

from scaler import decide as D

from .fleet import ARCHIVE, DAY, DECIDE_EVERY, HOUR, MINUTE, PRICE, STALE, T0, TICK, Fleet, SimScaler

DEFAULT = {
    'schema': 1, 'mode': 'act', 'min_recent': 2, 'max_recent': 6, 'size': 's-4vcpu-8gb',
    'capacity_qps_per_replica': 8.0, 'headroom': 1.5, 'max_step': 3, 'stale_seconds': 45, 'window_seconds': 300,
    'backstop': {'error_rate': 0.05, 'error_seconds': 120, 'p99_seconds': 1.4, 'p99_hold_seconds': 300},
    'scale_in': {'hold_seconds': 3600, 'p99_seconds': 0.8},
    'cooldown_out_seconds': 900, 'cooldown_in_seconds': 3600, 'replace_unhealthy_seconds': 900,
    'daily_actions': 6, 'daily_destroys': 2, 'monthly_cost_cap_usd': 400, 'paused_until_unix': None,
}


class Result:
    def __init__(self, scenario, fleet, scaler, serving):
        self.scenario = scenario
        self.violations = fleet.violations
        self.events = fleet.events
        self.requests = [e for e in fleet.events if e[1] == 'request']
        self.flags = scaler.flags
        self.decisions = scaler.decisions
        self.serving = serving
        self.max_serving = max(s for _, s in serving) if serving else 0
        self.final_serving = serving[-1][1] if serving else 0


def needed(policy, qps):
    raw = math.ceil(qps * policy['headroom'] / policy['capacity_qps_per_replica'] - 1e-9)
    return max(policy['min_recent'], min(policy['max_recent'], raw))


class Checks:
    def __init__(self, fleet, policy):
        self.fleet = fleet
        self.policy = D.validate_policy(policy)
        self.reached = False
        self.requests = []  # (t, action, member, destroy)

    def request(self, request, state):
        fleet, p, now = self.fleet, self.policy, self.fleet.now
        action = request['action']
        if fleet.actuator.op is not None:
            fleet.violation(f'{action} requested while operation {fleet.actuator.op["id"]} is open')
        if fleet.request is not None and fleet.request['decision_id'] not in fleet.actuator.consumed:
            fleet.violation(f'{action} requested while {fleet.request["decision_id"]} is unconsumed')
        member = request.get('member')
        target = fleet.workers.get(member)
        if member in ARCHIVE:
            fleet.violation(f'{action} names archive member {member}')
        elif member is not None and target is None:
            fleet.violation(f'{action} names unknown member {member}')
        if action == 'scale_in' and target is not None and target.origin != 'elastic':
            fleet.violation(f'scale_in names static member {member}')
        # Ground-truth freshness of every signal the decision depends on.
        membership = fleet.membership_cache
        if membership is None or now - membership['updated_unix'] > STALE:
            fleet.violation(f'{action} with a stale membership')
        if now - getattr(fleet, 'publisher_seen', -math.inf) > STALE:
            fleet.violation(f'{action} with a stale publisher status')
        if fleet.lag_since is not None and now - fleet.lag_since > 180 + TICK:
            fleet.violation(f'{action} while the public publication lags {now - fleet.lag_since:.0f} s')
        if fleet.in_window('maintenance') or fleet.withdrawn:
            fleet.violation(f'{action} during maintenance or withdrawal')
        for w in fleet.rendered():
            if w.last_scrape_ok is None or now - w.last_scrape_ok > STALE:
                fleet.violation(f'{action} while {w.id} metrics are stale')
            if now - w.boot_t < 60:
                fleet.violation(f'{action} with {w.id} restarted {now - w.boot_t:.0f} s ago')
        destroy = action == 'scale_in' or (action == 'replace' and target is not None and target.origin == 'elastic')
        self.requests.append((now, action, member, destroy))
        recent = [r for r in self.requests if now - r[0] < DAY]
        if len(recent) > p['daily_actions']:
            fleet.violation(f'{len(recent)} actions in 24 h')
        if sum(1 for r in recent if r[3]) > p['daily_destroys']:
            fleet.violation('destroy budget exceeded')
        adds = request.get('count') or (1 if action == 'replace' else 0)
        cost = PRICE * (sum(1 for w in fleet.workers.values() if w.droplet) + adds)
        if cost > p['monthly_cost_cap_usd']:
            fleet.violation(f'{action} would cost {cost} a month')
        if action == 'scale_in':
            for t, other, _, _ in self.requests:
                if other == 'scale_out' and now - t < p['cooldown_in_seconds']:
                    fleet.violation(f'scale_in {now - t:.0f} s after a scale_out (flapping)')

    def tick(self):
        fleet, p = self.fleet, self.policy
        serving = fleet.serving()
        faulted = [w for w in fleet.workers.values() if w.intent == 'enrolled' and w.faulted and w not in serving]
        if len(serving) >= 2:
            self.reached = True
        excused = fleet.withdrawn or fleet.in_window('maintenance')
        if self.reached and not excused and len(serving) + len(faulted) < 2:
            fleet.violation(f'serving recent {len(serving)} (+{len(faulted)} faulted) below 2')
        cost = PRICE * sum(1 for w in fleet.workers.values() if w.droplet)
        if cost > p['monthly_cost_cap_usd']:
            fleet.violation(f'fleet costs {cost} a month')
        if any(w.id in ARCHIVE for w in fleet.workers.values()):
            fleet.violation('archive member in the recent fleet')


def run(scenario, decide=None, setup=None):
    """Run `scenario`; `decide` replaces the scaler's decision (mutation tests),
    `setup(fleet)` adjusts the fleet before the first tick."""
    policy = {**DEFAULT, **(scenario.policy or {})}
    fleet = Fleet(scenario, policy)
    if setup is not None:
        setup(fleet)
    scaler = SimScaler(fleet, policy, decide)
    checks = Checks(fleet, policy)
    fleet.check_request = checks.request
    ticks = int(scenario.hours * HOUR / TICK)
    serving = []
    for i in range(ticks):
        fleet.tick()
        scaler.scrape()
        if i % DECIDE_EVERY == DECIDE_EVERY - 1:
            scaler.decide()
        fleet.actuator.tick(fleet.now)
        checks.tick()
        serving.append((fleet.now, len(fleet.serving())))
    result = Result(scenario, fleet, scaler, serving)
    after_run(result, fleet, checks.policy)
    return result


def serving_at(serving, t):
    value = serving[0][1]
    for when, count in serving:
        if when > t:
            break
        value = count
    return value


def after_run(result, fleet, p):
    sc = result.scenario
    if sc.faults or sc.capacity != 1.0:
        return
    end = T0 + sc.hours * HOUR
    requests = [(t, a) for t, _, a, *_ in result.requests]
    ops = [(t, kind) for t, kind, *_ in result.events if kind in ('op-start', 'op-done')]
    for at, before, after in sc.trace.steps:
        s = T0 + at
        if after > before:
            want = needed(p, after)
            have = serving_at(result.serving, s)
            busy = any(s - p['cooldown_out_seconds'] - 15 * MINUTE <= t < s for t, _ in ops)
            spent = sum(1 for t, _ in requests if s - DAY < t <= s) >= p['daily_actions']
            if want - have <= p['max_step'] and not busy and not spent and s + 20 * MINUTE <= end:
                got = serving_at(result.serving, s + 20 * MINUTE)
                if got < want:
                    fleet.violation(f'step to {after} qps at {at:.0f}s: {got} serving 20 min later, {want} needed')
        else:
            want = needed(p, after)
            have = serving_at(result.serving, s)
            deadline = s + max(p['scale_in']['hold_seconds'], p['cooldown_in_seconds']) + 45 * MINUTE
            destroyed = sum(1 for t, a in requests if s - DAY < t <= s and a == 'scale_in')
            if have > want and deadline <= end and destroyed < p['daily_destroys']:
                if not any(s < t <= deadline and a == 'scale_in' for t, a in requests):
                    fleet.violation(f'no scale-in within {deadline - s:.0f} s of the load dropping to {after} qps')
                if serving_at(result.serving, end) >= have:
                    fleet.violation(f'still {have} serving at the end after the load dropped')
