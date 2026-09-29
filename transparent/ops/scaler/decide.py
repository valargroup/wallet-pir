"""The recent-tier scaler's decision: `decide(snapshot, policy, state, now)`.

Pure: no I/O, no clock, no randomness. Given a snapshot from
`signals.Collector.snapshot`, the operator's policy, the previous scaler state
and the current time it returns `(decision, new_state)`:

    decision = {'action': 'hold' | 'scale_out' | 'scale_in' | 'replace',
                'count': int (scale_out), 'member': id (scale_in, replace),
                'reason': str, 'flags': [str], 'decision_id': uuid (acting modes)}

The rules, in order (transparent/docs/elastic-recent.md is the contract):

1. Holds that stop everything: invalid policy or inputs, kill switch, pause,
   maintenance, withdrawal, a stale or unknown reconciler, publisher or
   inventory, a public publication more than 180 s behind the node, an open
   or fenced actuator operation, a request not yet consumed, and any stale or
   unknown signal for a member that serves or carries traffic.
2. Replacement: a recent member that has not attested, and shown no warm
   progress, for `replace_unhealthy_seconds` is replaced make-before-break,
   unless another member is also unhealthy (correlated failures hold and
   flag). A failed static member is replaced by an elastic one and listed in
   `awaiting_operator`: only an operator removes its droplet.
3. Scale-out: desired = clamp(ceil(offered × headroom / capacity), min, max);
   when desired exceeds serving plus pending members for one confirmation
   period, add the deficit, at most `max_step`. Backstops add one when the
   error ratio or p99 stays above its limit, and flag capacity-model drift
   when the model alone saw no deficit. Cooldown from the completion of the
   last scale-out; a scale-out that lowered the load per replica but did not
   improve p99 or errors within 15 minutes holds further scale-outs and
   flags.
4. Scale-in: desired below serving for `scale_in.hold_seconds`, zero
   rejections and p99 under `scale_in.p99_seconds` over the window, nothing
   warming, booting, draining or failed, `cooldown_in_seconds` since the last
   action: drain the highest-ordinal elastic member, one at a time, never
   below max(min_recent, 2) serving and never a static member.

Latency is the snapshot's `p99_seconds`: the lower quartile over scrape
intervals of each interval's tier-wide p99 (`signals.sustained_p99`), so the
tail rebuild every replica does before each publication never trips a
backstop or blocks a scale-in on its own.

Every action must fit the rolling-24 h action and destroy budgets, the
monthly cost cap and 32 inventory members of every intent. Only `act-dry`
and `act` record actions (and so spend budget and start cooldowns);
`observe` and `recommend` report what would be done.
"""
from __future__ import annotations

import copy
import math
import re
import uuid

MODES = ('observe', 'recommend', 'act-dry', 'act')
ACTING = ('act-dry', 'act')
RECENT = 'recent-replica'
ARCHIVE = 'archive-owner'
DAY = 86400.0
MAX_MEMBERS = 32
PUBLIC_LAG_LIMIT = 180.0
NON_IMPROVEMENT_SECONDS = 900.0
DEFAULT_PRICES = {'s-4vcpu-8gb': 48.0}
NAMESPACE = uuid.UUID('4c0f3d2e-1f7a-5b1e-9a57-7e1a5c0de5ca')

DEFAULT_POLICY = {
    'min_recent': 2, 'max_recent': 6, 'size': 's-4vcpu-8gb', 'capacity_qps_per_replica': 8.0,
    'headroom': 1.5, 'max_step': 3, 'stale_seconds': 45, 'window_seconds': 300,
    'backstop': {'error_rate': 0.05, 'error_seconds': 120, 'p99_seconds': 1.4, 'p99_hold_seconds': 300},
    'scale_in': {'hold_seconds': 3600, 'p99_seconds': 0.8},
    'cooldown_out_seconds': 900, 'cooldown_in_seconds': 3600, 'replace_unhealthy_seconds': 900,
    'daily_actions': 6, 'daily_destroys': 2, 'monthly_cost_cap_usd': 400, 'paused_until_unix': None,
    # Not in the contract's example; defaults chosen here.
    'recent_window_seconds': 90, 'scale_out_confirm_seconds': 60, 'request_ttl_seconds': 600,
    'no_improvement_hold_seconds': 3600, 'forecast_alert_days': 30, 'latency_interval_quantile': 0.25,
}


class PolicyError(ValueError):
    pass


def _positive(policy, key, integer=False, zero=False):
    value = policy[key]
    kind = int if integer else (int, float)
    if isinstance(value, bool) or not isinstance(value, kind) or not math.isfinite(value):
        raise PolicyError(f'{key} must be a number')
    if value < 0 or (value == 0 and not zero):
        raise PolicyError(f'{key} must be positive')
    return value


def validate_policy(policy):
    """The policy with defaults filled in; raises PolicyError when unusable."""
    if not isinstance(policy, dict):
        raise PolicyError('policy is not an object')
    if policy.get('schema') != 1:
        raise PolicyError('policy schema must be 1')
    if policy.get('mode') not in MODES:
        raise PolicyError('mode must be one of ' + ', '.join(MODES))
    p = copy.deepcopy(DEFAULT_POLICY)
    for key, value in policy.items():
        if isinstance(value, dict) and isinstance(p.get(key), dict):
            p[key] = {**p[key], **value}
        else:
            p[key] = value
    for key in ('min_recent', 'max_recent', 'max_step', 'daily_actions'):
        _positive(p, key, integer=True)
    _positive(p, 'daily_destroys', integer=True, zero=True)
    for key in ('capacity_qps_per_replica', 'headroom', 'stale_seconds', 'window_seconds',
                'cooldown_out_seconds', 'cooldown_in_seconds', 'replace_unhealthy_seconds',
                'monthly_cost_cap_usd', 'recent_window_seconds', 'request_ttl_seconds',
                'no_improvement_hold_seconds', 'forecast_alert_days'):
        _positive(p, key)
    _positive(p, 'scale_out_confirm_seconds', zero=True)
    _positive(p, 'latency_interval_quantile')
    if p['latency_interval_quantile'] > 1:
        raise PolicyError('latency_interval_quantile must be in (0, 1]')
    for group, keys in (('backstop', ('error_rate', 'error_seconds', 'p99_seconds', 'p99_hold_seconds')),
                        ('scale_in', ('hold_seconds', 'p99_seconds'))):
        for key in keys:
            _positive(p[group], key, zero=key.endswith('_seconds') and key != 'p99_seconds')
    if p['min_recent'] < 2:
        raise PolicyError('min_recent must be at least 2: the recent tier never serves on fewer')
    if p['max_recent'] < p['min_recent'] or p['max_recent'] > MAX_MEMBERS:
        raise PolicyError('max_recent must be between min_recent and 32')
    if p['headroom'] < 1:
        raise PolicyError('headroom must be at least 1')
    if not isinstance(p['size'], str) or not p['size']:
        raise PolicyError('size must be a droplet size slug')
    prices = p.get('prices_usd_monthly', DEFAULT_PRICES)
    if not isinstance(prices, dict) or not all(
            isinstance(k, str) and isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0
            for k, v in prices.items()):
        raise PolicyError('prices_usd_monthly must map sizes to positive prices')
    p['prices_usd_monthly'] = dict(prices)
    paused = p.get('paused_until_unix')
    if paused is not None and (isinstance(paused, bool) or not isinstance(paused, (int, float))):
        raise PolicyError('paused_until_unix must be null or a unix time')
    return p


def initial_state(instance='0' * 32):
    """A new scaler state. `instance` makes decision ids unique across state resets."""
    return {'schema': 1, 'instance': instance, 'sequence': 0, 'actions': [], 'last_action_unix': None,
            'last_complete_unix': None, 'last_scale_out_complete_unix': None, 'request': None,
            'pending_eval': None, 'no_improvement_until': None, 'out_since': None, 'below_since': None,
            'error_breach_since': None, 'p99_breach_since': None, 'members': {}, 'awaiting_operator': []}


def ordinal(member_id):
    match = re.search(r'(\d+)$', member_id)
    return int(match.group(1)) if match else -1


def _fmt(value, digits=1):
    return 'unknown' if value is None else f'{value:.{digits}f}'


def _hold(reason, flags):
    return {'action': 'hold', 'reason': reason, 'flags': flags}


def price(policy, size):
    return policy['prices_usd_monthly'].get(size or policy['size'])


def budget(state, snapshot, policy, now):
    recent = [a for a in state['actions'] if now - a['unix'] < DAY]
    cost, unknown = 0.0, []
    for member_id, member in snapshot.get('members', {}).items():
        if member['role'] != RECENT or member['intent'] == 'retired':
            continue
        value = price(policy, member.get('size'))
        if value is None:
            unknown.append(member.get('size') or policy['size'])
        else:
            cost += value
    return {'actions_left': policy['daily_actions'] - len(recent),
            'destroys_left': policy['daily_destroys'] - sum(1 for a in recent if a.get('destroy')),
            'monthly_cost_usd': None if unknown else round(cost, 2),
            'unknown_sizes': sorted(set(unknown))}


def _breached(load, backstop):
    p99, err = load.get('p99_seconds'), load.get('error_ratio')
    return ((p99 is not None and p99 > backstop['p99_seconds'])
            or (err is not None and err > backstop['error_rate']))


def _improved(load, baseline, backstop):
    if not _breached(load, backstop):
        return True
    p99, err = load.get('p99_seconds'), load.get('error_ratio')
    return ((p99 is not None and baseline.get('p99') is not None and p99 < baseline['p99'])
            or (err is not None and baseline.get('error_ratio') is not None and err < baseline['error_ratio']))


def _track_request(st, snapshot, policy, now, flags):
    """Follow our last request through the actuator journal."""
    request = st.get('request')
    if request is None or not snapshot.get('journal_ok', False):
        return
    operation = snapshot.get('operation')
    completed = request['decision_id'] in (snapshot.get('done_decision_ids') or [])
    if not completed and operation is not None:
        if operation.get('decision_id') == request['decision_id']:
            request['consumed'] = True
        elif (operation.get('decision_id') is None and operation.get('age_seconds') is not None
              and now - operation['age_seconds'] >= request['created_unix'] - 1):
            request['consumed'] = True
    if not completed and request['consumed'] and (
            operation is None or (operation.get('decision_id') not in (None, request['decision_id']))):
        completed = True
    if completed:
        st['request'] = None
        st['last_complete_unix'] = now
        if request['action'] == 'scale_out':
            st['last_scale_out_complete_unix'] = now
            st['pending_eval'] = {'complete_unix': now, 'baseline': request.get('baseline') or {}}
        # Breaches seen by the fleet before the change are no evidence against the new one.
        st['error_breach_since'] = st['p99_breach_since'] = None
        return
    if not request['consumed'] and now - request['created_unix'] > policy['request_ttl_seconds']:
        flags.append(f"request {request['decision_id']} not consumed within "
                     f"{policy['request_ttl_seconds']:.0f} s")
        st['request'] = None


def _track_members(st, snapshot, policy, now):
    """Unhealthy-since per enrolled member, updated only from a fresh membership."""
    trackers = st.setdefault('members', {})
    members = snapshot.get('members', {})
    for member_id in list(trackers):
        if member_id not in members:
            del trackers[member_id]
    for member_id, member in members.items():
        tracker = trackers.setdefault(member_id, {'unhealthy_since': None, 'first_seen_unix': now})
        if member['attesting'] or member['intent'] != 'enrolled':
            tracker['unhealthy_since'] = None
        elif tracker['unhealthy_since'] is None:
            tracker['unhealthy_since'] = now


def _global_holds(snapshot, policy, st, now, flags):
    holds = []
    stale = policy['stale_seconds']
    for error in snapshot.get('errors') or []:
        holds.append('input: ' + error)
    if snapshot.get('disabled'):
        holds.append('kill switch: scaler/disabled exists')
    paused = policy.get('paused_until_unix')
    if paused is not None and now < paused:
        holds.append(f'paused until {paused:.0f}')
    if snapshot.get('maintenance') is None:
        holds.append('maintenance state unknown')
    elif snapshot['maintenance']:
        holds.append('fleet maintenance')
    if snapshot.get('withdrawn') is None:
        holds.append('withdrawal state unknown')
    elif snapshot['withdrawn']:
        holds.append('publication withdrawn')
    membership = snapshot.get('membership')
    if membership is None or membership.get('age_seconds') is None:
        holds.append('membership unknown')
    elif not 0 <= membership['age_seconds'] <= stale:
        holds.append(f"membership stale ({membership['age_seconds']:.0f} s)")
    if snapshot.get('inventory') is None:
        holds.append('inventory unknown')
    publisher = snapshot.get('publisher') or {}
    if publisher.get('serving') is None or publisher.get('age_seconds') is None:
        holds.append('publisher status unknown' + (f" ({publisher['error']})" if publisher.get('error') else ''))
    elif not 0 <= publisher['age_seconds'] <= stale:
        holds.append(f"publisher status stale ({publisher['age_seconds']:.0f} s)")
    elif not publisher['serving']:
        holds.append(f"publisher not serving (phase {publisher.get('phase')})")
    elif publisher.get('lag_seconds') is None:
        holds.append('public publication age unknown')
    elif publisher['lag_seconds'] > PUBLIC_LAG_LIMIT:
        holds.append(f"public publication {publisher['lag_seconds']:.0f} s behind the node")
    if not snapshot.get('journal_ok', False):
        holds.append('actuator journal unreadable')
    operation = snapshot.get('operation')
    if operation is not None:
        holds.append(f"actuator operation open: {operation.get('id')} {operation.get('action')} "
                     f"phase {operation.get('phase')} ({_fmt(operation.get('age_seconds'), 0)} s)")
        if operation.get('fenced'):
            holds.append(f"actuator operation fenced: {operation.get('id')}")
            flags.append(f"operation {operation.get('id')} fenced: resolve-apply required")
        if operation.get('deadline_exceeded'):
            flags.append(f"operation {operation.get('id')} exceeded its deadline")
    request = st.get('request')
    if request is not None and not request.get('consumed'):
        holds.append(f"request {request['decision_id']} awaiting the actuator")
    for member_id, member in sorted(snapshot.get('members', {}).items()):
        if member['role'] != RECENT or not (member['serving'] or member['rendered']):
            continue
        age = member.get('observed_age_seconds')
        if age is None or not 0 <= age <= stale:
            holds.append(f'stale signal: {member_id} (membership observation {_fmt(age, 0)} s)')
            continue
        sample = member.get('sample_age_seconds')
        if sample is None or not 0 <= sample <= stale:
            detail = member.get('metrics_error') or f'sample {_fmt(sample, 0)} s old'
            holds.append(f'stale signal: {member_id} (metrics: {detail})')
        elif member.get('window') is None:
            detail = member.get('metrics_error') or 'window too short since start or counter reset'
            holds.append(f'stale signal: {member_id} (metrics: {detail})')
    return holds


def decide(snapshot, policy, state, now):
    """One decision. See the module docstring for the rules."""
    st = copy.deepcopy(state) if state else initial_state()
    for key, value in initial_state(st.get('instance', '0' * 32)).items():
        st.setdefault(key, value)
    flags = []
    try:
        p = validate_policy(policy)
    except PolicyError as error:
        reason = f'policy invalid: {error}'
        st['summary'] = {'mode': None, 'holds': [reason], 'flags': [reason], 'desired_recent': None,
                         'serving_recent': None, 'offered_qps': None, 'budget': None,
                         'awaiting_operator': st.get('awaiting_operator', [])}
        return _hold(reason, [reason]), st
    st['actions'] = [a for a in st['actions'] if now - a['unix'] < 2 * DAY]
    members = snapshot.get('members', {})
    load = snapshot.get('load') or {}
    membership = snapshot.get('membership') or {}
    membership_fresh = (membership.get('age_seconds') is not None
                        and 0 <= membership['age_seconds'] <= p['stale_seconds'])

    _track_request(st, snapshot, p, now, flags)
    if membership_fresh:
        _track_members(st, snapshot, p, now)
    trackers = st['members']

    def unhealthy_for(member_id):
        since = trackers.get(member_id, {}).get('unhealthy_since')
        return None if since is None else now - since

    recent = {m: v for m, v in members.items() if v['role'] == RECENT}
    serving = sorted(m for m, v in recent.items() if v['serving'])
    failed = []
    for member_id, member in recent.items():
        duration = unhealthy_for(member_id)
        progress = member.get('last_progress_unix')
        if (member['intent'] == 'enrolled' and not member['attesting'] and duration is not None
                and duration >= p['replace_unhealthy_seconds']
                and (progress is None or now - progress >= p['replace_unhealthy_seconds'])):
            failed.append(member_id)
    failed.sort(key=lambda m: (-(unhealthy_for(m) or 0), m))
    unhealthy = sorted(m for m, v in members.items()
                       if v['intent'] == 'enrolled' and not v['attesting']
                       and (unhealthy_for(m) or 0) >= p['stale_seconds'])
    pending = sorted(m for m, v in recent.items()
                     if v['intent'] == 'enrolled' and not v['serving'] and m not in failed)
    current = len(serving) + len(pending)

    # Awaiting operator: static recent members quarantined or named in a replace.
    awaiting = {m for m in st.get('awaiting_operator', []) if m in members}
    awaiting |= {m for m, v in recent.items() if v['static'] and v['intent'] == 'quarantined'}

    money = budget(st, snapshot, p, now)
    unit_price = price(p, p['size'])

    # Capacity model.
    offered, offered_recent = load.get('offered_qps'), load.get('offered_qps_recent')
    model_qps = None if offered is None else max(offered, offered_recent if offered_recent is not None else 0.0)
    desired = None
    if model_qps is not None:
        raw = math.ceil(model_qps * p['headroom'] / p['capacity_qps_per_replica'] - 1e-9)
        desired = max(p['min_recent'], min(p['max_recent'], raw))

    # Timers that need continuity. Unknown resets them: never act on a guess.
    backstop = p['backstop']
    err, p99 = load.get('error_ratio'), load.get('p99_seconds')
    load_known = model_qps is not None
    st['error_breach_since'] = ((st['error_breach_since'] or now)
                                if load_known and err is not None and err > backstop['error_rate'] else None)
    st['p99_breach_since'] = ((st['p99_breach_since'] or now)
                              if load_known and p99 is not None and p99 > backstop['p99_seconds'] else None)
    st['out_since'] = (st['out_since'] or now) if desired is not None and desired > current else None
    st['below_since'] = ((st['below_since'] or now)
                         if desired is not None and desired < len(serving) else None)

    # Did the last scale-out help? Only a scale-out that lowered the load per
    # serving replica can be judged: when demand outgrew the added capacity,
    # a p99 that did not improve says nothing about whether scaling helps.
    evaluation = st.get('pending_eval')
    if evaluation is not None and load_known:
        base = evaluation['baseline']
        per_replica = model_qps / max(1, len(serving))
        outgrown = base.get('per_replica_qps') is not None and per_replica >= 0.95 * base['per_replica_qps']
        if _improved(load, base, backstop):
            st['pending_eval'] = None
        elif now - evaluation['complete_unix'] >= NON_IMPROVEMENT_SECONDS:
            st['pending_eval'] = None
            if not outgrown:
                st['no_improvement_until'] = now + p['no_improvement_hold_seconds']
    until = st.get('no_improvement_until')
    if until is not None and (now >= until or (load_known and not _breached(load, backstop))):
        st['no_improvement_until'] = until = None
    if until is not None:
        flags.append('scale-out did not improve p99 or errors within 15 min; further scale-outs held')

    holds = _global_holds(snapshot, p, st, now, flags)
    decision = None
    if holds:
        decision = _hold('; '.join(holds), flags)

    # Replacement comes before any load decision.
    if decision is None and failed:
        target = failed[0]
        if len(unhealthy) >= 2:
            reason = 'correlated failures: ' + ', '.join(unhealthy)
            flags.append(reason)
            holds.append(reason)
        else:
            elastic = not recent[target]['static']
            blocked = _add_blockers(p, money, snapshot, unit_price, 1, flags)
            if elastic and money['destroys_left'] < 1:
                blocked.append('daily destroy budget exhausted')
                flags.append('daily destroy budget exhausted')
            if blocked:
                holds.extend(f'replace {target}: {b}' for b in blocked)
            else:
                decision = {'action': 'replace', 'member': target, 'flags': flags,
                            'reason': f'{target} has not attested for {unhealthy_for(target):.0f} s '
                                      f'without warm progress'}
        if decision is None:
            decision = _hold('; '.join(holds), flags)

    if decision is None and not load_known:
        unknown = load.get('unknown') or []
        holds.append('load unknown' + (': ' + ', '.join(unknown) if unknown else ''))
        decision = _hold('; '.join(holds), flags)

    if decision is None:
        decision = _scale_out(st, p, load, desired, current, serving, pending, money, snapshot,
                              unit_price, now, flags, holds)
    if decision is None:
        decision = _scale_in(st, p, load, desired, serving, recent, failed, money, now, flags, holds)
    if decision is None:
        reason = (f'steady: desired {desired}, serving {len(serving)}'
                  f"{f', pending {len(pending)}' if pending else ''}, offered {_fmt(model_qps)} qps")
        decision = _hold('; '.join(holds) if holds else reason, flags)

    if decision['action'] != 'hold' and p['mode'] in ACTING:
        st['sequence'] += 1
        decision_id = str(uuid.uuid5(NAMESPACE, f"{st['instance']}:{st['sequence']}:{now!r}"))
        decision['decision_id'] = decision_id
        destroy = decision['action'] == 'scale_in' or (
            decision['action'] == 'replace' and not recent[decision['member']]['static'])
        st['actions'].append({'unix': now, 'action': decision['action'], 'count': decision.get('count'),
                              'member': decision.get('member'), 'decision_id': decision_id,
                              'destroy': destroy})
        st['last_action_unix'] = now
        st['request'] = {'decision_id': decision_id, 'action': decision['action'], 'created_unix': now,
                         'consumed': False, 'count': decision.get('count'), 'member': decision.get('member'),
                         'baseline': {'p99': p99, 'error_ratio': err,
                                      'per_replica_qps': None if model_qps is None
                                      else model_qps / max(1, len(serving))}}
        st['out_since'] = st['below_since'] = None
        money = budget(st, snapshot, p, now)
        if decision['action'] == 'replace' and recent[decision['member']]['static']:
            # Its droplet stays until an operator removes it.
            awaiting.add(decision['member'])
    st['awaiting_operator'] = sorted(awaiting)
    if st['awaiting_operator']:
        flags.append('awaiting operator: ' + ', '.join(st['awaiting_operator']))
    decision['flags'] = list(dict.fromkeys(flags))
    st['summary'] = {
        'mode': p['mode'], 'holds': holds if decision['action'] == 'hold' else [],
        'flags': decision['flags'], 'desired_recent': desired, 'serving_recent': len(serving),
        'pending_recent': len(pending), 'failed': failed, 'unhealthy': unhealthy,
        'offered_qps': offered, 'offered_qps_recent': offered_recent, 'budget': money,
        'awaiting_operator': st['awaiting_operator'],
    }
    return decision, st


def _add_blockers(p, money, snapshot, unit_price, count, flags):
    """Why adding `count` members is refused (empty when it is allowed)."""
    blocked = []
    if money['actions_left'] < 1:
        blocked.append('daily action budget exhausted')
        flags.append('daily action budget exhausted')
    total = (snapshot.get('inventory') or {}).get('total_members')
    if total is None or total + count > MAX_MEMBERS:
        blocked.append(f'inventory would exceed {MAX_MEMBERS} members')
        flags.append(f'inventory member limit {MAX_MEMBERS} reached')
    if unit_price is None or money['monthly_cost_usd'] is None:
        blocked.append('droplet price unknown')
        flags.append('droplet price unknown for ' + ', '.join(money['unknown_sizes'] or [p['size']]))
    elif money['monthly_cost_usd'] + count * unit_price > p['monthly_cost_cap_usd'] + 1e-9:
        blocked.append(f"monthly cost cap {p['monthly_cost_cap_usd']} USD")
        flags.append(f"monthly cost cap {p['monthly_cost_cap_usd']} USD reached")
    return blocked


def _scale_out(st, p, load, desired, current, serving, pending, money, snapshot, unit_price, now, flags, holds):
    model_count = 0
    if desired > current and st['out_since'] is not None and now - st['out_since'] >= p['scale_out_confirm_seconds']:
        model_count = desired - current
    backstop = p['backstop']
    fired = []
    if st['error_breach_since'] is not None and now - st['error_breach_since'] >= backstop['error_seconds']:
        fired.append(f"error ratio {load['error_ratio']:.3f} > {backstop['error_rate']} for "
                     f"{now - st['error_breach_since']:.0f} s")
    if st['p99_breach_since'] is not None and now - st['p99_breach_since'] >= backstop['p99_hold_seconds']:
        fired.append(f"p99 {load['p99_seconds']:.2f} s > {backstop['p99_seconds']} s for "
                     f"{now - st['p99_breach_since']:.0f} s")
    if not model_count and not fired:
        if desired > current:
            holds.append(f"desired {desired} > {current}: confirming for {p['scale_out_confirm_seconds']:.0f} s")
        return None
    # A cooldown or a failed improvement holds before anything is judged:
    # the window still shows the fleet as it was before the last change.
    blocked = []
    last = st.get('last_scale_out_complete_unix')
    if last is not None and now - last < p['cooldown_out_seconds']:
        blocked.append(f"scale-out cooldown ({p['cooldown_out_seconds'] - (now - last):.0f} s left)")
    if st.get('no_improvement_until') is not None:
        blocked.append('last scale-out did not improve p99 or errors')
    if blocked:
        holds.extend('scale-out: ' + b for b in blocked)
        return _hold('; '.join(holds), flags)
    count = max(model_count, 1 if fired else 0)
    if fired and not model_count:
        flags.append('capacity model drift: ' + '; '.join(fired))
    room = p['max_recent'] - current
    if room <= 0:
        blocked.append(f"at max_recent {p['max_recent']}")
        if fired:
            flags.append(f"capacity ceiling: max_recent {p['max_recent']} reached while backstop fires")
    count = min(count, p['max_step'], max(room, 0))
    if not blocked:
        total = (snapshot.get('inventory') or {}).get('total_members')
        if total is not None:
            count = min(count, MAX_MEMBERS - total)
        if unit_price is not None and money['monthly_cost_usd'] is not None:
            affordable = math.floor((p['monthly_cost_cap_usd'] - money['monthly_cost_usd']) / unit_price + 1e-9)
            count = min(count, affordable)
        blocked = _add_blockers(p, money, snapshot, unit_price, max(count, 1), flags)
    if blocked:
        holds.extend('scale-out: ' + b for b in blocked)
        return _hold('; '.join(holds), flags)
    offered = max(load['offered_qps'], load.get('offered_qps_recent') or 0.0)
    reasons = []
    if model_count:
        reasons.append(f"offered {offered:.1f} qps x {p['headroom']} / {p['capacity_qps_per_replica']} "
                       f"needs {desired} > {len(serving)} serving + {len(pending)} pending")
    reasons.extend('backstop: ' + f for f in fired)
    return {'action': 'scale_out', 'count': count, 'reason': '; '.join(reasons), 'flags': flags}


def _scale_in(st, p, load, desired, serving, recent, failed, money, now, flags, holds):
    if desired >= len(serving):
        return None
    blocked = []
    held_for = 0 if st['below_since'] is None else now - st['below_since']
    if held_for < p['scale_in']['hold_seconds']:
        blocked.append(f"desired {desired} < {len(serving)} for {held_for:.0f} of "
                       f"{p['scale_in']['hold_seconds']:.0f} s")
    if load.get('rejections') is None or load['rejections'] > 0:
        blocked.append(f"rejections in window: {_fmt(load.get('rejections'), 0)}")
    p99 = load.get('p99_seconds')
    if p99 is None and load.get('latency_count') != 0:
        blocked.append('p99 unknown')
    elif p99 is not None and p99 >= p['scale_in']['p99_seconds']:
        blocked.append(f"p99 {p99:.2f} s >= {p['scale_in']['p99_seconds']} s")
    busy = sorted(m for m, v in recent.items()
                  if m in failed or v['intent'] == 'draining'
                  or (v['intent'] == 'enrolled' and not v['serving']))
    if busy:
        blocked.append('members warming, booting, draining or failed: ' + ', '.join(busy))
    last = max([t for t in (st.get('last_action_unix'), st.get('last_complete_unix')) if t is not None],
               default=None)
    if last is not None and now - last < p['cooldown_in_seconds']:
        blocked.append(f"cooldown after last action ({p['cooldown_in_seconds'] - (now - last):.0f} s left)")
    floor = max(p['min_recent'], 2)
    if len(serving) - 1 < floor:
        blocked.append(f'would leave fewer than {floor} serving')
    victims = sorted((m for m in serving if not recent[m]['static'] and recent[m]['intent'] == 'enrolled'),
                     key=lambda m: (ordinal(m), m))
    if not victims:
        blocked.append('no elastic member to remove')
    if money['actions_left'] < 1:
        blocked.append('daily action budget exhausted')
        flags.append('daily action budget exhausted')
    if money['destroys_left'] < 1:
        blocked.append('daily destroy budget exhausted')
        flags.append('daily destroy budget exhausted')
    if blocked:
        holds.extend('scale-in: ' + b for b in blocked)
        return _hold('; '.join(holds), flags)
    victim = victims[-1]
    return {'action': 'scale_in', 'member': victim, 'flags': flags,
            'reason': f"desired {desired} < {len(serving)} serving for {held_for:.0f} s, "
                      f"p99 {_fmt(p99, 2)} s, no rejections"}
