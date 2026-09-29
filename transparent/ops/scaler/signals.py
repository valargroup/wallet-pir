"""Signals for the recent-tier scaler: one snapshot of the fleet per decision.

Inputs (all on the coordinator; see transparent/docs/elastic-recent.md):

* `state/membership.json` (reconciler), `state/inventory.json` (intent),
  `state/maintenance.json`, `state/withdrawn.json`;
* `scaler/journal/operation.json` and `scaler/journal/done/` (actuator),
  `scaler/disabled` (kill switch);
* worker `/metrics` of the recent replicas in membership, scraped by
  `Collector.scrape` every few seconds into per-worker `metrics.History`;
* the publisher's `/v1/status`.

`read_inputs` does the file I/O; `Collector.snapshot` is deterministic given
the inputs and the scrape histories, so the simulator drives it directly.

Every value that cannot be established is None, and the reason is in
`snapshot['errors']` or on the member. Nothing unknown is reported as zero:
`decide` turns unknowns into holds.
"""
from __future__ import annotations

import concurrent.futures
import json
import math
import os
import re
import urllib.request
from pathlib import Path

from . import metrics as M

INVENTORY_SCHEMA = 'transparent-fleet-inventory-v1'
RECENT = 'recent-replica'
ARCHIVE = 'archive-owner'
INTENTS = ('enrolled', 'draining', 'retired', 'quarantined')
# Membership states in order of progress toward serving.
STATE_RANK = {'unreachable': 0, 'booting': 1, 'invalidated': 1, 'lagging': 2, 'warming': 3,
              'prepared': 4, 'serving': 5}
PUBLISHER_SERVING = ('serving', 'shadow_verified')
_ID = re.compile(r'^[A-Za-z0-9_.:-]+$')
MAX_INPUT_BYTES = 8 * 1024 * 1024


def _read_json(path):
    """(value, None), (None, None) if absent, or (None, error)."""
    try:
        with open(path, 'rb') as stream:
            raw = stream.read(MAX_INPUT_BYTES + 1)
    except FileNotFoundError:
        return None, None
    except OSError as error:
        return None, f'{path}: {type(error).__name__}: {error}'
    if len(raw) > MAX_INPUT_BYTES:
        return None, f'{path}: larger than {MAX_INPUT_BYTES} bytes'
    try:
        return json.loads(raw), None
    except ValueError as error:
        return None, f'{path}: invalid JSON: {error}'


def _mtime(path):
    try:
        return os.stat(path).st_mtime
    except OSError:
        return None


def read_inputs(state_dir, scaler_dir, done_limit=32):
    """Read every file input once. Returns a dict of raw values and errors."""
    state_dir, scaler_dir = Path(state_dir), Path(scaler_dir)
    inputs = {'errors': {}}
    for key, path in (('membership', state_dir / 'membership.json'),
                      ('inventory', state_dir / 'inventory.json'),
                      ('maintenance', state_dir / 'maintenance.json'),
                      ('withdrawn', state_dir / 'withdrawn.json'),
                      ('operation', scaler_dir / 'journal' / 'operation.json')):
        value, error = _read_json(path)
        inputs[key] = value
        inputs[key + '_present'] = value is not None or error is not None
        if error:
            inputs['errors'][key] = error
    inputs['operation_mtime'] = _mtime(scaler_dir / 'journal' / 'operation.json')
    inputs['disabled'] = (scaler_dir / 'disabled').exists()
    done = []
    try:
        entries = sorted((scaler_dir / 'journal' / 'done').glob('*.json'),
                         key=lambda p: (_mtime(p) or 0, p.name))[-done_limit:]
    except OSError as error:
        entries = []
        inputs['errors']['done'] = f'journal/done: {error}'
    for path in entries:
        value, error = _read_json(path)
        if isinstance(value, dict):
            done.append(value)
    inputs['done'] = done
    return inputs


def fetch_json(url, timeout=3.0):
    with urllib.request.urlopen(urllib.request.Request(url), timeout=timeout) as response:
        if response.status != 200:
            raise OSError(f'{url}: HTTP {response.status}')
        return json.loads(response.read(MAX_INPUT_BYTES))


def ordinal(member_id):
    """Trailing integer of an id (`…-recent-07` → 7); -1 when there is none."""
    match = re.search(r'(\d+)$', member_id)
    return int(match.group(1)) if match else -1


def validate_inventory(value):
    """Members by id from a valid inventory; raises ValueError otherwise."""
    if not isinstance(value, dict) or value.get('schema') != INVENTORY_SCHEMA:
        raise ValueError('inventory schema is not ' + INVENTORY_SCHEMA)
    if type(value.get('revision')) is not int:
        raise ValueError('inventory revision missing')
    members = value.get('members')
    if not isinstance(members, list):
        raise ValueError('inventory members missing')
    by_id = {}
    for member in members:
        if not isinstance(member, dict):
            raise ValueError('inventory member is not an object')
        member_id = member.get('id')
        if not isinstance(member_id, str) or not _ID.match(member_id):
            raise ValueError('inventory member id invalid')
        if member_id in by_id:
            raise ValueError('inventory member id repeated: ' + member_id)
        if member.get('role') not in (RECENT, ARCHIVE):
            raise ValueError(f'{member_id}: unknown role')
        if member.get('intent') not in INTENTS:
            raise ValueError(f'{member_id}: unknown intent')
        if member.get('origin') not in ('static', 'elastic'):
            raise ValueError(f'{member_id}: unknown origin')
        if member['role'] == ARCHIVE and member['origin'] != 'static':
            raise ValueError(f'{member_id}: archive members are static')
        by_id[member_id] = member
    return by_id


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def operation_summary(value, mtime, now):
    """The open actuator operation, normalised; None when there is none."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError('operation journal is not an object')
    if value.get('open') is False or value.get('phase') in ('done', 'completed'):
        return None
    started = next((_number(value.get(k)) for k in ('started_unix', 'created_unix', 'updated_unix')
                    if _number(value.get(k)) is not None), mtime)
    deadline = _number(value.get('deadline_unix'))
    members = value.get('members') if isinstance(value.get('members'), list) else []
    return {
        'id': str(value.get('id') or value.get('operation_id') or ''),
        'action': value.get('action'),
        'phase': value.get('phase'),
        'decision_id': value.get('decision_id'),
        'member': value.get('member'),
        'members': [m for m in members if isinstance(m, str)],
        'age_seconds': None if started is None else max(0.0, now - started),
        'fenced': bool(value.get('fenced')) or value.get('phase') == 'fenced',
        'deadline_exceeded': bool(value.get('deadline_exceeded'))
                             or (deadline is not None and now > deadline),
    }


class Collector:
    """Scrape histories and the small trackers a snapshot needs between cycles."""

    def __init__(self, keep_seconds=900.0, started=None):
        self.histories = {}
        self.keep_seconds = keep_seconds
        self.started = started
        self.progress = {}      # member id -> unix of last observed warm progress
        self.state_seen = {}    # member id -> last membership state
        self.publisher = None   # last /v1/status dict
        self.publisher_t = None
        self.publisher_error = None
        self.public = None      # (public_height, lag_since or None)

    # -- recording -----------------------------------------------------------------
    def history(self, member_id):
        if member_id not in self.histories:
            self.histories[member_id] = M.History(self.keep_seconds)
        return self.histories[member_id]

    def record_sample(self, member_id, sample):
        history = self.history(member_id)
        previous = history.latest()
        reset = history.add(sample)
        latest = history.latest()
        if not reset and previous is not None and latest is sample and _warm_progress(previous, sample):
            self.progress[member_id] = sample.t
        return reset

    def record_failure(self, member_id, t, error):
        self.history(member_id).failed(t, error)

    def record_publisher(self, status, t, error=None):
        if error is not None or not isinstance(status, dict):
            self.publisher_error = error or 'publisher status is not an object'
            return
        self.publisher, self.publisher_t, self.publisher_error = status, t, None
        public, node = status.get('public_height'), status.get('node_height')
        if type(public) is not int:
            return
        if self.public is None or self.public[0] != public:
            self.public = (public, None)
        if type(node) is int and node > public:
            if self.public[1] is None:
                self.public = (public, t)
        else:
            self.public = (public, None)

    def forget(self, keep_ids):
        for member_id in list(self.histories):
            if member_id not in keep_ids:
                del self.histories[member_id]
                self.progress.pop(member_id, None)
                self.state_seen.pop(member_id, None)

    # -- scraping (I/O) ------------------------------------------------------------
    @staticmethod
    def targets(inputs):
        """(member id, metrics URL) for each recent replica in membership."""
        membership = inputs.get('membership')
        inventory = inputs.get('inventory')
        try:
            by_id = validate_inventory(inventory)
        except ValueError:
            by_id = {}
        out = []
        members = membership.get('members') if isinstance(membership, dict) else None
        if not isinstance(members, dict):
            return out
        for member_id, observed in sorted(members.items()):
            if not isinstance(observed, dict) or observed.get('role') != RECENT:
                continue
            upstream = (by_id.get(member_id) or {}).get('upstream')
            if isinstance(upstream, str) and _ID.match(upstream):
                out.append((member_id, f'http://{upstream}/metrics'))
        return out

    def scrape(self, targets, t, timeout=3.0, fetch=M.fetch, publisher_url=None, fetch_status=fetch_json):
        """Scrape every target (and the publisher) concurrently; never raises."""
        def one(target):
            member_id, url = target
            try:
                return member_id, fetch(url, timeout), None
            except Exception as error:  # noqa: BLE001 - any failure is an unknown sample
                return member_id, None, f'{type(error).__name__}: {error}'[:300]
        results = []
        if targets:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(16, len(targets))) as pool:
                results = list(pool.map(one, targets))
        for member_id, scrape, error in results:
            if scrape is None:
                self.record_failure(member_id, t, error)
                continue
            try:
                self.record_sample(member_id, M.worker_sample(scrape, t))
            except Exception as error:  # noqa: BLE001
                self.record_failure(member_id, t, f'{type(error).__name__}: {error}'[:300])
        if publisher_url:
            try:
                self.record_publisher(fetch_status(publisher_url, timeout), t)
            except Exception as error:  # noqa: BLE001
                self.record_publisher(None, t, f'{type(error).__name__}: {error}'[:300])

    # -- snapshot (deterministic) ----------------------------------------------------
    def snapshot(self, inputs, policy, now):
        stale = float(policy.get('stale_seconds', 45))
        window = float(policy.get('window_seconds', 300))
        recent_window = float(policy.get('recent_window_seconds', 90))
        errors = []
        for key, error in sorted((inputs.get('errors') or {}).items()):
            errors.append(error)
        snap = {'now': now, 'errors': errors, 'disabled': bool(inputs.get('disabled'))}

        # Inventory: the durable intent. Unreadable means unknown.
        inventory = None
        if inputs.get('inventory') is None:
            if 'inventory' not in (inputs.get('errors') or {}):
                errors.append('inventory missing')
        else:
            try:
                inventory = validate_inventory(inputs['inventory'])
            except ValueError as error:
                errors.append('inventory invalid: ' + str(error))
        snap['inventory'] = None if inventory is None else {
            'revision': inputs['inventory'].get('revision'), 'total_members': len(inventory)}

        # Membership: the reconciler's view, rewritten every second.
        membership = inputs.get('membership')
        observed = {}
        snap['membership'] = None
        if membership is None:
            if 'membership' not in (inputs.get('errors') or {}):
                errors.append('membership missing')
        elif not isinstance(membership, dict) or not isinstance(membership.get('members'), dict):
            errors.append('membership invalid')
        else:
            updated = _number(membership.get('updated_unix'))
            observed = {k: v for k, v in membership['members'].items() if isinstance(v, dict)}
            snap['membership'] = {
                'age_seconds': None if updated is None else now - updated,
                'routing_generation': membership.get('routing_generation'),
                'active_map_sha256': membership.get('active_map_sha256')}

        snap['maintenance'] = _flag(inputs, 'maintenance', 'enabled', errors)
        snap['withdrawn'] = _flag(inputs, 'withdrawn', 'withdrawn', errors)
        try:
            snap['operation'] = operation_summary(inputs.get('operation'), inputs.get('operation_mtime'), now)
            snap['journal_ok'] = 'operation' not in (inputs.get('errors') or {})
        except ValueError as error:
            snap['operation'], snap['journal_ok'] = None, False
            errors.append(str(error))
        snap['done_decision_ids'] = [d.get('decision_id') for d in inputs.get('done') or []
                                     if isinstance(d.get('decision_id'), str)]
        snap['publisher'] = self._publisher(now)

        # Members: inventory intent joined with membership observation and metrics.
        members = {}
        ids = set(inventory or {}) | {k for k, v in observed.items() if v.get('role') in (RECENT, ARCHIVE)}
        for member_id in sorted(ids):
            record = (inventory or {}).get(member_id)
            seen = observed.get(member_id)
            if record is not None and record.get('intent') == 'retired':
                continue
            members[member_id] = self._member(member_id, record, seen, now, stale, window, recent_window)
        snap['members'] = members
        self.forget({m for m, v in members.items() if v['role'] == RECENT})
        snap['load'] = self._load(members, window, recent_window,
                                  float(policy.get('latency_interval_quantile', 0.25)))
        return snap

    def _publisher(self, now):
        status = self.publisher
        value = {'phase': None, 'public_height': None, 'node_height': None, 'freshness_seconds': None,
                 'ready_replicas': None, 'lag_seconds': None, 'age_seconds': None,
                 'serving': None, 'error': self.publisher_error}
        if status is None or self.publisher_t is None:
            return value
        value.update(phase=status.get('phase'), public_height=status.get('public_height'),
                     node_height=status.get('node_height'),
                     freshness_seconds=_number(status.get('freshness_seconds')),
                     ready_replicas=status.get('ready_replicas'),
                     age_seconds=now - self.publisher_t,
                     serving=status.get('phase') in PUBLISHER_SERVING
                     and type(status.get('public_height')) is int)
        if self.public is not None and type(status.get('public_height')) is int:
            since = self.public[1]
            value['lag_seconds'] = 0.0 if since is None else max(0.0, now - since)
        return value

    def _member(self, member_id, record, seen, now, stale, window, recent_window):
        role = (record or {}).get('role') or (seen or {}).get('role')
        # Unknown origin is treated as static: it can never be scaled in.
        origin = (record or {}).get('origin', 'static')
        intent = (record or {}).get('intent')
        if intent is None and seen is not None:
            intent = seen.get('intent')
        state = (seen or {}).get('state')
        routed = (seen or {}).get('routed')
        rendered = (seen or {}).get('rendered', routed)
        attesting = bool(routed) if routed is not None else state == 'serving'
        observed_unix = _number((seen or {}).get('observed_unix'))
        value = {
            'role': role, 'origin': origin, 'static': origin != 'elastic', 'intent': intent,
            'size': (record or {}).get('size'), 'in_inventory': record is not None,
            'in_membership': seen is not None, 'state': state,
            'routed': routed, 'rendered': rendered, 'attesting': attesting,
            'serving': bool(intent == 'enrolled' and attesting and rendered is not False),
            'observed_age_seconds': None if observed_unix is None else now - observed_unix,
            'transport_failures': (seen or {}).get('transport_failures'),
            'sample_age_seconds': None, 'metrics_error': None, 'last_progress_unix': None,
            'resets': 0, 'window': None, 'recent': None, 'latest': None,
        }
        if state is not None:
            previous = self.state_seen.get(member_id)
            if previous is not None and STATE_RANK.get(state, 0) > STATE_RANK.get(previous, 0):
                self.progress[member_id] = now
            self.state_seen[member_id] = state
        if role != RECENT:
            return value
        history = self.histories.get(member_id)
        if history is not None:
            latest = history.latest()
            value['metrics_error'] = history.last_error
            value['resets'] = history.resets
            if latest is not None:
                value['sample_age_seconds'] = now - latest.t
                value['latest'] = latest
                value['window'] = history.window(window, min(60.0, window))
                value['recent'] = history.window(recent_window, min(0.75 * recent_window, recent_window))
                for key in ('slots', 'warm_runtimes', 'target_runtimes', 'cache_resident_bytes',
                            'cache_budget_bytes', 'cgroup_memory_bytes', 'cgroup_memory_max_bytes'):
                    value[key] = latest.gauges.get(key)
        else:
            value['metrics_error'] = 'not scraped'
        value['last_progress_unix'] = self.progress.get(member_id)
        # A sample that is too old is no sample.
        if value['sample_age_seconds'] is not None and value['sample_age_seconds'] > stale:
            value['window'] = value['recent'] = None
        return value

    @staticmethod
    def _load(members, window, recent_window, interval_quantile=0.25):
        """Tier-wide rates over the recent replicas that carry traffic."""
        traffic = {m: v for m, v in members.items()
                   if v['role'] == RECENT and (v['serving'] or v['rendered'])}
        load = {'members': sorted(traffic), 'unknown': sorted(m for m, v in traffic.items() if v['window'] is None),
                'offered_qps': None, 'offered_qps_recent': None, 'queries_per_second': None,
                'errors_per_second': None, 'error_ratio': None, 'rejections': None,
                'p50_seconds': None, 'p99_seconds': None, 'p99_window_seconds': None,
                'latency_intervals': 0, 'latency_count': None, 'utilization': None,
                'window_seconds': window}
        if not traffic or load['unknown']:
            return load
        windows = [v['window'] for v in traffic.values()]
        queries = math.fsum(w.rate('queries') for w in windows)
        rejected = math.fsum(w.rate('queue_rejections') for w in windows)
        errors = math.fsum(w.rate('queue_rejections') + w.rate('overloads') + w.rate('deadline_exceeded')
                           for w in windows)
        load['queries_per_second'] = queries
        load['offered_qps'] = queries + rejected
        load['errors_per_second'] = errors
        load['error_ratio'] = errors / (queries + errors) if queries + errors > 0 else 0.0
        load['rejections'] = math.fsum(w.delta('queue_rejections') + w.delta('overloads')
                                       + w.delta('deadline_exceeded') for w in windows)
        latency = M.merge_buckets([w.latency() for w in windows])
        if latency is not None:
            load['latency_count'] = latency[-1][1] if latency else 0.0
            load['p50_seconds'] = M.quantile(0.5, latency)
            load['p99_window_seconds'] = M.quantile(0.99, latency)
            load['p99_seconds'], load['latency_intervals'] = sustained_p99(windows, interval_quantile)
        slots = math.fsum(w.last.gauges.get('slots') or 0.0 for w in windows)
        if slots > 0:
            load['utilization'] = math.fsum(w.rate('slot_busy_us') for w in windows) / 1e6 / slots
        recents = [v['recent'] for v in traffic.values()]
        if all(r is not None for r in recents):
            load['offered_qps_recent'] = math.fsum(r.rate('queries') + r.rate('queue_rejections')
                                                   for r in recents)
        return load


def sustained_p99(windows, interval_quantile=0.25):
    """(p99 that persists across scrape intervals, number of intervals with queries).

    Every recent replica rebuilds its tail runtimes in the ~9 s before each
    publication activates (~75 s mean block interval), all at once: the ~2%
    of queries caught there put the window's plain p99 at 2.5-2.8 s whatever
    the load (measured live on 2026-09-29), and more replicas would not help.
    So the p99 the scaler acts on is computed per scrape interval over the
    whole tier, and the window's value is the `interval_quantile` of those:
    the p99 that at least 1 - q of the intervals exceed. A 9 s stall touches
    1.6 of the ~5 fifteen-second intervals of a block on average (under two
    thirds of them even when blocks come twice as fast), so with q = 0.25 the
    condition must hold across most intervals of the 300 s window, which spans
    about four publications. Queueing under real overload slows every
    interval and moves it.
    """
    merged = {}
    for window in windows:
        for end, interval in window.intervals():
            if interval is None:
                continue
            key = round(end, 3)
            bounds, counts = interval
            if key not in merged:
                merged[key] = (bounds, list(counts))
            elif merged[key] is not None and merged[key][0] == bounds:
                total = merged[key][1]
                for i, count in enumerate(counts):
                    total[i] += count
            else:
                merged[key] = None
    values = sorted(M.quantile(0.99, list(zip(*entry))) for entry in merged.values()
                    if entry is not None and entry[1] and entry[1][-1] > 0)
    if not values:
        return None, 0
    rank = min(len(values) - 1, max(0, math.ceil(interval_quantile * len(values)) - 1))
    return values[rank], len(values)


def _flag(inputs, key, field, errors):
    """A boolean from a small state file: absent is False, unreadable is None."""
    if key in (inputs.get('errors') or {}):
        return None
    value = inputs.get(key)
    if value is None:
        return False
    if not isinstance(value, dict) or not isinstance(value.get(field, False), bool):
        errors.append(f'{key} state invalid')
        return None
    return value.get(field, False)


def _warm_progress(previous, sample):
    for key in ('warm_runtimes',):
        before, after = previous.gauges.get(key), sample.gauges.get(key)
        if before is not None and after is not None and after > before:
            return True
    before, after = previous.counters.get('prewarm_ops'), sample.counters.get('prewarm_ops')
    return before is not None and after is not None and after > before


def public_members(snapshot):
    """The snapshot's members without the in-memory windows (for status/logging)."""
    out = {}
    for member_id, value in snapshot.get('members', {}).items():
        out[member_id] = {k: v for k, v in value.items() if k not in ('window', 'recent', 'latest')}
    return out
