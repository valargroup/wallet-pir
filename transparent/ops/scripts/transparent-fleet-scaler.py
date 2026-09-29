#!/usr/bin/env python3
"""Recent-tier scaler daemon: observe the fleet, decide, and ask the actuator.

    transparent-fleet-scaler.py --state-dir /opt/transparent-publisher/state \
        --scaler-dir /opt/transparent-publisher/scaler

Scrapes the recent replicas' `/metrics` and the publisher's `/v1/status`
every 15 s and decides every 60 s (transparent/docs/elastic-recent.md). It
holds no credentials and never touches a host: its only outputs are files in
the scaler directory, each replaced atomically:

* `state.json`: the decision state (budgets, cooldowns, trackers, forecast).
* `status.json`: the contract's status for APM, plus `awaiting_operator`,
  `orphans` (always empty here: the scaler cannot see DigitalOcean), `flags`
  and, in `recommend` mode, `recommendation`. Rewritten on every scrape;
  `updated_unix` and `heartbeat_unix` date the write, `decision.decided_unix`
  the decision.
* `decisions.jsonl`: one line per decision; rotated daily, 14 kept.
* `request.json`: only in `act-dry` and `act`, only for a non-hold decision,
  with a fresh `decision_id`. The state recording the action is written
  first, so a crash can lose a request but never spend budget twice.

One instance per directory (`scaler.lock`). Bad inputs are logged and hold;
nothing an input contains can stop the loop.
"""
from __future__ import annotations

import argparse
import datetime
import fcntl
import json
import math
import os
from pathlib import Path
import signal
import sys
import threading
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scaler import decide as D  # noqa: E402
from scaler import forecast as F  # noqa: E402
from scaler import metrics as M  # noqa: E402
from scaler import signals as S  # noqa: E402

KEEP_ROTATED = 14
PUBLISHER_URL = 'http://127.0.0.1:8094/v1/status'


def log(event, **fields):
    print(json.dumps({'event': event, **fields}, default=str), file=sys.stderr, flush=True)


def atomic_json(path, value):
    path = Path(path)
    tmp = path.with_name(path.name + '.tmp')
    with tmp.open('w') as stream:
        json.dump(value, stream, sort_keys=True, default=str)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def utc_day(unix):
    return datetime.datetime.fromtimestamp(unix, datetime.timezone.utc).strftime('%Y%m%d')


class Scaler:
    def __init__(self, state_dir, scaler_dir, publisher_url=PUBLISHER_URL, clock=time.time,
                 fetch=M.fetch, fetch_status=S.fetch_json, scrape_timeout=3.0):
        self.state_dir = Path(state_dir)
        self.dir = Path(scaler_dir)
        self.publisher_url = publisher_url
        self.clock = clock
        self.fetch = fetch
        self.fetch_status = fetch_status
        self.scrape_timeout = scrape_timeout
        self.collector = S.Collector(keep_seconds=1800.0, started=clock())
        self.state = None
        self.status = None
        self.policy_window = 300.0

    # -- state ---------------------------------------------------------------------
    def load_state(self):
        """The persisted state, or a new one whose action history is rebuilt from the log."""
        path = self.dir / 'state.json'
        try:
            value = json.loads(path.read_text())
            if not isinstance(value, dict) or value.get('schema') != 1 or not isinstance(value.get('actions'), list):
                raise ValueError('unexpected state schema')
            return value
        except FileNotFoundError:
            state = D.initial_state(uuid.uuid4().hex)
        except (OSError, ValueError) as error:
            broken = path.with_name(f'state.json.corrupt-{int(self.clock())}')
            try:
                os.replace(path, broken)
            except OSError:
                pass
            log('scaler_state_unreadable', error=str(error), kept=str(broken))
            state = D.initial_state(uuid.uuid4().hex)
        # Budgets must survive a lost state: rebuild the last two days of actions.
        state['actions'] = self.recover_actions(self.clock())
        if state['actions']:
            state['last_action_unix'] = max(a['unix'] for a in state['actions'])
        return state

    def log_files(self):
        rotated = sorted(self.dir.glob('decisions-*.jsonl'))
        current = self.dir / 'decisions.jsonl'
        return rotated + ([current] if current.exists() else [])

    def recover_actions(self, now):
        actions = []
        for path in self.log_files()[-3:]:
            try:
                lines = path.read_text().splitlines()
            except OSError:
                continue
            for line in lines:
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                record = entry.get('action_record') if isinstance(entry, dict) else None
                if isinstance(record, dict) and isinstance(record.get('unix'), (int, float)) \
                        and now - record['unix'] < 2 * D.DAY:
                    actions.append(record)
        return actions

    def load_policy(self):
        path = self.dir / 'policy.json'
        try:
            return json.loads(path.read_text()), None
        except FileNotFoundError:
            return None, 'policy missing: ' + str(path)
        except (OSError, ValueError) as error:
            return None, f'policy unreadable: {error}'

    # -- scrape ---------------------------------------------------------------------
    def scrape(self):
        now = self.clock()
        inputs = S.read_inputs(self.state_dir, self.dir)
        self.collector.scrape(S.Collector.targets(inputs), now, timeout=self.scrape_timeout,
                              fetch=self.fetch, publisher_url=self.publisher_url,
                              fetch_status=self.fetch_status)

    # -- decide ---------------------------------------------------------------------
    def cycle(self):
        now = self.clock()
        if self.state is None:
            self.state = self.load_state()
        inputs = S.read_inputs(self.state_dir, self.dir)
        policy, policy_error = self.load_policy()
        effective = policy if isinstance(policy, dict) else {}
        snapshot = self.collector.snapshot(inputs, effective, now)
        if policy_error:
            snapshot['errors'].append(policy_error)
        decision, state = D.decide(snapshot, policy, self.state, now)
        state['forecast_samples'] = F.update(self.state.get('forecast_samples'), snapshot, now)
        state['updated_unix'] = now
        summary = state.get('summary') or {}
        mode = summary.get('mode')
        record = None
        if decision['action'] != 'hold' and mode in D.ACTING:
            record = state['actions'][-1]
        # State first: an action is spent before it is requested.
        atomic_json(self.dir / 'state.json', state)
        self.state = state
        if record is not None:
            request = {'schema': 1, 'decision_id': decision['decision_id'], 'created_unix': now,
                       'action': decision['action'], 'reason': decision['reason']}
            if decision['action'] == 'scale_out':
                request['count'] = decision['count']
            else:
                request['member'] = decision['member']
            atomic_json(self.dir / 'request.json', request)
        alert_days = (policy or {}).get('forecast_alert_days', D.DEFAULT_POLICY['forecast_alert_days']) \
            if isinstance(policy, dict) else D.DEFAULT_POLICY['forecast_alert_days']
        try:
            prediction = F.forecast(state['forecast_samples'], now, float(alert_days))
        except (TypeError, ValueError):
            prediction = F.forecast(state['forecast_samples'], now)
        self.status = self.render_status(now, decision, summary, snapshot, prediction)
        atomic_json(self.dir / 'status.json', self.status)
        self.append_log(now, {
            'unix': now, 'mode': mode, 'decision': decision, 'holds': summary.get('holds'),
            'desired_recent': summary.get('desired_recent'), 'serving_recent': summary.get('serving_recent'),
            'load': {k: v for k, v in (snapshot.get('load') or {}).items()},
            'publisher': snapshot.get('publisher'), 'operation': snapshot.get('operation'),
            'members': {m: {k: v.get(k) for k in ('role', 'origin', 'intent', 'state', 'serving', 'attesting',
                                                  'observed_age_seconds', 'sample_age_seconds', 'metrics_error')}
                        for m, v in snapshot.get('members', {}).items()},
            'forecast': prediction, 'request_written': record is not None, 'action_record': record})
        log('scaler_decision', action=decision['action'], reason=decision['reason'][:500], mode=mode,
            flags=decision.get('flags'))
        return decision

    def render_status(self, now, decision, summary, snapshot, prediction):
        operation = snapshot.get('operation')
        brief = {k: decision[k] for k in ('action', 'reason', 'count', 'member', 'decision_id') if k in decision}
        brief['decided_unix'] = now
        budget = summary.get('budget') or {}
        days = prediction.get('days_to_recent_budget')
        if not isinstance(days, (int, float)) or not math.isfinite(days):
            days = None
        status = {
            'schema': 1, 'updated_unix': now, 'heartbeat_unix': now,
            'mode': summary.get('mode') or 'invalid',
            'decision': brief,
            'desired_recent': summary.get('desired_recent'),
            'serving_recent': summary.get('serving_recent'),
            'offered_qps': summary.get('offered_qps'),
            'holds': summary.get('holds') or [],
            'operation': None if operation is None else {
                k: operation.get(k) for k in ('id', 'phase', 'age_seconds', 'deadline_exceeded', 'fenced')},
            'budget': {k: budget.get(k) for k in ('actions_left', 'destroys_left', 'monthly_cost_usd')},
            'forecast': {'days_to_recent_budget': days},
            'awaiting_operator': summary.get('awaiting_operator') or [],
            # Tagged recent droplets unknown to the inventory. The scaler has no
            # DigitalOcean access; the actuator reports them separately.
            'orphans': [],
            'flags': decision.get('flags') or [],
        }
        if prediction.get('alert'):
            status['flags'] = status['flags'] + [
                f"recent tier reaches its cache or memory limit in {prediction['days_to_recent_budget']} days"]
        if summary.get('mode') == 'recommend':
            status['recommendation'] = brief
        return status

    def heartbeat(self):
        """Rewrite status.json; `updated_unix` always dates the file as written."""
        if self.status is None:
            return
        now = self.clock()
        self.status = {**self.status, 'heartbeat_unix': now, 'updated_unix': now}
        atomic_json(self.dir / 'status.json', self.status)

    def fail(self, error):
        """Report an internal error as a hold; never raises."""
        now = self.clock()
        reason = f'scaler error: {type(error).__name__}: {error}'[:500]
        log('scaler_error', error=reason)
        try:
            status = dict(self.status or {
                'schema': 1, 'mode': 'invalid', 'desired_recent': None, 'serving_recent': None,
                'offered_qps': None, 'operation': None,
                'budget': {'actions_left': None, 'destroys_left': None, 'monthly_cost_usd': None},
                'forecast': {'days_to_recent_budget': None}, 'awaiting_operator': [], 'orphans': []})
            status.update(updated_unix=now, heartbeat_unix=now,
                          decision={'action': 'hold', 'reason': reason, 'decided_unix': now},
                          holds=[reason], flags=[reason])
            status.pop('recommendation', None)
            self.status = status
            atomic_json(self.dir / 'status.json', status)
        except Exception as nested:  # noqa: BLE001
            log('scaler_status_write_failed', error=str(nested))

    # -- log ----------------------------------------------------------------------------
    def append_log(self, now, entry):
        path = self.dir / 'decisions.jsonl'
        try:
            if path.exists() and utc_day(path.stat().st_mtime) != utc_day(now):
                target = self.dir / f'decisions-{utc_day(path.stat().st_mtime)}.jsonl'
                if target.exists():
                    with target.open('a') as out, path.open() as source:
                        out.write(source.read())
                    path.unlink()
                else:
                    os.replace(path, target)
                for old in sorted(self.dir.glob('decisions-*.jsonl'))[:-KEEP_ROTATED]:
                    old.unlink()
            with path.open('a') as stream:
                stream.write(json.dumps(entry, sort_keys=True, default=str) + '\n')
        except OSError as error:
            log('scaler_log_write_failed', error=str(error))

    # -- loop ---------------------------------------------------------------------------
    def run(self, stop, scrape_interval=15.0, decide_interval=60.0, once=False):
        next_decide = time.monotonic() + (0 if once else min(decide_interval, 2 * scrape_interval))
        while not stop.is_set():
            started = time.monotonic()
            try:
                self.scrape()
            except Exception as error:  # noqa: BLE001
                log('scaler_scrape_failed', error=f'{type(error).__name__}: {error}')
            if once or time.monotonic() >= next_decide:
                next_decide = time.monotonic() + decide_interval
                try:
                    self.cycle()
                except Exception as error:  # noqa: BLE001
                    self.fail(error)
            else:
                try:
                    self.heartbeat()
                except Exception as error:  # noqa: BLE001
                    log('scaler_heartbeat_failed', error=str(error))
            if once:
                return
            stop.wait(max(0.0, scrape_interval - (time.monotonic() - started)))


def lock(directory):
    stream = (Path(directory) / 'scaler.lock').open('a')
    try:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        stream.close()
        return None
    return stream


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--state-dir', default='/opt/transparent-publisher/state')
    parser.add_argument('--scaler-dir', default='/opt/transparent-publisher/scaler')
    parser.add_argument('--publisher-url', default=PUBLISHER_URL)
    parser.add_argument('--scrape-interval', type=float, default=15.0)
    parser.add_argument('--decide-interval', type=float, default=60.0)
    parser.add_argument('--once', action='store_true', help='one scrape and one decision, then exit')
    args = parser.parse_args(argv)
    scaler_dir = Path(args.scaler_dir)
    scaler_dir.mkdir(parents=True, exist_ok=True)
    held = lock(scaler_dir)
    if held is None:
        log('scaler_already_running', scaler_dir=str(scaler_dir))
        return 1
    stop = threading.Event()
    previous = {}
    try:
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGTERM, signal.SIGINT):
                previous[signum] = signal.signal(signum, lambda *_: stop.set())
        scaler = Scaler(args.state_dir, scaler_dir, args.publisher_url)
        log('scaler_started', state_dir=args.state_dir, scaler_dir=str(scaler_dir), pid=os.getpid())
        scaler.run(stop, args.scrape_interval, args.decide_interval, once=args.once)
        log('scaler_stopped')
        return 0
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        held.close()


if __name__ == '__main__':
    sys.exit(main())
