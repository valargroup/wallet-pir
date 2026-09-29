"""Shared builders for the scaler tests: a healthy snapshot and a policy."""
import copy
import json
from pathlib import Path
import sys

OPS = Path(__file__).resolve().parents[1]
if str(OPS) not in sys.path:
    sys.path.insert(0, str(OPS))

from scaler import decide as D  # noqa: E402

NOW = 1_790_680_000.0
POLICY = json.loads((OPS / 'deploy' / 'transparent-fleet-scaler.policy.example.json').read_text())


def policy(**overrides):
    value = copy.deepcopy(POLICY)
    value['mode'] = 'act'
    for key, item in overrides.items():
        if isinstance(item, dict) and isinstance(value.get(key), dict):
            value[key] = {**value[key], **item}
        else:
            value[key] = item
    return value


def member(role='recent-replica', origin='static', intent='enrolled', serving=True, attesting=None,
           size=None, progress=None, **extra):
    attesting = serving if attesting is None else attesting
    value = {'role': role, 'origin': origin, 'static': origin != 'elastic', 'intent': intent,
             'size': size, 'in_inventory': True, 'in_membership': True,
             'state': 'serving' if attesting else 'warming', 'routed': attesting,
             'rendered': serving, 'attesting': attesting, 'serving': serving and intent == 'enrolled',
             'observed_age_seconds': 1.0, 'transport_failures': 0,
             'sample_age_seconds': 5.0 if role == 'recent-replica' else None,
             'metrics_error': None, 'last_progress_unix': progress, 'resets': 0,
             'window': True if role == 'recent-replica' else None, 'recent': True, 'latest': None,
             'slots': 2.0}
    value.update(extra)
    return value


def load(offered=4.0, recent=None, p99=0.3, error_ratio=0.0, rejections=0.0, latency_count=1000.0):
    return {'members': [], 'unknown': [], 'offered_qps': offered,
            'offered_qps_recent': offered if recent is None else recent,
            'queries_per_second': offered, 'errors_per_second': 0.0, 'error_ratio': error_ratio,
            'rejections': rejections, 'p50_seconds': 0.1 if p99 is not None else None, 'p99_seconds': p99,
            'latency_count': latency_count, 'utilization': 0.2, 'window_seconds': 300}


def snapshot(members=None, **overrides):
    if members is None:
        members = {
            'transparent-pir-archive-01': member(role='archive-owner'),
            'transparent-pir-archive-02': member(role='archive-owner'),
            'transparent-pir-recent-01': member(),
            'transparent-pir-recent-02': member(),
        }
    value = {
        'now': NOW, 'errors': [], 'disabled': False,
        'inventory': {'revision': 3, 'total_members': len(members)},
        'membership': {'age_seconds': 0.5, 'routing_generation': 4, 'active_map_sha256': 'a' * 64},
        'maintenance': False, 'withdrawn': False, 'operation': None, 'journal_ok': True,
        'done_decision_ids': [],
        'publisher': {'phase': 'serving', 'public_height': 100, 'node_height': 100, 'freshness_seconds': 30.0,
                      'ready_replicas': 2, 'lag_seconds': 0.0, 'age_seconds': 3.0, 'serving': True, 'error': None},
        'members': members, 'load': load(),
    }
    value.update(overrides)
    return value


def elastic(n, **extra):
    return f'transparent-pir-recent-{n:02d}', member(origin='elastic', size='s-4vcpu-8gb', **extra)


def fleet(*elastic_ids, **extra):
    members = snapshot()['members']
    for n in elastic_ids:
        key, value = elastic(n, **extra)
        members[key] = value
    return members


def run(snap, pol=None, state=None, now=NOW):
    return D.decide(snap, pol if pol is not None else policy(), state or D.initial_state('f' * 32), now)
