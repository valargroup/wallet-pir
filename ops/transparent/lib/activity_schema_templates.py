"""Bind only the reviewed transaction ID and its defined rollback root.

Commands, install targets, credentials and arbitrary JSON strings are never
interpolated. Concrete host/routing validators run before and after binding.
"""
import copy
import importlib.util
from pathlib import Path


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


HERE = Path(__file__).parent
H = module('template_host', HERE/'activity_schema_host.py')
R = module('template_routing', HERE/'activity_schema_routing.py')
TOKEN = '{transaction}'
ROOT = '/opt/transparent-publisher/schema-rollback/'
VALIDATION_ID = 'transparent-schema-reviewed-template'


def bind(plan, kind, transaction):
    H.require(kind in ('host', 'routing') and isinstance(transaction, str) and H.TXN.fullmatch(transaction), 'invalid template binding')
    root_field = 'baseline_root' if kind == 'host' else 'coordinator_baseline'
    H.require(plan.get('transaction') == TOKEN and plan.get(root_field) == ROOT+TOKEN, 'template requires exactly the defined transaction fields')
    result = copy.deepcopy(plan)
    result['transaction'] = transaction
    result[root_field] = ROOT+transaction
    def check(value):
        if isinstance(value, dict):
            for key, item in value.items():
                check(key)
                check(item)
        elif isinstance(value, list):
            for item in value:
                check(item)
        elif isinstance(value, str):
            H.require('{' not in value and '}' not in value, 'template token outside defined identity fields')
    check(result)
    return (H.validate if kind == 'host' else R.validate)(result)


def validate(plan, kind):
    bind(plan, kind, VALIDATION_ID)
    return plan
