"""Captured standalone service shapes used only to exercise historical recovery."""
from dataclasses import replace
from pathlib import Path
from wallet_pir_ops.deploy import descriptors

UNITS = Path(__file__).parent / 'fixtures/historical'


def services(current):
    result = {name: service for name, service in current.items() if name != 'publisher'}
    for name, role in {
        'enhance': descriptors.Role('coordinator', 'enhance-pir-server.service', 'exec-drop-in',
            'http://127.0.0.1:8080/v1/health', ready_url='http://127.0.0.1:8080/ready',
            ready_timeout=600, adoptable_drop_ins=('90-v7.conf', 'zz-cleanup-*.conf')),
        'status': descriptors.Role('controller', 'status-controller-qualification.service', 'exec-drop-in',
            'http://127.0.0.1:8480/internal/health',
            template=UNITS / 'status-controller-qualification.service.in'),
    }.items():
        service = result[name]
        result[name] = replace(service, roles={**service.roles, role.name: role},
                               order=(*service.order, role.name))
    return result
