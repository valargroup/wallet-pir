"""One operator fleet view derived from the existing inventory and role registry.

This is desired configuration, not observed serving health. Dynamic membership
and publication state remain owned by each product's existing controllers.
"""
from dataclasses import dataclass

from .deploy import descriptors
from .paths import ROOT


@dataclass(frozen=True)
class Fleet:
    inventory: descriptors.Inventory
    services: dict

    @classmethod
    def load(cls, inventory_path, registry=ROOT / 'ops/deploy/services.toml'):
        return cls(descriptors.load_inventory(inventory_path), descriptors.load_descriptors(registry))

    def describe(self):
        unknown = self.inventory.services.keys() - self.services.keys()
        if unknown:
            raise ValueError('inventory has unregistered services: ' + ', '.join(sorted(unknown)))
        roles, owned = [], set()
        for name in sorted(self.inventory.services):
            service = self.services[name]
            for target in descriptors.targets(service, self.inventory):
                identity = (target.host, target.unit)
                if identity in owned:
                    raise ValueError('two services own the same host/unit: %s/%s' % identity)
                owned.add(identity)
                roles.append({'service': name, 'role': target.role.name, 'host': target.host,
                              'unit': target.unit, 'artifact_kind': target.artifact_kind,
                              'artifact_kinds': list(service.artifact_kinds),
                              'health': target.url(target.role.health),
                              'ready': target.role.ready, 'ready_timeout': target.role.ready_timeout,
                              'configuration_mode': target.role.mode})
                roles[-1]['lifecycle_driver'] = service.driver
        return {'view': 'configured', 'hosts': sorted(self.inventory.hosts), 'roles': roles}
