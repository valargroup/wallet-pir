"""One operator fleet view derived from the existing inventory and role registry.

This is desired configuration, not observed serving health. Dynamic membership
and publication state remain owned by each product's existing controllers.

A product inventory may list only the services it operates; `describe()` checks
what it lists. `describe(complete=True)` additionally requires the whole fleet:
every required service and role, each declared responsibility held by exactly
one instance, every required ancillary kind consumed, and no field that would
duplicate controller-owned membership.
"""
from dataclasses import dataclass

from .deploy import descriptors
from .paths import ROOT

# Fields a role entry may carry. Anything else (shard assignments, members,
# publication revisions) belongs to the product controller, not the inventory.
ROLE_FIELDS = {'host', 'unit', 'vars', 'health_equals', 'artifact_kind', 'responsibilities', 'settings', 'install'}
ANCILLARY_FIELDS = {'host', 'unit', 'kind', 'consumers', 'operations', 'note'}
SERVICE_FIELDS = {'roles', 'vars', 'template_vars', 'exact_check', 'managed_products'}


def machine(inventory, host):
    """The identity two host names share when they reach the same machine."""
    entry = inventory.hosts[host]
    return (entry.get('address') or entry.get('ssh_alias') or host).lower()


@dataclass(frozen=True)
class Fleet:
    inventory: descriptors.Inventory
    services: dict

    @classmethod
    def load(cls, inventory_path, registry=ROOT / 'tools/deploy/services.toml'):
        return cls(descriptors.load_inventory(inventory_path), descriptors.load_descriptors(registry))

    def problems(self, complete=False):
        """Every reason this inventory is not a valid (complete) fleet, without contacting hosts."""
        problems = []
        inventory = self.inventory
        managed = inventory.services.get('publisher', {}).get('managed_products', [])
        unified_roles = {descriptors.PUBLISHER_PRODUCTS[p] for p in managed}
        names = {}
        for host in inventory.hosts:
            names.setdefault(host.lower(), []).append(host)
        problems += ['host names differ only in case: %s' % sorted(v) for v in names.values() if len(v) > 1]
        unknown = inventory.services.keys() - self.services.keys()
        if unknown:
            problems.append('inventory has unregistered services: ' + ', '.join(sorted(unknown)))
        owned = {}
        def own(host, unit, owner):
            identity = (machine(inventory, host), unit)
            if identity in owned:
                problems.append('%s and %s both own %s on %s' % (owned[identity], owner, unit, host))
            owned.setdefault(identity, owner)
        for name in sorted(inventory.services.keys() & self.services.keys()):
            service, spec = self.services[name], inventory.services[name]
            if complete:
                extra = set(spec) - SERVICE_FIELDS
                if extra:
                    problems.append('%s: fields %s belong to the product controller, not the inventory'
                                    % (name, sorted(extra)))
                for role, entries in spec.get('roles', {}).items():
                    for entry in entries:
                        extra = set(entry) - ROLE_FIELDS
                        if extra:
                            problems.append('%s.%s@%s: fields %s belong to the product controller, not the inventory'
                                            % (name, role, entry.get('host'), sorted(extra)))
            try:
                targets = descriptors.targets(service, inventory)
            except descriptors.DescriptorError as error:
                problems.append(str(error))
                continue
            for target in targets:
                own(target.host, target.unit, '%s.%s' % (name, target.role.name))
                if not complete:
                    continue
                accelerator = inventory.hosts[target.host].get('accelerator')
                cuda = (target.artifact_kind or '').endswith('-cuda')
                if cuda != (accelerator == 'cuda') and len(service.artifact_kinds) > 1:
                    problems.append('%s: a CUDA host runs only an explicit CUDA artifact_kind, and only a CUDA '
                                    'host runs one' % target.key)
                if cuda and target.health_equals.get('matvec.matvec_backend') != 'cuda':
                    problems.append('%s: a CUDA artifact must be verified by health_equals '
                                    'matvec.matvec_backend = "cuda"' % target.key)
            if complete:
                present = {t.role.name for t in targets}
                missing = [r for r in service.order if r not in present and not service.roles[r].optional
                           and (name, r) not in unified_roles]
                if missing:
                    problems.append('%s: required roles missing: %s' % (name, missing))
                for role in service.roles.values():
                    if (name, role.name) in unified_roles:
                        continue
                    for duty in role.responsibilities:
                        holders = [t.key for t in targets if duty in t.responsibilities]
                        if len(holders) != 1:
                            problems.append('%s: exactly one %s must hold %s; found %s'
                                            % (name, role.name, duty, holders))
        consumed = {}
        for name, entry in sorted(inventory.ancillary.items()):
            label = 'ancillary %s' % name
            extra = set(entry) - ANCILLARY_FIELDS
            if extra:
                problems.append('%s: unknown fields %s' % (label, sorted(extra)))
            host = entry.get('host')
            if host in inventory.retired_hosts:
                problems.append('%s: host %s is retired' % (label, host))
                continue
            if host not in inventory.hosts:
                problems.append('%s: unknown host %r' % (label, host))
                continue
            if not descriptors.UNIT.match(entry.get('unit') or ''):
                problems.append('%s: invalid unit' % label)
                continue
            if entry.get('kind') not in descriptors.ANCILLARY_KINDS:
                problems.append('%s: kind must be one of %s' % (label, descriptors.ANCILLARY_KINDS))
            operations = entry.get('operations', list(descriptors.ANCILLARY_OPERATIONS))
            unsupported = set(operations) - set(descriptors.ANCILLARY_OPERATIONS)
            if unsupported:
                problems.append('%s: unsupported operations %s; ancillary units are inspected only'
                                % (label, sorted(unsupported)))
            consumers = entry.get('consumers')
            if not isinstance(consumers, list) or not consumers:
                problems.append('%s: consumers must list the services that use it' % label)
                consumers = []
            for consumer in consumers:
                if consumer not in self.services:
                    problems.append('%s: unknown consumer %r' % (label, consumer))
                elif complete and consumer not in inventory.services:
                    problems.append('%s: consumer %s is not in the inventory' % (label, consumer))
                consumed.setdefault(consumer, set()).add(entry.get('kind'))
            own(host, entry['unit'], label)
        if complete:
            # A service made entirely of optional roles is itself opt-in.
            required = {name for name, service in self.services.items()
                        if any(not role.optional for role in service.roles.values())}
            missing = sorted(required - inventory.services.keys())
            if missing:
                problems.append('services missing from a complete fleet: %s' % missing)
            for name, service in sorted(self.services.items()):
                for kind in service.ancillary:
                    if kind not in consumed.get(name, set()):
                        problems.append('%s: no ancillary %s unit names it as a consumer' % (name, kind))
        return problems

    def describe(self, complete=False):
        problems = self.problems(complete)
        if problems:
            raise ValueError('invalid %sfleet inventory:\n  %s' % ('complete ' if complete else '',
                                                                  '\n  '.join(problems)))
        roles = []
        for name in sorted(self.inventory.services):
            service = self.services[name]
            for target in descriptors.targets(service, self.inventory):
                roles.append({'service': name, 'role': target.role.name, 'host': target.host,
                              'unit': target.unit, 'artifact_kind': target.artifact_kind,
                              'artifact_kinds': list(service.artifact_kinds),
                              'health': target.url(target.role.health),
                              'ready': target.role.ready, 'ready_timeout': target.role.ready_timeout,
                              'configuration_mode': target.role.mode,
                              'responsibilities': list(target.responsibilities),
                              'lifecycle_driver': service.driver,
                              'operations': list(descriptors.OPERATIONS[service.driver])})
        ancillary = [{'name': name, 'host': entry['host'], 'unit': entry['unit'], 'kind': entry['kind'],
                      'consumers': list(entry['consumers']),
                      'operations': list(entry.get('operations', descriptors.ANCILLARY_OPERATIONS))}
                     for name, entry in sorted(self.inventory.ancillary.items())]
        # Configured, not observed: nothing here says a role is running or reachable.
        return {'view': 'configured', 'complete': complete, 'hosts': sorted(self.inventory.hosts),
                'retired_hosts': sorted(self.inventory.retired_hosts), 'roles': roles, 'ancillary': ancillary}
