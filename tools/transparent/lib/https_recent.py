"""Lifecycle guard for the managed HTTPS recent replica (`https-recent`).

The process is the standalone provider: it holds an exclusively locked state
directory and updates its data only through its own HTTPS synchronization.
The shared deploy engine renders, stages, restarts and restores its unit; this
guard keeps every engine step off that live state and inside the fleet's
replica policy:

- plan: the unit user and the live state directory already exist, the
  directory owned by that user and writable by no one else, with room for a
  disposable bootstrap. The deploy creates neither users nor state.
- deploy preflight: the staged release's strict loader
  (`pir-query-server --config <file> --verify-only`) runs as the unit user over
  a transaction-owned copy of the configuration whose shard_dir is a fresh
  transaction-owned state directory, never the live one, which only the unit's
  own `ExecStartPre` checks, after the old process stopped.
- restart and restore: at least two other recent replicas stay served, read
  from the fleet's membership record under the deploy lock. A restore also
  needs the previous executable and proof, again in disposable state, that it
  accepts current canonical publications. Live state is never restored from a
  backup or deleted, so observed invalidations survive a rollback.
- ready: the provider reports the fleet's exact active publication, warm and
  not invalidated.
"""
import json
import posixpath
import shlex
import time

from wallet_pir_ops.deploy import units

MIN_OTHER_SERVING = 2
MEMBERSHIP_MAX_AGE = 30
DEFAULT_BOOTSTRAP_BYTES = 64 << 30
RECENT_ROLES = ('recent-replica', 'https-recent')
SYSTEM = '/etc/systemd/system'
GETENT, STAT, INSTALL, SETPRIV = '/usr/bin/getent', '/usr/bin/stat', '/usr/bin/install', '/usr/bin/setpriv'


def service_section(text):
    return units.effective([text]).get('Service', {})


def unit_identity(text):
    service = service_section(text)
    user, group = service.get('User'), service.get('Group')
    if not user or not group or user == 'root':
        raise ValueError('the unit must name its own non-root User and Group')
    return user, group


def start_check(text):
    """The unit's strict start check, `ExecStartPre`, as an argv."""
    value = service_section(text).get('ExecStartPre')
    lines = value if isinstance(value, list) else [value] if value else []
    if len(lines) != 1:
        raise ValueError('the unit must have exactly one strict ExecStartPre check')
    return shlex.split(lines[0])


def disposable(argv, live_state, scratch):
    """`argv` with its `--shard-dir` moved from the live state to `scratch`.

    A unit from before `pir-query-server --config` names its state on the
    command line; restoring such a unit still checks it this way.
    """
    if argv.count('--shard-dir') != 1 or argv[argv.index('--shard-dir') + 1] != live_state:
        raise ValueError('the check must name the live state exactly once, as --shard-dir')
    argv = list(argv)
    argv[argv.index('--shard-dir') + 1] = scratch
    if any(a == live_state or a.startswith(live_state.rstrip('/') + '/') for a in argv):
        raise ValueError('the disposable check would still reach live state')
    return argv


def reaches(value, live_state):
    """Whether any string in a JSON value is the live state or below it."""
    if isinstance(value, dict):
        return any(reaches(v, live_state) for v in value.values())
    if isinstance(value, list):
        return any(reaches(v, live_state) for v in value)
    return isinstance(value, str) and (value == live_state or value.startswith(live_state.rstrip('/') + '/'))


def disposable_config(text, live_state, scratch):
    """A `pir-query-server` history configuration with its shard_dir moved to `scratch`."""
    document = json.loads(text) if text is not None else None
    if not isinstance(document, dict) or set(document) != {'management_listen', 'transparent_history'}:
        raise ValueError('the check configuration must hold only the transparent_history section')
    section = document['transparent_history']
    section = section if isinstance(section, dict) else {}
    settings = section.get('settings')
    if section.get('enabled') is not True or not isinstance(settings, dict) or 'verify_only' in settings:
        raise ValueError('the check configuration must enable transparent_history without verify_only')
    if settings.get('shard_dir') != live_state:
        raise ValueError('the check configuration must name the live state as its shard_dir')
    settings['shard_dir'] = scratch
    if reaches(document, live_state):
        raise ValueError('the disposable check would still reach live state')
    return json.dumps(document, sort_keys=True, indent=2) + '\n'


class Guard:
    clock = staticmethod(time.time)

    # ------------------------------------------------------------ inputs

    @staticmethod
    def target(deployer, key):
        return next(t for t in deployer.targets if t.key == key)

    def fleet(self, deployer, settings, name):
        text = deployer.ex.read(settings['fleet_host'], posixpath.join(settings['fleet_state'], name))
        if text is None:
            raise ValueError('fleet %s is absent on %s' % (name, settings['fleet_host']))
        return json.loads(text)

    def others_serving(self, deployer, settings):
        """Refusals unless at least two other recent replicas are routed now."""
        try:
            membership = self.fleet(deployer, settings, 'membership.json')
        except (ValueError, OSError) as error:
            return ['replica policy unobserved: %s' % error]
        age = self.clock() - membership.get('updated_unix', 0)
        if not 0 <= age <= MEMBERSHIP_MAX_AGE:
            return ['fleet membership is %ds old; is the reconciler running?' % age]
        others = sorted(i for i, m in membership.get('members', {}).items()
                        if i != settings['member_id'] and m.get('role') in RECENT_ROLES
                        and m.get('state') == 'serving' and m.get('rendered') is True)
        if len(others) < MIN_OTHER_SERVING:
            return ['only %d other recent replicas are serving (%s); %d must keep serving'
                    % (len(others), ', '.join(others) or 'none', MIN_OTHER_SERVING)]
        return []

    # --------------------------------------------------------- engine hooks

    def plan(self, deployer, plan):
        """Read-only refusals before anything is staged or changed."""
        target, ex = plan.target, deployer.ex
        settings = target.settings
        path = '%s/%s' % (SYSTEM, target.unit)
        text = next((c['new'] for c in plan.changes if c['path'] == path), plan.state.get('fragment_text'))
        try:
            user, group = unit_identity(text)
        except ValueError as error:
            return [str(error)]
        refusals = []
        if ex.run(target.host, [GETENT, 'passwd', user], 10)[0] or ex.run(target.host, [GETENT, 'group', group], 10)[0]:
            refusals.append('unit user %s:%s does not exist; provision it first, the deploy creates no credentials'
                            % (user, group))
        state = settings['state_dir']
        code, output = ex.run(target.host, [STAT, '-c', '%U:%G:%a', state], 10)
        owner, _, mode = output.strip().rpartition(':')
        if code or owner != '%s:%s' % (user, group) or not mode.isdigit() or int(mode, 8) & 0o022:
            refusals.append('live state %s must already exist, owned by %s:%s and writable by no one else; '
                            'the deploy creates no state' % (state, user, group))
        need = int(settings.get('bootstrap_max_bytes', DEFAULT_BOOTSTRAP_BYTES)) + deployer.service.min_free_bytes
        if ex.free_bytes(target.host, deployer.service.root) < need:
            refusals.append('less than %d bytes free under %s for the disposable bootstrap'
                            % (need, deployer.service.root))
        try:
            self.fleet(deployer, settings, 'active.json')['map_sha256']
        except (ValueError, OSError, KeyError) as error:
            refusals.append('no fleet active publication to verify against: %s' % error)
        if plan.action == 'restart':
            refusals += self.others_serving(deployer, settings)
        return refusals

    def before_restart(self, deployer, record):
        if record['action'] != 'restart':
            return []
        return self.others_serving(deployer, self.target(deployer, record['key']).settings)

    def check_in_disposable_state(self, deployer, journal, record, argv, user, group, label, which='new'):
        """Run `argv` as the unit user over a fresh transaction-owned state; (code, output).

        A `--config` check runs over a transaction-owned copy of that
        configuration (`which`: the record's rendered or previous bytes, else
        the installed file) whose shard_dir is the disposable state.
        """
        host, settings = record['host'], self.target(deployer, record['key']).settings
        owner = deployer.service.transaction_dir(journal.id)
        scratch = '%s/%s.%s-state' % (owner, record['unit'], label)
        copy = None
        if '--config' in argv:
            if argv.count('--config') != 1:
                raise ValueError('the check must name one configuration')
            at = argv.index('--config') + 1
            path = argv[at]
            change = next((c for c in record['changes'] if c['path'] == path), None)
            text = change[which] if change is not None else deployer.ex.read(host, path)
            work = '%s/%s.%s-check' % (owner, record['unit'], label)
            scratch = work + '/state'
            copy = work + '/config.json'
            body = disposable_config(text, settings['state_dir'], scratch)
            argv = [*argv[:at], copy, *argv[at + 1:]]
            if any(a == settings['state_dir'] or a.startswith(settings['state_dir'].rstrip('/') + '/') for a in argv):
                raise ValueError('the disposable check would still reach live state')
        else:
            argv = disposable(argv, settings['state_dir'], scratch)
        deployer.lock.verify()
        if copy is not None:
            code, output = deployer.ex.run(host, [INSTALL, '-d', '-o', 'root', '-g', 'root', '-m', '0755', work], 30)
            if code:
                return code, 'cannot create disposable check directory: ' + output
            deployer.ex.write(host, copy, body.encode(), 0o644)
        code, output = deployer.ex.run(host, [INSTALL, '-d', '-o', user, '-g', group, '-m', '0700', scratch], 30)
        if code:
            if copy is not None:
                deployer.ex.remove_tree(host, work, owner)
            return code, 'cannot create disposable state: ' + output
        try:
            argv = [SETPRIV, '--reuid', user, '--regid', group, '--init-groups', '--no-new-privs', '--', *argv]
            journal.event('disposable check', host=host, unit=record['unit'], argv=argv)
            code, output = deployer.ex.run(host, argv, 900)
            journal.event('disposable check finished', host=host, returncode=code, output=output[-2000:])
            return code, output
        finally:
            deployer.lock.verify()
            deployer.ex.remove_tree(host, scratch if copy is None else work, owner)

    def preflight(self, deployer, journal, record, sha):
        from wallet_pir_ops.deploy.engine import DeployError
        unit = next(c['new'] for c in record['changes'] if c['path'] == '%s/%s' % (SYSTEM, record['unit']))
        user, group = unit_identity(unit)
        argv = [deployer.service.release_binary(sha), *record['check']]
        code, output = self.check_in_disposable_state(deployer, journal, record, argv, user, group, 'candidate')
        if code:
            raise DeployError('%s: the release refused current publications in disposable state (exit %d): %s'
                              % (record['key'], code, output[-1000:]))

    def ready(self, deployer, record, expected, document):
        """Exact fleet publication, warm and valid; a reachable URL alone is not ready."""
        try:
            active = self.fleet(deployer, self.target(deployer, record['key']).settings, 'active.json')['map_sha256']
        except (ValueError, OSError, KeyError) as error:
            return False, 'fleet active publication unobserved: %s' % error
        document = document if isinstance(document, dict) else {}
        loaded = (document.get('verified_publication') or {}).get('map_sha256')
        runtime = document.get('runtime_readiness') or {}
        if loaded != active:
            return False, 'serves publication %s, fleet active is %s' % (loaded, active)
        if runtime.get('warm') is not True or runtime.get('invalidated') is not False:
            return False, 'publication is not warm and valid (%r)' % runtime
        return True, 'ready'

    def before_restore(self, deployer, journal, record):
        """Gates a restore must pass before it changes anything."""
        settings = self.target(deployer, record['key']).settings
        gates = self.others_serving(deployer, settings)
        if record['action'] == 'install':
            # Removal stops and removes the unit; its state stays where it is.
            return gates
        previous = record['previous']
        if deployer.ex.sha256(record['host'], previous['exec_binary'] or '') != previous['exe_sha256']:
            return gates + ['previous executable %s is not present with digest %s'
                            % (previous['exec_binary'], previous['exe_sha256'])]
        unit = next((c['previous'] for c in record['changes'] if c['path'] == '%s/%s' % (SYSTEM, record['unit'])),
                    None)
        if unit is None:
            return gates + ['no previous unit is recorded; restore it by hand under a reviewed plan']
        if gates:
            return gates
        try:
            user, group = unit_identity(unit)
            argv = start_check(unit)
            code, output = self.check_in_disposable_state(deployer, journal, record, argv, user, group, 'previous',
                                                          'previous')
        except ValueError as error:
            return ['previous unit: %s' % error]
        if code:
            return ['the previous executable refuses current publications (exit %d); roll forward instead: %s'
                    % (code, output[-500:])]
        return []
