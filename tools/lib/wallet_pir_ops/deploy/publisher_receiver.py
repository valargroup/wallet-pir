"""Reingest Receiver in the shared publisher, then hand over its HTTPS origin.

The journal owns source shutdown, authority handover and edge changes. The old
host stays an HTTPS proxy and retains its index for rollback. Only authority
counters cross hosts; databases and publications are built on the destination.
"""
import json
import re
import shlex
from urllib.parse import urlsplit

from . import coordinator_edge, units
from .publisher import require, stable_unit

UNIT = 'receiver-pir.service'
EDGE = '/etc/caddy/Caddyfile'
STATE = '/srv/receiver-pir/index'
RECEIVER = 'receiver-pir.valargroup.dev'


def validate(request, inventory):
    adoption = request.get('receiver_adoption')
    if adoption is None:
        return
    require(isinstance(adoption, dict) and set(adoption) - {'mode'} == {
        'host', 'machine_id', 'config_path', 'data_dir', 'publisher_origin'},
        'Receiver adoption needs source host, machine_id, config_path, data_dir and publisher_origin')
    require(adoption.get('mode') == 'reingest',
            'Receiver adoption requires mode reingest; recover old transfers with their recorded operations package')
    require(adoption['host'] in inventory.hosts and adoption['host'] != request['host'],
            'Receiver adoption must name a separate known source host')
    require(isinstance(adoption['machine_id'], str) and re.fullmatch('[a-f0-9]{32}', adoption['machine_id']),
            'invalid Receiver source machine identity')
    require(adoption['config_path'] == '/etc/receiver-pir-publisher.json' and adoption['data_dir'] == STATE,
            'Receiver adoption must retain the installed configuration and state paths')
    origin = urlsplit(adoption['publisher_origin'])
    require(origin.scheme == 'https' and origin.hostname and not origin.username and not origin.password
            and not origin.path and not origin.query and not origin.fragment and origin.port in (None, 443)
            and origin.hostname != RECEIVER, 'Receiver adoption needs the main publisher HTTPS origin')
    receiver = request['configuration'].get('receiver', {})
    require(receiver.get('enabled') is True and receiver.get('settings', {}).get('data_dir') == STATE
            and receiver['settings'].get('bind') == '127.0.0.1:18380',
            'adopted Receiver must use its existing state path and a loopback distribution listener')


class Receiver:
    def __init__(self, owner):
        self.owner = owner
        self.request = owner.request.get('receiver_adoption')

    def reingesting(self):
        return self.request is not None and self.request.get('mode') == 'reingest'

    def run(self, host, program, *arguments):
        code, output = self.owner.ex.run(host, ['python3', '-c', program, *arguments], 60)
        require(code == 0, 'Receiver adoption check failed: ' + output[-1000:])
        return json.loads(output)

    def edge(self, text):
        if not self.request:
            return text
        span = coordinator_edge.site_span(text, urlsplit(self.request['publisher_origin']).hostname)
        def edit(site):
            require('/provider/receiver/v1/' not in site, 'main publisher already has a Receiver distribution route')
            offset = site.index('\n') + 1
            return (site[:offset] + '\thandle /provider/receiver/v1/* {\n'
                    '\t\treverse_proxy 127.0.0.1:18380\n\t}\n' + site[offset:])
        return coordinator_edge.replace_in_site(text, span, edit)

    def source_edge(self, text):
        origin = self.request['publisher_origin']
        def edit(site):
            for marker in ('@provider', '/.well-known/pir-service-discovery'):
                pattern = (r'(?m)(^[ \t]*handle ' + re.escape(marker)
                           + r' \{\n)[ \t]*reverse_proxy [^\s{}]+\n([ \t]*\})')
                site, count = re.subn(pattern, lambda match: match[1]
                    + '\t\treverse_proxy ' + origin + ' {\n'
                    '\t\t\theader_up Host {upstream_hostport}\n\t\t}\n' + match[2], site)
                require(count == 1, 'Receiver edge has an unexpected provider or discovery handler')
            return site
        return coordinator_edge.replace_in_site(text, coordinator_edge.site_span(text, RECEIVER), edit)

    def capture(self):
        if not self.request:
            return None
        owner, source = self.owner, self.request['host']
        owner.ex.check_identity(source)
        identity = owner.ex.publisher_resources(source, [self.request['config_path']])
        require(identity['machine_id'] == self.request['machine_id'], 'Receiver source machine identity differs')
        state = stable_unit(owner.ex.probe_unit(source, UNIT))
        require(state['active_state'] == 'active' and state['main_pid'] and not state['need_daemon_reload'],
                'Receiver source publisher must be running before adoption')
        effective = units.effective([state['fragment_text'], *(d['text'] for d in state['drop_ins'])])
        _, executable, arguments = units.split_exec(units.exec_start(effective))
        require(executable.endswith('/pir-publisher') and owner.ex.sha256(source, executable) == state['exe_sha256'],
                'Receiver source executable is not a retained root publisher')
        require(shlex.split(arguments) == ['--config', self.request['config_path']],
                'Receiver source unit uses another configuration')
        configuration = owner.ex.read(source, self.request['config_path'])
        settings = json.loads(configuration)['receiver']['settings']
        require({**settings, 'bind': '127.0.0.1:18380'} == owner.request['configuration']['receiver']['settings'],
                'Receiver adoption must preserve every indexing and publication setting')
        environments = effective.get('Service', {}).get('EnvironmentFile', [])
        for reference in environments:
            path = reference.removeprefix('-')
            require(path.startswith('/etc/receiver-pir/near-') and path.endswith('.env'),
                    'unrecognized Receiver environment reference')
            source_digest = owner.ex.sha256(source, path)
            require(owner.ex.sha256(owner.host, path) == source_digest,
                    'provision the same Infisical Receiver environment on the destination: ' + path)
        info = self.run(source, 'import json,pathlib,sys; p=pathlib.Path(sys.argv[1]); '
                        'print(json.dumps({"bytes":sum(x.stat().st_size for x in p.rglob("*") if x.is_file())}))', STATE)
        absent = self.run(owner.host, 'import json,pathlib,sys; p=pathlib.Path(sys.argv[1]); '
                          'print(json.dumps(not p.exists() and not p.is_symlink()))', STATE)
        require(absent, 'Receiver destination state already exists; inspect its owner before adopting')
        retained = None
        previous = owner.latest()
        if (self.reingesting() and previous and previous['phase'] == 'rolled-back'
                and previous['request'].get('receiver_adoption', {}).get('mode') == 'reingest'
                and previous['request']['configuration'].get('receiver') == owner.request['configuration']['receiver']
                and previous['request']['release'] == owner.request['release']):
            candidate = STATE + '.rolled-back-' + previous['id']
            if self.run(owner.host, 'import json,pathlib,sys; '
                        'print(json.dumps(pathlib.Path(sys.argv[1]).is_dir()))', candidate):
                retained = candidate
        owner.resources(allocations={STATE + '/distribution/authority.json': info['bytes']})
        before = owner.ex.read(source, EDGE)
        after = self.source_edge(before)
        # Validate with the source host's installed Caddy before touching its owner.
        owner.serving.validate_edge(after, host=source)
        return {'unit': state, 'executable': executable, 'configuration': configuration,
                **({'reingested_state': retained} if retained else {}),
                'user': effective.get('Service', {}).get('User', 'root'),
                'environments': environments, 'edge_before': before, 'edge_after': after}

    def guard(self, plan, staging=False):
        if plan is None:
            return
        owner, source = self.owner, self.request['host']
        owner.ex.check_identity(source)
        require(owner.ex.publisher_resources(source, [self.request['config_path']])['machine_id']
                == self.request['machine_id'], 'Receiver source machine identity changed')
        require(owner.ex.read(source, self.request['config_path']) == plan['configuration']
                and owner.ex.read(source, EDGE) in (plan['edge_before'], plan['edge_after']),
                'Receiver source configuration or edge changed outside the transaction')
        state = stable_unit(owner.ex.probe_unit(source, UNIT))
        require(all(state[k] == plan['unit'][k] for k in ('fragment_text', 'drop_ins'))
                and owner.ex.sha256(source, plan['executable']) == plan['unit']['exe_sha256'],
                'Receiver source unit or rollback executable changed')
        if staging:
            require(state == plan['unit'], 'Receiver source owner changed during staging')

    def adopt(self, journal):
        plan = journal['plan'].get('receiver')
        if plan is None:
            return
        owner = self.owner
        if plan.get('reingested_state'):
            owner.effect(journal, 'reuse completed Receiver ingestion',
                         lambda: owner.ex.rename(owner.host, plan['reingested_state'], STATE))
        else:
            owner.effect(journal, 'prepare fresh Receiver index',
                         lambda: owner.ex.mkdir(owner.host, STATE, 0o700))
        journal['receiver_reingesting'] = True
        owner.save(journal)

    def route(self, journal):
        plan = journal['plan'].get('receiver')
        if plan is None:
            return
        self.guard(plan)
        owner, source = self.owner, self.request['host']
        if self.reingesting() and not journal.get('receiver_authority_handed_over'):
            # The old HTTPS origin keeps serving throughout the new ingestion.
            # Only counters cross hosts, after both writers have stopped.
            journal['receiver_source_touched'] = True
            owner.effect(journal, 'stop Receiver source after reingestion',
                         lambda: owner.ex.systemctl(source, 'stop', UNIT))
            stopped = owner.ex.probe_unit(source, UNIT)
            require(not stopped['main_pid'], 'Receiver source did not stop')
            owner.effect(journal, 'disable Receiver source', lambda: owner.ex.systemctl(source, 'disable', UNIT))
            from .publisher import UNIT as SHARED_UNIT
            owner.effect(journal, 'stop shared publisher for authority handover', lambda: owner.stop(SHARED_UNIT))
            owner.ex.publisher_unlocked(source, [STATE + '/publisher.lock'])
            owner.unlocked()
            self.handover_authority(journal, source, owner.host)
            journal['receiver_authority_handed_over'] = True
            owner.save(journal)
            owner.effect(journal, 'restart shared publisher after authority handover',
                         lambda: owner.ex.systemctl(owner.host, 'start', SHARED_UNIT))
            owner.ready()
        state = owner.ex.probe_unit(source, UNIT)
        require(not state['main_pid'] and state['unit_file_state'] == 'disabled', 'Receiver source became active again')
        owner.effect(journal, 'route Receiver to shared publisher',
                     lambda: owner.ex.write(source, EDGE, plan['edge_after'].encode(), 0o644))
        owner.effect(journal, 'reload Receiver edge', lambda: owner.ex.systemctl(source, 'reload', 'caddy.service'))

    def handover_authority(self, journal, source, destination, source_root=STATE):
        """Preserve counter and withdrawal floors without copying index data."""
        path = STATE + '/distribution/authority.json'
        previous = json.loads(self.owner.ex.read(source, source_root + '/distribution/authority.json'))
        current = json.loads(self.owner.ex.read(destination, path))
        require(previous['scope'] == current['scope'], 'Receiver authority scope differs')
        previous['sequence'] = max(previous['sequence'], current['sequence'])
        for name in ('recovery_epoch', 'latest_withdrawal'):
            previous['fence'][name] = max(previous['fence'][name], current['fence'][name])
        self.owner.effect(journal, 'handover Receiver authority to ' + destination,
                          lambda: self.owner.ex.write(destination, path,
                              (json.dumps(previous) + '\n').encode(), 0o644))

    def restore_state(self, journal):
        plan = journal['plan'].get('receiver')
        if plan is None:
            return
        owner, source = self.owner, self.request['host']
        state = owner.ex.probe_unit(source, UNIT)
        # A failure before the source stop must leave its live state untouched.
        if not journal.get('receiver_source_touched'):
            if self.reingesting() and journal.get('receiver_reingesting'):
                owner.effect(journal, 'retain incomplete Receiver ingestion', lambda: owner.ex.rename(
                    owner.host, STATE, STATE + '.rolled-back-' + journal['id']))
            return
        require(not state['main_pid'], 'Receiver source must stop before authority returns')
        retained = STATE + '.rolled-back-' + journal['id']
        present = self.run(owner.host, 'import json,pathlib,sys; '
                           'print(json.dumps(pathlib.Path(sys.argv[1]).exists()))', STATE)
        self.handover_authority(journal, owner.host, source, STATE if present else retained)
        owner.effect(journal, 'restore Receiver edge',
                     lambda: owner.ex.write(source, EDGE, plan['edge_before'].encode(), 0o644))
        owner.effect(journal, 'reload restored Receiver edge',
                     lambda: owner.ex.systemctl(source, 'reload', 'caddy.service'))
        if present:
            owner.effect(journal, 'retain Receiver ingestion after rollback', lambda: owner.ex.rename(
                owner.host, STATE, retained))
        return

    def restore_service(self, journal):
        plan = journal['plan'].get('receiver')
        if plan is None or not journal.get('receiver_source_touched'):
            return
        owner, source = self.owner, self.request['host']
        enabled = plan['unit']['unit_file_state'] in ('enabled', 'enabled-runtime')
        owner.effect(journal, 'restore Receiver enablement',
                     lambda: owner.ex.systemctl(source, 'enable' if enabled else 'disable', UNIT))
        owner.effect(journal, 'restart Receiver source', lambda: owner.ex.systemctl(source, 'start', UNIT))
        management = json.loads(plan['configuration'])['management_listen']
        deadline = owner.clock() + 900
        while True:
            owner.lock.verify()
            status, body = owner.ex.http_get(source, 'http://' + management + '/ready')
            if status == 200 and json.loads(body).get('ready') is True:
                return
            require(owner.clock() < deadline, 'restored Receiver publisher readiness deadline exceeded')
            owner.sleep(3)
