"""Adopt one stopped Receiver publisher into the existing shared process.

The parent publisher journal owns the source unit, state transfer and edge
changes. The original host stays an HTTPS proxy. Rollback returns the latest
state before restarting that host, so query-server authority never goes back to
a pre-cutover counter. No node, wallet or publication format changes here.
"""
import json
from pathlib import PurePosixPath
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
    require(isinstance(adoption, dict) and set(adoption) == {
        'host', 'machine_id', 'config_path', 'data_dir', 'publisher_origin'},
        'Receiver adoption needs source host, machine_id, config_path, data_dir and publisher_origin')
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

    def run(self, host, program, *arguments):
        code, output = self.owner.ex.run(host, ['python3', '-c', program, *arguments], 60)
        require(code == 0, 'Receiver adoption check failed: ' + output[-1000:])
        return json.loads(output)

    def receipt(self, host, root):
        return self.run(host, 'import json,pathlib,sys; p=pathlib.Path(sys.argv[1]); '
                        'print(p.read_text() if p.exists() else "null")', root + '/.receiver-transfer.json')

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
        owner.resources(allocations={STATE + '/distribution/authority.json': info['bytes']})
        before = owner.ex.read(source, EDGE)
        after = self.source_edge(before)
        # Validate with the source host's installed Caddy before touching its owner.
        from tools.receiver.lib.routing import Routing
        Routing(owner.inventory, owner.ex, owner.state_dir, {'host': source, 'phase': 'distribution',
            'query_host': owner.host}).validate(after)
        return {'unit': state, 'executable': executable, 'configuration': configuration,
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
        owner, source = self.owner, self.request['host']
        def stop():
            owner.ex.systemctl(source, 'stop', UNIT)
            state = owner.ex.probe_unit(source, UNIT)
            require(not state['main_pid'] and state['active_state'] in ('inactive', 'failed'),
                    'Receiver source did not stop')
            owner.ex.publisher_unlocked(source, [STATE + '/publisher.lock'])
        journal['receiver_source_touched'] = True
        owner.effect(journal, 'stop Receiver source', stop)
        owner.effect(journal, 'disable Receiver source', lambda: owner.ex.systemctl(source, 'disable', UNIT))
        parent = str(PurePosixPath(STATE).parent)
        staged = parent + '/.receiver-adoption-' + journal['id']
        owner.effect(journal, 'prepare Receiver destination', lambda: owner.ex.mkdir(owner.host, parent, 0o700))
        def transfer():
            journal['receiver_transfer'] = owner.ex.receiver_copy_state(source, owner.host, STATE,
                                                                        staged, journal['id'])
        owner.effect(journal, 'copy Receiver state', transfer)
        owner.effect(journal, 'install Receiver state', lambda: owner.ex.rename(owner.host, staged, STATE))

    def route(self, journal):
        plan = journal['plan'].get('receiver')
        if plan is None:
            return
        self.guard(plan)
        owner, source = self.owner, self.request['host']
        state = owner.ex.probe_unit(source, UNIT)
        require(not state['main_pid'] and state['unit_file_state'] == 'disabled', 'Receiver source became active again')
        owner.effect(journal, 'route Receiver to shared publisher',
                     lambda: owner.ex.write(source, EDGE, plan['edge_after'].encode(), 0o644))
        owner.effect(journal, 'reload Receiver edge', lambda: owner.ex.systemctl(source, 'reload', 'caddy.service'))

    def restore_state(self, journal):
        plan = journal['plan'].get('receiver')
        if plan is None:
            return
        owner, source = self.owner, self.request['host']
        state = owner.ex.probe_unit(source, UNIT)
        # A failure before the source stop must leave its live state untouched.
        if not journal.get('receiver_source_touched'):
            return
        receipt = self.receipt(owner.host, STATE)
        if receipt is not None:
            require(not state['main_pid'], 'Receiver source must stay stopped until its latest state is returned')
            require(receipt.get('transaction') == journal['id'], 'Receiver destination belongs to another transaction')
            staged = str(PurePosixPath(STATE).parent) + '/.receiver-return-' + journal['id']
            returned = self.receipt(source, STATE)
            if returned is None or returned.get('transaction') != journal['id']:
                def transfer():
                    owner.ex.receiver_copy_state(owner.host, source, STATE, staged, journal['id'], plan['user'])
                staged_receipt = self.receipt(source, staged)
                if staged_receipt is None or staged_receipt.get('transaction') != journal['id']:
                    owner.effect(journal, 'return latest Receiver state', transfer)
                backup = STATE + '.before-' + journal['id']
                exists = self.run(source, 'import json,pathlib,sys; '
                                  'print(json.dumps(pathlib.Path(sys.argv[1]).exists()))', STATE)
                if exists:
                    owner.effect(journal, 'retain original Receiver state', lambda: owner.ex.rename(source, STATE, backup))
                owner.effect(journal, 'restore latest Receiver state', lambda: owner.ex.rename(source, staged, STATE))
            retained = STATE + '.rolled-back-' + journal['id']
            owner.effect(journal, 'retain returned Receiver destination', lambda: owner.ex.rename(owner.host, STATE, retained))
        owner.effect(journal, 'restore Receiver edge',
                     lambda: owner.ex.write(source, EDGE, plan['edge_before'].encode(), 0o644))
        owner.effect(journal, 'reload restored Receiver edge', lambda: owner.ex.systemctl(source, 'reload', 'caddy.service'))

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
