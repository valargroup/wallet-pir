"""Changed-native candidate isolation, bundle checks and old-proof refusal.

Fixture artifacts replace the pinned digests only inside these tests; nothing
here is a passing qualification report or a real candidate artifact.
"""
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]/'ops/lib'))
sys.path.insert(0, str(HERE))
from wallet_pir_ops import schema_fence
from test_activity_input_stage import Lock

FENCE = schema_fence.local_schema_fence


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


PRODUCT = module('candidate_product_test', HERE.parent/'lib/activity_schema_product.py')
I = PRODUCT.I
C = I.C
R = PRODUCT.R
RELEASE = module('candidate_release_test', HERE.parents[2]/'tools/ci/release.py')
EVIDENCE = HERE.parents[1]/'evidence'
OLD = '12ce12918446eaa56e2d766ec2f43d82c531abb9'
PUBLICATION = '34e3ebe3510206617460cebc87528e56f01949b971797e6f17cc8ca958f23f5d'


def elf(name, glibc=b'2.34', machine=0x3e):
    header = bytearray(64)
    header[:7] = b'\x7fELF\x02\x01\x01'
    header[16:18] = (3).to_bytes(2, 'little')
    header[18:20] = machine.to_bytes(2, 'little')
    return bytes(header)+b'fixture '+name.encode()+b'\x00GLIBC_2.2.5\x00GLIBC_'+glibc+b'\x00'


def tar_bytes(members):
    """members: (name, bytes|None for directory|('link', target))."""
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode='w:gz') as archive:
        for name, data in members:
            info = tarfile.TarInfo(name)
            if data is None:
                info.type = tarfile.DIRTYPE; archive.addfile(info)
            elif isinstance(data, tuple):
                info.type = tarfile.SYMTYPE; info.linkname = data[1]; archive.addfile(info)
            else:
                info.size = len(data); archive.addfile(info, io.BytesIO(data))
    return raw.getvalue()


class Fixture(unittest.TestCase):
    """Fixture bytes with matching fixture pins; the real pins stay in the module."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        self.data = {name: elf(name) for name in C.ARTIFACTS}
        pins = {name: hashlib.sha256(data).hexdigest() for name, data in self.data.items()}
        self.result = {'source_sha':C.SOURCE_SHA, 'status':'passed', 'build_exit':0, 'profile':'release',
                       'toolchain':'1.97.1', 'rustflags':C.BUILD['rustflags'],
                       'input_sha256':{'Cargo.toml':C.BUILD['cargo_toml_sha256'], 'Cargo.lock':C.BUILD['cargo_lock_sha256'],
                                       'rust-toolchain.toml':C.BUILD['rust_toolchain_sha256']},
                       'stages':[{'exit_code':0, 'native_cpu_flag_seen':False, 'requested_flags_seen':True}],
                       'ci_built_roles_present_in_lane':{name:False for name in C.CI_PINS},
                       'artifacts':{name:{'sha256':pins[name]} for name in C.SUPPLEMENTAL_PINS}}
        self.archives = {'transparent-publisher':self.ci('transparent-publisher'),
                         'transparent-filter':self.ci('transparent-filter'),
                         'supplemental':self.supplemental()}
        for target, value in ((C.ARTIFACTS, pins), (C.CI_PINS, {k:pins[k] for k in C.CI_PINS}),
                              (C.SUPPLEMENTAL_PINS, {k:pins[k] for k in C.SUPPLEMENTAL_PINS})):
            p = patch.dict(target, value); p.start(); self.addCleanup(p.stop)
        for name, value in (('SUPPLEMENTAL_ARCHIVE_SHA256', C.checksum(self.archives['supplemental'])),
                            ('ROOT', self.root/'candidates'), ('TARGET', self.root/'candidates'/('release-'+C.SOURCE_SHA)),
                            ('OWNER', os.getuid())):
            p = patch.object(C, name, value); p.start(); self.addCleanup(p.stop)
        # Fixture archives use private scratch on either host OS; the production
        # reader keeps its explicit Linux tmpfs requirement.
        scratch = SimpleNamespace(TemporaryDirectory=lambda **options:
                                  tempfile.TemporaryDirectory(**dict(options, dir=self.root)))
        p = patch.object(C, 'tempfile', scratch); p.start(); self.addCleanup(p.stop)

    def ci(self, kind, *, revision=C.SOURCE_SHA, data=None, name=None):
        payload = {n:(data or self.data).get(n, self.data[n]) for n in RELEASE.BINARIES[kind]}
        payload.update({Path(f).name:b'unit\n' for f in RELEASE.FILES[kind]})
        payload['revision'] = revision.encode()+b'\n'
        payload['SHA256SUMS'] = ''.join('%s  %s\n' % (hashlib.sha256(v).hexdigest(), k) for k, v in sorted(payload.items())).encode()
        path = self.root/(name or kind+'.tar.gz')
        path.write_bytes(tar_bytes(sorted(payload.items())))
        return str(path)

    def supplemental(self, result=None, extra=(), name='supplemental.tar.gz'):
        prefix = 'release-'+C.SOURCE_SHA
        members = [(prefix+'/result.json', json.dumps(result or self.result).encode()), (prefix+'/artifacts', None),
                   (prefix+'/artifacts/examples', None)]
        members += [(prefix+'/artifacts/'+n, self.data[n]) for n in sorted(C.SUPPLEMENTAL_PINS)]
        path = self.root/name
        path.write_bytes(tar_bytes(members+list(extra)))
        return str(path)


class Identity(Fixture):
    def test_real_pins_match_manifest_and_prompted_ci_roles_without_relabeling_history(self):
        manifest = json.loads((EVIDENCE/'activity-metadata-2026-10-04/manifest.json').read_text())
        real = module('candidate_real_pins', HERE.parent/'lib/activity_candidate.py')
        self.assertEqual(real.SUPPLEMENTAL_PINS, {k:v['sha256'] for k, v in manifest['artifacts'].items()})
        self.assertEqual(real.SUPPLEMENTAL_ARCHIVE_SHA256, manifest['archive']['sha256'])
        self.assertEqual(manifest['source_sha'], real.SOURCE_SHA)
        self.assertEqual(str(real.CI_RUN), manifest['context']['ci_release']['run'])
        self.assertEqual(sorted(real.CI_PINS), sorted(manifest['context']['ci_release']['inventory_roles_from_ci']))
        old = json.loads((EVIDENCE/'activity-metadata-2026-09-30/release-12ce1291.json').read_text())
        self.assertEqual(set(real.ARTIFACTS), set(old['binaries']))
        self.assertFalse(set(real.ARTIFACTS.values()) & {v['sha256'] for v in old['binaries'].values()})
        # Historical identities are unchanged: publication job, assignment and samples keep 12ce.
        self.assertEqual(real.HISTORICAL_SHA, OLD)
        self.assertEqual(PRODUCT.NATIVE, OLD)
        self.assertEqual(I.P.RELEASE_SHA, OLD)
        self.assertEqual(str(PRODUCT.LOAD_BINARY), old['binaries']['examples/rate-query']['retained_path'])
        self.assertEqual(R.READER, old['binaries']['transparent-loadtest']['retained_path'])
        self.assertTrue(str(real.TARGET).startswith('/srv/transparent-activity/candidates/'))
        self.assertFalse(str(real.TARGET).startswith(str(I.P.ROOT/'build')))

    def test_fixture_archives_collect_all_eighteen_with_provenance(self):
        files = C.files(self.archives)
        self.assertEqual(set(files), {'provenance.json', *('artifacts/'+n for n in C.ARTIFACTS)})
        record = json.loads(files['provenance.json'])
        self.assertEqual(record['identity'], C.identity()); self.assertEqual(record['source_sha'], C.SOURCE_SHA)
        self.assertEqual(record['historical_release_sha'], OLD); self.assertEqual(record['qualification'], 'unqualified')

    def test_ci_revision_inventory_and_checksum_refuse(self):
        cases = [
            ('old revision', lambda: self.ci('transparent-publisher', revision=OLD, name='old.tar.gz')),
            ('foreign binary', lambda: self.ci('transparent-publisher', data={'shard-control':elf('other')}, name='foreign.tar.gz')),
        ]
        for label, make in cases:
            with self.subTest(label):
                archives = dict(self.archives, **{'transparent-publisher':make()})
                with self.assertRaises(ValueError): C.files(archives)
        archives = dict(self.archives, **{'transparent-filter':self.archives['transparent-publisher']})
        with self.assertRaises(ValueError): C.files(archives)
        with self.assertRaisesRegex(ValueError, 'archive set'): C.files({k:v for k, v in self.archives.items() if k != 'supplemental'})

    def test_supplemental_digest_links_traversal_duplicates_and_build_flags_refuse(self):
        prefix = 'release-'+C.SOURCE_SHA
        bad = [
            ('link', {'extra':[(prefix+'/logs', None), (prefix+'/logs/escape', ('link', '/etc/passwd'))]}),
            ('traversal', {'extra':[(prefix+'/../escape', b'x')]}),
            ('duplicate', {'extra':[(prefix+'/artifacts/shard-publish', self.data['shard-publish'])]}),
            ('foreign artifact', {'extra':[(prefix+'/artifacts/shard-prune', elf('shard-prune'))]}),
            ('native cpu', {'result':dict(self.result, stages=[{'exit_code':0, 'native_cpu_flag_seen':True, 'requested_flags_seen':True}])}),
            ('rustflags', {'result':dict(self.result, rustflags='-C target-cpu=native')}),
            ('profile', {'result':dict(self.result, profile='release-fast')}),
            ('ci role rebuilt', {'result':dict(self.result, ci_built_roles_present_in_lane={'shard-prune':True})}),
            ('source', {'result':dict(self.result, source_sha=OLD)}),
        ]
        for label, options in bad:
            with self.subTest(label):
                path = self.supplemental(name=label+'.tar.gz', **options)
                with patch.object(C, 'SUPPLEMENTAL_ARCHIVE_SHA256', C.checksum(path)), self.assertRaises(ValueError):
                    C.read_supplemental(path)
        # The pinned archive digest refuses any other bytes before parsing.
        with self.assertRaisesRegex(ValueError, 'digest'):
            C.read_supplemental(self.supplemental(name='changed.tar.gz', result=dict(self.result, jobs=4)))
        link = self.root/'linked.tar.gz'; link.symlink_to(self.archives['supplemental'])
        with self.assertRaisesRegex(ValueError, 'symlink'): C.read_supplemental(str(link))

    def test_abi_bounds_refuse_newer_glibc_foreign_machine_and_non_elf(self):
        C.abi('ok', elf('ok', b'2.39'))
        for data in (elf('new', b'2.40'), elf('arm', machine=0xb7), b'#!/bin/sh\n'+b'\x00'*64, elf('x')[:32]):
            with self.assertRaises(ValueError): C.abi('bad', data)
        data = dict(self.data, **{'transparent-loadtest':elf('transparent-loadtest', b'2.41')})
        pins = {k:hashlib.sha256(v).hexdigest() for k, v in data.items()}
        self.data = data
        with patch.dict(C.ARTIFACTS, pins), patch.dict(C.SUPPLEMENTAL_PINS, {k:pins[k] for k in C.SUPPLEMENTAL_PINS}):
            self.result['artifacts'] = {name:{'sha256':pins[name]} for name in C.SUPPLEMENTAL_PINS}
            path = self.supplemental(name='newer.tar.gz')
            with patch.object(C, 'SUPPLEMENTAL_ARCHIVE_SHA256', C.checksum(path)), self.assertRaisesRegex(ValueError, 'glibc'):
                C.files(dict(self.archives, supplemental=path))


class BundlePreparation(Fixture):
    def setUp(self):
        super().setUp()
        self.owners = self.root/'owners'; self.lock = Lock(self.root/'lock')
        self.inventory = SimpleNamespace(hosts={}, ssh={'mode':'config'}, lock={'type':'pinned_host','machine_id':'9'*32}, services={})
        self.request = {'version':1, 'source_sha':'e'*40, 'candidate_sha':C.SOURCE_SHA, 'ci_run':C.CI_RUN,
                        'attempt':1, 'archives':self.archives}
        # Historical bytes that preparation must never touch.
        self.old = self.root/'build/evidence'/('release-'+OLD)/'artifacts'; self.old.mkdir(parents=True)
        (self.old/'transparent-shard-server').write_bytes(b'frozen 12ce worker')
        self.publication = self.root/'full-v11/publications/initial'; self.publication.mkdir(parents=True)
        (self.publication/'shards.json').write_bytes(b'immutable publication')
        self.before = self.snapshot()
        for thing, name, value in ((I, 'OWNERS', self.owners), (I, 'ProductionLock', lambda _: self.lock),
                                   (I.schema_fence, 'local_schema_fence', lambda **_: None), (I, 'resources', lambda *_: {})):
            p = patch.object(thing, name, value); p.start(); self.addCleanup(p.stop)
        self.job = self.make(self.request)

    def make(self, request):
        job = I.CandidatePreparation(self.inventory, request, I.digest(request))
        p = patch.object(job, 'identity'); p.start(); self.addCleanup(p.stop)
        return job

    def snapshot(self):
        return {str(p):p.read_bytes() for p in (self.old/'transparent-shard-server', self.publication/'shards.json')}

    def stage(self, job=None):
        job = job or self.job
        return job.run('stage', I.digest(job.run('plan')))

    def test_plan_and_preflight_are_read_only_then_stage_retains_exact_isolated_bundle(self):
        plan = self.job.run('plan'); self.job.run('preflight')
        self.assertEqual(plan['target'], str(C.TARGET))
        self.assertFalse(self.owners.exists()); self.assertFalse(C.ROOT.exists())
        record = self.stage()
        self.assertEqual(record['status'], 'staged'); self.assertEqual(record['kind'], 'candidate-artifact-preparation')
        self.assertEqual(C.verify_bundle(), C.identity())
        self.assertEqual((C.TARGET/'artifacts/shard-control').stat().st_mode & 0o777, 0o555)
        self.assertEqual((C.TARGET/'provenance.json').stat().st_mode & 0o777, 0o400)
        self.assertEqual(C.binary('transparent-shard-server'), C.TARGET/'artifacts/transparent-shard-server')
        self.assertEqual(self.job.run('status')['status'], 'staged')
        self.assertEqual(json.loads((self.owners/'latest.json').read_text()), {'request_sha256':self.job.identifier})
        self.assertEqual(self.snapshot(), self.before)
        self.assertEqual(sorted(p.name for p in C.ROOT.iterdir()), [C.TARGET.name])
        with self.assertRaisesRegex(ValueError, 'already owned'): self.stage()

    def test_changed_plan_or_request_identity_refuses_before_owner(self):
        with self.assertRaisesRegex(ValueError, 'plan changed'): self.job.run('stage', '0'*64)
        self.assertFalse(self.owners.exists())
        for key, value in (('candidate_sha', OLD), ('ci_run', 1), ('attempt', 0), ('extra', 1)):
            bad = copy.deepcopy(self.request); bad[key] = value
            with self.assertRaises(ValueError): I.CandidatePreparation(self.inventory, bad, I.digest(bad))
        for archives in ({**self.archives, 'supplemental':'relative.tar.gz'},
                         {**self.archives, 'supplemental':self.archives['transparent-filter']},
                         {k:v for k, v in self.archives.items() if k != 'transparent-filter'}):
            bad = dict(self.request, archives=archives)
            with self.assertRaises(ValueError): I.CandidatePreparation(self.inventory, bad, I.digest(bad))
        with self.assertRaises(ValueError): I.CandidatePreparation(self.inventory, self.request, '0'*64)

    def test_interrupted_stage_fences_other_mutations_until_explicit_same_fs_reconcile(self):
        files = C.files(self.archives); plan = I.digest(self.job.run('plan'))
        def interrupted(_files):
            (self.job.partial/'artifacts').mkdir(); (self.job.partial/'artifacts/partial').write_bytes(b'preserve')
            raise subprocess.TimeoutExpired('copy', 1)
        with patch.object(self.job, 'render', return_value=files), patch.object(self.job, 'write', interrupted), \
                self.assertRaises(subprocess.TimeoutExpired):
            self.job.run('stage', plan)
        self.assertEqual(self.job.run('status')['status'], 'interrupted')
        with patch.object(schema_fence, 'INPUT_STAGING', self.owners), patch.object(schema_fence, 'HOST_ACTIONS', self.root/'none'):
            with self.assertRaisesRegex(ValueError, 'unfinished input staging owner'):
                FENCE(self.root/'schema')
            with self.assertRaisesRegex(ValueError, 'already owned'): self.stage()
            self.job.run('reconcile')
            FENCE(self.root/'schema')
        abandoned = C.ROOT/(self.job.partial.name+'.abandoned-'+self.job.identifier)
        self.assertEqual((abandoned/'artifacts/partial').read_bytes(), b'preserve')
        self.assertEqual(self.job.run('status')['status'], 'reconciled')
        with self.assertRaises(ValueError): self.stage()
        retry = self.make(dict(self.request, attempt=2))
        self.assertEqual(self.stage(retry)['status'], 'staged')
        self.assertTrue(abandoned.exists()); self.assertEqual(self.snapshot(), self.before)

    def test_staged_bundle_drift_extra_link_hardlink_mode_and_provenance_refuse(self):
        self.stage()
        target = C.TARGET/'artifacts/shard-assign'
        changes = [
            lambda: (C.TARGET/'artifacts/extra').write_bytes(b'x'),
            lambda: (C.TARGET/'artifacts/alias').symlink_to(target),
            lambda: os.link(target, self.root/'outside-hard-link'),
            lambda: target.chmod(0o755),
        ]
        undo = [lambda: (C.TARGET/'artifacts/extra').unlink(), lambda: (C.TARGET/'artifacts/alias').unlink(),
                lambda: (self.root/'outside-hard-link').unlink(), lambda: target.chmod(0o555)]
        for change, revert in zip(changes, undo):
            change()
            with self.assertRaises(ValueError): self.job.run('status')
            revert()
        self.job.run('status')
        provenance = C.TARGET/'provenance.json'
        record = json.loads(provenance.read_text()); record['historical_release_sha'] = C.SOURCE_SHA
        provenance.chmod(0o600); provenance.write_text(json.dumps(record)); provenance.chmod(0o400)
        with self.assertRaisesRegex(ValueError, 'provenance'): C.verify_bundle()


def report(gate, **changes):
    value = {'status':'passed', 'gate':gate, 'native_source_sha':C.SOURCE_SHA, 'publication_sha256':PUBLICATION,
             'candidate_sha256':C.identity(), 'binaries':{n:C.ARTIFACTS[n] for n in C.GATES[gate]}}
    if gate == 'native-certificates':
        value.update(floors=dict(C.FLOORS), setup_bindings=[{'shard_id':0}], segments=1)
    if gate == 'comprehensive-ci':
        value.update(ci_run=C.CI_RUN, head_sha=C.SOURCE_SHA, conclusion='success', jobs=12, jobs_passed=12)
    value.update(changes)
    return value


class Gates(unittest.TestCase):
    def test_candidate_bound_reports_pass_only_for_the_exact_gate_and_publication(self):
        for gate in C.GATES:
            C.verify_gate(report(gate), gate, PUBLICATION)
            with self.assertRaises(ValueError): C.verify_gate(report(gate), gate, 'f'*64)
        with self.assertRaises(ValueError): C.verify_gate(report('native-certificates'), 'independent-chain-oracle', PUBLICATION)

    def test_retained_12ce_reports_and_relabelled_copies_never_qualify_candidate(self):
        for name, gate in (('full-native-certificates-801d7a62.json', 'native-certificates'),
                           ('full-chain-oracle-801d7a62.json', 'independent-chain-oracle')):
            old = json.loads((EVIDENCE/'activity-metadata-2026-09-30'/name).read_text())
            self.assertEqual(old['native_source_sha'], OLD)
            with self.assertRaises(ValueError): C.verify_gate(old, gate, PUBLICATION)
            relabelled = dict(old, native_source_sha=C.SOURCE_SHA)
            with self.assertRaises(ValueError): C.verify_gate(relabelled, gate, PUBLICATION)
            relabelled.update(candidate_sha256=C.identity(), binaries={n:C.ARTIFACTS[n] for n in C.GATES[gate]})
            with self.assertRaisesRegex(ValueError, 'foreign binary'): C.verify_gate(relabelled, gate, PUBLICATION)

    def test_mixed_missing_foreign_and_altered_floor_reports_refuse(self):
        old_cert = json.loads((EVIDENCE/'activity-metadata-2026-09-30/full-native-certificates-801d7a62.json').read_text())
        cases = [
            report('native-certificates', binaries={'examples/native_certificate':old_cert['binary_sha256']}),
            report('native-certificates', binaries={}),
            report('native-certificates', binaries={'examples/native_certificate':C.ARTIFACTS['examples/native_certificate'],
                                                    'shard-verify':C.ARTIFACTS['shard-verify']}),
            report('native-certificates', floors={'archive-wide-pages':80, 'otherwise':128}),
            report('native-certificates', floors={'otherwise':128}),
            report('native-certificates', segments=2),
            report('native-certificates', setup_bindings=[]),
            report('native-certificates', candidate_sha256='0'*64),
            report('native-certificates', status='pending'),
            report('native-certificates', native_source_sha=OLD),
            report('comprehensive-ci', ci_run=36819961986),
            report('comprehensive-ci', head_sha=OLD),
            report('comprehensive-ci', jobs_passed=11),
            report('artifact-verification', binary_sha256=C.ARTIFACTS['shard-verify'][::-1]),
        ]
        for value in cases:
            with self.assertRaises(ValueError): C.verify_gate(value, value['gate'], PUBLICATION)
        without = report('independent-chain-oracle'); del without['binaries']
        with self.assertRaises(ValueError): C.verify_gate(without, 'independent-chain-oracle', PUBLICATION)
        with self.assertRaises(ValueError): C.verify_gate(report('native-certificates'), 'unknown-gate', PUBLICATION)


class CutoverProofs(unittest.TestCase):
    """Legacy proofs stay valid for 12ce only; candidate proofs require version 2."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        output = root/'initial'; output.mkdir(); (output/'shards.json').write_bytes(b'publication')
        self.publication = hashlib.sha256(b'publication').hexdigest()
        evidence = root/'evidence'; evidence.mkdir()
        (evidence/'result.json').write_text(json.dumps({'status':'passed', 'map_sha256':self.publication}))
        for thing, name, value in ((I.P, 'OUTPUT', output), (I.P, 'EVIDENCE', evidence), (I.P, 'verify_release', lambda _: None)):
            p = patch.object(thing, name, value); p.start(); self.addCleanup(p.stop)
        self.inventory = SimpleNamespace(hosts={}, ssh={'mode':'config'}, lock={'type':'pinned_host','machine_id':'9'*32}, services={})
        sample = {'schema':'transparent-script-sample-v1', 'anchor_height':3500738, 'tool_sha':OLD,
                  'clients':[{'scripts':['s'], 'journal_events':1, 'expected_digest':'a'*64}]}
        self.files = {'inventory.json':json.dumps(vars(self.inventory)), 'v10-sample.json':json.dumps(sample),
                      'v11-sample.json':json.dumps(sample)}

    def proof(self, reports, candidate):
        request = {'version':2 if candidate else 1, 'source_sha':'e'*40, 'release_result_sha256':'f'*64, 'attempt':1,
                   'files':dict(self.files, **{g+'.json':json.dumps(r) for g, r in reports.items()})}
        if candidate: request['candidate_sha'] = C.SOURCE_SHA
        return I.CutoverPreparation(self.inventory, request, I.digest(request)).render()

    def test_legacy_reports_remain_valid_only_for_the_old_build(self):
        legacy = {g:{'status':'passed', 'gate':g, 'native_source_sha':OLD, 'publication_sha256':self.publication} for g in C.GATES}
        self.proof(legacy, False)
        with self.assertRaises(ValueError): self.proof(legacy, True)
        current = {g:report(g, publication_sha256=self.publication) for g in C.GATES}
        self.proof(current, True)
        with self.assertRaises(ValueError): self.proof(current, False)
        mixed = dict(current, **{'independent-chain-oracle':legacy['independent-chain-oracle']})
        with self.assertRaises(ValueError): self.proof(mixed, True)
        missing = {g:r for g, r in current.items() if g != 'comprehensive-ci'}
        with self.assertRaises(ValueError): self.proof(missing, True)

    def test_candidate_samples_keep_historical_tool_provenance(self):
        current = {g:report(g, publication_sha256=self.publication) for g in C.GATES}
        relabelled = json.loads(self.files['v11-sample.json']); relabelled['tool_sha'] = C.SOURCE_SHA
        self.files['v11-sample.json'] = json.dumps(relabelled)
        with self.assertRaisesRegex(ValueError, 'sample'): self.proof(current, True)


class ServiceInputs(unittest.TestCase):
    """The real service renderer binds worker pins and controller source per build."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        output = root/'full-v11/publications/initial'; output.mkdir(parents=True)
        shards = [{'shard_id':0, 'geometry':'archive-wide', 'start_height':0, 'manifest_digest':'a'*64},
                  {'shard_id':1, 'geometry':'recent-4k-8k', 'start_height':3000000, 'manifest_digest':'b'*64}]
        raw = json.dumps({'shards':shards}).encode(); (output/'shards.json').write_bytes(raw)
        evidence = root/'evidence'; evidence.mkdir()
        (evidence/'result.json').write_text(json.dumps({'status':'passed', 'map_sha256':hashlib.sha256(raw).hexdigest()}))
        for thing, name, value in ((I.P, 'OUTPUT', output), (I.P, 'EVIDENCE', evidence), (I.P, 'verify_release', lambda _: None)):
            p = patch.object(thing, name, value); p.start(); self.addCleanup(p.stop)
        self.inventory = SimpleNamespace(hosts={}, ssh={}, lock={}, services={})
        self.source = 'e'*40

    def request(self, version, pins, controller_source):
        source = I.SOURCE/self.source/'transparent/ops/scripts'
        controller = {'data_dir':str(I.P.JOURNAL), 'publication_root':str(I.P.OUTPUT.parent), 'initial_publication':str(I.P.OUTPUT),
                      'fleet_config':'/opt/transparent-publisher/v11/fleet.json', 'fleet_command':str(source/'transparent-live-fleet.py'),
                      'source_sha':controller_source, 'shadow':False, 'recent_from':3000000, 'recent_geometry':'recent-4k-8k',
                      'archive_geometry':'archive-wide', 'directory_choice':'all', 'range_profile':'zcash-transparent-range-v2'}
        fleet = {'state_dir':'/opt/transparent-publisher/v11/state', 'roster':'/opt/transparent-publisher/v11/roster.json',
                 'worker_schema':'transparent-shard-v11', 'worker_active_record':'/opt/transparent-publisher/v11/active.json',
                 'worker_runtime_cache_dir':'/srv/transparent-pir/v11/runtime-cache', 'worker_root':'/srv/transparent-pir/v11/publications'}
        roster = [{'id':'r1', 'role':'recent-replica'}, {'id':'r2', 'role':'recent-replica'},
                  {'id':'a1', 'role':'archive-owner', 'archive_range':[0, 0]}]
        fixture = {'schema':'transparent-shard-v11', 'tables':[
            {'shard_id':i, 'revision':d, 'geometry':g, 'table':t}
            for i, d, g in ((0, 'a'*64, 'archive-wide'), (1, 'b'*64, 'recent-4k-8k')) for t in ('directory', 'pages')]}
        units = {'transparent-publish-controller':'/usr/local/bin/transparent-publish-controller --config /opt/transparent-publisher/v11/controller.json',
                 'transparent-filter-server':'/usr/local/bin/transparent-filter-server --shard-dir '+str(I.P.OUTPUT),
                 'transparent-5qps-continuous':'/usr/bin/python3 -B '+str(source/'transparent-quality-load.py'),
                 'transparent-fleet-scaler':'/usr/bin/python3 -B '+str(source/'transparent-fleet-scaler.py'),
                 'transparent-replica-reconciler':'/usr/bin/python3 -B '+str(source/'transparent-live-fleet.py'),
                 'transparent-control-sessions':'/usr/bin/python3 -B '+str(source/'transparent-live-fleet.py')}
        files = {'controller.json':json.dumps(controller), 'fleet.json':json.dumps(fleet), 'roster.json':json.dumps(roster),
                 'fixture.json':json.dumps(fixture), 'pins.json':json.dumps({w['id']:pins for w in roster}),
                 'policy.json':json.dumps({'mode':'observe'}),
                 **{name+'.service':'[Service]\nExecStart='+argv+'\n' for name, argv in units.items()}}
        value = {'version':version, 'source_sha':self.source, 'release_result_sha256':'f'*64, 'attempt':1, 'files':files}
        if version == 2: value['candidate_sha'] = C.SOURCE_SHA
        return value

    def render(self, *args):
        request = self.request(*args)
        return I.ServicePreparation(self.inventory, request, I.digest(request)).render()

    def test_candidate_inputs_bind_candidate_worker_pins_and_controller_source(self):
        candidate, portable = C.ARTIFACTS['transparent-shard-server'], I.WORKER_HASHES['transparent-shard-server']
        self.render(2, candidate, C.SOURCE_SHA)
        self.render(1, portable, OLD)
        for version, pins, source in ((2, portable, C.SOURCE_SHA), (2, candidate, OLD), (1, candidate, OLD), (1, portable, C.SOURCE_SHA)):
            with self.subTest(version=version, pins=pins[:8], source=source[:8]), self.assertRaises(ValueError):
                self.render(version, pins, source)

    def test_request_version_and_candidate_identity_are_closed(self):
        bad = self.request(2, C.ARTIFACTS['transparent-shard-server'], C.SOURCE_SHA); bad['candidate_sha'] = OLD
        with self.assertRaises(ValueError): I.ServicePreparation(self.inventory, bad, I.digest(bad))
        bad = self.request(1, I.WORKER_HASHES['transparent-shard-server'], OLD); bad['candidate_sha'] = C.SOURCE_SHA
        with self.assertRaises(ValueError): I.ServicePreparation(self.inventory, bad, I.digest(bad))
        bad = self.request(2, C.ARTIFACTS['transparent-shard-server'], C.SOURCE_SHA); del bad['candidate_sha']
        with self.assertRaises(ValueError): I.ServicePreparation(self.inventory, bad, I.digest(bad))


class CandidateWorkers(Fixture):
    """Candidate executables stage beside, never into, the immutable publication."""

    def setUp(self):
        super().setUp()
        self.owners = self.root/'owners'; self.owners.mkdir(); self.lock = Lock(self.root/'lock')
        (C.TARGET/'artifacts/examples').mkdir(parents=True)
        for name in C.ARTIFACTS:
            path = C.path(name); path.write_bytes(self.data[name]); path.chmod(0o555)
        self.publication_root = self.root/'publications'
        self.mapping, self.assignment = b'{"map":1}', b'{"assignment":1}'
        names = {'shards.json':self.mapping, 'assignment.json':self.assignment, '.input-transparent-shard-server':b'old server',
                 '.input-shard-control':b'old control', '.input-transparent-shard-server.service':b'unit', 'a'*64+'/manifest.json':b'm'}
        self.published = self.publication_root/hashlib.sha256(self.mapping).hexdigest()
        files = []
        for name, data in sorted(names.items()):
            path = self.published/name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(data)
            mode = I.INPUTS.get(name, 0o600); path.chmod(mode)
            files.append({'path':name, 'source':'/coordinator/'+name, 'size':len(data), 'sha256':hashlib.sha256(data).hexdigest(), 'mode':mode})
        self.staged = {'version':1, 'source_sha':'b'*40, 'machine_id':'c'*32, 'worker_id':'worker-1',
                       'map_sha256':hashlib.sha256(self.mapping).hexdigest(), 'assignment_sha256':hashlib.sha256(self.assignment).hexdigest(),
                       'release_result_sha256':'d'*64, 'attempt':1, 'cache_bytes':1 << 30, 'files':files}
        self.staged_id = I.digest(self.staged)
        I.durable.atomic_json(self.owners/(self.staged_id+'.request.json'), self.staged)
        I.durable.atomic_json(self.owners/(self.staged_id+'.json'), {'status':'staged', 'request_sha256':self.staged_id})
        self.inventory = SimpleNamespace(hosts={'w1':{'machine_id':'c'*32}})
        for thing, name, value in ((I, 'OWNERS', self.owners), (I, 'resources', lambda *_: {}),
                                   (I.schema_fence, 'local_schema_fence', lambda **_: None)):
            p = patch.object(thing, name, value); p.start(); self.addCleanup(p.stop)
        self.request = I.build_candidate(self.inventory, 'w1', 'b'*40, self.staged_id, 1)

    def receiver(self, request=None):
        return I.Receiver(request or self.request, root=self.publication_root, owners=self.owners,
                          candidates=self.root/'worker-candidates', lock_factory=lambda: self.lock)

    def body(self):
        return b''.join(self.data[f['path'].removeprefix('.input-')] for f in self.request['files'])

    def test_render_binds_staged_publication_and_candidate_pair_only(self):
        self.assertEqual(self.request['version'], 2)
        self.assertEqual([f['path'] for f in self.request['files']], list(I.CANDIDATE_INPUTS))
        self.assertEqual({f['sha256'] for f in self.request['files']},
                         {C.ARTIFACTS['transparent-shard-server'], C.ARTIFACTS['shard-control']})
        with self.assertRaisesRegex(ValueError, 'staged publication'):
            I.build_candidate(SimpleNamespace(hosts={'w1':{'machine_id':'e'*32}}), 'w1', 'b'*40, self.staged_id, 1)
        I.durable.atomic_json(self.owners/(self.staged_id+'.json'), {'status':'failed', 'request_sha256':self.staged_id})
        with self.assertRaisesRegex(ValueError, 'staged publication'):
            I.build_candidate(self.inventory, 'w1', 'b'*40, self.staged_id, 1)
        for change in (lambda r: r['files'].pop(), lambda r: r['files'][0].update(sha256=I.WORKER_HASHES['transparent-shard-server']),
                       lambda r: r['files'][0].update(source='/srv/transparent-activity/portable-workers/x'),
                       lambda r: r.update(candidate_sha=OLD), lambda r: r['files'].append(copy.deepcopy(r['files'][0]))):
            bad = copy.deepcopy(self.request); change(bad)
            with self.assertRaises(ValueError): I.validate(bad)

    def test_stage_verifies_publication_with_candidate_binary_without_touching_it(self):
        before = {p:p.read_bytes() for p in self.published.rglob('*') if p.is_file()}
        receiver = self.receiver(); calls = []
        def native(lock):
            calls.append(lock.descriptors())
            self.assertEqual((receiver.partial/'.input-transparent-shard-server').read_bytes(), self.data['transparent-shard-server'])
        with patch.object(receiver, 'native', native):
            result = receiver.stage(io.BytesIO(self.body()))
        self.assertEqual(result['status'], 'staged'); self.assertEqual(len(calls), 1)
        self.assertEqual(receiver.target, self.root/'worker-candidates'/('release-'+C.SOURCE_SHA))
        self.assertEqual(sorted(p.name for p in receiver.target.iterdir()), sorted(I.CANDIDATE_INPUTS))
        self.assertEqual({p:p.read_bytes() for p in self.published.rglob('*') if p.is_file()}, before)
        self.assertEqual(receiver.status()['status'], 'staged')

    def test_changed_publication_or_foreign_worker_refuses_before_native_or_rename(self):
        (self.published/'assignment.json').chmod(0o600); (self.published/'assignment.json').write_bytes(b'{"assignment":2}')
        receiver = self.receiver()
        with patch.object(receiver, 'native') as native, self.assertRaises(ValueError):
            receiver.stage(io.BytesIO(self.body()))
        native.assert_not_called(); self.assertFalse(receiver.target.exists())
        self.assertEqual(receiver.status()['status'], 'failed')
        foreign = copy.deepcopy(self.request); foreign['worker_id'] = 'worker-2'
        with self.assertRaisesRegex(ValueError, 'staged publication'): self.receiver(foreign).preflight()

    def test_native_verification_uses_candidate_binary_against_staged_publication(self):
        receiver = self.receiver()
        argv = []
        class Process:
            pid = 1; returncode = 0
            def poll(self): return 0
        def popen(arguments, **_):
            argv.append(arguments); return Process()
        receiver.owners = self.owners
        with self.lock, patch.object(I.subprocess, 'Popen', popen):
            receiver.partial.mkdir(parents=True)
            (receiver.partial/'.input-transparent-shard-server').write_bytes(self.data['transparent-shard-server'])
            receiver.native(self.lock)
        self.assertEqual(argv[0][0], str(receiver.partial/'.input-transparent-shard-server'))
        self.assertEqual(argv[0][argv[0].index('--shard-dir')+1], str(self.published))
        self.assertEqual(argv[0][argv[0].index('--assignment')+1], str(self.published/'assignment.json'))
        self.assertIn('--verify-only', argv[0])


class ProductSelection(unittest.TestCase):
    """Version 2 binds candidate tools; rollback keeps captured historical identities."""

    def spec(self, version):
        def entry(path, sha='5'*64): return {'path':path, 'sha256':sha}
        def worker(n, binary):
            return {'host':'w%d' % n, 'plan':{'source_sha':'a'*40, 'machine_id':str(n)*32, 'role':'worker',
                    'worker':{'id':'w%d' % n, 'map_file_sha256':PUBLICATION, 'assignment_sha256':'6'*64, 'binary_sha256':binary}}}
        binary = C.ARTIFACTS['transparent-shard-server'] if version == 2 else I.WORKER_HASHES['transparent-shard-server']
        hosts = [{'host':'coordinator', 'plan':{'source_sha':'a'*40, 'machine_id':'7'*32, 'role':'coordinator'}},
                 {'host':'router', 'plan':{'source_sha':'a'*40, 'machine_id':'8'*32, 'role':'router'}},
                 *(worker(n, binary) for n in (1, 2, 3))]
        recovery = {'v10':{'binary':R.READER, 'binary_sha256':'1'*64, 'sample':'/s10', 'sample_sha256':'2'*64},
                    'v11':{'binary':R.READER, 'binary_sha256':'1'*64, 'sample':'/s11', 'sample_sha256':'3'*64}}
        load = {'binary':entry(str(PRODUCT.LOAD_BINARY)), 'fixture':entry('/f'), 'pins':entry('/p'), 'policy':entry('/o')}
        if version == 2:
            recovery['v11'].update(binary=R.CANDIDATE_READER, binary_sha256=C.ARTIFACTS['transparent-loadtest'])
            load['binary'] = entry(str(C.path('examples/rate-query')), C.ARTIFACTS['examples/rate-query'])
        spec = {'version':version, 'source_sha':'a'*40, 'inventory':entry('/inventory.json'), 'publication_sha256':PUBLICATION,
                'assignment':entry('/assignment.json', '6'*64), 'recent_from':3000000, 'hosts':hosts,
                'routing':{'machine_id':'7'*32, 'source_sha':'a'*40, 'recovery':recovery}, 'load':load,
                'gates':{g:entry('/'+g) for g in C.GATES}}
        if version == 2: spec['candidate_sha'] = C.SOURCE_SHA
        return spec

    def validate(self, spec):
        with patch.object(PRODUCT.T, 'validate', lambda *_: None):
            return PRODUCT.validate(spec)

    def test_candidate_and_legacy_tools_are_not_interchangeable(self):
        for version in (1, 2): self.validate(self.spec(version))
        cases = {
            'legacy load in candidate': (2, lambda s: s['load'].update(binary=self.spec(1)['load']['binary'])),
            'candidate load in legacy': (1, lambda s: s['load'].update(binary=self.spec(2)['load']['binary'])),
            'legacy forward reader in candidate': (2, lambda s: s['routing']['recovery']['v11'].update(binary=R.READER)),
            'candidate forward reader in legacy': (1, lambda s: s['routing']['recovery'].update(v11=self.spec(2)['routing']['recovery']['v11'])),
            'candidate rollback reader': (2, lambda s: s['routing']['recovery']['v10'].update(binary=R.CANDIDATE_READER)),
            'changed candidate reader bytes': (2, lambda s: s['routing']['recovery']['v11'].update(binary_sha256='9'*64)),
            'portable worker in candidate': (2, lambda s: s['hosts'][2]['plan']['worker'].update(binary_sha256=I.WORKER_HASHES['transparent-shard-server'])),
            'foreign candidate': (2, lambda s: s.update(candidate_sha=OLD)),
            'implicit candidate': (1, lambda s: s.update(candidate_sha=C.SOURCE_SHA)),
            'missing candidate': (2, lambda s: s.pop('candidate_sha')),
            'missing gate': (2, lambda s: s['gates'].pop('comprehensive-ci')),
        }
        for label, (version, change) in cases.items():
            spec = self.spec(version); change(spec)
            with self.subTest(label), self.assertRaises((ValueError, KeyError)): self.validate(spec)

    def test_candidate_inputs_require_candidate_bound_gates_not_historical_reports(self):
        product = object.__new__(PRODUCT.Product)
        product.spec = self.spec(2)
        old = json.loads((EVIDENCE/'activity-metadata-2026-09-30/full-chain-oracle-801d7a62.json').read_text())
        reports = {g:report(g) for g in C.GATES}
        reports['independent-chain-oracle'] = old
        loads = []
        def load(path):
            loads.append(path)
            return reports[Path(path).name]
        verified = []
        product.workers = [{'plan':{'worker':{'id':i, 'map_sha256':'m'}}} for i in ('r1', 'r2', 'a1')]
        product.hosts = []
        mapping = {'start_height':0, 'shards':[{'end_height':3500738}]}
        product.routing = SimpleNamespace(private_router=lambda: None)
        product.spec['routing']['private_router'] = None
        assignment = {'set':{'map_sha256':'m', 'shard_schema':'transparent-shard-v11'}, 'unassigned':[],
                      'generated_by':{'source_sha':OLD}, 'workers':[{'id':'r1', 'role':'recent-replica'}, {'id':'r2', 'role':'recent-replica'},
                                                                  {'id':'a1', 'role':'archive-owner'}]}
        with patch.object(PRODUCT.D.S, 'verify_receipt'), patch.object(PRODUCT, 'checked', side_effect=lambda e: Path(e['path'])), \
                patch.object(PRODUCT.H, 'load', side_effect=lambda p: {'archive_sha256':'x'} if 'staging' in str(p) else
                             mapping if str(p).endswith('shards.json') else assignment if str(p).endswith('assignment.json') else load(p)), \
                patch.object(PRODUCT.H, 'checksum', return_value=PUBLICATION), patch.object(PRODUCT.Path, 'is_file', return_value=True), \
                patch.object(PRODUCT.H.transparent_map, 'served_sha256', return_value='m'), patch.object(PRODUCT.R, 'assignment_digest'), \
                patch.object(C, 'verify_bundle', side_effect=lambda: verified.append(True)):
            with self.assertRaisesRegex(ValueError, 'independent-chain-oracle'): product.inputs()
            self.assertEqual(verified, [True])
            # The same historical report remains the legacy build's accepted gate shape.
            legacy = self.spec(1); legacy['routing']['private_router'] = None; product.spec = legacy
            reports.update({g:{'status':'passed', 'gate':g, 'native_source_sha':OLD, 'publication_sha256':PUBLICATION} for g in C.GATES})
            reports['independent-chain-oracle'] = old
            # All four historical gates pass; the fixture stops at the next input (load pins).
            with self.assertRaises(KeyError) as caught: product.inputs()
            self.assertEqual(caught.exception.args, ('p',))

    def test_routing_rollback_reader_stays_historical_even_for_candidate(self):
        plan = {'version':1, 'source_sha':'a'*40, 'machine_id':'b'*32, 'transaction':'transparent-schema-20261004-x',
                'old_fleet':{'path':'/opt/transparent-publisher/fleet.json', 'sha256':'c'*64},
                'new_fleet':{'path':'/opt/transparent-publisher/v11/fleet.json', 'sha256':'d'*64},
                'coordinator_baseline':'/opt/transparent-publisher/schema-rollback/transparent-schema-20261004-x',
                'original_coordinator_sha256':'e'*64, 'private_router':'10.142.0.5:8080',
                'recovery':{'v10':{'binary':R.READER, 'binary_sha256':'1'*64, 'sample':'/s10', 'sample_sha256':'2'*64},
                            'v11':{'binary':R.CANDIDATE_READER, 'binary_sha256':'3'*64, 'sample':'/s11', 'sample_sha256':'4'*64}}}
        R.validate(plan)
        bad = copy.deepcopy(plan); bad['recovery']['v10']['binary'] = R.CANDIDATE_READER
        with self.assertRaisesRegex(ValueError, 'retained compatible'): R.validate(bad)
        bad = copy.deepcopy(plan); bad['recovery']['v11']['binary'] = '/tmp/transparent-loadtest'
        with self.assertRaises(ValueError): R.validate(bad)
        self.assertEqual(sum(PRODUCT.ROLLBACK_TIMEOUTS.values()), 740)

    def test_candidate_preflight_refuses_rollback_sample_relabelled_with_candidate_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); paths = {kind:root/(kind+'.json') for kind in ('v10', 'v11')}
            candidate = C.ARTIFACTS['transparent-shard-server']
            paths['v10'].write_text(json.dumps({'cutover_worker_pins':{'w':candidate}}))
            paths['v11'].write_text(json.dumps({'cutover_worker_pins':{'w':candidate}}))
            product = object.__new__(PRODUCT.Product); product.bound = lambda _: None; product.inputs = lambda: None
            effects = []; product.local = SimpleNamespace(identity=lambda: None, preflight=lambda: effects.append('host'))
            product.coordinator = {'host':'coordinator', 'plan':{'machine_id':'a'*32}}
            product.workers = [{'host':'w', 'plan':{'worker':{'id':'w', 'binary_sha256':candidate}}}]
            product.hosts = [product.coordinator]
            product.inventory = SimpleNamespace(lock={'type':'pinned_host','machine_id':'a'*32}, hosts={'coordinator':{'machine_id':'a'*32}})
            product.spec = {'version':2, 'routing':{'recovery':{k:{'sample':str(p), 'sample_sha256':'a'*64} for k, p in paths.items()}}}
            with patch.object(PRODUCT, 'checked', side_effect=lambda e: Path(e['path'])):
                with self.assertRaisesRegex(ValueError, 'captured predecessor'): product.preflight()
            self.assertFalse(effects)

    def test_recipe_binds_candidate_module_as_checksum_dependency(self):
        spec = {'source_sha':'a'*40, 'publication_sha256':'b'*64, 'inventory':{'path':'/inventory.json'}}
        with patch.object(PRODUCT, 'checked', return_value=Path('/spec.json')), patch.object(PRODUCT, 'validate', return_value=spec), \
                patch.object(PRODUCT.H, 'load', return_value=spec), patch.object(PRODUCT.H, 'checksum', return_value='c'*64):
            recipe = PRODUCT.recipe('/spec.json', 'c'*64)
        self.assertIn('/srv/transparent-activity/ops/sources/'+'a'*40+'/transparent/ops/lib/activity_candidate.py',
                      {entry['path'] for entry in recipe['inputs']})


if __name__ == '__main__':
    unittest.main()
