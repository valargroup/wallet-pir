"""Changed-native activity candidate identity, bundle and gate checks.

The immutable v11 publication, its initial assignment, journal, recovery samples
and rollback evidence keep the frozen 12ce identity
(`activity_publication_job.RELEASE_SHA`). This module is the separate executable
and qualification identity of candidate `c3c66b9b`: five roles from exact-head
comprehensive CI plus 13 supplemental fat-LTO tools, prepared inertly under
their own coordinator namespace. Old 12ce gate reports never qualify it; root
must supply new candidate-bound reports from retained raw results.
"""
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import re
import stat
import tarfile
import tempfile

from wallet_pir_ops import durable

HERE = Path(__file__).parent


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


P = module('candidate_historical_publication', HERE/'activity_publication_job.py')

SOURCE_SHA = 'c3c66b9b51a5e2f4a6ac9261a941a397d119e920'
HISTORICAL_SHA = P.RELEASE_SHA
CI_RUN = 37173250956
CI_JOBS = 12
# Five serving roles from the exact-head CI release bundles of run 37173250956.
CI_KINDS = {
    'transparent-publisher': ('transparent-publish-controller', 'transparent-shard-server', 'shard-control', 'shard-assign'),
    'transparent-filter': ('transparent-filter-server',),
}
CI_PINS = {
    'shard-assign': '1504f8707eb9811b81f1c4f9816d0c7ccc5676d02017f77a522ad9c9ffd67a9f',
    'shard-control': '87e173e9eac63955cc761cc755f9382ec724d3c16f1352095f76ed5f8c8b5b36',
    'transparent-filter-server': 'e619894ed6c231bd592098711e141e4993df7eea73e03bb04d54bf7c19b56313',
    'transparent-publish-controller': 'de29fb887c880dc9623afdb4ad4019eaa7ddbc877f2d4e14b16b4def9d822da5',
    'transparent-shard-server': '3e929fdad00cad02935c2e3c35fdcb15681f1ba7a32be63c37736cfb8dc8c5ff',
}
# transparent/evidence/activity-metadata-2026-10-04/manifest.json (supplemental 13).
SUPPLEMENTAL_ARCHIVE_SHA256 = 'd3a8f60a76e0fa59288fe670275abed75b304b7d390cde368effb842c840a9c3'
SUPPLEMENTAL_PINS = {
    'transparent-event-ingest': '3a55bad8bd3f067f02afe4b962922ce21de125e14b7ba395deec5f5e34f1fd4c',
    'shard-publish': 'e3c97822ce6bd47c227f73ca0eaa1d3148db0b49997213d23ad5a5320ba94c25',
    'event-spotcheck': 'b41f39892685a79942d113723a63fa280e0fcde0e3dde1f55b303a62cadaf617',
    'shard-verify': '94678c6b15dd04b79419899f4f2509d9b0dc97288ee2dff85e3f29616d1c4f5a',
    'script-sample': 'a8ee0aca180692ebf286f9236eecd32a9bc4a7ec84e768125ae99d450671c7a3',
    'shard-cutoff': '227f2f93640a686860e3a2079a4612b4563e31099180d27e61200b0aaf13df7b',
    'journal-inventory': '3f0d369d92fc49eb0fc9763118d2053fadc4107863b60b876a20df3790c07b80',
    'shard-census': 'fd24af1bf3595443f7e159d9b66e5266ca9b989c80ffa5c003b9a5f2852a019f',
    'transparent-loadtest': 'd4f6aa14729d1ab11954fcaf6d189d2d00788e64e1fce17efeff7aa82b3140c9',
    'transparent-measure': '44a867ac8e41c50387e266cc929a4b1a9ce8a9607e910664f1e5b33c453f2165',
    'examples/rate-query': 'cd0a8910e52c310a258dcd1845d5d06c49850692015089c53d40f17e810af2de',
    'examples/native_certificate': '73fa6513a2d87f31c6f7c356079592a7ca4508b8130d4204635edcd6c2613e6a',
    'examples/activity-reopen': '493fc2ad8e9d9a70c98724cf007e0b1db8e6095ccca616cbcf6ce043bd575220',
}
ARTIFACTS = {**CI_PINS, **SUPPLEMENTAL_PINS}
BUILD = {'profile':'release', 'lto':'fat', 'codegen_units':1, 'toolchain':'1.97.1',
         'rustflags':'-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq',
         'cargo_toml_sha256':'adcbb246f0b25c030564c469ddcc9d983fa7f204ddd0a45e27c193058da7fbb6',
         'cargo_lock_sha256':'bf3559f2ad10f1fe91a9e729b8c9f3dbff7c68ca1d17f65a25b68c8837956377',
         'rust_toolchain_sha256':'5d959dfcc98b53886ee772ba216c4f9a1b31f093b46b5b263c0d084af54e821d'}
# Read-only ABI report of the five production hosts: glibc 2.39. This bounds
# loadability only; it is not runtime or hardware qualification.
GLIBC_CEILING = (2, 39)
ROOT = Path('/srv/transparent-activity/candidates')
TARGET = ROOT/('release-'+SOURCE_SHA)
MAX_ARCHIVE = 1 << 30
MAX_ARTIFACT = 256 << 20
MAX_MEMBERS = 4096
BINARY_MODE = 0o555
OWNER = 0
GATES = {'artifact-verification':('shard-verify',),
         'native-certificates':('examples/native_certificate',),
         'independent-chain-oracle':('event-spotcheck',),
         'comprehensive-ci':tuple(sorted(CI_PINS))}
FLOORS = {'archive-wide-pages':83, 'otherwise':128}
HEX = re.compile('[0-9a-f]{64}')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def provenance():
    """Static candidate identity; gate reports bind its digest, IDENTITY."""
    return {'version':1, 'kind':'activity-candidate-release', 'source_sha':SOURCE_SHA,
            'historical_release_sha':HISTORICAL_SHA, 'ci_run':CI_RUN, 'ci_kinds':{k:list(v) for k,v in CI_KINDS.items()},
            'supplemental_archive_sha256':SUPPLEMENTAL_ARCHIVE_SHA256, 'build':BUILD,
            'glibc_ceiling':'%d.%d' % GLIBC_CEILING, 'artifacts':ARTIFACTS, 'qualification':'unqualified'}


def identity():
    return durable.digest(provenance())


def path(name, root=None):
    require(name in ARTIFACTS, 'not a candidate artifact')
    return Path(root or TARGET)/'artifacts'/name


def checksum(file):
    with Path(file).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def no_links(file):
    for parent in [Path(file), *Path(file).parents]:
        require(not parent.is_symlink(), 'candidate path contains a symlink')


def abi(name, data):
    """Linux x86-64 ELF whose versioned glibc requirement fits the fleet."""
    require(len(data) >= 64 and data[:4] == b'\x7fELF' and data[4] == 2 and data[5] == 1 and
            int.from_bytes(data[16:18], 'little') in (2, 3) and int.from_bytes(data[18:20], 'little') == 0x3e,
            'candidate artifact is not a Linux x86-64 ELF executable: '+name)
    versions = {(int(a), int(b)) for a, b in re.findall(rb'GLIBC_(\d+)\.(\d+)(?:\.\d+)?\x00', data)}
    require(versions and max(versions) <= GLIBC_CEILING, 'candidate artifact exceeds the fleet glibc ABI: '+name)


def binary(name, root=None):
    """One verified candidate executable; refuses links, mode and byte drift."""
    file = path(name, root)
    no_links(file)
    info = file.lstat()
    require(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == BINARY_MODE and info.st_uid == OWNER and
            checksum(file) == ARTIFACTS[name], 'candidate artifact identity/mode differs: '+name)
    return file


def regular(file, bound):
    file = Path(file)
    require(file.is_absolute(), 'candidate input must be absolute')
    no_links(file)
    info = file.lstat()
    require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= bound, 'candidate input is not a bounded regular file')
    return file


def read_ci(archive, kind):
    """Five CI roles through the release tool's exact-revision bundle checks."""
    release = module('candidate_release', HERE.parents[2]/'tools/ci/release.py')
    regular(archive, MAX_ARCHIVE)
    require(tuple(release.BINARIES[kind]) == CI_KINDS[kind], 'CI release kind inventory changed')
    with tempfile.TemporaryDirectory(prefix='wallet-pir-candidate-', dir='/dev/shm') as tmp:
        destination = Path(tmp)/'bundle'
        # Unique flat regular members, revision, SHA256SUMS and exact inventory.
        release.extract(Path(archive), destination, SOURCE_SHA, kind)
        payload = {name:(destination/name).read_bytes() for name in CI_KINDS[kind]}
    return payload, checksum(archive)


def read_supplemental(archive):
    """The 13 tools retained in the supplemental archive, bound by its digest."""
    archive = regular(archive, MAX_ARCHIVE)
    require(checksum(archive) == SUPPLEMENTAL_ARCHIVE_SHA256, 'supplemental archive digest differs from manifest')
    prefix = PurePosixPath('release-'+SOURCE_SHA)
    wanted = {str(prefix/'artifacts'/name):name for name in SUPPLEMENTAL_PINS}
    payload, result, seen = {}, None, set()
    with tarfile.open(archive, 'r:gz') as stream:
        for count, member in enumerate(stream):
            require(count < MAX_MEMBERS, 'supplemental archive has too many members')
            name = PurePosixPath(member.name)
            require(member.name not in seen and str(name) == member.name and not name.is_absolute() and
                    '..' not in name.parts and name.parts[0] == prefix.name, 'unsafe or duplicate supplemental member')
            seen.add(member.name)
            require(member.isfile() or member.isdir(), 'supplemental archive contains links or special files')
            if member.name in wanted:
                require(member.isfile() and member.size <= MAX_ARTIFACT, 'supplemental artifact exceeds bound')
                payload[wanted[member.name]] = stream.extractfile(member).read()
            elif member.name == str(prefix/'result.json'):
                require(member.isfile() and member.size <= 1 << 20, 'supplemental result exceeds bound')
                result = json.loads(stream.extractfile(member).read())
            else:
                require(not PurePosixPath(member.name).is_relative_to(prefix/'artifacts') or member.isdir(),
                        'foreign supplemental artifact')
    require(set(payload) == set(SUPPLEMENTAL_PINS) and isinstance(result, dict), 'incomplete supplemental archive')
    stages = result.get('stages')
    require(result.get('source_sha') == SOURCE_SHA and result.get('status') == 'passed' and result.get('build_exit') == 0 and
            result.get('profile') == BUILD['profile'] and result.get('toolchain') == BUILD['toolchain'] and
            result.get('rustflags') == BUILD['rustflags'] and
            {k:result.get('input_sha256', {}).get(k) for k in ('Cargo.toml','Cargo.lock','rust-toolchain.toml')} ==
            {'Cargo.toml':BUILD['cargo_toml_sha256'], 'Cargo.lock':BUILD['cargo_lock_sha256'],
             'rust-toolchain.toml':BUILD['rust_toolchain_sha256']} and
            isinstance(stages, list) and stages and
            all(s.get('exit_code') == 0 and s.get('native_cpu_flag_seen') is False and s.get('requested_flags_seen') is True
                for s in stages) and
            not any(result.get('ci_built_roles_present_in_lane', {'missing':True}).values()) and
            {k:v.get('sha256') for k, v in result.get('artifacts', {}).items()} == SUPPLEMENTAL_PINS,
            'supplemental build provenance differs from the approved fat-LTO candidate')
    return payload


def collect(archives):
    """All 18 candidate artifacts, verified before any byte is retained."""
    require(isinstance(archives, dict) and set(archives) == {*CI_KINDS, 'supplemental'}, 'candidate archive set differs')
    payload, digests = {}, {}
    for kind in CI_KINDS:
        items, digests[kind] = read_ci(archives[kind], kind)
        payload.update(items)
    payload.update(read_supplemental(archives['supplemental']))
    digests['supplemental'] = SUPPLEMENTAL_ARCHIVE_SHA256
    require(set(payload) == set(ARTIFACTS), 'candidate artifact inventory differs')
    for name, data in payload.items():
        require(len(data) <= MAX_ARTIFACT and hashlib.sha256(data).hexdigest() == ARTIFACTS[name],
                'candidate artifact checksum differs: '+name)
        abi(name, data)
    return payload, digests


def files(archives):
    """Exact retained bundle: provenance plus every artifact under artifacts/."""
    payload, digests = collect(archives)
    record = {**provenance(), 'identity':identity(), 'archives':digests}
    return {'provenance.json':durable.canonical(record)+b'\n',
            **{'artifacts/'+name:data for name, data in payload.items()}}


def verify_bundle(root=None):
    """Whole retained bundle: exact files, no links, modes, provenance and ABI."""
    root = Path(root or TARGET)
    no_links(root)
    expected = {'provenance.json', *('artifacts/'+name for name in ARTIFACTS)}
    directories = {'artifacts', 'artifacts/examples'}
    found = set()
    for entry in root.rglob('*'):
        relative = entry.relative_to(root).as_posix()
        info = entry.lstat()
        require(not stat.S_ISLNK(info.st_mode) and info.st_uid == OWNER, 'candidate bundle contains a link or foreign owner')
        if stat.S_ISDIR(info.st_mode):
            require(relative in directories, 'unexpected candidate bundle directory')
            continue
        require(stat.S_ISREG(info.st_mode) and relative in expected and info.st_nlink == 1, 'unexpected candidate bundle file')
        found.add(relative)
    require(found == expected, 'candidate bundle file set differs')
    record = json.loads((root/'provenance.json').read_bytes())
    require(stat.S_IMODE((root/'provenance.json').lstat().st_mode) == 0o400 and
            {k:v for k, v in record.items() if k not in ('identity', 'archives')} == provenance() and
            record.get('identity') == identity() and isinstance(record.get('archives'), dict) and
            set(record['archives']) == {*CI_KINDS, 'supplemental'} and
            record['archives']['supplemental'] == SUPPLEMENTAL_ARCHIVE_SHA256 and
            all(isinstance(v, str) and HEX.fullmatch(v) for v in record['archives'].values()),
            'candidate provenance differs')
    for name in ARTIFACTS:
        file = binary(name, root)
        require(file.stat().st_size <= MAX_ARTIFACT, 'candidate artifact exceeds bound')
        abi(name, file.read_bytes())
    return identity()


def verify_gate(report, gate, publication_sha256):
    """A passed report bound to this candidate's source, binaries and floors.

    Historical 12ce reports and relabelled documents refuse: the closed binary
    map must equal the candidate pins of exactly the gate's executables.
    """
    require(gate in GATES and isinstance(report, dict), 'unknown candidate gate')
    names = GATES[gate]
    require(report.get('status') == 'passed' and report.get('gate') == gate and
            report.get('native_source_sha') == SOURCE_SHA and report.get('publication_sha256') == publication_sha256 and
            report.get('candidate_sha256') == identity() and
            report.get('binaries') == {name:ARTIFACTS[name] for name in names},
            'candidate gate is not bound to the candidate source and binaries: '+gate)
    require('binary_sha256' not in report or (len(names) == 1 and report['binary_sha256'] == ARTIFACTS[names[0]]),
            'candidate gate names a foreign binary: '+gate)
    if gate == 'native-certificates':
        bindings = report.get('setup_bindings')
        require(report.get('floors') == FLOORS and isinstance(bindings, list) and bindings and
                report.get('segments') == len(bindings), 'candidate certificate floors or coverage differ')
    if gate == 'comprehensive-ci':
        require(report.get('ci_run') == CI_RUN and report.get('head_sha') == SOURCE_SHA and
                report.get('conclusion') == 'success' and report.get('jobs') == CI_JOBS and
                report.get('jobs_passed') == CI_JOBS, 'candidate comprehensive CI identity differs')
    return report
