"""The Publisher-Query Server contract policy, as operations applies it.

This mirrors `src/shared/pir-service-discovery`: a contract version is a stable
SemVer version with major at least 1 and no prerelease, and a query server may
use a publisher exactly when both implement the same major version, whatever
their minor and patch versions. Deployment reads a staged executable's
`--contract-info` report and a running publisher's discovery document through
this module instead of comparing hard-coded majors per product.

Compiled products are what an executable can serve; enabled products are the
configuration sections a host turns on. A unified node needs every enabled
product compiled into its one executable.
"""
from dataclasses import dataclass
import json
import re
import tomllib

from .paths import ROOT

DISCOVERY_PATH = '/.well-known/pir-service-discovery'
PUBLISHER_SERVICE = 'pir-publisher'
CONTRACT_INFO_SCHEMA = 'wallet-pir-contract-info/1'
CONTRACT_INFO_ARGV = ('--contract-info',)
EXECUTABLES = ('pir-publisher', 'pir-query-server')
# The discovery crate's limit on a document a consumer reads.
MAX_DOCUMENT_BYTES = 16 << 10

# SemVer 2.0 core with optional prerelease and build metadata.
_SEMVER = re.compile(r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)'
                     r'(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?'
                     r'(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?')


class ContractError(ValueError):
    """Contract metadata is missing, malformed or incompatible."""


def contract_major(text):
    """Return the major of a stable contract version, refusing anything else."""
    match = _SEMVER.fullmatch(text) if isinstance(text, str) else None
    if match is None:
        raise ContractError('invalid contract version %r' % (text,))
    if match.group(4) is not None:
        raise ContractError('prerelease version %r is not a contract version' % text)
    if any(int(match.group(index)) > (1 << 64) - 1 for index in (1, 2, 3)):
        raise ContractError('contract version component exceeds an unsigned 64-bit integer')
    major = int(match.group(1))
    if major == 0:
        raise ContractError('0.x version %r is not a stable contract version' % text)
    return major


def source_major():
    """The contract major of the discovery crate in this operations checkout.

    This is the version a query server built from the same source compiles in;
    a staged executable's own `--contract-info` is authoritative for its bytes.
    """
    manifest = tomllib.loads((ROOT / 'src/shared/pir-service-discovery/Cargo.toml').read_text())
    return contract_major(manifest['package']['version'])


def compatible(query_server_version, publisher_version):
    """Whether a query server at one version may use a publisher at the other."""
    return contract_major(query_server_version) == contract_major(publisher_version)


@dataclass(frozen=True)
class ContractInfo:
    """One executable's compiled identity, bound to the bytes that reported it."""
    executable: str
    sha256: str
    contract_version: str
    contract_major: int
    products: tuple
    backends: tuple

    def to_json(self):
        return {'executable': self.executable, 'sha256': self.sha256,
                'contract_version': self.contract_version, 'contract_major': self.contract_major,
                'products': list(self.products), 'backends': list(self.backends)}


def parse_contract_info(output, *, executable, sha256):
    """Parse a `--contract-info` report from the executable whose digest is `sha256`.

    Refuses a report from another executable, an unknown schema, a major that
    disagrees with its own version, or malformed product lists.
    """
    try:
        document = json.loads(output)
    except (TypeError, ValueError) as error:
        raise ContractError('%s %s: contract info is not JSON: %s' % (executable, sha256[:12], error))
    if not isinstance(document, dict) or document.get('schema') != CONTRACT_INFO_SCHEMA:
        raise ContractError('%s %s: unknown contract info schema' % (executable, sha256[:12]))
    if document.get('executable') != executable:
        raise ContractError('%s %s: contract info names executable %r'
                            % (executable, sha256[:12], document.get('executable')))
    version = document.get('contract_version')
    major = contract_major(version)
    if type(document.get('contract_major')) is not int or document['contract_major'] != major:
        raise ContractError('%s %s: contract major %r disagrees with version %s'
                            % (executable, sha256[:12], document.get('contract_major'), version))
    lists = {}
    for field in ('products', 'backends'):
        value = document.get(field)
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value) \
                or len(set(value)) != len(value):
            raise ContractError('%s %s: contract info %s must be distinct names' % (executable, sha256[:12], field))
        lists[field] = tuple(sorted(value))
    return ContractInfo(executable, sha256, version, major, lists['products'], lists['backends'])


def publisher_document_major(status, body):
    """The contract major a publisher's discovery answer advertises.

    Raises `ContractError` for anything other than a valid pir-publisher
    document, so unknown compatibility is never treated as compatible.
    """
    if status != 200:
        raise ContractError('discovery answered HTTP %s' % status)
    if not isinstance(body, (str, bytes)):
        raise ContractError('discovery document must be UTF-8 JSON')
    try:
        encoded = body.encode('utf-8') if isinstance(body, str) else body
        body = encoded.decode('utf-8')
    except UnicodeError as error:
        raise ContractError('discovery document must be UTF-8 JSON') from error
    if len(encoded) > MAX_DOCUMENT_BYTES:
        raise ContractError('discovery document is missing or larger than %d bytes' % MAX_DOCUMENT_BYTES)
    try:
        document = json.loads(body)
    except (TypeError, ValueError):
        raise ContractError('discovery answer is not JSON')
    if not isinstance(document, dict) or document.get('service') != PUBLISHER_SERVICE:
        raise ContractError('discovery does not name service %s' % PUBLISHER_SERVICE)
    return contract_major(document.get('version'))


def missing_products(info, enabled):
    """Enabled products the executable does not compile, sorted."""
    return sorted(set(enabled) - set(info.products))
