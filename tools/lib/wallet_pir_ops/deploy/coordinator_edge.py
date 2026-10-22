"""Coordinator Caddyfile rendering and per-site selection.

The coordinator edge serves two TLS sites: the Enhance product host (whose name
is substituted from ENHANCE_PUBLIC_URL) and the unified wallet host. Helpers that
rewrite one site's routes select that site with `site_span` rather than counting
markers across the whole file, so the second site never changes their result.
"""
from pathlib import Path
import argparse
import re
import sys
from urllib.parse import urlsplit

from .engine import DeployError

TOKEN = 'ENHANCE_PUBLIC_HOST'
UNIFIED_HOST = 'wallet-pir.valargroup.dev'
TEMPLATE = Path(__file__).resolve().parents[3] / 'deploy/coordinator/Caddyfile'
HOSTNAME = re.compile(r'[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+')


class EdgeError(DeployError):
    pass


def require(condition, message):
    if not condition:
        raise EdgeError('coordinator edge: ' + message)


def sites(text):
    """Return `{address: (start, end)}` for each top-level site block.

    `start` is the offset of the address line and `end` the offset just past
    the closing brace. Comment lines and quoted strings are skipped when
    balancing braces.
    """
    found, depth, start, address = {}, 0, None, None
    offset = 0
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if depth == 0 and stripped.endswith('{') and not stripped.startswith('#'):
            label = stripped[:-1].strip()
            if label:
                start, address = offset, label
        if not stripped.startswith('#'):
            code = re.sub(r'"(?:[^"\\]|\\.)*"', '""', line)
            depth += code.count('{') - code.count('}')
            require(depth >= 0, 'unbalanced braces')
            if depth == 0 and start is not None:
                require(address not in found, 'duplicate site ' + address)
                found[address] = (start, offset + len(line))
                start = address = None
        offset += len(line)
    require(depth == 0, 'unbalanced braces')
    return found


def site_span(text, address):
    span = sites(text).get(address)
    require(span is not None, 'no site for ' + address)
    return span


def site_containing(text, marker):
    """The span of the single site whose block contains `marker`."""
    spans = [span for span in sites(text).values() if marker in text[span[0]:span[1]]]
    require(len(spans) == 1, 'expected exactly one site containing ' + marker.strip())
    return spans[0]


def replace_in_site(text, span, edit):
    """Apply `edit` to the block at `span`, leaving every other byte alone."""
    start, end = span
    return text[:start] + edit(text[start:end]) + text[end:]


def render(template, enhance_url):
    """Substitute the Enhance host and check both sites survive.

    Refuses an Enhance host equal to the unified host, which would collapse
    both sites into one and route the unified host to itself.
    """
    host = urlsplit(enhance_url).hostname if '://' in enhance_url else enhance_url
    require(isinstance(host, str) and HOSTNAME.fullmatch(host), 'invalid Enhance public host')
    require(host != UNIFIED_HOST, 'Enhance public host must differ from the unified host')
    require(template.count(TOKEN + ' {') == 1, 'template must name the Enhance site once')
    rendered = template.replace(TOKEN, host)
    found = sites(rendered)
    require(set(found) == {host, UNIFIED_HOST}, 'rendered edge must serve exactly the Enhance and unified sites')
    unified = rendered[slice(*found[UNIFIED_HOST])]
    upstreams = re.findall(r'(?m)^\s*reverse_proxy\s+(\S+)', unified)
    require(upstreams and not any(UNIFIED_HOST in u for u in upstreams),
            'unified site must not proxy to itself')
    return rendered


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--enhance-public-url', required=True)
    parser.add_argument('--template', type=Path, default=TEMPLATE)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        args.output.write_text(render(args.template.read_text(), args.enhance_public_url))
    except EdgeError as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
