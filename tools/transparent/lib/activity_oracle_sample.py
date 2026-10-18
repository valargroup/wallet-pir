"""Re-derive the closed candidate oracle sample from verified snapshot blocks.

The locked caller must first verify the complete safe snapshot and restoration
with the reviewed snapshot module. This component checks independent immutable
block bytes and the native selection recipe; it never opens the live journal,
constructs a snapshot or emits a candidate gate.
"""
import hashlib
from pathlib import Path
import stat
import struct

from activity_candidate_reports import C, require

EXPLICIT = (0, 200000, 419200, 903000, 1687104, 2726400, 3500738)
THROUGH = 3500738
TOP, RANDOM, LAST, SEED = 3, 5, 3, 20261001
RECORD = struct.Struct('<32sQQ')
CHUNK = RECORD.size*4096


def fixed_heights():
    chosen = set(EXPLICIT)
    value = SEED ^ 0x9E3779B97F4A7C15
    for _ in range(RANDOM):
        value = (value*6364136223846793005+1442695040888963407) & ((1 << 64)-1)
        chosen.add((value >> 11) % (THROUGH+1))
    chosen.update(range(THROUGH-LAST+1, THROUGH+1))
    return chosen


class Densest:
    """Native tuple ordering on initial ties; later equal counts do not replace."""
    def __init__(self):
        self.entries = []

    def add(self, count, height, identity):
        if len(self.entries) < TOP:
            self.entries.append((count, height, identity))
            self.entries.sort(reverse=True)
        elif count > self.entries[-1][0]:
            self.entries.pop()
            self.entries.append((count, height, identity))
            self.entries.sort(reverse=True)


def derive(path, expected_size, expected_sha256, covered_through, check):
    """Streaming checksum/selection; every chunk checks owner's budget/resources."""
    path = Path(path)
    C.no_links(path)
    require(path.is_absolute() and callable(check) and type(covered_through) is int and
            THROUGH <= covered_through < 1 << 32 and type(expected_size) is int and
            expected_size == (covered_through+1)*RECORD.size and
            isinstance(expected_sha256, str) and C.HEX.fullmatch(expected_sha256),
            'oracle snapshot block identity differs')
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size == expected_size and
            stat.S_IMODE(info.st_mode) == 0o400, 'oracle blocks are not independent immutable records')
    check()
    selected, hashes, densest = fixed_heights(), {}, Densest()
    digest, height, read_bytes = hashlib.sha256(), 0, 0
    with path.open('rb') as stream:
        while raw := stream.read(CHUNK):
            check()
            read_bytes += len(raw)
            require(read_bytes <= expected_size, 'oracle snapshot blocks grew beyond committed length')
            require(len(raw) % RECORD.size == 0, 'truncated oracle block record')
            digest.update(raw)
            for internal, _offset, count in RECORD.iter_unpack(raw):
                identity = internal[::-1].hex()
                if height in selected:
                    hashes[height] = identity
                if height <= THROUGH:
                    densest.add(count, height, identity)
                height += 1
    check()
    require(height == covered_through+1 and digest.hexdigest() == expected_sha256,
            'oracle snapshot blocks changed or checksum differs')
    for _count, index, identity in densest.entries:
        selected.add(index); hashes[index] = identity
    require(len(selected) == 17 and set(hashes) == selected, 'oracle sample does not cover exactly17bindings')
    return {'recipe':{'explicit':list(EXPLICIT), 'top':TOP, 'random':RANDOM, 'seed':SEED,
                      'last':LAST, 'through':THROUGH, 'heights':sorted(selected)},
            'canonical_hashes':{str(h):hashes[h] for h in sorted(selected)},
            'blocks_sha256':expected_sha256, 'blocks_bytes':expected_size}


def bind_native(native, sample, *, journal_dir, genesis_hash, covered_through):
    """Bind the pinned reader output to a re-derived, verified snapshot sample.

    The caller must obtain `sample` using derive AFTER complete safe-snapshot
    verification. These bindings alone prove neither that verification nor
    raw node counts, owner identity, temporal anchors or resource coverage.
    """
    directory = Path(journal_dir)
    C.no_links(directory)
    require(directory.is_absolute() and isinstance(genesis_hash, str) and
            C.HEX.fullmatch(genesis_hash) and type(covered_through) is int and
            THROUGH <= covered_through < 1 << 32, 'oracle journal binding differs')
    require(isinstance(sample, dict) and isinstance(sample.get('recipe'), dict) and
            isinstance(sample.get('canonical_hashes'), dict), 're-derived oracle sample is missing')
    recipe = sample['recipe']; heights = recipe.get('heights')
    require(isinstance(heights, list) and len(heights) == 17 and
            all(type(h) is int and 0 <= h <= THROUGH for h in heights) and
            heights == sorted(set(heights)) and fixed_heights() <= set(heights) and
            set(sample['canonical_hashes']) == {str(h) for h in heights} and
            all(isinstance(v, str) and C.HEX.fullmatch(v) for v in sample['canonical_hashes'].values()),
            're-derived oracle height/hash coverage differs')
    require(sample['canonical_hashes']['0'] == genesis_hash,
            'oracle sample genesis differs from verified snapshot')
    expected = {'explicit':list(EXPLICIT), 'top':TOP, 'random':RANDOM, 'seed':SEED,
                'last':LAST, 'through':THROUGH, 'heights':heights}
    require(recipe == expected and isinstance(native, dict) and
            native.get('schema') == 'transparent-event-spotcheck-v2' and
            native.get('tool_sha') == C.SOURCE_SHA and native.get('data_dir') == str(directory) and
            native.get('genesis_hash') == genesis_hash and
            native.get('journal') == {'start_height':0, 'covered_through':covered_through} and
            all(type(native['journal'].get(k)) is int for k in ('start_height', 'covered_through')),
            'native oracle source or verified journal differs')
    actual = native.get('sample')
    require(isinstance(actual, dict) and actual == expected and
            all(type(actual.get(k)) is int for k in ('top','random','seed','last','through')) and
            all(type(v) is int for k in ('explicit','heights') for v in actual[k]),
            'native oracle sampling recipe differs')
    blocks = native.get('blocks')
    require(type(native.get('blocks_compared')) is int and native['blocks_compared'] == 17 and
            type(native.get('blocks_disagreeing')) is int and native['blocks_disagreeing'] == 0 and
            isinstance(blocks, list) and len(blocks) == 17 and
            all(isinstance(b, dict) and type(b.get('height')) is int for b in blocks) and
            [b['height'] for b in blocks] == heights, 'native oracle block coverage differs')
    for block in blocks:
        identity = sample['canonical_hashes'][str(block['height'])]
        require(block.get('journal_hash') == block.get('node_hash') == identity and
                block.get('agrees') is True and
                all(type(block.get(k)) is int and block[k] == 0 for k in ('missing_count','extra_count')) and
                block.get('missing_from_journal') == [] and block.get('extra_in_journal') == [],
                'native oracle multiset or canonical identity differs')
    return native
