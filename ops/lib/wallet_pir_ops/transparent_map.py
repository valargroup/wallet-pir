"""Transparent map identity as the pinned native service serializes it.

The publication file checksum binds retained bytes. The protocol checksum binds
Serde's compact declaration order, including sorted seal policies and omission
of absent optional txid segments. They are deliberately separate identities.
Reject unknown fields rather than silently projecting a different wire version.
"""
import hashlib
import json


def ordered(value, keys, optional=()):
    if not isinstance(value, dict) or not set(keys) <= set(value) or set(value)-set(keys)-set(optional):
        raise ValueError('map fields differ from the pinned native wire version')
    return {key:value[key] for key in (*keys, *optional) if key in value}


def served_bytes(mapping):
    """Return native ShardMap JSON bytes; unsupported shapes fail closed."""
    result = ordered(mapping, ('genesis_hash','network','profile','range_envelope_version','start_height','seal','shards'))
    def uint(value, bits=64):
        if type(value) is not int or not 0 <= value < 2**bits:
            raise ValueError('map unsigned integer differs from native wire type')
    if any(not isinstance(mapping[key],str) for key in ('genesis_hash','network','profile')):
        raise ValueError('invalid map strings')
    uint(mapping['range_envelope_version'],16);uint(mapping['start_height'])
    if not isinstance(mapping['seal'], dict) or not isinstance(mapping['shards'], list):
        raise ValueError('invalid map collections')
    result['seal'] = {key:ordered(mapping['seal'][key], ('max_scripts','max_page_rows','max_txids'))
                      for key in sorted(mapping['seal'])}
    for policy in result['seal'].values():
        for value in policy.values(): uint(value)
    entries = []
    before = ('shard_id','geometry','start_height','end_height','parent_block_hash','terminal_block_hash',
              'filter_hash','scripts','page_rows','txids','directory_segments','page_segments')
    after = ('manifest_digest','revision','sealed')
    for entry in mapping['shards']:
        ordered(entry, (*before,*after), ('txid_segments',))
        for key in ('shard_id','start_height','end_height','scripts','page_rows','txids'):uint(entry[key])
        for key in ('directory_segments','page_segments','revision'):uint(entry[key],32)
        if (type(entry['sealed']) is not bool or any(not isinstance(entry[key],str) for key in
                ('geometry','parent_block_hash','terminal_block_hash','filter_hash','manifest_digest'))):
            raise ValueError('invalid map entry wire types')
        row = {key:entry[key] for key in before}
        if entry.get('txid_segments') is not None:
            segments=entry['txid_segments']
            if not isinstance(segments,list) or len(segments)!=2 or any(type(v) is not int or not 0<=v<2**32 for v in segments):
                raise ValueError('invalid optional txid segment counts')
            row['txid_segments']=segments
        row.update({key:entry[key] for key in after})
        entries.append(row)
    result['shards']=entries
    return json.dumps(result, ensure_ascii=False, separators=(',',':'), allow_nan=False).encode()


def served_sha256(mapping):
    """Protocol map checksum; never substitute a publication file checksum."""
    return hashlib.sha256(served_bytes(mapping)).hexdigest()
