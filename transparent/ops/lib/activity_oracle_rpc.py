"""Validate retained node RPC bodies independently of native journal extraction.

This is raw-evidence validation, not a candidate gate or snapshot constructor.
No reader touches a live journal. Native multiset agreement and reviewed safe
snapshot/restoration evidence remain separate prerequisites for qualification.
"""
import gzip
import hashlib
import io
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from activity_candidate_reports import blob, require, unique

MAX_BODY = 128 << 20
MAX_ATTEMPTS = 4096
METHODS = {'getblockhash', 'getblock', 'getblockheader', 'getrawtransaction'}
CATEGORIES = {'coinbase', 'non_coinbase', 'transparent_only', 'mixed_transparent_shielded',
              'multiple_inputs', 'multiple_outputs', 'sprout_component_transactions',
              'sapling_component_transactions', 'orchard_component_transactions',
              'ironwood_component_transactions', 'multiple_shielded_pools'}


def decode(reference):
    require(isinstance(reference, dict) and set(reference) in ({'path', 'sha256'},
            {'path', 'sha256', 'decoded_sha256'}), 'invalid raw RPC body reference')
    raw = blob({k: reference[k] for k in ('path', 'sha256')}, MAX_BODY)
    if 'decoded_sha256' in reference:
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
            raw = stream.read(MAX_BODY+1)
        require(len(raw) <= MAX_BODY and hashlib.sha256(raw).hexdigest() == reference['decoded_sha256'],
                'compressed RPC body size/checksum differs')
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda v: (_ for _ in ()).throw(ValueError('nonfinite RPC JSON')))


def integer(value, minimum=0):
    require(type(value) is int and value >= minimum, 'RPC integer is missing or invalid')
    return value


def text(value):
    require(isinstance(value, str) and len(value) == 64 and
            all(c in '0123456789abcdef' for c in value), 'RPC hash identity differs')
    return value


def transactions(attempts):
    require(isinstance(attempts, list) and 0 < len(attempts) <= MAX_ATTEMPTS, 'raw RPC attempts are missing/oversized')
    blocks, txs, anchors, refusals = {}, {}, {}, []
    counts = {method: 0 for method in METHODS}
    attempted = {method: 0 for method in METHODS}
    pending = set()
    for index, attempt in enumerate(attempts):
        require(isinstance(attempt, dict) and set(attempt) == {'status', 'request', 'response'} and
                type(attempt['status']) is int, 'invalid retained RPC attempt')
        requests, responses = decode(attempt['request']), decode(attempt['response'])
        requests = requests if isinstance(requests, list) else [requests]
        responses = responses if isinstance(responses, list) else [responses]
        require(requests and all(isinstance(call, dict) and call.get('method') in METHODS and
                isinstance(call.get('params'), list) and type(call.get('id')) in (int, str)
                for call in requests), 'unsupported/incomplete raw RPC request')
        require(len({call['id'] for call in requests}) == len(requests), 'duplicate raw RPC request id')
        for call in requests:
            attempted[call['method']] += 1
        # A too-large batch can be one error object for the entire request.
        batch_refusal = len(responses) == 1 and isinstance(responses[0], dict) and \
            isinstance(responses[0].get('error'), dict) and type(responses[0]['error'].get('code')) is int and responses[0]['error']['code'] == -32011
        if batch_refusal:
            require(all(call['method'] == 'getrawtransaction' and len(call['params']) == 2 and
                        type(call['params'][1]) is int and call['params'][1] == 1 for call in requests), 'oversize refusal for unsupported RPC')
            for call in requests:
                pending.add(text(call['params'][0]))
            refusals.append({'attempt': index, 'code': -32011, 'request': attempt['request'],
                             'response': attempt['response'], 'status': attempt['status']})
            continue
        require(attempt['status'] == 200 and len(responses) == len(requests) and
                all(isinstance(response, dict) and response.get('error') is None and
                    type(response.get('id')) in (int, str) for response in responses) and
                len({response['id'] for response in responses}) == len(responses), 'raw RPC request failed/incomplete')
        by_id = {response['id']: response for response in responses}
        require(set(by_id) == {call['id'] for call in requests}, 'raw RPC response id mismatch')
        for call in requests:
            method, params = call['method'], call['params']; counts[method] += 1
            result = by_id[call['id']].get('result')
            if method == 'getrawtransaction':
                require(len(params) == 2 and type(params[1]) is int and params[1] == 1 and isinstance(result, dict) and
                        result.get('txid') == text(params[0]), 'verbose transaction identity differs')
                identity = params[0]
                projected = project(result)
                require(identity not in txs or txs[identity] == projected, 'conflicting retained transaction')
                txs[identity] = projected; pending.discard(identity)
            elif method == 'getblock':
                require(len(params) == 2 and type(params[1]) is int and params[1] == 1 and isinstance(result, dict), 'block RPC is not verbose')
                height = integer(result.get('height')); text(result.get('hash'))
                require(str(params[0]) == str(height) or params[0] == result['hash'], 'block request height/hash differs')
                require(isinstance(result.get('tx'), list) and result['tx'] and
                        len(set(result['tx'])) == len(result['tx']), 'block transaction coverage differs')
                for identity in result['tx']: text(identity)
                require(height not in blocks or blocks[height] == result, 'conflicting retained block')
                blocks[height] = result
            elif method == 'getblockhash':
                require(len(params) == 1, 'invalid canonical hash request')
                height = integer(params[0]); identity = text(result)
                require(height not in anchors or anchors[height] == identity, 'canonical anchor changed')
                anchors[height] = identity
            else:
                require(len(params) in (1, 2) and isinstance(result, dict) and
                        result.get('hash') == text(params[0]), 'block header identity differs')
    require(not pending, 'oversize RPC refusal has unresolved transactions')
    return blocks, txs, anchors, counts, attempted, refusals


def outputs(tx):
    if '_scripts' in tx:
        return tx['_scripts']
    require(isinstance(tx.get('vin'), list) and isinstance(tx.get('vout'), list),
            'verbose transaction input/output arrays are missing')
    scripts = []
    for index, output in enumerate(tx['vout']):
        require(isinstance(output, dict) and integer(output.get('n')) == index and
                isinstance(output.get('scriptPubKey'), dict) and
                isinstance(output['scriptPubKey'].get('hex'), str), 'verbose output is malformed')
        encoded = output['scriptPubKey']['hex']
        require(len(encoded) % 2 == 0 and all(c in '0123456789abcdefABCDEF' for c in encoded),
                'invalid verbose script encoding')
        try:
            script = bytes.fromhex(encoded)
        except ValueError as error:
            raise ValueError('invalid verbose script bytes') from error
        scripts.append(bool(script) and script[0] != 0x6a)
    return scripts


def project(tx):
    # Verbose bodies retain large shielded proofs. Keep only the independent
    # indexing/category projection in memory; all original bytes stay bound.
    scripts = outputs(tx)
    inputs = []
    for vin in tx['vin']:
        require(isinstance(vin, dict), 'invalid verbose input')
        inputs.append({'coinbase': True} if 'coinbase' in vin else
                      {'txid': text(vin.get('txid')), 'vout': integer(vin.get('vout'))})
    pools = []
    for key in ('vjoinsplit', 'vShieldedSpend', 'vShieldedOutput'):
        require(key not in tx or tx[key] is None or isinstance(tx[key], list), 'invalid shielded array')
    pools.extend((bool(tx.get('vjoinsplit')), bool(tx.get('vShieldedSpend') or tx.get('vShieldedOutput'))))
    for key in ('orchard', 'ironwood'):
        bundle = tx.get(key)
        require(bundle is None or (isinstance(bundle, dict) and isinstance(bundle.get('actions'), list)),
                'invalid shielded bundle')
        pools.append(bool(bundle and bundle['actions']))
    return {'txid': tx['txid'], 'vin': inputs, '_scripts': scripts, '_pools': pools}


def compare(attempts, native):
    blocks, txs, anchors, methods, attempted, refusals = transactions(attempts)
    require(isinstance(native, dict) and isinstance(native.get('blocks'), list) and
            type(native.get('blocks_compared')) is int and native['blocks_compared'] == 17 and
            type(native.get('blocks_disagreeing')) is int and native['blocks_disagreeing'] == 0 and
            len(native['blocks']) == 17, 'native oracle coverage/agreement is incomplete')
    heights = [integer(block.get('height')) for block in native['blocks']]
    require(len(set(heights)) == 17 and set(blocks) == set(heights), 'raw block/native coverage differs')
    categories = dict.fromkeys(CATEGORIES, 0); sampled = set(); events = 0
    for comparison in native['blocks']:
        height = comparison['height']; block = blocks[height]; receives = spends = 0
        require(comparison.get('agrees') is True and comparison.get('journal_hash') == block['hash'] and
                comparison.get('node_hash') == block['hash'] and
                type(comparison.get('missing_count')) is int and comparison['missing_count'] == 0 and
                type(comparison.get('extra_count')) is int and comparison['extra_count'] == 0 and
                comparison.get('missing_from_journal') == [] and comparison.get('extra_in_journal') == [],
                'native block does not agree with retained RPC')
        for identity in block['tx']:
            require(identity in txs, 'sampled verbose transaction is missing')
            tx = txs[identity]; scripts = outputs(tx)
            coinbase = any(isinstance(vin, dict) and 'coinbase' in vin for vin in tx['vin'])
            receives += sum(scripts)
            if not coinbase:
                for vin in tx['vin']:
                    require(isinstance(vin, dict), 'invalid verbose input')
                    previous = text(vin.get('txid')); index = integer(vin.get('vout'))
                    require(previous in txs, 'raw previous transaction is missing')
                    previous_scripts = outputs(txs[previous])
                    require(index < len(previous_scripts), 'raw previous output is missing')
                    spends += int(previous_scripts[index])
            if identity in sampled: continue
            sampled.add(identity)
            pools = tx['_pools']
            categories['coinbase' if coinbase else 'non_coinbase'] += 1
            categories['transparent_only'] += int(not any(pools))
            categories['mixed_transparent_shielded'] += int(any(pools) and
                (bool(tx['_scripts']) or (not coinbase and bool(tx['vin']))))
            categories['multiple_inputs'] += int(not coinbase and len(tx['vin']) > 1)
            categories['multiple_outputs'] += int(len(tx['_scripts']) > 1)
            categories['multiple_shielded_pools'] += int(sum(pools) > 1)
            for key, present in zip(('sprout', 'sapling', 'orchard', 'ironwood'), pools):
                categories[key+'_component_transactions'] += int(present)
        for field, expected in (('node_receives', receives), ('node_spends', spends),
                                ('node_events', receives+spends), ('journal_events', receives+spends)):
            require(type(comparison.get(field)) is int and comparison[field] == expected,
                    'retained RPC/native event counts differ')
        events += receives+spends
    return {'blocks_compared': 17, 'blocks_disagreeing': 0, 'events_compared': events,
            'raw_rpc_attempts': len(attempts), 'rpc_method_counts': methods, 'rpc_attempted_method_counts': attempted,
            'raw_rpc_application_refusals': refusals, 'raw_pool_category_coverage':
            {'sampled_transactions': len(sampled), **categories}, 'canonical_anchors': anchors}
