#!/usr/bin/env python3
"""Check library HTTP recovery against retained raw block and prevout RPC inputs.

No publisher journal, codec or fee function is imported. Arithmetic uses exact
Decimal values and the public node's full transaction/prevout responses.
"""
import argparse
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3

MAX_MONEY = 21_000_000 * 100_000_000


def amount(obj, integer, decimal):
    if integer in obj:
        value = obj[integer]
        if type(value) is not int: raise ValueError('noninteger monetary amount')
        return value
    value = Decimal(obj[decimal]) * 100_000_000
    if value != value.to_integral_value(): raise ValueError('fractional zatoshi')
    return int(value)


def output_value(output):
    value = amount(output, 'valueZat' if 'valueZat' in output else 'valueSat', 'value')
    if not 0 <= value <= MAX_MONEY: raise ValueError('output monetary bound')
    return value


def read_capture(directory):
    def body(digest):
        raw = gzip.decompress((directory/'bodies'/(digest+'.json.gz')).read_bytes())
        if hashlib.sha256(raw).hexdigest()!=digest: raise ValueError('raw input digest mismatch')
        return json.loads(raw,parse_float=Decimal)
    blocks={}; transactions={}; failures=[]; attempts=0
    for line in (directory/'attempts.jsonl').read_text().splitlines():
        record=json.loads(line); attempts+=1
        if record.get('failure') or record['status']!=200:
            failures.append(record);continue
        requests=body(record['request_sha256']); responses=body(record['response_sha256'])
        requests=requests if isinstance(requests,list) else [requests]
        responses=responses if isinstance(responses,list) else [responses]
        # The node refuses an oversized batch with one global error and no
        # per-call IDs. Retain the failed attempt; the oracle retries a smaller
        # batch and the final block/prevout completeness checks still apply.
        if len(responses)==1 and responses[0].get('id') is None and responses[0].get('error'):
            code=responses[0]['error']['code']
            if code != -32011: raise ValueError('unexpected global RPC batch failure')
            failures.append({'attempt':attempts,'rpc_batch_error':code})
            continue
        by_id={response['id']:response for response in responses}
        if len(by_id)!=len(responses) or len(responses)!=len(requests): raise ValueError('RPC cardinality/id conflict')
        for request in requests:
            response=by_id[request['id']]
            if response.get('error') is not None:
                failures.append({'attempt':attempts,'rpc_error':response['error']['code']});continue
            result=response['result']
            if request['method']=='getrawtransaction':
                txid=request['params'][0]
                if result['txid']!=txid: raise ValueError('raw transaction identity mismatch')
                if txid in transactions and transactions[txid]['hex']!=result['hex']:
                    raise ValueError('contradictory raw transaction')
                transactions[txid]=result
            elif request['method']=='getblock':
                height=result['height']
                if str(request['params'][0])!=str(height): raise ValueError('block height identity mismatch')
                if height in blocks and blocks[height]['hash']!=result['hash']: raise ValueError('contradictory block')
                blocks[height]=result
    return blocks,transactions,failures,attempts


def validate_tx(tx):
    for n,output in enumerate(tx['vout']):
        if output['n']!=n: raise ValueError('referenced output index mismatch')
        output_value(output)
    coinbase=any('coinbase' in value for value in tx['vin'])
    if coinbase and (len(tx['vin'])!=1 or 'coinbase' not in tx['vin'][0]): raise ValueError('invalid coinbase inputs')
    return coinbase


def metadata(tx, transactions):
    coinbase=validate_tx(tx)
    shielded=bool(tx.get('vjoinsplit') or tx.get('vShieldedSpend') or tx.get('vShieldedOutput'))
    balance=0
    for transfer in tx.get('vjoinsplit',[]):
        balance += amount(transfer,'vpub_newZat','vpub_new')-amount(transfer,'vpub_oldZat','vpub_old')
    if 'valueBalanceZat' in tx or 'valueBalance' in tx:
        balance += amount(tx,'valueBalanceZat','valueBalance')
    elif tx.get('vShieldedSpend') or tx.get('vShieldedOutput'):
        raise ValueError('missing Sapling balance')
    for pool in ('orchard','ironwood'):
        bundle=tx.get(pool)
        if bundle is not None:
            shielded |= bool(bundle['actions'])
            balance += amount(bundle,'valueBalanceZat','valueBalance')
    if coinbase:
        fee={'state':'not-applicable'}; count=0
    else:
        total=0
        for value in tx['vin']:
            parent=transactions[value['txid']]
            validate_tx(parent)
            n=value['vout']
            if type(n) is not int or not 0<=n<len(parent['vout']): raise ValueError('invalid prevout index')
            total+=output_value(parent['vout'][n])
        exact=total-sum(output_value(value) for value in tx['vout'])+balance
        if not 0<=exact<=MAX_MONEY: raise ValueError('fee monetary bound')
        fee={'state':'exact','value':exact}; count=len(tx['vin'])
    return {'fee':fee,'input_count':count,'shielded':shielded}


def compare(args):
    observed=json.loads(args.library_result.read_text())
    snapshot=json.loads(args.snapshot.read_text())
    scripts=set(snapshot['scripts'])
    if scripts!=set(observed['fixture_scripts']) or any(observed[key]!=snapshot[key] for key in ('birthday','through')):
        raise ValueError('library workload differs from independent snapshot')
    blocks,transactions,failures,attempts=read_capture(args.capture)
    headers={value['height']:value for value in snapshot['headers']}
    receives=[];spends=[]
    for height in range(snapshot['birthday'],snapshot['through']+1):
        block=blocks[height]
        if block['hash']!=headers[height]['hash']: raise ValueError('block differs from independent accepted header')
        for txid in block['tx']:
            tx=transactions[txid]
            meta=metadata(tx,transactions)
            coinbase=validate_tx(tx)
            for n,output in enumerate(tx['vout']):
                script=output['scriptPubKey']['hex']
                if script in scripts:
                    receives.append({'txid':txid,'output_index':n,'script':script,'value':output_value(output),'coinbase':coinbase,'height':height,'metadata':meta})
            if not coinbase:
                for n,value in enumerate(tx['vin']):
                    parent=transactions[value['txid']]
                    script=parent['vout'][value['vout']]['scriptPubKey']['hex']
                    if script in scripts:
                        spends.append({'txid':txid,'input_index':n,'prevout_txid':value['txid'],'prevout_index':value['vout'],'script':script,'height':height,'metadata':meta})
    ordered=lambda values:sorted(json.dumps(value,sort_keys=True) for value in values)
    if ordered(receives)!=ordered(observed['receives']): raise ValueError('library receive facts differ from independent blocks/prevouts')
    if ordered(spends)!=ordered(observed['spends']): raise ValueError('library spend facts differ from independent blocks/prevouts')
    if not receives and not spends: raise ValueError('empty recovery cannot pass')
    if not observed['reopened_equal'] or observed['reader_version']!=7 or observed['qualified_revisions'] or observed['active_accounts']:
        raise ValueError('persistence/fence/authority gate failed')
    if args.wallet_db:
        with sqlite3.connect('file:'+str(args.wallet_db.resolve())+'?mode=ro',uri=True) as conn:
            for event in receives+spends:
                rows=conn.execute('SELECT fee_state,fee_zat,input_count,shielded FROM tpir_transaction_metadata WHERE txid=? AND mined_height=?',
                    (bytes.fromhex(event['txid'])[::-1],event['height'])).fetchall()
                meta=event['metadata']; fee=meta['fee']; state={'exact':0,'unknown':1,'not-applicable':2}[fee['state']]
                expected=(state,fee.get('value'),meta['input_count'],int(meta['shielded']))
                if not rows or any(row!=expected for row in rows): raise ValueError('persisted metadata differs from independent inputs')
    result={'schema':'activity-library-independent-check-v1','status':'passed','birthday':snapshot['birthday'],'through':snapshot['through'],
        'blocks':snapshot['through']-snapshot['birthday']+1,'raw_rpc_attempts':attempts,'raw_rpc_failures':failures,
        'receives':len(receives),'spends':len(spends),'fixture_scripts':len(scripts),'sqlite_metadata_checked':bool(args.wallet_db),
        'library_result_sha256':hashlib.sha256(args.library_result.read_bytes()).hexdigest(),
        'snapshot_sha256':hashlib.sha256(args.snapshot.read_bytes()).hexdigest(),
        'capture_index_sha256':hashlib.sha256((args.capture/'attempts.jsonl').read_bytes()).hexdigest(),
        'checker_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'limitation':observed['limitation']}
    with args.out.open('x') as output: json.dump(result,output,indent=2);output.write('\n')
    print(f"{len(receives)} receives and {len(spends)} spends match independently retained inputs")


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture',type=Path,required=True)
    parser.add_argument('--snapshot',type=Path,required=True)
    parser.add_argument('--library-result',type=Path,required=True)
    parser.add_argument('--wallet-db',type=Path)
    parser.add_argument('--out',type=Path,required=True)
    compare(parser.parse_args())

if __name__=='__main__':main()
