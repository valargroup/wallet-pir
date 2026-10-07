#!/usr/bin/env python3
"""Bounded, parent-free probability sample; public immutable raw files stay outside Git."""
import argparse
from collections import Counter
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import random
import time
import census

SEED = 'wallet-pir/one-day/shape-v1/' + census.ANCHOR_HASH

def plan(n=1024):
    d = census.plan(per_stratum=n)
    d['schema'] = 'txid-sizing-parent-free-plan-v1'
    d['seed'] = SEED
    rng = random.Random(SEED)
    for s in d['strata']:
        s['heights'] = sorted(rng.sample(range(s['lo'], s['hi_exclusive']), s['n']))
    d['fee_policy'] = 'No parent requests. Canonical no-transparent-input exact fees; other noncoinbase unknown bounded with MAX_MONEY.'
    return d

def no_overlap():
    """Inspect executable and exact argv tokens; never print process arguments."""
    for p in Path('/proc').iterdir():
        if not p.name.isdigit() or int(p.name) == os.getpid(): continue
        try:
            exe = Path(os.readlink(p/'exe')).name
            argv = (p/'cmdline').read_bytes().split(b'\0')
            overlap = exe == 'ssh' and b'pir-census@167.99.42.60' in argv
            overlap |= exe == 'txid-sizing-export' and b'--census' in argv
            overlap |= exe in ('python', 'python3') and any(Path(os.fsdecode(a)).name in ('fullchain.py', 'census.py', 'day_sample.py') for a in argv[1:3])
            if overlap: raise RuntimeError('another census process is present; gateway not opened')
        except (FileNotFoundError, PermissionError, ProcessLookupError): continue

def run(root, status, n=1024):
    root = root.resolve()
    if root.is_relative_to(Path(__file__).resolve().parents[3]): raise ValueError('raw data must stay outside Git')
    root.mkdir(parents=True, exist_ok=True)
    (root/'blocks').mkdir(exist_ok=True)
    design = plan(n)
    if (root/'plan.json').exists() and json.loads((root/'plan.json').read_text()) != design:
        raise ValueError('immutable selection changed')
    census.atomic_json(root/'plan.json', design)
    no_overlap()
    start = time.monotonic()
    requests = 0
    def update(completed, phase, eta):
        census.atomic_json(status, dict(summary=f'Parent-free {phase}: {completed}/{9*n} sampled blocks; next sizing bounds and joint routes', eta=eta, needs_you='', state='working', phase=phase, source_gate='probability_sample_selected', progress={'blocks':completed,'rpc_calls':requests}, deadline_utc='2026-10-04T08:52:13Z'))
    with census.Gateway() as gateway:
        def call(method, params):
            nonlocal requests
            r = gateway(method, params); requests += 1
            if r.get('error') is not None or 'result' not in r: raise RuntimeError('sanctioned gateway rejected read; no alternate access attempted')
            return r['result']
        if call('getblockhash', [census.ANCHOR_HEIGHT]) != census.ANCHOR_HASH: raise ValueError('anchor mismatch')
        info = call('getblockchaininfo', [])
        census.atomic_json(root/'node-info.json', info)
        pins = []
        # Round-robin visits all eras early for throughput profiling.
        order = [(s,h) for i in range(n) for s in design['strata'] for h in s['heights'][i:i+1]]
        for s,h in order:
            dest = root/'blocks'/f'{h}.json'
            if not dest.exists():
                block_hash = call('getblockhash', [h])
                raw = call('getblock', [block_hash, 0])
                decoded = call('getblock', [block_hash, 2])
                if decoded['height'] != h or decoded['hash'] != block_hash: raise ValueError('RPC identity mismatch')
                census.atomic_json(dest, dict(height=h, hash=block_hash, raw_block=raw, rpc_block=decoded, stratum=s['name']))
            f = json.loads(dest.read_text())
            if f['height'] != h or f['stratum'] != s['name']: raise ValueError('checkpoint membership mismatch')
            pins.append(dict(height=h,hash=f['hash'],stratum=s['name'],raw_sha256=hashlib.sha256(bytes.fromhex(f['raw_block'])).hexdigest(),frame_sha256=census.digest(dest),raw_bytes=len(f['raw_block'])//2,transactions=f['rpc_block']['nTx']))
            completed=len(pins);elapsed=time.monotonic()-start
            if completed==45:
                census.atomic_json(root/'source-gate.json',dict(schema='txid-sizing-one-day-source-gate-v1',observed_utc=dt.datetime.now(dt.timezone.utc).isoformat(),source='existing single-session read-only JSON-RPC SSH gateway',bulk_selector_configured=False,archive_mount='retained denied access not retried or bypassed',profile_blocks=completed,profile_seconds=elapsed,requests=requests,blocks_per_second=completed/elapsed,projected_sample_seconds=elapsed/completed*len(order),full_gateway_minimum_hours=(census.ANCHOR_HEIGHT+1)*2/45/3600,decision='whole-range probability sample; no UTXO continuation; existing biased prefix untouched'))
                print(json.dumps({'profile_blocks':completed,'seconds':round(elapsed,2),'projected_sample_minutes':round(elapsed/completed*len(order)/60,2)}),flush=True)
            if completed%9==0: update(completed,'acquisition',f'{int(elapsed/completed*(len(order)-completed)/60)+1}m acquisition')
            if completed%256==0: print(json.dumps({'sample_blocks':completed,'elapsed_seconds':round(elapsed,2)}),flush=True)
            if elapsed > 5*3600: raise RuntimeError('bounded acquisition budget reached; checkpoint retained')
        for i,p in enumerate(pins):
            if call('getblockhash',[p['height']])!=p['hash']: raise ValueError('sample canonical recheck failed')
            if i%256==0: update(len(pins),'canonical_recheck',f'{int((len(pins)-i)/40/60)+1}m recheck')
        if call('getblockhash',[census.ANCHOR_HEIGHT])!=census.ANCHOR_HASH: raise ValueError('final anchor mismatch')
        receipt=dict(schema='txid-sizing-parent-free-receipt-v1',design=design,blocks=sorted(pins,key=lambda p:p['height']),new_rpc_calls=requests,acquisition_seconds=time.monotonic()-start,source='single existing pir-census read-only gateway; <=40 requests/s; no getrawtransaction',canonical_recheck='all sampled height/hash pins and fixed anchor rechecked on same session',node_info_sha256=census.digest(root/'node-info.json'))
        census.atomic_json(root/'receipt.json',receipt)
    update(len(pins),'acquired','analysis pending')
    print(json.dumps({'done':len(pins),'seconds':receipt['acquisition_seconds'],'rpc_calls':requests}),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('checkpoint',type=Path);p.add_argument('--status',type=Path,required=True);p.add_argument('--per-stratum',type=int,default=1024)
    args=p.parse_args()
    try: run(args.checkpoint,args.status,args.per_stratum)
    except Exception as e:
        census.atomic_json(args.status,dict(summary='Parent-free sample source/acquisition stopped; public checkpoint preserved',eta='',needs_you=str(e),state='blocked',phase='source_gate'))
        raise
