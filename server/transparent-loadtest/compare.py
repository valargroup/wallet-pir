#!/usr/bin/env python3
"""Run paired wallet recoveries with frozen inputs and continually written offline reports."""
import argparse
import hashlib
import html
import json
import os
import platform
from pathlib import Path
import signal
import shutil
import subprocess
import sys
import time
import urllib.request


def sha(data):
    return hashlib.sha256(data).hexdigest()


def atomic(path, data):
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(data)
    tmp.replace(path)


def read(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def validate_common_coverage(sample):
    # transparent_shard::MAX_SCRIPT_BYTES for the pinned v1 private-table format.
    for index, wallet in enumerate(sample['clients']):
        for script in wallet['scripts']:
            raw = bytes.fromhex(script)
            if not raw or raw[0] == 0x6a or len(raw) > 40:
                raise ValueError(f'wallet {index}: script is outside common PIR/block coverage '
                                 '(nonempty, no leading OP_RETURN, at most 40 bytes)')


def totals(user):
    if 'http_totals' in user:
        return user['http_totals']
    return {key: sum(stage.get(key,0) for stage in user.get('stages',{}).values())
            for key in ['bytes_up','bytes_down','calls','failures','retry_attempts']}


def enrich(user, report):
    result=dict(user)
    result['http_totals']=totals(user)
    rss=[p['rss_bytes'] for metric in (report or {}).get('metrics',[]) for p in metric.get('processes',[])
         if p['pid']==user.get('pid') and metric.get('at',0)>=user.get('started_at',0)
         and metric.get('at',0)<=user.get('finished_at',float('inf'))]
    if rss or user.get('client_end_rss_bytes') is not None:
        result['client_sampled_peak_rss_bytes']=max(rss+[user.get('client_end_rss_bytes',0)])
    return result


def pair_rows(pir, blocks, original):
    left = {u['sample_index']: u for u in (pir or {}).get('users', [])}
    right = {u['sample_index']: u for u in (blocks or {}).get('users', [])}
    rows = []
    for index in sorted(left.keys() | right.keys()):
        a, b = enrich(left.get(index, {}),pir), enrich(right.get(index, {}),blocks)
        sizes = b.get('encoded_dataset_bytes', {})
        extra = sizes.get('combined', 0) - sizes.get('shielded', 0) if sizes else None
        ad = a.get('http_totals', {}).get('bytes_down')
        bd = b.get('http_totals', {}).get('bytes_down')
        exact = 0 <= index < len(original) and a.get('outcome') == b.get('outcome') == 'exact'
        rows.append(dict(sample_index=index, source_sample_index=original[index] if 0 <= index < len(original) else None,
                         profile=a.get('profile', b.get('profile')), pir=a, blocks=b,
                         incremental_block_bytes=extra,
                         standalone_download_savings=(1-ad/bd) if exact and bd and ad is not None else None,
                         latency_ratio_blocks_over_pir=(b['seconds']/a['seconds']) if exact and a.get('seconds') else None))
    return rows


def server_summary(data):
    targets={}
    for metric in (data or {}).get('metrics',[]):
        if metric.get('target')=='load-client': continue
        series=targets.setdefault(metric['target'],[])
        values={}
        for line in metric.get('text','').splitlines():
            if not line or line.startswith('#'): continue
            name=line.split('{',1)[0].split()[0]
            try: values[name]=float(line.rsplit(' ',1)[1])
            except (ValueError,IndexError): continue
        def pick(*names):
            return next((values[n] for n in names if n in values),None)
        series.append(dict(at=metric['at'],error=metric.get('error'),
                           cpu=pick('process_cpu_seconds_total','transparent_shard_process_cpu_seconds_total'),
                           rss=pick('process_resident_memory_bytes','transparent_shard_process_rss_bytes'),
                           start=pick('process_start_time_seconds','transparent_shard_process_start_time_seconds'),
                           bytes=pick('transparent_blocks_batch_response_bytes_total')))
    result=[]
    for target,points in targets.items():
        points.sort(key=lambda p:p['at'])
        gaps=any(p['error'] for p in points) or any(b['at']-a['at']>3 for a,b in zip(points,points[1:]))
        starts={p['start'] for p in points if p['start'] is not None}
        cpus=[p['cpu'] for p in points if p['cpu'] is not None]
        reset=len(starts)>1 or any(b<a for a,b in zip(cpus,cpus[1:]))
        cpu=cpus[-1]-cpus[0] if len(cpus)>=2 and not gaps and not reset else None
        rss=[p['rss'] for p in points if p['rss'] is not None]
        egress=[p['bytes'] for p in points if p['bytes'] is not None]
        result.append(dict(target=target,cpu_seconds=cpu,peak_rss_bytes=max(rss) if rss else None,
                           response_payload_bytes=egress[-1]-egress[0] if len(egress)>=2 and not gaps and not reset else None,
                           evidence='partial / reset' if gaps or reset else 'observed'))
    return result


def report(out, manifest, status, errors):
    suites = {}
    original = manifest.get('source_sample_indices', [])
    for suite in ['mixed', 'fresh']:
        paths = manifest.get('runs', {}).get(suite, {})
        data = {backend: read(out/path/'report.json') for backend, path in paths.items()}
        rows=pair_rows(data.get('pir'), data.get('blocks'), original)
        complete_pairs=bool(original) and len(rows)==len(original) and all(
            row['source_sample_index'] is not None and row['pir'].get('outcome')==row['blocks'].get('outcome')=='exact'
            for row in rows)
        suites[suite] = dict(reports=paths, server_apm={backend:server_summary(value) for backend,value in data.items()}, success=bool(complete_pairs and len(data)==2 and all(d and d.get('success') and len(d.get('users',[]))==len(original) for d in data.values())), rows=rows)
    for data in suites.values():
        profiles={}
        for row in data['rows']:
            profile=profiles.setdefault(row['profile'],{})
            for backend in ['pir','blocks']:
                user=row[backend]
                summary=profile.setdefault(backend,dict(exact=0,unsuccessful=0,bytes_down=0,bytes_up=0,retry_attempts=0,client_cpu_seconds=0,process_written_bytes=0,sqlite_bytes=0))
                if user.get('outcome')=='exact': summary['exact']+=1
                elif user.get('outcome') not in [None,'running','scheduled']: summary['unsuccessful']+=1
                for key in ['bytes_down','bytes_up','retry_attempts']: summary[key]+=user.get('http_totals',{}).get(key,0)
                for key in ['client_cpu_seconds','process_written_bytes','sqlite_bytes']:
                    summary[key]=summary[key]+user[key] if summary[key] is not None and user.get(key) is not None else None
        data['profiles']=profiles
    result = dict(schema='transparent-comparison-v1', success=status=='complete' and all(s['success'] for s in suites.values()),
                  phase=status, errors=errors, manifest=manifest, suites=suites, updated_at=time.time())
    atomic(out/'report.json', json.dumps(result, indent=2))
    chunks = ['<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">',
              '<title>Transparent sync comparison</title><style>body{font:16px system-ui;margin:30px;max-width:1500px;color:#172033}table{border-collapse:collapse;width:100%;font-size:14px}td,th{padding:10px;border-bottom:1px solid #ddd;text-align:right}td:first-child,th:first-child{text-align:left}.scroll,pre{overflow:auto}.note{color:#536074}a{color:#165ad0}</style></head><body>',
              '<h1>Transparent sync comparison</h1>', f'<p><strong>{html.escape(status)}</strong></p>',
              '<p class="note">Same known scripts and accepted anchor. Incremental bytes are combined minus shielded-only encoded batches; they are not a measured shielded-wallet latency saving. Wallet totals are HTTP payloads, excluding TLS, headers, controller validation and preparation. One pair per suite gives preliminary latency observations.</p>']
    if manifest.get('conditions'):
        chunks.append('<details><summary>Run conditions</summary><pre>'+html.escape(json.dumps(manifest['conditions'],indent=2))+'</pre></details>')
    for error in errors:
        chunks.append(f'<p>{html.escape(str(error))}</p>')
    for suite, data in suites.items():
        chunks.append(f'<h2>{suite.title()}: {"PASS" if data["success"] else "Pending or unsuccessful"}</h2>')
        for backend, path in data['reports'].items():
            chunks.append(f'<a href="{html.escape(path)}/report.html">{backend.upper()} detailed report / APM</a> &nbsp;')
        chunks.append('<div class="scroll"><table><tr><th>Wallet / profile</th><th>PIR result</th><th>Blocks result</th><th>PIR ↓ / ↑ MiB</th><th>Blocks ↓ / ↑ MiB</th><th>Incremental blocks MiB</th><th>PIR / blocks seconds</th><th>Download savings</th></tr>')
        def fmt(value, scale=1):
            return '—' if value is None else f'{value/scale:,.2f}'
        for row in data['rows']:
            a, b = row['pir'], row['blocks']
            at, bt = a.get('http_totals', {}), b.get('http_totals', {})
            cells = [f'{row["source_sample_index"]} / {row["profile"]}', a.get('outcome', 'pending'), b.get('outcome', 'pending'),
                     f'{fmt(at.get("bytes_down"),1048576)} / {fmt(at.get("bytes_up"),1048576)}',
                     f'{fmt(bt.get("bytes_down"),1048576)} / {fmt(bt.get("bytes_up"),1048576)}',
                     fmt(row['incremental_block_bytes'],1048576), f'{fmt(a.get("seconds"))} / {fmt(b.get("seconds"))}',
                     fmt(row['standalone_download_savings'],.01)+'%' if row['standalone_download_savings'] is not None else '—']
            chunks.append('<tr>'+''.join(f'<td>{html.escape(str(c))}</td>' for c in cells)+'</tr>')
        chunks.append('</table></div><details><summary>Per-wallet compute and storage</summary><div class="scroll"><table><tr><th>Wallet / method</th><th>Client CPU seconds</th><th>Sampled peak RSS MiB</th><th>OS process writes MiB</th><th>SQLite MiB</th></tr>')
        for row in data['rows']:
            for backend in ['pir','blocks']:
                user=row[backend]
                cells=[f'{row["source_sample_index"]} / {backend}',fmt(user.get('client_cpu_seconds')),fmt(user.get('client_sampled_peak_rss_bytes'),1048576),fmt(user.get('process_written_bytes'),1048576),fmt(user.get('sqlite_bytes'),1048576)]
                chunks.append('<tr>'+''.join(f'<td>{html.escape(str(c))}</td>' for c in cells)+'</tr>')
        chunks.append('</table></div></details><h3>Server APM</h3><p class="note">Counters include background traffic. Missing metrics are unavailable, not zero; CPU deltas are suppressed across gaps or resets. Egress below counts batch response payloads submitted by the block server; client totals also include metadata and retries.</p>')
        for backend,metrics in data['server_apm'].items():
            if not metrics: chunks.append(f'<p>{backend}: server APM unavailable</p>')
            for metric in metrics:
                chunks.append('<p>'+html.escape(f'{backend} / {metric["target"]}: CPU {fmt(metric["cpu_seconds"])} s; peak RSS {fmt(metric["peak_rss_bytes"],1048576)} MiB; response payload {fmt(metric["response_payload_bytes"],1048576)} MiB; {metric["evidence"]}')+'</p>')
        chunks.append('<h3>Per-profile totals</h3><div class="scroll"><table><tr><th>Profile / method</th><th>Exact / unsuccessful</th><th>Download / upload MiB</th><th>Retries</th><th>Client CPU seconds</th><th>OS process writes MiB</th><th>SQLite MiB</th></tr>')
        for profile, methods in data['profiles'].items():
            for backend, values in methods.items():
                cells=[f'{profile} / {backend}',f'{values["exact"]} / {values["unsuccessful"]}',f'{fmt(values["bytes_down"],1048576)} / {fmt(values["bytes_up"],1048576)}',values['retry_attempts'],fmt(values['client_cpu_seconds']),fmt(values['process_written_bytes'],1048576),fmt(values['sqlite_bytes'],1048576)]
                chunks.append('<tr>'+''.join(f'<td>{html.escape(str(c))}</td>' for c in cells)+'</tr>')
        chunks.append('</table></div>')
    chunks.append('<p><a href="report.json">Full comparison JSON</a> · Detailed reports retain CPU/RSS, retries, SQLite size and per-profile metrics. Missing server metrics are not zero.</p></body></html>')
    atomic(out/'report.html',''.join(chunks))
    return result


def run_child(args, out, manifest, status, errors, binary, config, directory, extra=()):
    config_path=out/(directory.replace('/','-')+'.json')
    atomic(config_path,json.dumps(config,indent=2))
    target=out/directory
    target.parent.mkdir(parents=True,exist_ok=True)
    previous=read(target/'report.json')
    if previous and previous.get('success'):
        return True
    if target.exists():
        target.rename(target.with_name(target.name+f'-interrupted-{time.time_ns()}'))
    log=out/(directory.replace('/','-')+'.log')
    with log.open('a') as stream:
        child=subprocess.Popen([binary,'--scenario',str(config_path),'--out-dir',str(target),*extra],stdout=stream,stderr=subprocess.STDOUT)
        try:
            last_checkpoint=None
            while child.poll() is None:
                try: checkpoint=(target/'report.json').stat().st_mtime_ns
                except FileNotFoundError: checkpoint=0
                if checkpoint!=last_checkpoint:
                    report(out,manifest,status,errors)
                    last_checkpoint=checkpoint
                time.sleep(2)
        except BaseException:
            child.send_signal(signal.SIGTERM)
            try: child.wait(timeout=30)
            except subprocess.TimeoutExpired: child.kill(); child.wait()
            raise
    return child.returncode==0


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario',type=Path,required=True)
    parser.add_argument('--fresh-sample',type=Path,required=True)
    parser.add_argument('--block-url',required=True)
    parser.add_argument('--out-dir',type=Path,required=True)
    parser.add_argument('--binary',default='target/release/transparent-loadtest')
    parser.add_argument('--encoding',choices=['gzip','identity'],default='gzip')
    parser.add_argument('--metadata',type=Path,help='JSON describing host hardware, cache conditions and background traffic')
    parser.add_argument('--preparation-concurrency',type=int)
    parser.add_argument('--preparation-cache',choices=['reuse','refresh','off'])
    parser.add_argument('--preparation-cache-dir')
    parser.add_argument('--measured-http-attempts',type=int)
    parser.add_argument('--shard-url')
    parser.add_argument('--filter-url')
    parser.add_argument('--block-metrics-url')
    parser.add_argument('--pir-metrics',action='append',default=[],metavar='NAME=URL')
    args=parser.parse_args()
    out=args.out_dir.resolve();out.mkdir(parents=True,exist_ok=True)
    binary=str(Path(args.binary).resolve())
    if not (out/'report.html').exists(): report(out,{},'validating inputs',[])
    metadata=json.loads(args.metadata.read_text()) if args.metadata else {}
    config=json.loads(args.scenario.read_text())
    config['sample']=str((args.scenario.parent/config['sample']).resolve())
    if args.shard_url: config['shard_url']=args.shard_url
    if args.filter_url: config['filter_url']=args.filter_url
    for field in ['preparation_concurrency','preparation_cache','preparation_cache_dir','measured_http_attempts']:
        if getattr(args,field) is not None: config[field]=getattr(args,field)
    if config.get('mode')!='wave': raise ValueError('comparison requires wave mode')
    with urllib.request.urlopen(args.block_url.rstrip('/')+'/manifest',timeout=60) as response:
        dataset=json.load(response)
    # Canonical dataset ID comes from the server's exact Rust serialization, not a Python re-encoding.
    with urllib.request.urlopen(args.block_url.rstrip('/')+'/identity',timeout=60) as response:
        dataset_id=json.load(response)['dataset_id']
    key=sha(json.dumps(dict(metadata=metadata,binary_sha256=sha(Path(binary).read_bytes()),config=config,fresh_sha256=sha(args.fresh_sample.read_bytes()),dataset_id=dataset_id,encoding=args.encoding,block_url=args.block_url,pir_metrics=args.pir_metrics,block_metrics=args.block_metrics_url),sort_keys=True).encode())
    manifest_path=out/'comparison.json'
    manifest=read(manifest_path)
    if manifest:
        if manifest['input_key']!=key: raise ValueError('comparison inputs changed; use a new output directory')
    else:
        selected=out/'selected-sample.json'
        selection_config=out/'selection-scenario.json'
        atomic(selection_config,json.dumps(config,indent=2))
        subprocess.run([binary,'--scenario',str(selection_config),'--selected-sample',str(selected)],check=True)
        chosen=json.loads(selected.read_text())
        validate_common_coverage(chosen)
        fresh=json.loads(args.fresh_sample.read_text())
        for field in ['genesis_hash','anchor_height','anchor_hash']:
            if chosen[field]!=fresh[field] or chosen[field]!=dataset[field]: raise ValueError(f'{field} mismatch')
        if not dataset['complete']: raise ValueError('incomplete dataset')
        lookup={tuple(sorted(c['scripts'])):c for c in fresh['clients']}
        fresh['clients']=[dict(lookup[tuple(sorted(c['scripts']))], **{'class':c['class'],'scripts':c['scripts']}) for c in chosen['clients']]
        if any(c['required_from']!=0 for c in fresh['clients']) or fresh['start_height']!=0: raise ValueError('fresh oracle must start at genesis')
        fresh_path=out/'fresh-sample.json';atomic(fresh_path,json.dumps(fresh,indent=2))
        manifest=dict(input_key=key,created_at=time.time(),conditions=dict(client=dict(platform=platform.platform(),machine=platform.machine(),logical_cpus=os.cpu_count()),supplied=metadata,dataset=dict(source=dataset.get('source'),batches=len(dataset.get('batches',[])),maximum_blocks_per_batch=max((b['end']-b['start']+1 for b in dataset.get('batches',[])),default=0),compression_unit='one immutable batch',per_wallet_prefetch=config.get('block_prefetch',4),per_wallet_prefetch_bytes=config.get('block_prefetch_bytes',64*1024*1024))),source_sample_indices=chosen['source_sample_indices'],dataset_id=dataset_id,
                      selected_sha256=sha(selected.read_bytes()),fresh_sha256=sha(fresh_path.read_bytes()),runs={},orders={'mixed':['pir','blocks'],'fresh':['blocks','pir']})
        atomic(manifest_path,json.dumps(manifest,indent=2))
    for file,field in [('selected-sample.json','selected_sha256'),('fresh-sample.json','fresh_sha256')]:
        if sha((out/file).read_bytes())!=manifest[field]: raise ValueError('frozen sample changed')
    frozen_binary=out/'transparent-loadtest'
    if not frozen_binary.exists(): shutil.copy2(binary,frozen_binary)
    if sha(frozen_binary.read_bytes())!=sha(Path(binary).read_bytes()): raise ValueError('frozen executable changed')
    binary=str(frozen_binary)
    pir_metrics=dict(config.get('metrics_targets',{}))
    pir_metrics.update(item.split('=',1) for item in args.pir_metrics)
    errors=[];status='preparing'
    report(out,manifest,status,errors)
    try:
        base=dict(config,backend='pir',sample=str(out/'selected-sample.json'))
        if not run_child(args,out,manifest,status,errors,binary,base,'shared-preparation',('--prepare-only',)):
            raise RuntimeError('shared preparation unsuccessful; see its report')
        seed_hashes={p.name:sha(p.read_bytes()) for p in sorted((out/'shared-preparation/seeds').glob('*.json'))}
        if 'seed_sha256' in manifest and manifest['seed_sha256']!=seed_hashes: raise ValueError('shared ledger seeds changed')
        manifest['seed_sha256']=seed_hashes
        atomic(manifest_path,json.dumps(manifest,indent=2))
        for suite in ['mixed','fresh']:
            for backend in manifest['orders'][suite]:
                name=f'{suite}/{backend}'
                manifest['runs'].setdefault(suite,{})[backend]=name
                atomic(manifest_path,json.dumps(manifest,indent=2))
                status=f'{suite}: {backend}'
                child=dict(base,backend=backend,name=f'{config["name"]}: {suite} {backend}',
                           sample=str(out/('selected-sample.json' if suite=='mixed' else 'fresh-sample.json')),
                           prepared_seeds=str(out/'shared-preparation/seeds') if suite=='mixed' else None,
                           recovery_deadline_seconds=config.get('recovery_deadline_seconds',600) if suite=='mixed' else 21600,
                           metrics_targets=pir_metrics if backend=='pir' else ({'blocks':args.block_metrics_url} if args.block_metrics_url else {}))
                if backend=='blocks': child.update(block_url=args.block_url,dataset_id=dataset_id,block_encoding=args.encoding)
                if not run_child(args,out,manifest,status,errors,binary,child,name): errors.append(f'{suite} {backend} unsuccessful; see detailed report')
        status='complete' if not errors else 'unsuccessful'
    except BaseException as error:
        status='interrupted' if isinstance(error,KeyboardInterrupt) else 'failed'
        errors.append(str(error) or status)
        raise
    finally:
        result=report(out,manifest,status,errors)
        print(f'Comparison report: {out / "report.html"}',flush=True)
    return 0 if result['success'] else 1

if __name__=='__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, interrupted)
    try:
        raise SystemExit(main())
    except (Exception, KeyboardInterrupt) as error:
        if '--out-dir' in sys.argv:
            out=Path(sys.argv[sys.argv.index('--out-dir')+1]).resolve()
            out.mkdir(parents=True,exist_ok=True)
            previous=read(out/'report.json') or {}
            if not previous.get('success'):
                report(out,read(out/'comparison.json') or {},'interrupted' if isinstance(error,KeyboardInterrupt) else 'failed',[str(error) or 'interrupted'])
        print(f'Comparison unsuccessful: {error}',file=sys.stderr)
        raise SystemExit(1)
