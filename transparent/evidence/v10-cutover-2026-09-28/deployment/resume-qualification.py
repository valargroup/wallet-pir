#!/usr/bin/env python3
"""Resume at the same-anchor comparison; retain the failed first attempt."""
import datetime,json,pathlib,subprocess
root=pathlib.Path('/opt/transparent-v10-8e69ea75');out=root/'qualification'
sha='8e69ea75b1e0071e3b978b0c78cc0487377f9e82'
assert not (out/'complete.json').exists()
verified=json.loads((out/'verify.json').read_text())
assert verified['tool_sha']==sha and not verified['failures'] and all(c['ok'] for c in verified['checks'])
assert len([c for c in verified['checks'] if c['check'].startswith('rebuild-')])==8
matched=json.loads((out/'census-publication-match.json').read_text())
assert matched['source_sha']==sha and matched['all_shards_match_census'] and matched['shards']==86
certified=json.loads((out/'certificates/summary.json').read_text())
assert certified['source_sha']==sha and len(certified['segments'])==172
for item in certified['segments']:
 floor=83 if item['geometry']=='archive-wide' and item['table']=='pages' else 128
 assert item['failure_bits']>=floor
comparison=out/'fixture-compare-same-anchor.json'
with (out/'fixture-compare-same-anchor.log').open('w') as log:
 subprocess.run(['python3',str(root/'source/transparent/ops/scripts/compare-regression-fixtures.py'),'--previous',str(root/'source/transparent/tools/transparent-regression/fixtures/mainnet.json'),'--next',str(out/'mainnet-v10.json'),'--same-anchor','--out',str(comparison)],stdout=log,stderr=subprocess.STDOUT,check=True)
result=json.loads(comparison.read_text());assert not result['blocking'] and not result['review']
(out/'complete.json').write_text(json.dumps({'source_sha':sha,'shards':86,'certificate_segments':172,'fixture_comparison':comparison.name,'comparison_ops_sha':(root/'ops-revision').read_text().strip(),'resumed_after':'Original comparison required the anchor to advance. Explicit same-anchor mode additionally requires identical cases, expectations, cutoff and map identity. Original failed log and report retained.','completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n')
print((out/'complete.json').read_text(),flush=True)
