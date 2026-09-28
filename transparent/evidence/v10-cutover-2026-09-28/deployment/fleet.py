#!/usr/bin/env python3
"""Direct SSH rollout entrypoint using the repository deployment scripts."""
import json,os,pathlib,subprocess,sys
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
sha='8e69ea75b1e0071e3b978b0c78cc0487377f9e82'
mode=sys.argv[1]
if mode not in ['fleet-preflight','fleet-deploy','fleet-rollback','publisher-shadow','publisher-activate']: raise ValueError(mode)
fleet=json.loads((pathlib.Path('/opt/transparent-publisher')/'fleet.json').read_text())
env=dict(os.environ)
env.update(TRANSPARENT_FLEET_JSON=(root/'pre-roster.json').read_text(),TRANSPARENT_ASSIGNMENT=str(root/'assignment.json'),WALLET_PIR_DEPLOY_USER='root',TRANSPARENT_RELEASE_SHA=sha,TRANSPARENT_ARTIFACT_DIR=str(root/'artifacts/transparent-shard'),TRANSPARENT_SHARD_DIR='/srv/transparent-pir/sets-v10',TRANSPARENT_RUNTIME_CACHE_DIR='/srv/transparent-pir/runtime-cache-v10',TRANSPARENT_PUBLIC_URL='https://transparent-pir.valargroup.dev',TRANSPARENT_SHARD_SOURCE='/srv/zakura/transparent-shards-v10-full',TRANSPARENT_ROUTER_HOST=fleet['router_host'],TRANSPARENT_ROUTER_INTERNAL_PORT='8080',TRANSPARENT_SSH_KEY_PATH=fleet['ssh_key'],TRANSPARENT_KNOWN_HOSTS_PATH=fleet['known_hosts'],TRANSPARENT_SCHEMA_CUTOVER='true',TRANSPARENT_DEFER_PUBLIC_VERIFY='true',TRANSPARENT_FORCE_REDEPLOY='true',TRANSPARENT_OWNER_ACTIVATION='parallel',TRANSPARENT_REPLICA_ACTIVATION='paired',TRANSPARENT_TRANSACTION_DIR=str(root/'transactions'))
if mode in ['fleet-preflight','fleet-deploy','publisher-shadow','publisher-activate']:
 complete=json.loads((root/'qualification/complete.json').read_text())
 assert complete['source_sha']==sha
if mode=='fleet-deploy':
 for unit in ['transparent-publish-controller','transparent-replica-reconciler']:
  assert subprocess.run(['systemctl','is-active','--quiet',unit]).returncode!=0, 'old publisher must be stopped before cutover'
if mode.startswith('fleet-'):
 subprocess.run(['bash',str(root/'source/transparent/ops/scripts/deploy-transparent-shard.sh'),mode],env=env,check=True)
 if mode=='fleet-preflight': (root/'fleet-preflight.complete').write_text(sha+'\n')
else:
 env['WALLET_PIR_DEPLOY_SSH_KEY']=pathlib.Path(fleet['ssh_key']).read_text()
 env['TRANSPARENT_SSH_KNOWN_HOSTS']=pathlib.Path(fleet['known_hosts']).read_text()
 subprocess.run(['python3',str(root/'source/transparent/ops/scripts/deploy-transparent-publisher.py'),mode.removeprefix('publisher-'),'--artifacts',str(root/'artifacts/transparent-publisher'),'--initial-publication','/srv/zakura/transparent-publications/initial','--source-sha',sha,'--range-profile','zcash-transparent-range-v2','--recent-geometry','recent-4k-8k','--data-dir','/srv/zakura/transparent-event-data-v2','--directory-choice','all'],env=env,check=True)
