import hashlib,json,pathlib,subprocess,shlex
base=pathlib.Path('/opt/transparent-timing-slots-20260911')
out=base/'cpu-profile'
out.mkdir()
binary=base/'burst-test'
wrapper=out/'profile-test'
wrapper.write_text('''#!/bin/sh
if [ "$1" = "burst::publication_burst_under_exact_load" ]; then
  exec /usr/bin/perf record -e cpu-clock -F 99 --call-graph dwarf,4096 -o /opt/transparent-timing-slots-20260911/cpu-profile/worker.perf.data -- /opt/transparent-timing-slots-20260911/burst-test "$@"
fi
exec /opt/transparent-timing-slots-20260911/burst-test "$@"
''')
wrapper.chmod(0o755)
(out/'provenance.json').write_text(json.dumps(dict(source_sha='3d13da6',scope='CPU profiling only; instrumentation overhead excludes performance acceptance',binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),wrapper_sha256=hashlib.sha256(wrapper.read_bytes()).hexdigest()),indent=2)+'\n')
cmd=['systemd-run','--unit=transparent-m1-cpu-profile','--property=WorkingDirectory='+str(base),'/usr/bin/python3','ops/scripts/run-transparent-burst.py','--systemd','--external-clients','--worker-budget-seconds','14','--host-overhead-bytes','805306368','--fixture','/opt/transparent-full-fixture-20260909/fixture-canonical.json','--test-binary',str(wrapper),'--source-sha','3d13da6','--build-slots','2','--repetitions','1','--out',str(out/'run')]
subprocess.run(cmd,check=True)
