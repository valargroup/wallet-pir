import json, os, pathlib, subprocess, datetime, sys

repo = pathlib.Path(sys.argv[1]).resolve()
output = pathlib.Path(sys.argv[2]).resolve()
output.mkdir(parents=True, exist_ok=True)
binary = repo / 'target/release/wallet-pir-stream-bench'
for scenario in ['cold', 'load', 'hint', 'retained']:
    for mode in ['baseline', 'candidate']:
        label = f'{scenario}-{mode}'
        artifact_root = output / f'artifacts-{mode}-{"cold" if scenario in ["cold", "load", "hint"] else scenario}'
        command = ['/usr/bin/time', '-l', str(binary), mode, scenario, str(artifact_root)]
        env = os.environ.copy()
        env['BENCH_HOLD'] = '1'
        with (output / f'{label}.time.txt').open('w') as timing, (output / f'{label}.jsonl').open('w') as raw:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=timing, stdin=subprocess.PIPE, text=True, env=env)
            for line in process.stdout:
                raw.write(line); raw.flush()
                event = json.loads(line)
                if event.get('phase') == 'retained':
                    files = subprocess.run(['lsof', '-nP', '-a', '-p', str(event['pid']), '-F', 'fins'], capture_output=True, text=True, check=False)
                    (output / f'{label}.open-files.txt').write_text(files.stdout)
                    disk = {str(p.relative_to(artifact_root)): p.stat().st_size for p in artifact_root.rglob('*') if p.is_file()}
                    (output / f'{label}.visible-files.json').write_text(json.dumps(disk, indent=2)+'\n')
                    process.stdin.write('\n'); process.stdin.flush()
                print(label, line.rstrip(), flush=True)
            code = process.wait()
            if code: raise SystemExit(f'{label} failed: {code}')
        (output / f'{label}.command.json').write_text(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(), 'command': command,'env':{'BENCH_HOLD':'1'},'exit_code':code}, indent=2)+'\n')
import hashlib
compatibility = {}
for scenario in ['cold', 'retained']:
    for name in ['database.u16le', 'partial-crs.bin', 'metadata.json']:
        entries = []
        for mode in ['baseline', 'candidate']:
            path = output / f'artifacts-{mode}-{scenario}' / 'enhance/shard-00000000' / name
            with path.open('rb') as handle:
                digest = hashlib.file_digest(handle, 'sha256').hexdigest()
            entries.append({'mode':mode,'sha256':digest,'length':path.stat().st_size})
        assert entries[0]['sha256'] == entries[1]['sha256'], (scenario, name, entries)
        compatibility[f'{scenario}/{name}'] = entries
(output / 'compatibility.json').write_text(json.dumps(compatibility, indent=2)+'\n')
