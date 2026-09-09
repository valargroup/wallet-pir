#!/usr/bin/env python3
"""Supervised canary, gated maintenance batch, then 24-hour fleet observation.

Run under systemd on the coordinator. A failed command stops progression and
retains all logs. No timer, stale gate file, or service restart advances a phase.
"""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent


async def command(args, logfile):
    with logfile.open('wb') as log:
        process = await asyncio.create_subprocess_exec(*map(str, args), stdout=log, stderr=asyncio.subprocess.STDOUT)
        try:
            code = await process.wait()
            if code:
                raise RuntimeError(f'{Path(str(args[1])).name} exited {code}; see {logfile}')
        finally:
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 10)
                except TimeoutError:
                    process.kill()
                    await process.wait()


async def run(args):
    args.out.mkdir(parents=True, exist_ok=False)
    binary = hashlib.sha256((args.artifacts/'transparent-shard-server').read_bytes()).hexdigest()
    canary = 'transparent-pir-recent-01'
    query = args.artifacts/'soak-query'
    upgrade = [sys.executable, HERE/'upgrade-transparent-fleet.py', '--fleet-config', args.fleet_config,
               '--artifacts', args.artifacts, '--source-sha', args.source_sha, '--query-binary', query]
    monitor = [sys.executable, HERE/'observe-transparent-hardening.py', '--fleet-config', args.fleet_config,
               '--fleet-script', HERE/'transparent-live-fleet.py', '--binary-sha256', binary,
               '--freshness-seconds', '30', '--replica-freshness-seconds', '60']
    def phase(name, **extra):
        value = dict(phase=name, binary_sha256=binary, source_sha=args.source_sha, **extra)
        temporary = args.out/'status.next'
        temporary.write_text(json.dumps(value, indent=2)+'\n')
        temporary.replace(args.out/'status.json')
        print(json.dumps(value), flush=True)
    try:
        phase('canary_upgrade')
        await command(upgrade+['--worker', canary, '--out', args.out/'canary-upgrade'], args.out/'canary-upgrade.log')
        phase('canary_observation')
        await command(monitor+['--worker', canary, '--query-binary', query, '--seconds', '21600', '--blocks', '300',
                               '--out', args.out/'canary'], args.out/'canary.log')
        phase('fleet_upgrade')
        await command(upgrade+['--canary-result', args.out/'canary/result.json', '--out', args.out/'fleet-upgrade'], args.out/'fleet-upgrade.log')
        phase('fleet_observation')
        config = json.loads(args.fleet_config.read_text())
        roster = json.loads(Path(config['roster']).read_text())
        async with asyncio.TaskGroup() as group:
            for worker in roster:
                load = ['--query-binary', query] if worker['id'] == canary else []
                group.create_task(command(monitor+['--worker', worker['id'], '--seconds', '86400', '--blocks', '300',
                                                   '--out', args.out/('fleet-'+worker['id'])]+load,
                                          args.out/('fleet-'+worker['id']+'.log')))
        phase('complete')
    except BaseException as error:
        phase('failed', error=str(error))
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fleet-config', type=Path, default=Path('/opt/transparent-publisher/fleet.json'))
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--out', type=Path, required=True)
    asyncio.run(run(parser.parse_args()))


if __name__ == '__main__':
    main()
