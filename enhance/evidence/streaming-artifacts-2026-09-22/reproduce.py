"""Run on macOS with cargo and lsof; use a fresh output directory."""
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

BASELINE = '93fa9da7ba274e4d7f6f14e4b1b514b16bdca8be'
here = pathlib.Path(__file__).resolve().parent
repo = here.parents[2]
output = pathlib.Path(sys.argv[1]).resolve()
output.mkdir(parents=True, exist_ok=False)
with tempfile.TemporaryDirectory(prefix='wallet-pir-stream-bench-') as scratch:
    scratch = pathlib.Path(scratch)
    (scratch / 'src').mkdir()
    shutil.copyfile(here / 'harness.rs', scratch / 'src/main.rs')
    for module in ['ipir', 'wire']:
        source = subprocess.check_output([
            'git', 'show', f'{BASELINE}:enhance/services/enhance-pir-server/src/{module}.rs'
        ], cwd=repo)
        (scratch / f'src/baseline_{module}.rs').write_bytes(source)
    (scratch / 'Cargo.toml').write_text(
        (here / 'Cargo.toml.template').read_text().replace('__REPO__', str(repo))
    )
    shutil.copyfile(repo / 'Cargo.lock', scratch / 'Cargo.lock')
    env = dict(os.environ, CARGO_TARGET_DIR=str(repo / 'target'))
    with (output / 'build.log').open('w') as log:
        subprocess.run(['cargo', 'build', '--release', '--manifest-path', str(scratch / 'Cargo.toml')],
                       cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    shutil.copyfile(scratch / 'Cargo.lock', output / 'harness.Cargo.lock')
    subprocess.run([sys.executable, str(here / 'measure.py'), str(repo), str(output)], check=True)
