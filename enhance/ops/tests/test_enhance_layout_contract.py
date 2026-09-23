"""Historical schema-9 script consistency and schema-11 retirement guards."""
import re
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / 'enhance/ops/scripts/deploy-enhance-pir.sh'
PREPARE = ROOT / 'enhance/ops/scripts/prepare-enhance-pir.sh'
AUTOSCALE = ROOT / 'enhance/ops/scripts/enhance-autoscale.py'
ROLLOUT = ROOT / 'enhance/ops/scripts/rollout-schema9-ssh.sh'
TYPES = ROOT / 'enhance/crates/enhance-pir/src/types.rs'


def rust_const(name, cast=int):
    """The value of a `pub const NAME: T = <literal>;` in types.rs."""
    match = re.search(rf'^pub const {name}: \w+ = ([^;]+);', TYPES.read_text(), re.M)
    assert match, f'{name} not found in {TYPES}'
    return cast(match.group(1).strip().strip('"').replace('_', ''))


def shell_const(path, name):
    match = re.search(rf'^(?:readonly )?{name}=(\S+)', path.read_text(), re.M)
    assert match, f'{name} not found in {path}'
    return match.group(1)


class ServedLayout(unittest.TestCase):
    def setUp(self):
        self.schema = 9  # Frozen historical deployment contract.
        self.records_per_row = rust_const('RECORDS_PER_ROW')
        self.record_bytes = 737  # Retired scripts cannot deploy suffix records.
        self.shard_rows = rust_const('SHARD_ROWS')
        self.row_bytes = self.record_bytes * self.records_per_row

    def test_legacy_deployment_commands_are_retired_before_environment_checks(self):
        for script, mode in [(DEPLOY, 'deploy'), (PREPARE, ''), (ROLLOUT, 'cutover')]:
            result = subprocess.run(['bash', str(script), mode], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('retired', result.stderr)

    def test_deploy_target_constants_match_the_crate(self):
        self.assertEqual(int(shell_const(DEPLOY, 'ENHANCE_TARGET_SCHEMA')), self.schema)
        self.assertEqual(int(shell_const(DEPLOY, 'ENHANCE_TARGET_RECORDS_PER_ROW')),
                         self.records_per_row)
        self.assertEqual(int(shell_const(DEPLOY, 'ENHANCE_TARGET_ROW_BYTES')), self.row_bytes)

    def test_init_gate_asserts_the_served_layout(self):
        programs = subprocess.check_output([str(DEPLOY), 'jq-programs']).decode()
        gate = dict(row.split('\t', 1) for row in programs.split('\0') if row)['JQ_ENHANCE_INIT_COMPLETE']
        for field, value in [('schema_version', self.schema),
                             ('record_bytes', self.record_bytes),
                             ('records_per_row', self.records_per_row),
                             ('row_bytes', self.row_bytes),
                             ('shard_rows', self.shard_rows)]:
            self.assertIn(f'.generation.{field} == {value}', gate,
                          f'the deploy gate does not pin {field}={value}')

    def test_units_serve_the_directory_the_deploy_script_prepares(self):
        unit = (ROOT / 'enhance/ops/deploy/enhance-pir-server.service').read_text()
        self.assertIn(f'--data-dir {shell_const(DEPLOY, "ENHANCE_DATA_DIR")}', unit)

    def test_autoscale_group_capacity_follows_the_layout(self):
        match = re.search(r'^GROUP_POSITIONS = (\d+) \* (\d+)', AUTOSCALE.read_text(), re.M)
        self.assertIsNotNone(match)
        shards_per_group, shard_positions = int(match.group(1)), int(match.group(2))
        self.assertEqual(shards_per_group, rust_const('SHARDS_PER_GROUP'))
        self.assertEqual(shard_positions, self.shard_rows * self.records_per_row)


if __name__ == '__main__':
    unittest.main()
