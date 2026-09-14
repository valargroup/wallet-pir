import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('runtime', Path(__file__).parents[1] / 'scripts/wallet-pir-runtime.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class RuntimeCredentials(unittest.TestCase):
    def test_exact_credential_set_preserves_multiline_key(self):
        with tempfile.TemporaryDirectory() as directory:
            values = {key: 'fixture' for key in module.KEYS}
            values['WALLET_PIR_DEPLOY_SSH_KEY'] = 'fixture-line-1\nfixture-line-2\n'
            (Path(directory) / 'runtime').write_text(json.dumps(values))
            self.assertEqual(module.credentials(directory), values)

    def test_invalid_payload_never_leaks_values(self):
        with tempfile.TemporaryDirectory() as directory:
            valid = {key: 'sensitive-fixture' for key in module.KEYS}
            for payload in [dict(valid, PATH='sensitive-fixture'), {}, [],
                            {**valid, 'CF_API_TOKEN': None},
                            {**valid, 'CF_API_TOKEN': 'sensitive-fixture\0'}]:
                (Path(directory) / 'runtime').write_text(json.dumps(payload))
                with self.assertRaisesRegex(RuntimeError, '^Wallet PIR runtime credential is missing or invalid$'):
                    module.credentials(directory)
            (Path(directory) / 'runtime').write_text('sensitive-fixture invalid JSON')
            with self.assertRaisesRegex(RuntimeError, '^Wallet PIR runtime credential is missing or invalid$'):
                module.credentials(directory)
