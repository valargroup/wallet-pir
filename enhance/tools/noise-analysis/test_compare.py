import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class CompleteCampaignTests(unittest.TestCase):
    def test_empty_or_disjoint_campaign_cannot_pass_release_comparison(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            left, right = root / 'left', root / 'right'
            left.mkdir(); right.mkdir()
            command = [sys.executable, str(Path(__file__).with_name('compare.py')),
                       str(left), str(right), '--expected-cases', '432']
            for disjoint in (False, True):
                if disjoint:
                    (left / 'r4096-u1-zero-s0.json').write_text('{}')
                    (right / 'r4096-u1-zero-s1.json').write_text('{}')
                result = subprocess.run(command, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(json.loads(result.stdout)['matching_cases'], 0)


if __name__ == '__main__':
    unittest.main()
