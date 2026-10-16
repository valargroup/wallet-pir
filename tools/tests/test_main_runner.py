"""Cold branch/PR feedback must not occupy main's reserved runner queue."""
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]


class RunnerRoutingTests(unittest.TestCase):
    def test_only_main_uses_the_reserved_fast_runner(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        runner = next(line for line in workflow.splitlines() if 'runs-on:' in line)
        condition = runner.split('fromJSON(', 1)[1].split('&& vars.', 1)[0].strip()
        for event, ref, same_repo, expected in [
            ('push', 'refs/heads/main', True, True),
            ('pull_request', 'refs/pull/1/merge', True, False),
            ('pull_request', 'refs/pull/1/merge', False, False),
            ('workflow_dispatch', 'refs/heads/upgrade', True, False),
            ('workflow_dispatch', 'refs/heads/main', True, True),
        ]:
            values = {'github.event_name': event, 'github.ref': ref,
                      'github.repository': 'valargroup/wallet-pir',
                      'github.event.pull_request.head.repo.full_name': 'valargroup/wallet-pir' if same_repo else 'fork/wallet-pir'}
            expression = condition
            for key in sorted(values, key=len, reverse=True):
                expression = expression.replace(key, repr(values[key]))
            expression = expression.replace('&&', ' and ').replace('||', ' or ')
            with self.subTest(event=event, ref=ref, same_repo=same_repo):
                self.assertEqual(bool(eval(expression, {'__builtins__': {}}, {})), expected)
