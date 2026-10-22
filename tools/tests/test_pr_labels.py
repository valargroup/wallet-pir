"""Catalog synchronization only updates named labels and stops on API failure."""
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
import pr_labels
import fast


class PrLabelTests(unittest.TestCase):
    def setUp(self):
        self.rules = json.loads((ROOT / '.github/labeler.yml').read_text())

    def test_automatic_labels_exist_in_catalog_and_exclude_semantic_judgments(self):
        catalog = pr_labels.catalog()
        rules = json.loads((ROOT / '.github/labeler.yml').read_text())
        self.assertLessEqual(set(rules), set(catalog))
        self.assertEqual(set(catalog) - set(rules), {
            'wallet-api-breaking', 'publish-api-breaking', 'journal-breaking',
            'security-review', 'privacy-review', 'pir-scheme'})
        for name, matches in rules.items():
            with self.subTest(label=name):
                paths = matches[0]['changed-files'][0]['any-glob-to-any-file']
                self.assertTrue(paths)
                self.assertTrue(all(isinstance(path, str) and path for path in paths))

    def test_sync_only_upserts_catalog_labels_with_bounded_cli_calls(self):
        labels = {'area:publisher': {'color': '7057ff', 'description': 'Publication.'}}
        with patch.object(pr_labels, 'catalog', return_value=labels), \
                patch.object(pr_labels.subprocess, 'run') as run, redirect_stdout(io.StringIO()):
            pr_labels.sync('fixture/repository')
            pr_labels.sync('fixture/repository')
        expected = ['gh', 'label', 'create', 'area:publisher', '--repo', 'fixture/repository',
                    '--color', '7057ff', '--description', 'Publication.', '--force']
        self.assertEqual(run.call_count, 2)
        for call in run.call_args_list:
            self.assertEqual(call.args, (expected,))
            self.assertEqual(call.kwargs, {'check': True, 'timeout': 30})

    def test_invalid_catalog_is_rejected_before_any_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            catalog = Path(directory) / 'labels.json'
            valid = {'area:publisher': {'color': '7057ff', 'description': 'Publication.'}}
            invalid = [[], {}, {**valid, 'bad': {'color': 'red', 'description': 'Bad color'}},
                       {**valid, 'bad name': {'color': '7057ff', 'description': 'Bad name'}},
                       {**valid, 'bad': {'color': '7057ff', 'description': ''}}]
            for labels in invalid:
                catalog.write_text(json.dumps(labels))
                with self.subTest(labels=labels), patch.object(pr_labels, 'CATALOG', catalog), \
                        patch.object(pr_labels.subprocess, 'run') as run:
                    with self.assertRaises(ValueError):
                        pr_labels.sync('fixture/repository')
                    run.assert_not_called()

    def test_api_failure_stops_sync_and_is_not_reported_as_success(self):
        error = subprocess.CalledProcessError(1, ['gh'])
        labels = pr_labels.catalog()
        with patch.object(pr_labels, 'catalog', return_value=labels), \
                patch.object(pr_labels.subprocess, 'run', side_effect=error) as run:
            with self.assertRaises(subprocess.CalledProcessError):
                pr_labels.sync('fixture/repository')
        self.assertEqual(run.call_count, 1)

    def test_receiver_source_selects_product_and_two_main_roles(self):
        files = [{'filename': path, 'changes': changes} for path, changes in [
            ('src/receiver/receiver-indexer/src/service.rs', 40),
            ('src/receiver/receiver-pir-server/src/lib.rs', 30),
            ('docs/receiver/README.md', 2),
            ('tools/receiver/digitalocean/Caddyfile', 1)]]
        self.assertEqual(pr_labels.select(files, [], self.rules),
                         ['product:receiver', 'area:publisher', 'area:query-server'])

    def test_risk_labels_displace_components_without_exceeding_three(self):
        files = [{'filename': 'src/enhance/enhance-publisher/src/lib.rs', 'changes': 10}]
        selected = pr_labels.select(files, ['pir-scheme', 'wallet-api-breaking'], self.rules)
        self.assertEqual(selected, ['pir-scheme', 'wallet-api-breaking', 'product:enhance'])
        self.assertEqual(pr_labels.select(files, list(pr_labels.RISK_PRIORITY), self.rules),
                         ['pir-scheme', 'wallet-api-breaking', 'publish-api-breaking'])

    def test_large_area_wins_and_old_component_labels_do_not_accumulate(self):
        files = [{'filename': 'src/status/status-query-server/src/lib.rs', 'changes': 80},
                 {'filename': 'src/transparent/transparent-query-server/src/lib.rs', 'changes': 10},
                 {'filename': 'docs/enhance/README.md', 'changes': 1}]
        selected = pr_labels.select(files, ['product:receiver', 'area:publisher'], self.rules)
        self.assertEqual(selected, ['area:query-server', 'product:status', 'product:transparent'])

    def test_other_manual_labels_count_and_selection_is_stable(self):
        files = [{'filename': 'src/shared/pir-runtime/src/params.rs', 'changes': 0}]
        selected = pr_labels.select(files, ['bug', 'pir-scheme'], self.rules)
        self.assertEqual(selected, ['pir-scheme', 'bug', 'area:shared'])
        self.assertEqual(pr_labels.select(files, selected, self.rules), selected)

    def test_globs_distinguish_direct_children_recursive_paths_and_root_markdown(self):
        for path in ['README.md', 'docs/receiver/README.md']:
            self.assertTrue(pr_labels.path_pattern('**/*.md').fullmatch(path))
        self.assertFalse(pr_labels.path_pattern('tools/tests/test_*.py').fullmatch(
            'tools/tests/operations/test_receiver.py'))
        self.assertTrue(pr_labels.path_pattern('tools/*/deploy/*worker*').fullmatch(
            'tools/status/deploy/status-worker.role.service'))

    def test_pr_update_replaces_the_set_once_and_unchanged_reruns_do_not_write(self):
        labels = ['product:receiver', 'area:publisher', 'area:query-server', 'documentation']
        files = [[{'filename': 'src/receiver/receiver-indexer/src/service.rs', 'changes': 10}]]
        calls = []
        def api(repository, path, **kwargs):
            calls.append((path, kwargs))
            if path.endswith('/files?per_page=100'):
                return files
            if kwargs.get('data') is not None:
                labels[:] = kwargs['data']['labels']
                return []
            return {'state': 'open', 'labels': [{'name': name} for name in labels]}
        with patch.object(pr_labels, 'api', side_effect=api), redirect_stdout(io.StringIO()):
            pr_labels.label_pr('fixture/repository', 123)
            pr_labels.label_pr('fixture/repository', 123)
        writes = [kwargs['data'] for _, kwargs in calls if 'data' in kwargs]
        self.assertEqual(writes, [{'labels': ['product:receiver', 'area:publisher']}])

    def test_closed_pr_does_not_fetch_files_or_write_labels(self):
        with patch.object(pr_labels, 'api', return_value={'state': 'closed'}) as api, \
                redirect_stdout(io.StringIO()):
            pr_labels.label_pr('fixture/repository', 123)
        self.assertEqual(api.call_count, 1)

    def test_label_policy_changes_select_tooling_without_rust(self):
        packages = fast.workspace_packages()
        for path in ['.github/labeler.yml', '.github/labels.json', '.github/workflows/pr-labels.yml']:
            with self.subTest(path=path):
                plan = fast.plan(packages, [path])
                self.assertEqual(plan['packages'], [])
                self.assertIn('check-tools', plan['helpers'])


if __name__ == '__main__':
    unittest.main()
