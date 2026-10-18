"""Mount policy must fail closed and preserve unrelated durability settings."""
import json
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

SCRIPTS=Path(__file__).parents[1]/'scripts'
def load(name):
    spec=importlib.util.spec_from_file_location(name,SCRIPTS/(name+'.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
H=load('transparent-storage-policy')
M=load('observe-transparent-hardening')
U=load('upgrade-transparent-fleet')

class StorageTests(unittest.TestCase):
    def setUp(self):
        self.before=dict(target='/',source='/dev/vda1',fstype='ext4',options='rw,relatime,discard,commit=30,errors=remount-ro')
        self.after={**self.before,'options':'rw,relatime,commit=30,errors=remount-ro'}

    def test_missing_cache_preflight_uses_existing_parent_without_creating_it(self):
        raw=json.dumps({'filesystems':[self.before]})
        with patch.object(H.Path,'exists',lambda path: str(path) != H.PATHS[1]), patch.object(H.subprocess,'check_output',return_value=raw) as query, patch.object(H.Path,'mkdir') as mkdir:
            self.assertEqual(H.inspect(),self.before)
            self.assertEqual(query.call_args_list[-1].args[0],['findmnt','-J','-T','/srv/transparent-pir'])
            mkdir.assert_not_called()

    def test_preflight_and_check_never_remount(self):
        with patch.object(H,'inspect',return_value=self.before),patch.object(H.subprocess,'run') as run:
            self.assertTrue(H.configure(preflight=True)['online_discard'])
            with self.assertRaisesRegex(RuntimeError,'remains enabled'):H.configure()
            run.assert_not_called()

    def test_repeated_apply_and_rollback_preserve_other_mount_options(self):
        for before,after,restore,desired in [(self.before,self.after,None,'nodiscard'),(self.after,self.after,None,'nodiscard'),(self.after,self.before,self.before['options'],'discard'),(self.after,self.after,self.after['options'],'nodiscard')]:
            with patch.object(H,'inspect',side_effect=[before,after]),patch.object(H.subprocess,'run') as run:
                H.configure(apply=restore is None,restore_options=restore)
                run.assert_called_once_with(['mount','-o','remount,'+desired,'/'],check=True)

    def test_failed_remount_or_unrelated_change_cannot_attest_success(self):
        with patch.object(H,'inspect',return_value=self.before),patch.object(H.subprocess,'run',side_effect=RuntimeError('mount failed')):
            with self.assertRaisesRegex(RuntimeError,'mount failed'):H.configure(apply=True)
        for after in [self.before,{**self.after,'options':self.after['options']+',nobarrier'},{**self.after,'source':'/dev/vdc1'}]:
            with patch.object(H,'inspect',side_effect=[self.before,after]),patch.object(H.subprocess,'run'):
                with self.assertRaises(RuntimeError):H.configure(apply=True)

    def test_unknown_or_unsafe_filesystems_refused(self):
        H.validate_mounts([self.before,self.before])
        for change in [{'target':'/srv'},{'fstype':'xfs'},{'source':'overlay'},{'source':'/dev/vdc1'},{'options':'ro,discard'},{'options':'rw,nobarrier'},{'options':'rw,barrier=0'},{'options':'rw,noload'}]:
            with self.assertRaises(RuntimeError):H.validate_mounts([self.before,{**self.before,**change}])
        with self.assertRaises(RuntimeError):H.validate_mounts([])

    def test_boot_persistence_and_observer_must_attest_policy(self):
        unit='UnitFileState=enabled\nExecStartPre={ argv[]=/usr/bin/python3 '+H.INSTALLED+' --apply ; }\n'
        H.check_persistence(unit)
        for bad in [unit.replace('enabled','disabled'),unit.replace('--apply','--preflight'),unit.replace('ExecStartPre=','Description=')]:
            with self.assertRaises(RuntimeError):H.check_persistence(bad)
        good={'helper_sha256':'expected','persistent':True,'online_discard':False}
        self.assertIsNone(M.storage_failure(good,'expected'))
        for changes in [{'helper_sha256':'old'},{'persistent':False},{'online_discard':True},{'online_discard':None}]:
            self.assertIsNotNone(M.storage_failure({**good,**changes},'expected'))

    def test_gate_rejects_result_from_different_storage_policy(self):
        result=dict(passed=True,binary_sha256='b',fleet_script_sha256='s',fleet_config_sha256='c',roster_sha256='r',worker='transparent-pir-recent-01',seconds=21600,blocks=300,replica_blocks=300,exact_queries=[1000,1000],public_budget_seconds=30,replica_budget_seconds=60,maximum_visibility_seconds=20,maximum_canary_visibility_seconds=25)
        with self.assertRaisesRegex(ValueError,'does not match'):U.validate_gate(result,'b','s','c','r',storage_helper='new')

        U.validate_gate({**result,"storage_helper_sha256":"new"},"b","s","c","r",storage_helper="new")
