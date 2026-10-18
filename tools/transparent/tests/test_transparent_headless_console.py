"""Privileged console changes must preserve access and fail before unsafe writes."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

SCRIPTS = Path(__file__).parents[1]/'scripts'
def load(name):
    spec=importlib.util.spec_from_file_location(name,SCRIPTS/(name+'.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
H=load('transparent-headless-console')
M=load('observe-transparent-hardening')


class HeadlessTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        (self.root/'proc').mkdir()
        (self.root/'proc/consoles').write_text('ttyS0 -W- (EC p a) 4:64\ntty1 -WU (E p) 4:1\n')
        (self.root/'proc/fb').write_text('0 virtio_gpudrmfb\n')
        self.console=self.root/'sys/class/vtconsole/vtcon0';self.console.mkdir(parents=True)
        (self.console/'name').write_text('(M) frame buffer device\n')
        (self.console/'bind').write_text('1\n')

    def test_preflight_is_read_only_and_apply_is_idempotent(self):
        H.configure(self.root,preflight=True)
        self.assertEqual((self.console/'bind').read_text(),'1\n')
        with self.assertRaisesRegex(RuntimeError,'still bound'):H.configure(self.root)
        for _ in range(2):
            state=H.configure(self.root,apply=True)
            self.assertTrue(state['serial_console_enabled'])
            self.assertFalse(state['bindings'][0]['bound'])
        self.assertIn('ttyS0',(self.root/'proc/consoles').read_text())

    def test_missing_serial_or_unknown_framebuffer_never_changes_binding(self):
        for file,value in [('consoles','tty1 -WU (E p) 4:1\n'),('fb','0 other_gpu\n')]:
            path=self.root/'proc'/file;before=path.read_text();path.write_text(value)
            with self.assertRaises(RuntimeError):H.configure(self.root,apply=True)
            self.assertEqual((self.console/'bind').read_text(),'1\n')
            path.write_text(before)

    def test_malformed_or_unidentifiable_binding_is_refused(self):
        (self.console/'bind').write_text('unknown\n')
        with self.assertRaises(RuntimeError):H.configure(self.root,apply=True)
        (self.console/'bind').write_text('1\n');(self.root/'proc/fb').write_text('')
        with self.assertRaises(RuntimeError):H.configure(self.root,apply=True)

    def test_no_framebuffer_is_already_headless(self):
        (self.root/'proc/fb').write_text('')
        (self.console/'name').write_text('(M) dummy device\n')
        self.assertEqual(H.configure(self.root)['bindings'],[])

    def test_persistence_requires_loaded_prestart_and_boot_enablement(self):
        good='UnitFileState=enabled\nExecStartPre={ argv[]=/usr/bin/python3 '+H.INSTALLED+' --apply ; }\n'
        H.check_persistence(good)
        for bad in [good.replace('enabled','disabled'),good.replace('--apply','--preflight'),good.replace('ExecStartPre=','Description=')]:
            with self.assertRaises(RuntimeError):H.check_persistence(bad)

    def test_observer_refuses_changed_helper_or_rebound_console(self):
        good={'helper_sha256':'expected','persistent':True,'serial_console_enabled':True,'bindings':[{'bound':False}]}
        self.assertIsNone(M.headless_failure(good,'expected'))
        for changes in [{'helper_sha256':'changed'},{'persistent':False},{'serial_console_enabled':False},{'bindings':[{'bound':True}]},{'bindings':None}]:
            self.assertIsNotNone(M.headless_failure({**good,**changes},'expected'))
