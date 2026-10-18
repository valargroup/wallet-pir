import contextlib,importlib.util,io,json,pathlib,tempfile,unittest
from unittest.mock import patch
from types import SimpleNamespace
spec=importlib.util.spec_from_file_location('supervise',pathlib.Path(__file__).with_name('transparent-quality-load.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Proc:
 def __init__(self,event):self.stdout=io.StringIO(json.dumps(event)+'\n');self.terminated=False
 def terminate(self):self.terminated=True
class Tests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();m.ROOT=pathlib.Path(self.tmp.name);m.LATCH.clear();m.COUNTS.clear();m.WINDOW.clear();m.STOP=False;m.PROC=None
 def tearDown(self):self.tmp.cleanup()
 def test_wrong_plaintext_is_persisted_and_kills_queries(self):
  event={'event':'error','kind':'query_or_decode','error':'decoded row differs from independent plaintext oracle'};p=Proc(event)
  with contextlib.redirect_stdout(io.StringIO()):m.reader(p)
  self.assertTrue(p.terminated);self.assertTrue((m.ROOT/'latched.json').exists());self.assertEqual(m.COUNTS['error'],1)
 def test_transport_503_does_not_become_correctness_failure(self):
  p=Proc({'event':'error','kind':'query_or_decode','error':'shard 3: HTTP 503 unavailable'})
  with contextlib.redirect_stdout(io.StringIO()):m.reader(p)
  self.assertFalse(p.terminated);self.assertFalse((m.ROOT/'latched.json').exists());self.assertEqual(m.COUNTS['error'],1)
 def test_decode_or_prepare_failure_is_critical(self):
  for v in [{'event':'error','kind':'query_or_decode','error':'response: bad epoch'},{'event':'error','kind':'prepare','error':'invalid row'}]:self.assertTrue(m.correctness_failure(v))
 def test_first_latch_survives_later_incident(self):
  m.persist_latch('first');m.persist_latch('later');self.assertEqual(json.loads((m.ROOT/'latched.json').read_text())['reason'],'first')
 def test_operator_stop_denies_permit(self):
  (m.ROOT/'permit').write_text('allow');m.stop(None,None);self.assertTrue(m.STOP);self.assertEqual((m.ROOT/'permit').read_text().strip(),'deny')
 def test_probes_actual_worker_and_coordinator_data_filesystems(self):
  m.FLEET={'ssh_key':'fixture-key','known_hosts':'fixture-hosts'}
  output='MemTotal: 1000 kB\nMemAvailable: 200 kB\n/dev/root 100 50 50 50% /\n/dev/data 100 80 20 80% /srv/transparent-pir\n'
  calls=[]
  def run(argv,**kwargs):calls.append(argv);return SimpleNamespace(stdout=output,returncode=0)
  with patch.object(m.subprocess,'run',side_effect=run),patch.object(m,'fetch',return_value={}):
   result=m.worker({'id':'worker','upstream':'10.142.0.1:8093','ssh_host':'fixture'})
   self.assertIn('df -P / /srv/transparent-pir',calls[-1][-1])
   self.assertEqual(result['disk_available_fractions'][0],.5)
   self.assertAlmostEqual(result['disk_available_fractions'][1],.2)
   m.worker({'id':'coordinator','local':True})
   self.assertIn('df -P / /srv/transparent-activity /srv/zakura',calls[-1][-1])
if __name__=='__main__':unittest.main()
