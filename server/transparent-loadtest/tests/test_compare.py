import importlib.util
from pathlib import Path
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('compare',Path(__file__).parents[1]/'compare.py')
compare=importlib.util.module_from_spec(spec);spec.loader.exec_module(compare)

class ComparisonTests(unittest.TestCase):
    def test_failed_wallets_do_not_generate_savings(self):
        a={'users':[{'sample_index':0,'outcome':'exact','seconds':2,'http_totals':{'bytes_down':10}}]}
        b={'users':[{'sample_index':0,'outcome':'failed','seconds':1,'http_totals':{'bytes_down':20}}]}
        row=compare.pair_rows(a,b,[90])[0]
        self.assertIsNone(row['standalone_download_savings'])
        self.assertIsNone(row['latency_ratio_blocks_over_pir'])
        b['users'][0].update(outcome='exact',encoded_dataset_bytes={'combined':100,'shielded':25})
        row=compare.pair_rows(a,b,[90])[0]
        self.assertEqual(row['incremental_block_bytes'],75)
        self.assertEqual(row['standalone_download_savings'],.5)
        self.assertEqual(row['source_sample_index'],90)

    def test_empty_and_partial_reports_are_readable(self):
        with tempfile.TemporaryDirectory() as directory:
            out=Path(directory)
            value=compare.report(out,{},'preparing',['<failure>'])
            self.assertFalse(value['success'])
            text=(out/'report.html').read_text()
            self.assertIn('preparing',text)
            self.assertIn('&lt;failure&gt;',text)
            self.assertIn('Full comparison JSON',text)

    def test_empty_successful_children_are_not_a_successful_comparison(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            out=Path(directory)
            for method in ['pir','blocks']:
                child=out/method;child.mkdir()
                (child/'report.json').write_text(json.dumps({'success':True,'users':[]}))
            result=compare.report(out,{'source_sample_indices':[1], 'runs':{'mixed':{'pir':'pir','blocks':'blocks'}}},'complete',[])
            self.assertFalse(result['suites']['mixed']['success'])
            self.assertFalse(result['success'])

    def test_server_restart_suppresses_cpu_delta(self):
        def point(at, cpu, start):
            return {'target':'blocks','at':at,'text':f'process_cpu_seconds_total {cpu}\nprocess_start_time_seconds {start}\nprocess_resident_memory_bytes 100\n'}
        clean=compare.server_summary({'metrics':[point(1,10,0),point(2,12,0)]})[0]
        self.assertEqual(clean['cpu_seconds'],2)
        restarted=compare.server_summary({'metrics':[point(1,10,0),point(2,12,1)]})[0]
        self.assertIsNone(restarted['cpu_seconds'])
        self.assertEqual(restarted['peak_rss_bytes'],100)

    def test_unexpected_wallet_index_does_not_break_report(self):
        data={'users':[{'sample_index':100,'outcome':'exact','seconds':1,'http_totals':{'bytes_down':10}}]}
        row=compare.pair_rows(data,data,[1])[0]
        self.assertIsNone(row['source_sample_index'])
        self.assertIsNone(row['standalone_download_savings'])


class ControllerTests(unittest.TestCase):
    def test_controller_freezes_inputs_and_resumes_successful_children(self):
        import json
        import os
        import subprocess
        import sys
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            sample={'genesis_hash':'00'*32,'start_height':0,'anchor_height':1,'anchor_hash':'01'*32,'clients':[{'class':'test','scripts':['aa'],'required_from':0,'journal_events':0,'expected_digest':'empty'}]}
            (root/'sample.json').write_text(json.dumps(sample))
            (root/'fresh.json').write_text(json.dumps(sample))
            config={'schema':'transparent-scenario-v1','name':'fixture','mode':'wave','sample':'sample.json','shard_url':'http://unused','profiles':{'test':1}}
            (root/'scenario.json').write_text(json.dumps(config))
            executable=root/'worker'
            executable.write_text('#!'+sys.executable+'''
import json,sys
from pathlib import Path
args=sys.argv
config=json.loads(Path(args[args.index('--scenario')+1]).read_text())
sample=json.loads((Path(args[args.index('--scenario')+1]).parent/ config['sample']).read_text())
if '--selected-sample' in args:
 sample['source_sample_indices']=[90]
 Path(args[args.index('--selected-sample')+1]).write_text(json.dumps(sample))
else:
 out=Path(args[args.index('--out-dir')+1]);out.mkdir();(out/'seeds').mkdir()
 report={'success':True,'users':[],'metrics':[]}
 if '--prepare-only' not in args:
  report['users']=[{'sample_index':0,'profile':'test','outcome':'exact','seconds':1,'http_totals':{'bytes_down':100,'bytes_up':0},'encoded_dataset_bytes':{'combined':100,'shielded':50}}]
 (out/'report.json').write_text(json.dumps(report));(out/'report.html').write_text('fixture report')
''')
            executable.chmod(0o755)
            class Handler(BaseHTTPRequestHandler):
                def do_GET(self):
                    value={'dataset_id':'ab'*32} if self.path=='/identity' else dict(sample,complete=True)
                    body=json.dumps(value).encode();self.send_response(200);self.end_headers();self.wfile.write(body)
                def log_message(self,*args): pass
            server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            try:
                args=[sys.executable,str(Path(__file__).parents[1]/'compare.py'),'--scenario',str(root/'scenario.json'),'--fresh-sample',str(root/'fresh.json'),'--block-url',f'http://127.0.0.1:{server.server_port}','--out-dir',str(root/'result'),'--binary',str(executable)]
                first=subprocess.run(args,capture_output=True,text=True,timeout=120)
                self.assertEqual(first.returncode,0,first.stderr)
                result=json.loads((root/'result/report.json').read_text())
                self.assertTrue(result['success'])
                self.assertEqual(result['suites']['fresh']['rows'][0]['source_sample_index'],90)
                before=(root/'result/mixed/pir/report.json').stat().st_mtime_ns
                second=subprocess.run(args,capture_output=True,text=True,timeout=30)
                self.assertEqual(second.returncode,0,second.stderr)
                self.assertEqual(before,(root/'result/mixed/pir/report.json').stat().st_mtime_ns)
                (root/'result/selected-sample.json').write_text('{}')
                changed=subprocess.run(args,capture_output=True,text=True,timeout=30)
                self.assertNotEqual(changed.returncode,0)
                self.assertIn('frozen sample changed',changed.stderr)
            finally:
                server.shutdown();server.server_close();thread.join()

if __name__=='__main__':unittest.main()
