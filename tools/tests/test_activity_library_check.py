"""Independent oracle boundary cases; all values are synthetic public fixtures."""
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from decimal import Decimal

SCRIPT=Path(__file__).resolve().parents[2]/'transparent/ops/scripts/activity-library-check.py'
spec=importlib.util.spec_from_file_location('activity_library_check',SCRIPT)
oracle=importlib.util.module_from_spec(spec);spec.loader.exec_module(oracle)

def transaction(txid,vin,outputs,**pools):
    return dict(txid=txid,hex=txid,vin=vin,vout=[{'n':n,'value':Decimal(v),'scriptPubKey':{'hex':'51'}} for n,v in enumerate(outputs)],**pools)

class LibraryOracleTests(unittest.TestCase):
    def test_exact_fee_counts_all_inputs_and_all_shielded_balances(self):
        parents={'parent':transaction('parent',[{'coinbase':'00'}],['1.00000001','2.00000002'])}
        tx=transaction('mixed',[{'txid':'parent','vout':0},{'txid':'parent','vout':1}],['2.50000000'],
            vjoinsplit=[{'vpub_new':Decimal('.2'),'vpub_old':Decimal('.1')}],
            vShieldedSpend=[{}],valueBalance=Decimal('-.3'),
            orchard={'actions':[{}],'valueBalance':Decimal('.4')},
            ironwood={'actions':[{}],'valueBalance':Decimal('-.2')})
        self.assertEqual(oracle.metadata(tx,parents),{'fee':{'state':'exact','value':50000003},'input_count':2,'shielded':True})
        tx['vin'][0]['vout']=5
        with self.assertRaises((ValueError,IndexError)):oracle.metadata(tx,parents)

    def test_coinbase_na_zero_fee_and_fractional_amount_are_distinct(self):
        coinbase=transaction('coinbase',[{'coinbase':'00'}],['1'])
        self.assertEqual(oracle.metadata(coinbase,{})['fee'],{'state':'not-applicable'})
        parent=transaction('parent',[{'coinbase':'00'}],['1'])
        tx=transaction('zero',[{'txid':'parent','vout':0}],['1'])
        self.assertEqual(oracle.metadata(tx,{'parent':parent})['fee'],{'state':'exact','value':0})
        tx['vout'][0]['value']=Decimal('1.000000001')
        with self.assertRaises(ValueError):oracle.metadata(tx,{'parent':parent})

    def test_global_batch_failure_is_retained_and_raw_hash_corruption_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'bodies').mkdir()
            request=[{'id':1,'method':'getrawtransaction','params':['tx',1]}]
            response={'id':None,'result':None,'error':{'code':-32011,'message':'too large'}}
            digests=[]
            for body in [request,response]:
                raw=json.dumps(body).encode();digest=hashlib.sha256(raw).hexdigest();digests.append(digest)
                (root/'bodies'/(digest+'.json.gz')).write_bytes(gzip.compress(raw))
            (root/'attempts.jsonl').write_text(json.dumps({'status':200,'request_sha256':digests[0],'response_sha256':digests[1]})+'\n')
            blocks,transactions,failures,attempts=oracle.read_capture(root)
            self.assertEqual((blocks,transactions,attempts),({},{},1))
            self.assertEqual(failures,[{'attempt':1,'rpc_batch_error':-32011}])
            (root/'bodies'/(digests[0]+'.json.gz')).write_bytes(gzip.compress(b'[]'))
            with self.assertRaises(ValueError):oracle.read_capture(root)

if __name__=='__main__': unittest.main()
