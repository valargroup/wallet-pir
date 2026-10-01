"""Probability-design, raw/decoded-oracle and checkpoint negative controls."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
import census
import survey
import analyze as a

class SurveyTests(unittest.TestCase):
    def test_disjoint_reproducible_known_probability_selection(self):
        p=census.plan();self.assertEqual(p,census.plan())
        self.assertEqual(sum(s["N"] for s in p["strata"]),census.ANCHOR_HEIGHT+1)
        heights=[h for s in p["strata"] for h in s["heights"]]
        self.assertEqual(len(heights),2304);self.assertEqual(len(set(heights)),len(heights))
        for s in p["strata"]:
            self.assertTrue(all(s["lo"]<=h<s["hi_exclusive"] for h in s["heights"]))
            self.assertEqual(s["inclusion_probability"],s["n"]/s["N"])

    def test_stratified_cluster_total_ratio_and_finite_population(self):
        design={"strata":[dict(name="a",n=2,N=4),dict(name="b",n=2,N=10)]}
        xs={"a":[1,3],"b":[0,2]};ys={"a":[2,4],"b":[2,2]}
        e=survey.estimate(xs,design)
        self.assertEqual(e["estimate"],18)
        self.assertAlmostEqual(e["standard_error"]**2,88)
        ratio=survey.estimate(xs,design,ys);self.assertAlmostEqual(ratio["estimate"],18/32)
        moments={k:(sum(v),sum(x*x for x in v)) for k,v in xs.items()}
        for k in ("estimate","standard_error","ci95","sample_sum"):
            self.assertEqual(e[k],survey.estimate_moments(moments,design)[k])
        complete={"strata":[dict(name="a",n=2,N=2),dict(name="b",n=2,N=2)]}
        self.assertEqual(survey.estimate(xs,complete)["standard_error"],0)
        with self.assertRaisesRegex(ValueError,"incomplete"):
            survey.estimate({"a":[1],"b":[0,2]},design)
        self.assertIsNone(survey.estimate({"a":[0,0],"b":[0,0]},design)["ci95"])

    def test_strict_more_than_eighty_and_cdf_interval(self):
        self.assertEqual(survey.hist_quantile({10:8,20:1,30:1},.8,strict=True),20)
        self.assertEqual(survey.hist_quantile({10:8,20:1,30:1},.8),10)
        design={"strata":[dict(name="a",n=2,N=4)]}
        blocks={"a":[dict(hist={"display-v1":{"all":[[10,8],[20,1],[30,1]]}})]*2}
        f=survey.frontier_intervals(blocks,design,"display-v1")
        self.assertEqual(f["0.8"]["estimated_bytes"],20)
        self.assertEqual(f["0.8"]["approximate_ci95_bytes"],[20,20])
        self.assertIsNone(f["0.99"]["approximate_ci95_bytes"][1])

    def test_rpc_oracle_detects_scripts_amounts_eligibility_and_identity(self):
        data=json.loads((Path(__file__).resolve().parents[3]/"transparent/evidence/txid-sizing/canonical-sample.json").read_text())
        r=copy.deepcopy(next(r for r in data["records"] if r["coinbase"]))
        r["transaction_index"]=0
        tx=dict(txid=bytes.fromhex(r["txid_internal"])[::-1].hex(),vin=[dict(coinbase="00")],
            vout=[dict(valueZat=o["value"],value=o["value"]/100000000,scriptPubKey=dict(hex=o["script"])) for o in r["outputs"]])
        frame=dict(height=r["height"],hash="block",stratum="a",parents=[],rpc_block=dict(nTx=1,tx=[tx]))
        extracted=dict(height=r["height"],hash="block",transactions=1,shielded_only=0,records=[r])
        summary=survey.block_summary(frame,extracted)
        self.assertEqual(summary["totals"]["eligible"],1)
        for field in ("valueZat","scriptPubKey"):
            bad=copy.deepcopy(frame)
            if field=="valueZat":bad["rpc_block"]["tx"][0]["vout"][0][field]+=1
            else:bad["rpc_block"]["tx"][0]["vout"][0][field]["hex"]="00"
            with self.assertRaisesRegex(ValueError,"exact output"):
                survey.block_summary(bad,extracted)
        bad=copy.deepcopy(extracted);bad["records"].append(r)
        with self.assertRaisesRegex(ValueError,"duplicate"):
            survey.block_summary(frame,bad)
        bad=copy.deepcopy(extracted);bad["records"]=[]
        with self.assertRaisesRegex(ValueError,"eligibility"):
            survey.block_summary(frame,bad)


    def test_transparent_fee_oracle_is_independent_of_encoder(self):
        r=dict(txid_internal="11"*32,height=1,transaction_index=0,coinbase=False,input_count=1,
            shielded_components=False,fee=5,outputs=[dict(value=15,script="00")])
        r["display_v1_hex"]=a.payload(r).hex()
        tx=dict(txid="11"*32,vin=[dict(txid="22"*32,vout=0)],vout=[dict(valueZat=15,value=.00000015,scriptPubKey=dict(hex="00"))])
        parent=dict(txid="22"*32,vout=[dict(valueZat=20,value=.00000020)])
        frame=dict(height=1,hash="block",stratum="a",parents=[parent],rpc_block=dict(nTx=1,tx=[tx]))
        extracted=dict(height=1,hash="block",transactions=1,shielded_only=0,records=[r])
        self.assertEqual(survey.block_summary(frame,extracted)["totals"]["transparent_fee_oracles"],1)
        bad=copy.deepcopy(extracted);bad["records"][0]["fee"]=6
        bad["records"][0]["display_v1_hex"]=a.payload(bad["records"][0]).hex()
        with self.assertRaisesRegex(ValueError,"fee oracle"):
            survey.block_summary(frame,bad)

    def test_atomic_checkpoint_and_membership_rejection(self):
        with tempfile.TemporaryDirectory() as temp:
            p=Path(temp)/"receipt.json";census.atomic_json(p,{"public":1})
            self.assertEqual(json.loads(p.read_text()),{"public":1})
            self.assertFalse(p.with_suffix(".json.tmp").exists())
        design={"strata":[dict(name="a",n=2,N=10,heights=[1,2])]}
        with self.assertRaisesRegex(ValueError,"membership"):
            survey.group_blocks([dict(stratum="a",height=1)]*2,design)

if __name__=="__main__":unittest.main()
