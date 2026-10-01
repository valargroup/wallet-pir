"""Focused independent byte/packing and disclosure regression controls."""
import copy
import json
from pathlib import Path
import unittest
import analyze as a
import geometry as g

ROOT = Path(__file__).resolve().parents[3]
EVIDENCE = ROOT / "transparent/evidence/txid-sizing"


def record(size=1):
    return dict(txid_internal="11"*32,height=1,coinbase=False,input_count=1,
        shielded_components=False,fee=0,outputs=[dict(value=0,script="00"*size)])


class AnalysisTests(unittest.TestCase):
    def test_exact_script_roundtrips_and_raw_escape(self):
        templates = ["76a914"+"11"*20+"88ac", "a914"+"22"*20+"87",
            "21"+"02"+"33"*32+"ac", "41"+"04"+"44"*64+"ac",
            "", "6a00", "00"*130, "76a914"+"11"*20+"88ad"]
        for script in templates:
            raw=bytes.fromhex(script)
            self.assertEqual(a.script_decode(a.script_encode(raw)),raw)
        self.assertEqual(a.script_encode(bytes.fromhex(templates[-1]))[0],0)
        with self.assertRaises(ValueError):
            a.script_decode(b"\x00\x80\x00")  # noncanonical zero
        with self.assertRaises(ValueError):
            a.script_decode(b"\x03\x04"+b"\x00"*32)

    def test_shared_metadata_states_and_common_count_escape(self):
        r=record(); exact=a.payload(r)
        r["fee"]="unknown"; unknown=a.payload(r)
        self.assertNotEqual(exact,unknown)
        self.assertEqual(len(exact),len(unknown)+1)
        r["coinbase"]=True
        with self.assertRaises(ValueError): a.payload(r)
        r["input_count"]=0; r["fee"]="not_applicable"
        self.assertEqual(a.payload(r)[1],33)
        r=record(); r["input_count"]=15; r["outputs"]*=15
        compressed=a.payload(r,"common-counts")
        self.assertEqual(compressed[:4],bytes([40,255,15,15]))
        r["outputs"]=[]; r["shielded_components"]=True
        self.assertEqual(len(a.payload(r)),5)

    def test_fragment_boundaries_and_distinct_transactions(self):
        # A one-row choice domain forces a real coincident-choice lookup.
        packed=a.packing([record()],"display-v1",128,directory_rows=1)
        self.assertEqual(packed["per_record"][0]["lookup_queries"],1)
        self.assertEqual(packed["encrypted_requests_per_tx_uniform"],1)
        self.assertEqual(len(a.fragments(4050)),1)
        self.assertEqual(len(a.fragments(4051)),2)
        for size in (1,4050,4051,100000):
            fr=a.fragments(size,True)
            self.assertEqual(sum(n for _,n,_ in fr),size)
            self.assertTrue(all(n+h+6<=4096 for _,n,h in fr))
        rows=[dict(txid="one",lookup=0),dict(txid="one",lookup=0),dict(txid="two",lookup=0)]
        self.assertEqual(a.classes(rows,("lookup",))["class_weighted"]["min"],2)

    def test_five_candidate_intersection_refresh_timing_controls(self):
        c=a.negative_controls()
        for label in ("global_overflow_narrow_lookup","repeated_fragments","new_revision_tail","timing_tail","segment_tail","dummy_and_empty_rows","coincident_lookup_choice_tail"):
            self.assertEqual(c[label]["class_weighted"]["min"],5)
            self.assertEqual(c[label]["classes_below_policy"],{"1000":1,"10000":1})
        self.assertEqual(c["global_lookup_count_cover"]["class_weighted"]["min"],20005)
        self.assertEqual(c["independent_routes_joint"]["class_weighted"]["min"],5)
        for label in ("independent_routes_lookup_marginal","independent_routes_overflow_marginal"):
            self.assertEqual(c[label]["class_weighted"]["min"],10000)

    def test_retained_canonical_codec_and_implemented_packer(self):
        data=json.loads((EVIDENCE/"canonical-sample.json").read_bytes())
        records=data["records"]
        for r in records:
            self.assertEqual(a.payload(r).hex(),r["display_v1_hex"])
        p=a.packing(records,"display-v1",128)
        for key in ("payload_bytes","directory_segments","page_segments","occupied_directory_rows","occupied_page_rows","occupied_bytes"):
            self.assertEqual(p[key],data["implemented_packing"][key],key)
        damaged=copy.deepcopy(data);damaged["records"][0]["outputs"][0]["value"]+=1
        with self.assertRaisesRegex(ValueError,"byte mismatch"): a.analyze(damaged)
        duplicated=copy.deepcopy(data);duplicated["records"].append(duplicated["records"][0])
        with self.assertRaisesRegex(ValueError,"duplicate"): a.analyze(duplicated)

    def test_independent_confirmed_facts_only_validate_overlap(self):
        # Existing demo oracle is a cross-check, never a population estimator.
        fixture=json.loads((ROOT/"transparent/services/transparent-shard-server/examples/fixtures/txid-confirmed.json").read_bytes())
        expected={r["txid"]:r for b in fixture["blocks"] for r in b["transactions"]}
        data=json.loads((EVIDENCE/"canonical-sample.json").read_bytes())
        overlap=0
        for r in data["records"]:
            e=expected.get(r["txid_internal"])
            if e is None: continue
            overlap+=1
            for key in ("coinbase","input_count","shielded_components","fee","outputs"):
                self.assertEqual(r[key],e[key],key)
        self.assertGreater(overlap,0)

    def test_cover_geometry_and_no_minimum_claim(self):
        plain=g.scenario(17000000,.9,80,8000,4,1,8192,32768)
        padded=g.scenario(17000000,.9,80,8000,4,1,8192,32768,cover=3)
        self.assertAlmostEqual(plain["encrypted_requests_per_tx"],2.3)
        self.assertEqual(padded["encrypted_requests_per_tx"],5)
        self.assertGreater(padded["segment_evaluations_per_tx"],plain["segment_evaluations_per_tx"])
        self.assertIsNone(plain["qualified_minimum_population"])


if __name__ == "__main__": unittest.main()
