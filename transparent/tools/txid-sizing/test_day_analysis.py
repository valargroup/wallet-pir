import copy
import unittest
import analyze as a
import day_analysis as d
import day_sample
import survey

class DaySizingTests(unittest.TestCase):
    def record(self, fee='unknown', coinbase=False):
        return dict(fee=fee,coinbase=coinbase,input_count=0 if coinbase else 1,
            shielded_components=True,outputs=[dict(value=0,script='6a00')])

    def test_unknown_fee_bounds_contain_all_canonical_uleb_lengths(self):
        r=self.record()
        for codec in d.CODECS:
            lo,hi=d.fee_size_bounds(r,codec)
            self.assertEqual(hi-lo,7)
            for fee in (0,127,128,16383,16384,a.MAX_MONEY):
                exact=dict(r,fee=fee)
                size=len(a.payload(exact,codec))
                self.assertLessEqual(lo,size);self.assertLessEqual(size,hi)
            self.assertEqual(lo,len(a.payload(dict(r,fee=0),codec)))
        self.assertEqual(d.MAX_FEE_BYTES,8)

    def test_fee_states_and_input_only_are_distinct(self):
        r=self.record();r['outputs']=[]
        self.assertNotEqual(a.payload(r),a.payload(dict(r,fee=0)))
        self.assertEqual(d.fee_size_bounds(dict(r,fee=0)),(5,5))
        cb=self.record('not_applicable',True)
        self.assertEqual(d.fee_size_bounds(cb),(len(a.payload(cb)),)*2)
        self.assertEqual(a.script_decode(a.script_encode(bytes.fromhex('6a00'))),bytes.fromhex('6a00'))

    def test_probability_design_is_disjoint_reproducible_and_whole_range(self):
        design=day_sample.plan(8)
        self.assertEqual(design,day_sample.plan(8));self.assertEqual(design['strata'][0]['lo'],0)
        self.assertEqual(design['strata'][-1]['hi_exclusive'],design['anchor_height']+1)
        last=0;seen=set()
        for s in design['strata']:
            self.assertEqual(s['lo'],last);last=s['hi_exclusive']
            self.assertEqual(s['inclusion_probability'],s['n']/s['N'])
            self.assertEqual(len(set(s['heights'])),s['n'])
            self.assertTrue(all(s['lo']<=h<last for h in s['heights']))
            self.assertFalse(seen.intersection(s['heights']));seen.update(s['heights'])

    def test_block_cluster_ratio_and_finite_population_variance(self):
        design={'strata':[{'name':'s','N':10,'n':2}]}
        e=survey.estimate({'s':[1,3]},design,{'s':[1,9]},proportion=True)
        self.assertAlmostEqual(e['estimate'],.4)
        self.assertGreater(e['standard_error'],.1)
        complete=copy.deepcopy(design);complete['strata'][0]['N']=2
        self.assertEqual(survey.estimate({'s':[1,3]},complete,{'s':[1,9]},proportion=True)['standard_error'],0)

    def test_inline_boundary_packing_is_not_monotone(self):
        b={'size_pairs':{'display-v1':[[128,135,1]]}}
        self.assertEqual(d.packing_bound(b,'display-v1',128,'directory_entry_bytes_min',False),48)
        self.assertEqual(d.packing_bound(b,'display-v1',128,'directory_entry_bytes_max',True),176)
        b={'size_pairs':{'display-v1':[[4050,4057,1]]}}
        self.assertEqual(d.packing_bound(b,'display-v1',128,'fragments',False),1)
        self.assertEqual(d.packing_bound(b,'display-v1',128,'fragments',True),2)

    def test_fee_ambiguity_count_cover_and_excess_requests(self):
        shape=[0,0,1,128,135];geo=('hash','global',4,1)
        self.assertEqual(len(d.transcript_keys(shape,10,128,geo,0)),2)
        self.assertEqual(len(d.transcript_keys(shape,10,128,geo,3)),1)
        self.assertEqual(len(d.transcript_keys([0,0,1,12150,12157],10,128,geo,3)),2)
        # Global overflow preserves both distinct observable lookup buckets.
        self.assertNotEqual(d.transcript_keys([0,0,2,200,200],10,128,geo),d.transcript_keys([1,0,2,200,200],10,128,geo))

    def test_five_real_candidates_not_fragments_or_padding(self):
        # Synthetic complete control; no real-population minimum claim.
        design={'strata':[{'name':'control','N':1,'n':1}]}
        shapes=[[0,0,2,s,s] for s in (200,300,4051,5000,9000)]
        report=d.route_report({'design':design,'blocks':[{'height':10,'stratum':'control','shapes':shapes}]},[128])
        c=report['128:global/global:1/1:cover3']
        self.assertEqual(c['sample_distinct_guaranteed_min'],5)
        self.assertEqual(c['observed_possible_classes'],1)
        self.assertEqual(c['smallest_classes'][0]['guaranteed']['estimate'],5)
        self.assertIsNone(c['qualified_population_minimum'])
        self.assertGreater(report['128:global/global:1/1:cover0']['observed_possible_classes'],1)

if __name__=='__main__':unittest.main()
