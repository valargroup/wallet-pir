"""Prometheus parsing, worker samples, quantiles and counter-reset windows."""
import math
import unittest

import scaler_fixtures  # noqa: F401 - puts transparent/ops on the path
from scaler import metrics as M

BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 600.0)


def render_worker(queries=100, rejections=2, overloads=1, deadline=3, busy=5_000_000, slots=2, start=1000.0,
                  success=None, warm=10, labels='worker_id="r1",role="recent-replica"'):
    """Text in the shape transparent-shard-server's metrics.rs renders."""
    success = success or {0.05: 60, 0.5: 30, 2.0: 10}
    lines = ['pir_http_observation_version 1', f'pir_http_process_start_time_seconds {start}']

    def line(name, kind, value):
        lines.extend([f'# HELP {name} help', f'# TYPE {name} {kind}', f'{name}{{{labels}}} {value}'])
    line('transparent_shard_queries_total', 'counter', queries)
    line('transparent_shard_queue_rejections_total', 'counter', rejections)
    line('transparent_shard_overloads_total', 'counter', overloads)
    line('transparent_shard_deadline_exceeded_total', 'counter', deadline)
    line('transparent_shard_query_slots', 'gauge', slots)
    line('transparent_shard_query_slot_busy_microseconds_total', 'counter', busy)
    line('transparent_shard_prewarm_operations_total', 'counter', warm)
    line('transparent_shard_cache_resident_bytes', 'gauge', 4_000_000_000)
    line('transparent_shard_cache_budget_bytes', 'gauge', 5_368_709_120)
    line('transparent_shard_warm_runtimes', 'gauge', warm)
    line('transparent_shard_cgroup_memory_current_bytes', 'gauge', 6_000_000_000)
    line('transparent_shard_cgroup_memory_max_bytes', 'gauge', 7_516_192_768)
    for outcome, counts in (('success', success), ('error', {5.0: 4}), ('cancelled', {})):
        if outcome == 'success':
            lines += ['# HELP transparent_shard_query_seconds h', '# TYPE transparent_shard_query_seconds histogram']
        cumulative = 0
        for le in BUCKETS:
            cumulative += counts.get(le, 0)
            lines.append(f'transparent_shard_query_seconds_bucket{{{labels},outcome="{outcome}",le="{le}"}} {cumulative}')
        lines.append(f'transparent_shard_query_seconds_bucket{{{labels},outcome="{outcome}",le="+Inf"}} {cumulative}')
        lines.append(f'transparent_shard_query_seconds_sum{{{labels},outcome="{outcome}"}} 12.5')
        lines.append(f'transparent_shard_query_seconds_count{{{labels},outcome="{outcome}"}} {cumulative}')
    return '\n'.join(lines) + '\n'


def sample(t, queries=0.0, start=1.0, latency=None, **counters):
    base = {'queries': queries, 'queue_rejections': 0.0, 'overloads': 0.0, 'deadline_exceeded': 0.0,
            'slot_busy_us': 0.0, 'prewarm_ops': 0.0}
    base.update(counters)
    buckets = latency if latency is not None else tuple((le, queries) for le in BUCKETS + (math.inf,))
    return M.Sample(t=t, start=start, counters=base, gauges={'slots': 2.0}, latency=buckets, complete=True)


class ParseTests(unittest.TestCase):
    def test_types_labels_escapes_and_special_values(self):
        text = ('# HELP a counter with help\n# TYPE a counter\n\n'
                'a{x="1",y="q\\"uote\\\\back\\nnl"} 3\n'
                'a{x="2",} 4 1700000000000\n'
                'b 5e3\n'
                'c{} +Inf\nd -Inf\ne NaN\n')
        scrape = M.parse(text)
        self.assertEqual(scrape.types['a'], 'counter')
        self.assertEqual(scrape.value('a'), 7.0)
        self.assertEqual(scrape.value('a', x='1'), 3.0)
        self.assertEqual(scrape.samples['a'][0][0]['y'], 'q"uote\\back\nnl')
        self.assertEqual(scrape.value('b'), 5000.0)
        self.assertEqual(scrape.value('c'), math.inf)
        self.assertEqual(scrape.value('d'), -math.inf)
        self.assertTrue(math.isnan(scrape.value('e')))
        self.assertIsNone(scrape.value('missing'))
        self.assertIsNone(scrape.value('a', x='3'))

    def test_malformed_input_raises(self):
        for bad in ('a{x="1" 3\n', 'a 1 2 3\n', 'a{x=1} 2\n', 'a\n', 'a{x="1"}{y="2"} 3\n', '1a 2\n',
                    'a{x="1"y="2"} 3\n', 'a notanumber\n'):
            with self.assertRaises(M.ParseError, msg=bad):
                M.parse(bad)

    def test_histogram_selection_and_validation(self):
        scrape = M.parse(render_worker())
        histogram = scrape.histogram('transparent_shard_query_seconds', outcome='success')
        self.assertEqual(histogram.count, 100)
        self.assertEqual(histogram.buckets[-1], (math.inf, 100.0))
        self.assertEqual(dict(histogram.buckets)[0.5], 90.0)
        errors = scrape.histogram('transparent_shard_query_seconds', outcome='error')
        self.assertEqual(errors.count, 4)
        self.assertIsNone(scrape.histogram('transparent_shard_query_seconds', outcome='nope'))
        with self.assertRaises(M.ParseError):
            M.parse('h_bucket{le="1"} 2\n').histogram('h')
        with self.assertRaises(M.ParseError):
            M.parse('h_bucket{le="1"} 2\nh_bucket{le="+Inf"} 1\n').histogram('h')


class WorkerSampleTests(unittest.TestCase):
    def test_worker_text_yields_a_complete_sample(self):
        value = M.worker_sample(M.parse(render_worker()), 50.0)
        self.assertTrue(value.complete, value.missing)
        self.assertEqual(value.start, 1000.0)
        self.assertEqual(value.counters['queries'], 100)
        self.assertEqual(value.counters['slot_busy_us'], 5_000_000)
        self.assertEqual(value.gauges['slots'], 2)
        self.assertEqual(value.gauges['cache_budget_bytes'], 5_368_709_120)
        self.assertEqual(value.latency[-1], (math.inf, 100.0))

    def test_falls_back_to_the_shard_start_time(self):
        text = render_worker().replace('pir_http_process_start_time_seconds 1000.0\n', '')
        text += 'transparent_shard_process_start_time_seconds{worker_id="r1"} 77\n'
        self.assertEqual(M.worker_sample(M.parse(text), 1.0).start, 77.0)

    def test_missing_or_negative_series_are_incomplete(self):
        text = '\n'.join(l for l in render_worker().splitlines() if 'queue_rejections' not in l)
        value = M.worker_sample(M.parse(text), 1.0)
        self.assertFalse(value.complete)
        self.assertIn('transparent_shard_queue_rejections_total', value.missing)
        self.assertIsNone(value.counters['queue_rejections'])
        value = M.worker_sample(M.parse(render_worker(queries=-1)), 1.0)
        self.assertFalse(value.complete)
        text = '\n'.join(l for l in render_worker().splitlines() if 'query_seconds' not in l)
        self.assertIn(M.LATENCY, M.worker_sample(M.parse(text), 1.0).missing)


class QuantileTests(unittest.TestCase):
    def test_interpolates_inside_the_bucket(self):
        buckets = [(0.1, 50.0), (0.5, 90.0), (1.0, 100.0), (math.inf, 100.0)]
        self.assertAlmostEqual(M.quantile(0.5, buckets), 0.1)
        self.assertAlmostEqual(M.quantile(0.7, buckets), 0.1 + 0.4 * 20 / 40)
        self.assertAlmostEqual(M.quantile(0.99, buckets), 0.5 + 0.5 * 9 / 10)
        self.assertAlmostEqual(M.quantile(0.25, buckets), 0.05)

    def test_inf_bucket_reports_highest_finite_bound(self):
        self.assertEqual(M.quantile(0.99, [(0.1, 1.0), (1.0, 2.0), (math.inf, 10.0)]), 1.0)

    def test_empty_is_unknown(self):
        self.assertIsNone(M.quantile(0.99, []))
        self.assertIsNone(M.quantile(0.99, [(0.1, 0.0), (math.inf, 0.0)]))

    def test_bucket_delta_and_merge(self):
        new = ((0.1, 10.0), (math.inf, 12.0))
        old = ((0.1, 4.0), (math.inf, 5.0))
        self.assertEqual(M.bucket_delta(new, old), [(0.1, 6.0), (math.inf, 7.0)])
        self.assertIsNone(M.bucket_delta(new, ((0.2, 1.0), (math.inf, 1.0))))
        self.assertIsNone(M.bucket_delta(new, None))
        self.assertEqual(M.merge_buckets([new, old]), [(0.1, 14.0), (math.inf, 17.0)])
        self.assertIsNone(M.merge_buckets([new, None]))
        self.assertIsNone(M.merge_buckets([new, ((0.2, 1.0), (math.inf, 1.0))]))

    def test_histogram_delta_gives_the_window_distribution(self):
        # 1000 fast queries before the window, then 90 fast and 10 slow inside it.
        before = M.worker_sample(M.parse(render_worker(queries=1000, success={0.05: 1000})), 0.0)
        after = M.worker_sample(M.parse(render_worker(queries=1100, success={0.05: 1090, 5.0: 10})), 300.0)
        delta = M.bucket_delta(after.latency, before.latency)
        self.assertEqual(delta[-1][1], 100.0)
        self.assertAlmostEqual(M.quantile(0.5, delta), 0.025 + 0.025 * 50 / 90)
        self.assertGreater(M.quantile(0.99, delta), 2.0)


class HistoryTests(unittest.TestCase):
    def test_window_spans_back_at_least_the_requested_seconds(self):
        history = M.History(keep_seconds=900)
        for i in range(30):
            history.add(sample(15.0 * i, queries=10.0 * i))
        window = history.window(300, 60)
        self.assertEqual(window.span, 300.0)
        self.assertAlmostEqual(window.rate('queries'), 10 / 15)
        self.assertEqual(len(window.intervals()), 20)
        self.assertEqual(window.intervals()[0][0], window.first.t + 15)

    def test_short_history_is_unknown_until_min_span(self):
        history = M.History()
        history.add(sample(0.0))
        self.assertIsNone(history.window(300, 60))
        history.add(sample(45.0, queries=4))
        self.assertIsNone(history.window(300, 60))
        history.add(sample(60.0, queries=5))
        self.assertEqual(history.window(300, 60).span, 60.0)

    def test_start_time_change_drops_the_window(self):
        history = M.History()
        for i in range(10):
            self.assertFalse(history.add(sample(15.0 * i, queries=100.0 + i, start=1.0)))
        self.assertTrue(history.add(sample(150.0, queries=105.0, start=2.0)))
        self.assertEqual(len(history.samples), 1)
        self.assertEqual(history.resets, 1)
        self.assertIsNone(history.window(300, 60))

    def test_counter_decrease_drops_the_window(self):
        history = M.History()
        history.add(sample(0.0, queries=100.0, start=None))
        history.add(sample(15.0, queries=110.0, start=None))
        self.assertTrue(history.add(sample(30.0, queries=3.0, start=None)))
        self.assertEqual([s.t for s in history.samples], [30.0])

    def test_bucket_decrease_drops_the_window(self):
        history = M.History()
        history.add(sample(0.0, queries=10.0))
        low = tuple((le, 5.0 if le == 0.001 else 10.0) for le in BUCKETS + (math.inf,))
        high = tuple((le, 4.0 if le == 0.001 else 11.0) for le in BUCKETS + (math.inf,))
        history.add(sample(15.0, queries=10.0, latency=low))
        self.assertTrue(history.add(sample(30.0, queries=11.0, latency=high)))

    def test_incomplete_and_out_of_order_samples_are_not_kept(self):
        history = M.History()
        history.add(sample(10.0))
        incomplete = M.Sample(t=20.0, start=1.0, counters={}, gauges={}, latency=None, complete=False,
                              missing=('x',))
        self.assertFalse(history.add(incomplete))
        self.assertIn('incomplete metrics: x', history.last_error)
        history.add(sample(5.0))
        self.assertEqual([s.t for s in history.samples], [10.0])

    def test_old_samples_are_pruned(self):
        history = M.History(keep_seconds=100)
        for i in range(40):
            history.add(sample(15.0 * i))
        self.assertLessEqual(history.samples[-1].t - history.samples[1].t, 100)


if __name__ == '__main__':
    unittest.main()
