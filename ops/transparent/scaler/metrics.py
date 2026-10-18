"""Worker metrics for the recent-tier scaler: parse, scrape, window.

The scaler reads each recent replica's Prometheus text directly from its
upstream (`http://<upstream>/metrics`, open to the coordinator only). This
module owns three things and nothing else:

* `parse`: the Prometheus text exposition format (counters, gauges and
  histograms, with labels) into a `Scrape`.
* `worker_sample`: the few series the scaler uses, extracted from one scrape
  into a flat `Sample`. A scrape that lacks a required series yields a sample
  marked incomplete, never zeros.
* `History`: one worker's samples over time, with counter-reset detection. A
  process restart (a changed start time) or any counter that decreased drops
  every earlier sample, so a window never spans two processes. Rates over a
  window shorter than the minimum span are unknown (`None`), never zero.

Stdlib only; no I/O except `fetch`.
"""
from __future__ import annotations

import math
import re
import urllib.request
from dataclasses import dataclass, field

MAX_SCRAPE_BYTES = 4 * 1024 * 1024


class ParseError(ValueError):
    """The text is not valid Prometheus exposition format."""


_NAME = r'[A-Za-z_:][A-Za-z0-9_:]*'
_LINE = re.compile(r'^(' + _NAME + r')(\{.*\})?\s+(\S+)(?:\s+(-?\d+))?\s*$')
_LABEL = re.compile(r'\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"((?:[^"\\]|\\.)*)"\s*(,|$)')


def _unescape(value):
    out, i = [], 0
    while i < len(value):
        c = value[i]
        if c == '\\' and i + 1 < len(value):
            nxt = value[i + 1]
            out.append({'n': '\n', '\\': '\\', '"': '"'}.get(nxt, '\\' + nxt))
            i += 2
            continue
        out.append(c)
        i += 1
    return ''.join(out)


def _labels(text):
    if text is None:
        return {}
    body = text[1:-1].strip()
    labels = {}
    pos = 0
    while pos < len(body):
        match = _LABEL.match(body, pos)
        if not match:
            raise ParseError('malformed labels: ' + text[:200])
        labels[match.group(1)] = _unescape(match.group(2))
        pos = match.end()
        if match.group(3) == '' and pos < len(body):
            raise ParseError('malformed labels: ' + text[:200])
    return labels


def _number(text):
    lowered = text.lower()
    if lowered in ('+inf', 'inf'):
        return math.inf
    if lowered == '-inf':
        return -math.inf
    if lowered == 'nan':
        return math.nan
    try:
        return float(text)
    except ValueError:
        raise ParseError('malformed value: ' + text[:100]) from None


@dataclass
class Histogram:
    """Cumulative buckets `[(le, count)]` sorted by `le`, ending at +Inf."""
    buckets: list
    count: float
    sum: float


@dataclass
class Scrape:
    """Parsed exposition: metric types and every sample by series name."""
    types: dict = field(default_factory=dict)
    samples: dict = field(default_factory=dict)

    def matching(self, name, labels):
        for sample_labels, value in self.samples.get(name, ()):
            if all(sample_labels.get(k) == v for k, v in labels.items()):
                yield sample_labels, value

    def value(self, name, **labels):
        """Sum of every sample of `name` whose labels include `labels`, or None."""
        found = [value for _, value in self.matching(name, labels)]
        if not found:
            return None
        return math.fsum(found)

    def histogram(self, name, **labels):
        """The histogram `name` restricted to `labels`, summed over the rest."""
        buckets = {}
        for sample_labels, value in self.matching(name + '_bucket', labels):
            if 'le' not in sample_labels:
                raise ParseError(name + '_bucket without le')
            le = _number(sample_labels['le'])
            buckets[le] = buckets.get(le, 0.0) + value
        if not buckets:
            return None
        if math.inf not in buckets:
            raise ParseError(name + ' has no +Inf bucket')
        ordered = sorted(buckets.items())
        for (_, low), (_, high) in zip(ordered, ordered[1:]):
            if high < low:
                raise ParseError(name + ' buckets are not cumulative')
        count = self.value(name + '_count', **labels)
        total = self.value(name + '_sum', **labels)
        return Histogram(ordered, buckets[math.inf] if count is None else count,
                         0.0 if total is None else total)


def parse(text):
    """Parse Prometheus text format. Raises ParseError on malformed input."""
    scrape = Scrape()
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith('#'):
            parts = line.split(None, 3)
            if len(parts) >= 4 and parts[1] == 'TYPE':
                scrape.types[parts[2]] = parts[3].strip()
            continue
        match = _LINE.match(line)
        if not match:
            raise ParseError('malformed line: ' + line[:200])
        name, labels, value = match.group(1), _labels(match.group(2)), _number(match.group(3))
        scrape.samples.setdefault(name, []).append((labels, value))
    return scrape


def fetch(url, timeout=3.0):
    """GET `url` and parse it. Raises OSError, ValueError or ParseError."""
    request = urllib.request.Request(url, headers={'Accept': 'text/plain'})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        if response.status != 200:
            raise OSError(f'{url}: HTTP {response.status}')
        body = response.read(MAX_SCRAPE_BYTES + 1)
    if len(body) > MAX_SCRAPE_BYTES:
        raise ValueError(f'{url}: metrics larger than {MAX_SCRAPE_BYTES} bytes')
    return parse(body.decode('utf-8', errors='replace'))


# Series the scaler reads from a worker. Counters must be monotone within one
# process; gauges may move freely.
COUNTERS = {
    'queries': 'transparent_shard_queries_total',
    'queue_rejections': 'transparent_shard_queue_rejections_total',
    'overloads': 'transparent_shard_overloads_total',
    'deadline_exceeded': 'transparent_shard_deadline_exceeded_total',
    'slot_busy_us': 'transparent_shard_query_slot_busy_microseconds_total',
    'prewarm_ops': 'transparent_shard_prewarm_operations_total',
}
REQUIRED_COUNTERS = ('queries', 'queue_rejections', 'overloads', 'deadline_exceeded', 'slot_busy_us')
GAUGES = {
    'slots': 'transparent_shard_query_slots',
    'cache_resident_bytes': 'transparent_shard_cache_resident_bytes',
    'cache_budget_bytes': 'transparent_shard_cache_budget_bytes',
    'cgroup_memory_bytes': 'transparent_shard_cgroup_memory_current_bytes',
    'cgroup_memory_max_bytes': 'transparent_shard_cgroup_memory_max_bytes',
    'warm_runtimes': 'transparent_shard_warm_runtimes',
    'target_runtimes': 'transparent_shard_target_runtimes',
}
REQUIRED_GAUGES = ('slots',)
LATENCY = 'transparent_shard_query_seconds'
START_TIMES = ('pir_http_process_start_time_seconds', 'transparent_shard_process_start_time_seconds')


@dataclass
class Sample:
    """What one scrape of one worker says, flattened. Missing series are None."""
    t: float
    start: float | None
    counters: dict
    gauges: dict
    latency: tuple | None  # ((le, cumulative), ...) for outcome="success"
    complete: bool
    missing: tuple = ()
    # Set by History.add: the success-latency buckets since the previous
    # sample of the same process, as (bounds, counts); None for the first.
    interval: tuple | None = None


def worker_sample(scrape, t):
    """Extract the scaler's series from one worker scrape taken at `t`."""
    counters = {key: scrape.value(name) for key, name in COUNTERS.items()}
    gauges = {key: scrape.value(name) for key, name in GAUGES.items()}
    start = None
    for name in START_TIMES:
        start = scrape.value(name)
        if start is not None:
            break
    histogram = scrape.histogram(LATENCY, outcome='success')
    latency = tuple(histogram.buckets) if histogram is not None else None
    missing = tuple([COUNTERS[k] for k in REQUIRED_COUNTERS if counters[k] is None]
                    + [GAUGES[k] for k in REQUIRED_GAUGES if gauges[k] is None]
                    + ([LATENCY] if latency is None else []))
    bad = any(v is not None and (math.isnan(v) or v < 0) for v in counters.values())
    return Sample(t=t, start=start, counters=counters, gauges=gauges, latency=latency,
                  complete=not missing and not bad, missing=missing)


def quantile(q, buckets):
    """`histogram_quantile` over cumulative `(le, count)` buckets; None if empty.

    Linear interpolation inside the bucket that holds the rank, from the
    previous bound (0 for the first). A rank in the +Inf bucket reports the
    highest finite bound, as Prometheus does.
    """
    if not buckets:
        return None
    total = buckets[-1][1]
    if total <= 0:
        return None
    rank = q * total
    previous_le, previous_count = 0.0, 0.0
    for le, count in buckets:
        if count >= rank:
            if math.isinf(le):
                return previous_le
            if count == previous_count:
                return le
            return previous_le + (le - previous_le) * (rank - previous_count) / (count - previous_count)
        previous_le, previous_count = le, count
    return previous_le


def bucket_delta(new, old):
    """Element-wise difference of two cumulative bucket tuples; None if the bounds differ."""
    if new is None or old is None or len(new) != len(old):
        return None
    out = []
    for (le_new, c_new), (le_old, c_old) in zip(new, old):
        if le_new != le_old:
            return None
        out.append((le_new, c_new - c_old))
    return out


def merge_buckets(bucket_lists):
    """Sum cumulative buckets with identical bounds; None if any list is None or differs."""
    merged = None
    for buckets in bucket_lists:
        if buckets is None:
            return None
        if merged is None:
            merged = [list(b) for b in buckets]
            continue
        if len(buckets) != len(merged) or any(le != m[0] for (le, _), m in zip(buckets, merged)):
            return None
        for (_, count), m in zip(buckets, merged):
            m[1] += count
    return [tuple(m) for m in merged] if merged is not None else []


def _counters_decreased(new, old):
    for key, value in new.counters.items():
        before = old.counters.get(key)
        if value is not None and before is not None and value < before:
            return True
    return False


def _interval(new, old):
    """(bounds, counts) of `new` minus `old`; None if the bounds differ; False if any count fell."""
    if len(new) != len(old):
        return None
    bounds, counts = [], []
    for (le, c_new), (le_old, c_old) in zip(new, old):
        if le != le_old:
            return None
        if c_new < c_old:
            return False
        bounds.append(le)
        counts.append(c_new - c_old)
    return tuple(bounds), tuple(counts)


@dataclass
class Window:
    """Deltas over one worker's window. `span` seconds between `first` and `last`;
    `samples` holds every sample from `first` to `last` inclusive."""
    span: float
    first: Sample
    last: Sample
    samples: tuple = ()

    def delta(self, key):
        return self.last.counters[key] - self.first.counters[key]

    def rate(self, key):
        return self.delta(key) / self.span

    def latency(self):
        return bucket_delta(self.last.latency, self.first.latency)

    def intervals(self):
        """`(end t, (bounds, counts))` for each scrape interval in the window; counts may be None."""
        return [(b.t, b.interval) for b in self.samples[1:]]


class History:
    """One worker's complete samples, newest last, bounded by age.

    `add` returns True when it detected a counter reset (process start time
    changed, or a counter or bucket decreased) and dropped the earlier samples.
    """

    def __init__(self, keep_seconds=900.0):
        self.keep_seconds = keep_seconds
        self.samples = []
        self.resets = 0
        self.last_reset_t = None
        self.last_attempt_t = None
        self.last_error = None

    def failed(self, t, error):
        self.last_attempt_t = t
        self.last_error = error

    def add(self, sample):
        self.last_attempt_t = sample.t
        if not sample.complete:
            self.last_error = 'incomplete metrics: ' + ', '.join(sample.missing or ('invalid values',))
            return False
        self.last_error = None
        reset = False
        if self.samples:
            last = self.samples[-1]
            if sample.t <= last.t:
                return False
            interval = _interval(sample.latency, last.latency)
            if ((sample.start is not None and last.start is not None and sample.start != last.start)
                    or interval is None or interval is False or _counters_decreased(sample, last)):
                self.samples = []
                self.resets += 1
                self.last_reset_t = sample.t
                reset = True
            else:
                sample.interval = interval
        self.samples.append(sample)
        horizon = sample.t - self.keep_seconds
        while len(self.samples) > 2 and self.samples[1].t <= horizon:
            self.samples.pop(0)
        return reset

    def latest(self):
        return self.samples[-1] if self.samples else None

    def window(self, seconds, min_span):
        """Deltas from the newest sample back `seconds`, or None if shorter than `min_span`.

        The base is the newest sample at least `seconds` older than the latest,
        or the oldest one kept when the history is younger than that.
        """
        if len(self.samples) < 2:
            return None
        last = self.samples[-1]
        cutoff = last.t - seconds
        index = 0
        for i in range(len(self.samples) - 1, -1, -1):
            if self.samples[i].t <= cutoff:
                index = i
                break
        base = self.samples[index]
        span = last.t - base.t
        if span < min_span or span <= 0:
            return None
        return Window(span, base, last, tuple(self.samples[index:]))
