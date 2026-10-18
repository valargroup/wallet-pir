"""Offered-load traces for the scaler simulator: qps as a function of seconds.

Each trace is a small callable object that also says where its steps are, so
the invariant checker knows when a reaction or a scale-back is due.
"""
import math

HOUR = 3600.0


class Flat:
    kind = 'flat'

    def __init__(self, qps):
        self.qps = qps
        self.steps = []

    def __call__(self, t):
        return self.qps


class Step:
    """`before` qps, then `after` from `at` seconds (up or down)."""
    kind = 'step'

    def __init__(self, before, after, at):
        self.before, self.after, self.at = before, after, at
        self.steps = [(at, before, after)]

    def __call__(self, t):
        return self.before if t < self.at else self.after


class UpDown:
    """`low`, then `high` from `up` seconds, then `low` again from `down`."""
    kind = 'up-down'

    def __init__(self, low, high, up, down):
        self.low, self.high, self.up, self.down = low, high, up, down
        self.steps = [(up, low, high), (down, high, low)]

    def __call__(self, t):
        return self.high if self.up <= t < self.down else self.low


class Diurnal:
    """A raised cosine between `low` and `high` over `period` seconds, lowest at 0."""
    kind = 'diurnal'

    def __init__(self, low, high, period=24 * HOUR):
        self.low, self.high, self.period = low, high, period
        self.steps = []

    def __call__(self, t):
        return self.low + (self.high - self.low) * (1 - math.cos(2 * math.pi * t / self.period)) / 2


class Spike:
    """`base` qps with `peak` for `width` seconds starting at each of `at`."""
    kind = 'spike'

    def __init__(self, base, peak, at, width):
        self.base, self.peak, self.at, self.width = base, peak, list(at), width
        self.steps = []

    def __call__(self, t):
        return self.peak if any(a <= t < a + self.width for a in self.at) else self.base


class Sawtooth:
    """Ramps from `low` to `high` over `ramp` seconds, drops to `low`, rests `rest`, repeats."""
    kind = 'sawtooth'

    def __init__(self, low, high, ramp, rest):
        self.low, self.high, self.ramp, self.rest = low, high, ramp, rest
        self.steps = []

    def __call__(self, t):
        phase = t % (self.ramp + self.rest)
        if phase >= self.ramp:
            return self.low
        return self.low + (self.high - self.low) * phase / self.ramp
