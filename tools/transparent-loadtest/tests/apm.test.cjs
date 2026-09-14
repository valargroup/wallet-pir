const { test } = require("node:test");
const assert = require("node:assert/strict");
const vm = require("node:vm");
const fs = require("node:fs");
const path = require("node:path");
const context = { start: 100 };
vm.createContext(context);
vm.runInContext(
  fs.readFileSync(path.join(__dirname, "../src/simulation_apm.js"), "utf8"),
  context,
);
const scrape = (at, n, startTime = 1, label = "a") => ({
  at,
  text: `transparent_shard_queries_total{worker_id="${label}"} ${n}\ntransparent_shard_process_start_time_seconds{worker_id="${label}"} ${startTime}\n`,
});
test("rates use elapsed time and break at failures, reset and identity change", () => {
  const samples = [
    scrape(100, 10),
    scrape(102, 20),
    { at: 103, error: "unavailable" },
    scrape(104, 22),
    scrape(105, 23),
    scrape(106, 1, 2),
    scrape(107, 3, 2),
    scrape(108, 4, 2, "b"),
  ];
  const points = context.metricPoints(
    samples,
    "transparent_shard_queries_total",
    true,
  );
  assert.deepEqual(
    Array.from(points, (p) => p[1]),
    [null, 5, null, null, 1, null, 2, null],
  );
});
test("missing gauges remain unavailable and label escapes parse", () => {
  assert.equal(
    context.metricPoints([{ at: 100, text: "" }], "missing")[0][1],
    null,
  );
  const rows = context.parseProm(
    'metric{worker_id="a\\"b",role="archive"} 4\n# TYPE ignored gauge\n',
  );
  assert.equal(rows[0].labels.worker_id, 'a"b');
  assert.equal(rows[0].value, 4);
});
const hist = (at, values, startTime = 1) => ({
  at,
  text:
    `transparent_shard_process_start_time_seconds ${startTime}\n` +
    ["0.1", "1", "+Inf"]
      .map(
        (le, i) => `duration_bucket{outcome="success",le="${le}"} ${values[i]}`,
      )
      .join("\n"),
});
test("histogram p95 uses cumulative interval deltas; gaps, zero observations and resets have no quantile", () => {
  const samples = [
    hist(100, [0, 0, 0]),
    hist(101, [4, 10, 10]),
    hist(102, [4, 10, 10]),
    { at: 103, error: "gap" },
    hist(104, [4, 11, 11]),
    hist(105, [0, 1, 1], 2),
  ];
  const values = context
    .histogramPoints(samples, "duration", "success")
    .map((p) => p[1]);
  assert.equal(values[0], null);
  assert.ok(Math.abs(values[1] - 0.925) < 1e-9);
  assert.equal(values[2], null);
  assert.equal(values[3], null);
  assert.equal(values[4], null);
  assert.equal(values[5], null);
});
test("overflow bucket does not invent a finite latency", () => {
  const values = context.histogramPoints(
    [hist(100, [0, 0, 0]), hist(101, [0, 0, 1])],
    "duration",
    "success",
  );
  assert.equal(values[1][1], null);
});

test("partial and non-cumulative histograms are unavailable", () => {
  const partial = hist(101, [1, 2, 2]);
  partial.text = partial.text.split("\n").slice(0, -1).join("\n");
  assert.equal(
    context.histogramPoints(
      [hist(100, [0, 0, 0]), partial],
      "duration",
      "success",
    )[1][1],
    null,
  );
  assert.equal(
    context.histogramPoints(
      [hist(100, [0, 0, 0]), hist(101, [5, 2, 6])],
      "duration",
      "success",
    )[1][1],
    null,
  );
});
