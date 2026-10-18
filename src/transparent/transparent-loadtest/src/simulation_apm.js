// Preserve complete label sets as series identities. A changed identity or a
// missing scrape breaks the line; negative deltas never become positive rates.
function parseProm(raw) {
  const result = [];
  for (const line of (raw || "").split("\n")) {
    if (!line || line.startsWith("#")) continue;
    const m = line.match(/^([a-zA-Z_:][\w:]*)(\{.*\})?\s+([^\s]+)(?:\s+.*)?$/);
    if (!m) continue;
    const labels = {};
    for (const l of (m[2] || "").matchAll(/([\w]+)="((?:\\.|[^"\\])*)"/g)) {
      try {
        labels[l[1]] = JSON.parse('"' + l[2] + '"');
      } catch {
        labels[l[1]] = l[2];
      }
    }
    const value = Number(m[3]);
    if (Number.isFinite(value)) result.push({ name: m[1], labels, value });
  }
  return result;
}
const key = (p) => p.name + JSON.stringify(Object.entries(p.labels).sort());
function single(rows, name) {
  const matches = rows.filter((r) => r.name === name);
  return matches.length === 1 ? matches[0] : null;
}
function metricPoints(scrapes, name, rate = false, scale = 1) {
  let prev = null;
  return scrapes.map((m) => {
    const rows = parseProm(m.text),
      p = single(rows, name);
    let value = null;
    if (p && !m.error) {
      if (!rate) value = p.value / scale;
      else if (prev) {
        const old = single(prev.rows, name),
          a = single(rows, "transparent_shard_process_start_time_seconds"),
          b = single(prev.rows, "transparent_shard_process_start_time_seconds");
        if (
          old &&
          key(old) === key(p) &&
          p.value >= old.value &&
          m.at > prev.at &&
          a &&
          b &&
          a.value === b.value
        )
          value = (p.value - old.value) / (m.at - prev.at) / scale;
      }
    }
    prev = m.error ? null : { rows, at: m.at };
    return [m.at - start, value];
  });
}
function histogramPoints(scrapes, name, outcome) {
  let prev = null;
  return scrapes.map((m) => {
    const rows = parseProm(m.text),
      selected = rows.filter(
        (p) =>
          p.name === name + "_bucket" &&
          (!outcome || p.labels.outcome === outcome),
      );
    let q = null;
    if (prev && !m.error && selected.length) {
      const a = single(rows, "transparent_shard_process_start_time_seconds"),
        b = single(prev.rows, "transparent_shard_process_start_time_seconds");
      if (a && b && a.value === b.value) {
        const deltas = selected.map((p) => {
          const old = prev.rows.find((o) => key(o) === key(p));
          return old && p.value >= old.value
            ? [
                Number(p.labels.le.replace("+Inf", "Infinity")),
                p.value - old.value,
              ]
            : null;
        });
        if (deltas.every(Boolean)) {
          deltas.sort((a, b) => a[0] - b[0]);
          // A partial or non-cumulative scrape cannot define a quantile.
          if (
            deltas.length < 2 ||
            deltas.at(-1)[0] !== Infinity ||
            deltas.some(
              ([bound, n], i) =>
                Number.isNaN(bound) ||
                bound < 0 ||
                (i > 0 && (bound <= deltas[i - 1][0] || n < deltas[i - 1][1])),
            )
          ) {
            prev = null;
            return [m.at - start, null];
          }
          const count = deltas.at(-1)[1],
            rank = count * 0.95;
          if (count > 0) {
            let lo = 0,
              prior = 0;
            for (const [bound, n] of deltas) {
              if (n >= rank) {
                q = Number.isFinite(bound)
                  ? lo +
                    ((bound - lo) * (rank - prior)) / Math.max(1, n - prior)
                  : null;
                break;
              }
              lo = bound;
              prior = n;
            }
          }
        }
      }
    }
    prev = m.error ? null : { rows };
    return [m.at - start, q];
  });
}
