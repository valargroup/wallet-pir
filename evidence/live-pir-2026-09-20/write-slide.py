import json,pathlib
p=pathlib.Path(__file__).resolve().parent;s=json.loads((p/'summary.json').read_text())
text='''# Live PIR measurements — 20 September 2026, Dubai

**Enhance database:** 450,163 encrypted records, **331.77 MB** raw, 7 populated shards (65,536-row logical domain).

**Transparent database:** 353.38 million events, **57.25 GB** padded tables + **63.43 MB** filters, 175 shards.

| Per encrypted query | Upload | Download | Server p50 / p95 |
|---|---:|---:|---:|
'''
for title,key,metric in [('Enhance','enhance','post_body_server_ms'),('Transparent recent directory','transparent-160-directory','evaluation_ms'),('Transparent recent page','transparent-160-pages','evaluation_ms'),('Transparent archive directory','transparent-0-directory','evaluation_ms'),('Transparent archive page','transparent-0-pages','evaluation_ms')]:
 r=s[key];t=r[metric];text+=f"| {title} | {r['upload_bytes'][0]/1024:.2f} KiB | {r['download_bytes'][0]/1024:.2f} KiB | {t['p50']:.1f} / {t['p95']:.1f} ms |\n"
text+='''
**Growth:** Enhance scans all populated groups; upload grows in geometry steps. Doubling the current record count projects upload **420 → 772 KiB**, with a **10 KiB response**. Transparent adds fixed-size shards: per-query cost stays stable if runtimes remain warm; more matching shards/history pages cause more queries. Doubling a table on the same archive worker measured:

'''
a=s['transparent-0-directory']['evaluation_ms']['p50'];b=s['transparent-0-pages']['evaluation_ms']['p50'];text+=f"**112 → 224 MiB table; 252 → 420 KiB upload; {a:.1f} → {b:.1f} ms evaluation; 5 KiB response unchanged.**\n"
text+='''
*Fresh live measurements, repeated single-client queries, actual payload lengths, server timing-counter deltas, and exact-answer checks. Enhance time includes coordinator post-upload processing and worker RPC; Transparent time is worker evaluation only. Times are successful-query percentiles, not whole-wallet recovery or maximum capacity. Transparent recovery needs two directory queries per matching script/shard plus any pages, filters and setup.*

**Current readiness caveat:** Transparent public initialization returns 404 and its public filter map is stale. The test bootstrapped from verified private metadata and used public query endpoints. Cache-admission and transport failures occurred; retain these caveats when presenting the performance numbers.

Source: [full report](REPORT.md), [machine-readable results](summary.json), and retained raw observations in this directory. Future-size numbers are projections; the current-size and archive 2×-table comparison are measured.
'''
(p/'PRESENTATION.md').write_text(text)
