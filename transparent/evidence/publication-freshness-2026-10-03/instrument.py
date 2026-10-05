#!/usr/bin/env python3
"""Temporary phase timers for publication profiling. Never committed."""
import re, sys
root = sys.argv[1]
def edit(path, pairs):
    p = f'{root}/{path}'
    s = open(p).read()
    for pattern, insert, where in pairs:
        m = re.search(pattern, s)
        if not m:
            sys.exit(f'{path}: no match for {pattern!r}')
        at = m.start() if where == 'before' else m.end()
        s = s[:at] + insert + s[at:]
    open(p, 'w').write(s)
edit('transparent/services/transparent-filter-server/src/publication.rs', [
    (r'\n        let events: Vec<_> = blocks\n', '\n        let t0 = std::time::Instant::now();', 'before'),
    (r'\n        let display = if cli.txid_display \{', '\n        eprintln!("PROF build_shard {:.3}", t0.elapsed().as_secs_f64()); let t0 = std::time::Instant::now();', 'before'),
    (r'\n        let digest = manifest.digest\(\);', '\n        eprintln!("PROF manifests {:.3}", t0.elapsed().as_secs_f64()); let t0 = std::time::Instant::now();', 'before'),
    (r'\n        eprintln!\(\n            "shard \{:>3\}', '\n        eprintln!("PROF write {:.3}", t0.elapsed().as_secs_f64());', 'before'),
    (r'\n    for height in resume_height..=covered \{', '\n    let tloop = std::time::Instant::now(); let mut tseal = std::time::Duration::ZERO;', 'before'),
    (r'for shard in sealer.push_block\(block.0, &block.1\)\? \{', '', 'before'),
    (r'\n    if let Some\(shard\) = sealer.finish\(\) \{', '\n    eprintln!("PROF loop {:.3} seal {:.3}", tloop.elapsed().as_secs_f64(), tseal.as_secs_f64());', 'before'),
    (r'\n    map.check_shape\(\)', '\n    eprintln!("PROF before-map {:.3}", started.elapsed().as_secs_f64());', 'before'),
])
p = f'{root}/transparent/services/transparent-filter-server/src/publication.rs'
s = open(p).read()
s = s.replace('for shard in sealer.push_block(block.0, &block.1)? {',
 'let ts = std::time::Instant::now(); let closed = sealer.push_block(block.0, &block.1)?; tseal += ts.elapsed();\n        for shard in closed {', 1)
open(p, 'w').write(s)
edit('transparent/crates/transparent-shard/src/build.rs', [
    (r'\n    let mut by_script: BTreeMap<', '\n    let tb = std::time::Instant::now();', 'before'),
    (r'\n    let filter = build_range_filter_for\(range, key, &filter_elements\)\?;', '\n    eprintln!("PROFB group+filter {:.3}", tb.elapsed().as_secs_f64()); let tb = std::time::Instant::now();', 'after'),
    (r'\n    let page_segments = segments_for\(', '\n    eprintln!("PROFB histories+pages {:.3}", tb.elapsed().as_secs_f64()); let tb = std::time::Instant::now();', 'before'),
    (r'\n    let \(directory, scripts, choice\) = place_directory\(shard_id, entries, geometry\)\?;', '\n    eprintln!("PROFB pagetable+directory+choice {:.3}", tb.elapsed().as_secs_f64());', 'after'),
])
