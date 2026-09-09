import json,re,sys
from pathlib import Path
out=[]
for path in sorted(Path(sys.argv[1]).glob('repeat-*.log')):
    phases=[]; stages={}; denials=0; retries=0
    for line in path.read_text().splitlines():
        if 'cold build admission wait ' in line:
            seconds=float(re.search(r'seconds=([0-9.e+-]+)',line).group(1))
            stages.setdefault('build_admission_wait',[]).append(seconds)
        if 'runtime stage ' in line:
            stage=re.search(r'stage="([^"]+)"',line).group(1)
            seconds=float(re.search(r'seconds=([0-9.e+-]+)',line).group(1))
            stages.setdefault(stage,[]).append(seconds)
        denials += 'work memory admission denied' in line
        retries += 'prewarm admission retry' in line
        if 'prewarm finished' in line:
            counts = re.search(r'warm=(\d+) target=(\d+) failed=(\d+)', line)
            phases.append({'phase':len(phases), 'warm_runtimes':int(counts[1]), 'target_runtimes':int(counts[2]), 'cumulative_prewarm_failures':int(counts[3]),'stages':{k:{'count':len(v),'sum_seconds':sum(v),'max_seconds':max(v)} for k,v in stages.items()},'memory_admission_denials':denials,'prewarm_retries':retries})
            stages={};denials=0;retries=0
        if 'publication preparation stages' in line and phases:
            phases[-1]['loading_seconds'] = float(re.search(r'loading_seconds=([0-9.e+-]+)', line).group(1))
            phases[-1]['warming_seconds'] = float(re.search(r'warming_seconds=([0-9.e+-]+)', line).group(1))
    out.append({'run':path.stem,'phases':phases})
print(json.dumps(out,indent=2))
