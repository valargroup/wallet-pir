import hashlib, collections, sys
def cands(key, d, rows):
    h = hashlib.sha256(key).digest()
    return [int.from_bytes(h[i*8:i*8+8],'little') % rows for i in range(d)]
def place(n, d, rows=4096, slots=14, seed=0, max_visits=512):
    occ=[[] for _ in range(rows)]; c={}
    for i in range(n):
        k=f"{seed}:{i}".encode(); cs=cands(k,d,rows); c[i]=cs
        order=sorted(set(cs), key=lambda r:(len(occ[r]), cs.index(r)))
        r=next((r for r in order if len(occ[r])<slots), None)
        if r is not None: occ[r].append(i); continue
        # BFS relocation
        came={}; q=collections.deque()
        for r in dict.fromkeys(cs): came[r]=None; q.append(r)
        visits=0; freed=None
        while q:
            row=q.popleft(); visits+=1
            if visits>max_visits: break
            for occupant in occ[row]:
                for alt in c[occupant]:
                    if alt==row or alt in came: continue
                    came[alt]=(row,occupant)
                    if len(occ[alt])<slots:
                        at=alt
                        while came[at] is not None:
                            prev,moved=came[at]; occ[prev].remove(moved); occ[at].append(moved); at=prev
                        freed=at; break
                    q.append(alt)
                if freed is not None: break
            if freed is not None: break
        if freed is None: return None, i
        occ[freed].append(i)
    loads=sorted(len(o) for o in occ)
    return loads[-1], sum(1 for l in loads if l==slots)
if len(sys.argv) == 1:
  for n in (45454, 49152, 53000, 55000):
    for d in (2,3,4):
        res=[place(n,d,seed=s) for s in range(3)]
        print(f"n={n} load={n/57344:.1%} d={d}: " + "  ".join("OVERFLOW@%d"%r[1] if r[0] is None else f"max {r[0]}, full rows {r[1]}" for r in res), flush=True)

if len(sys.argv) > 1 and sys.argv[1] == "seeds":
    for n in (49152, 51000):
        over = 0; worst_full = 0
        for s in range(100, 140):
            r = place(n, 2, seed=s)
            if r[0] is None: over += 1
            else: worst_full = max(worst_full, r[1])
        print(f"d=2 n={n} load={n/57344:.1%}: {over}/40 overflowed, most full rows {worst_full}", flush=True)
