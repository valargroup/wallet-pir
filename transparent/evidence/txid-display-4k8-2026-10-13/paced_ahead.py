# Read-only page-cache warmer for the txid-4k8 bootstrap. Follows the
# bootstrap position in events.bin (from /proc fdinfo), maps it to a height,
# and keeps the next AHEAD bytes of events.bin and those heights sidecars
# read, 32 reads at a time. A position drop (the second pass) restarts it.
import bisect, concurrent.futures as cf, glob, os, struct, sys, time
pid, n = sys.argv[1], sys.argv[2]
AHEAD, CHUNK = 4 << 30, 8 << 20
ev_len, bl_len = struct.unpack("<QQ", open(n + "/checkpoint.bin", "rb").read())
raw = open(n + "/blocks.bin", "rb").read(bl_len)
offs = [struct.unpack_from("<Q", raw, i + 32)[0] for i in range(0, len(raw), 48)]
names = [raw[i:i+32][::-1].hex() for i in range(0, len(raw), 48)]
def position():
    best = None
    for fd in glob.glob(f"/proc/{pid}/task/*/fd/*"):
        try:
            if os.readlink(fd).endswith("events.bin"):
                t, k = fd.split("/fd/")
                for line in open(f"{t}/fdinfo/{k}"):
                    if line.startswith("pos:"):
                        p = int(line.split()[1])
                        if p > 0: best = p if best is None else max(best, p)
        except OSError:
            pass
    return best
def side(h):
    try:
        with open(f"{n}/display-v2x/{names[h]}.bin", "rb") as f: f.read()
    except OSError: pass
def events(off):
    with open(n + "/events.bin", "rb") as f: f.seek(off); f.read(CHUNK)
pool = cf.ThreadPoolExecutor(32); ev = os.open(n + "/events.bin", os.O_RDONLY)
seen, done_h, done_off = 0, 0, 0
while os.path.exists(f"/proc/{pid}"):
    p = position()
    if p is not None:
        if p + (8 << 30) < seen:   # second pass began
            print("restart at", p, "after", seen, flush=True); done_h = done_off = 0; seen = p
        seen = max(seen, p)
    limit = min(seen + AHEAD, ev_len)
    if done_off < seen: done_off = seen - seen % CHUNK
    h0 = max(done_h, bisect.bisect_right(offs, seen) - 1)
    h1 = bisect.bisect_right(offs, limit)
    jobs = [pool.submit(side, h) for h in range(h0, h1)]
    jobs += [pool.submit(events, o) for o in range(done_off, limit, CHUNK)]
    for j in jobs: j.result()
    done_h, done_off = max(done_h, h1), max(done_off, limit)
    if not jobs: time.sleep(0.3)
print("PACED_DONE", flush=True)
