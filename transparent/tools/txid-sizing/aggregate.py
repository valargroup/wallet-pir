#!/usr/bin/env python3
"""Disk-backed exact census histograms; never probability-sample estimates."""
from collections import Counter
import json
import sqlite3
from pathlib import Path
import analyze as a
import survey

class Aggregate:
    def __init__(self, path, boundaries=None):
        self.db=sqlite3.connect(path)
        self.db.executescript("""PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS records(txid TEXT PRIMARY KEY,height INTEGER NOT NULL,coinbase INTEGER NOT NULL,
            inputs INTEGER NOT NULL,outputs INTEGER NOT NULL,shielded INTEGER NOT NULL,fee TEXT NOT NULL,missing INTEGER NOT NULL,
            sizes TEXT NOT NULL,scripts TEXT NOT NULL,empty INTEGER NOT NULL,op_return INTEGER NOT NULL) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS histogram(codec TEXT,category TEXT,size INTEGER,n INTEGER NOT NULL,PRIMARY KEY(codec,category,size)) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS totals(key TEXT PRIMARY KEY,n INTEGER NOT NULL) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS blocks(height INTEGER PRIMARY KEY,hash TEXT NOT NULL);
        """)
        self.boundaries=boundaries or [(0,'Sprout'),(347500,'Overwinter'),(419200,'Sapling'),(653600,'Blossom'),(903000,'Heartwood'),(1046400,'Canopy'),(1687104,'NU5'),(2726400,'NU6'),(3146400,'NU6.1'),(3364600,'NU6.2'),(3428143,'NU6.3')]
        self.hist=Counter();self.totals=Counter()
    def add(self, summary):
        try: return self._add(summary)
        except BaseException:
            self.db.rollback();self.hist.clear();self.totals.clear()
            raise
    def _add(self, summary):
        old=self.db.execute('SELECT hash FROM blocks WHERE height=?',(summary['height'],)).fetchone()
        if old:
            if old[0]!=summary['hash']:raise ValueError('aggregate checkpoint block identity mismatch')
            return
        height=summary['height'];era=next(name for start,name in reversed(self.boundaries) if start<=height)
        self.db.execute('INSERT INTO blocks VALUES(?,?)',(height,summary['hash']))
        for key in ('transactions','eligible','shielded_only','raw_bytes'):self.totals[key]+=summary[key]
        self.totals['blocks']+=1
        if len(summary['records'])!=summary['eligible']:raise ValueError('aggregate eligibility mismatch')
        for r in summary['records']:
            self.db.execute('INSERT INTO records VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                (r['txid_internal'],height,r['coinbase'],r['input_count'],r['output_count'],r['shielded_components'],json.dumps(r['fee']),r['missing_prevouts'],json.dumps(r['sizes']),json.dumps(r['scripts']),r['empty_scripts'],r['op_return_scripts']))
            for codec,size in zip(a.CODECS,r['sizes']):
                for category in ('all',era,'coinbase' if r['coinbase'] else 'non_coinbase'):
                    self.hist[codec,category,size]+=1
            self.totals['outputs']+=r['output_count'];self.totals['transparent_inputs']+=r['input_count']
            self.totals['coinbase']+=r['coinbase'];self.totals['non_coinbase']+=not r['coinbase']
            self.totals['missing_prevouts']+=r['missing_prevouts'];self.totals['records_with_missing_prevouts']+=r['missing_prevouts']>0
            self.totals['input_only']+=r['input_count']>0 and r['output_count']==0
            self.totals['no_inputs_with_shielded_outputs']+=r['input_count']==0 and not r['coinbase'] and r['shielded_components'] and r['output_count']>0
            self.totals['shielded_components']+=r['shielded_components']
            self.totals['empty_scripts']+=r['empty_scripts'];self.totals['op_return_scripts']+=r['op_return_scripts']
            state='exact_zero' if r['fee']==0 else ('exact_positive' if isinstance(r['fee'],int) else r['fee'])
            self.totals['fee_'+state]+=1
            for key,n in r['scripts'].items():self.totals['script_'+key]+=n
    def flush(self):
        self.db.executemany('INSERT INTO histogram VALUES(?,?,?,?) ON CONFLICT(codec,category,size) DO UPDATE SET n=n+excluded.n',[(*key,n) for key,n in self.hist.items()])
        self.db.executemany('INSERT INTO totals VALUES(?,?) ON CONFLICT(key) DO UPDATE SET n=n+excluded.n',list(self.totals.items()))
        self.db.commit();self.hist.clear();self.totals.clear()
    def close(self):
        self.flush();self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)');self.db.close()
    def report(self, full=False):
        self.flush()
        totals=dict(self.db.execute('SELECT key,n FROM totals'))
        categories=[r[0] for r in self.db.execute('SELECT DISTINCT category FROM histogram')]
        result=dict(schema='txid-full-chain-aggregates-v1',coverage='FULL_CHAIN' if full else 'INCOMPLETE',totals=totals,codecs={})
        for codec in a.CODECS:
            strata={}
            for category in categories:
                hist=dict(self.db.execute('SELECT size,n FROM histogram WHERE codec=? AND category=? ORDER BY size',(codec,category)))
                n=sum(hist.values())
                frontiers={str(p):survey.hist_quantile(hist,p,strict=p==.8) for p in (.8,.85,.9,.95,.99,.999)}
                cutoffs=sorted(set(a.THRESHOLDS)|{v for v in frontiers.values() if v is not None and v<=4044})
                thresholds=[]
                for cutoff in cutoffs:
                    inline=sum(count for size,count in hist.items() if size<=cutoff)
                    tail={size:count for size,count in hist.items() if size>cutoff}
                    fs=sum(len(a.fragments(size,codec!='display-v1'))*count for size,count in tail.items())
                    page_entries=sum(sum(2+header+chunk for _,chunk,header in a.fragments(size,codec!='display-v1'))*count for size,count in tail.items())
                    directory_entries=sum((48+(size if size<=cutoff else 0))*count for size,count in hist.items()) if codec=='display-v1' else None
                    thresholds.append(dict(cutoff=cutoff,inline=inline,overflow=n-inline,coverage=inline/n if n else None,
                        strictly_more_than_80_percent=inline*5>n*4,fragments=fs,overflow_entry_bytes=page_entries,directory_entry_bytes=directory_entries))
                strata[category]=dict(transactions=n,payload_bytes=sum(size*count for size,count in hist.items()),frontiers=frontiers,
                    percentiles={str(p):survey.hist_quantile(hist,p) for p in (.01,.05,.5,.85,.9,.95,.99,.999)},
                    minimum=min(hist) if hist else None,maximum=max(hist) if hist else None,thresholds=thresholds,
                    histogram=[[size,count] for size,count in sorted(hist.items())])
            result['codecs'][codec]=strata
        return result


def follow(cache,status,evidence):
    import time
    from census import atomic_json,digest,ANCHOR_HEIGHT
    aggregate=Aggregate(cache/'aggregates.sqlite')
    try:
        next_height=aggregate.db.execute('SELECT coalesce(max(height)+1,0) FROM blocks').fetchone()[0]
        while True:
            with sqlite3.connect(f"file:{cache/'census.sqlite'}?mode=ro",uri=True) as source:
                rows=source.execute('SELECT height,summary FROM blocks WHERE height>=? ORDER BY height LIMIT 1000',(next_height,)).fetchall()
            if rows:
                for height,summary in rows:
                    if height!=next_height:raise ValueError('aggregate source height gap')
                    aggregate.add(json.loads(summary));next_height+=1
                aggregate.flush()
                if next_height%10000<1000:
                    atomic_json(cache/'aggregate-progress.json',dict(scanned_blocks=next_height,totals=dict(aggregate.db.execute('SELECT key,n FROM totals'))))
            else:
                state=json.loads(status.read_text())
                if next_height==ANCHOR_HEIGHT+1 or state.get('state')=='blocked':break
                time.sleep(3)
        report=aggregate.report(full=next_height==ANCHOR_HEIGHT+1)
        atomic_json(cache/'aggregate-report.json',report)
        atomic_json(evidence/'full-chain-aggregates.json',report)
    finally:aggregate.close()
    atomic_json(cache/'aggregate-checkpoint.json',dict(scanned_blocks=next_height,database_sha256=digest(cache/'aggregates.sqlite'),report_sha256=digest(cache/'aggregate-report.json')))
    print(json.dumps(dict(scanned_blocks=next_height,coverage=report['coverage'])))

if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('cache',type=Path);parser.add_argument('--status',type=Path,required=True)
    parser.add_argument('--evidence',type=Path,default=Path('transparent/evidence/txid-sizing'))
    args=parser.parse_args();follow(args.cache,args.status,args.evidence)
