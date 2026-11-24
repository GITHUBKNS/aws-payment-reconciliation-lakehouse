"""Deterministic synthetic payment/settlement CDC; no real financial data."""
import argparse, csv, hashlib, json, sqlite3, time, uuid
from pathlib import Path
from collections import Counter

FIELDS = ('payment_id','version','operation','amount_cents')
STREAMS = ('payments','settlements')
RECON = """WITH keys AS (SELECT payment_id FROM payments WHERE operation='U'
 UNION SELECT payment_id FROM settlements WHERE operation='U'),
 compared AS (SELECT k.payment_id, p.amount_cents AS expected_cents,
 s.amount_cents AS settled_cents FROM keys k
 LEFT JOIN payments p ON k.payment_id=p.payment_id AND p.operation='U'
 LEFT JOIN settlements s ON k.payment_id=s.payment_id AND s.operation='U')
 SELECT payment_id, expected_cents, settled_cents,
 CASE WHEN expected_cents IS NULL THEN 'ORPHAN_SETTLEMENT'
 WHEN settled_cents IS NULL THEN 'MISSING_SETTLEMENT'
 WHEN expected_cents<>settled_cents THEN 'AMOUNT_MISMATCH'
 ELSE 'MATCHED' END AS status,
 COALESCE(settled_cents,0)-COALESCE(expected_cents,0) AS variance_cents
 FROM compared"""
SUMMARY = "SELECT status, COUNT(*) AS records, SUM(COALESCE(expected_cents,0)) AS expected_cents, SUM(COALESCE(settled_cents,0)) AS settled_cents, SUM(variance_cents) AS variance_cents FROM reconciliation GROUP BY status ORDER BY status"

def write_csv(path, rows, fields=FIELDS):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.writer(f); w.writerow(fields); w.writerows(rows)

def amount(i): return 1000+(i*37)%49000

def generate(root):
    root=Path(root)
    p=[(f'P{i:06}',1,'U',amount(i)) for i in range(1,10001)]
    s=[(f'P{i:06}',1,'U',amount(i)+(100 if 11<=i<=20 else 0)) for i in range(11,10001)]
    s += [(f'O{i:06}',1,'U',500) for i in range(1,11)]
    for stream,rows in [('payments',p),('settlements',s)]: write_csv(root/'batch1'/f'{stream}.csv', rows)
    p2=[(f'P{i:06}',2,'U',amount(i)-500) for i in range(101,201)]
    p2 += [(f'P{i:06}',2,'D',0) for i in range(201,221)]
    p2 += [p2[0], ('P000101',1,'U',amount(101)), ('P000201',1,'U',amount(201)), ('BAD',1,'U',-1)]
    s2=[(f'P{i:06}',2,'U',amount(i)-500) for i in range(101,191)]
    s2 += [(f'P{i:06}',1,'U',amount(i)) for i in range(1,11)] # late arrivals repair missing settlements
    s2 += [(f'P{i:06}',2,'U',amount(i)) for i in range(11,21)] # correct mismatches
    s2 += [(f'P{i:06}',2,'D',0) for i in range(201,211)] # reverse half of deleted payments
    s2 += [s2[0], ('P000101',1,'U',amount(101)), ('BAD',1,'U','oops')]
    for stream,rows in [('payments',p2),('settlements',s2)]: write_csv(root/'batch2'/f'{stream}.csv', rows)
    return root

def prepare(path):
    """Reject malformed rows, fail closed on contradictory same-version events, keep latest per key."""
    raw=0; bad=[]; seen={}; duplicates=0
    with path.open(newline='',encoding='utf-8') as f:
        reader=csv.DictReader(f)
        if tuple(reader.fieldnames or ()) != FIELDS: raise ValueError(f'Unexpected header: {path}')
        for row in reader:
            raw+=1
            try:
                k=row['payment_id']; v=int(row['version']); op=row['operation']; a=int(row['amount_cents'])
                if None in row or not k or len(k)>32 or not k.isascii() or not k.isalnum() or not 0<v<2**31 or op not in ('U','D') or not 0<=a<=10**12 or (op=='D' and a!=0): raise ValueError('invalid event')
            except (ValueError,TypeError,AttributeError):
                bad.append((raw,json.dumps(row,sort_keys=False),'invalid_event')); continue
            event=(k,v,op,a); key=(k,v)
            if key in seen:
                if seen[key]!=event: raise ValueError(f'Conflicting events for {key}')
                duplicates+=1
            else: seen[key]=event
    latest={}
    for event in seen.values():
        k,v,*_=event
        if k not in latest or v>latest[k][1]: latest[k]=event
    return list(seen.values()),list(latest.values()),bad,{'raw':raw,'invalid':len(bad),'duplicates':duplicates,'superseded_in_batch':len(seen)-len(latest),'candidate_keys':len(latest)}

def connect(path):
    c=sqlite3.connect(path); c.row_factory=sqlite3.Row
    for t in STREAMS:
        c.execute(f'CREATE TABLE IF NOT EXISTS {t}(payment_id TEXT PRIMARY KEY, version INTEGER NOT NULL, operation TEXT NOT NULL, amount_cents INTEGER NOT NULL)')
    c.execute('CREATE TABLE IF NOT EXISTS audit(run_id TEXT PRIMARY KEY,batch TEXT,status TEXT,metrics TEXT,error TEXT,elapsed_ms INTEGER)')
    c.execute('CREATE TABLE IF NOT EXISTS batch_manifest(batch TEXT PRIMARY KEY,digest TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS quarantine(batch TEXT,stream TEXT,line INTEGER,payload TEXT,reason TEXT,PRIMARY KEY(batch,stream,line))')
    c.execute('CREATE VIEW IF NOT EXISTS reconciliation AS '+RECON); c.commit(); return c

def digest_batch(folder):
    h=hashlib.sha256()
    for t in STREAMS: h.update(t.encode()); h.update((folder/f'{t}.csv').read_bytes())
    return h.hexdigest()

def apply(c,folder,fail_after_first=False):
    folder=Path(folder); batch=folder.name; run=uuid.uuid4().hex; start=time.monotonic(); metrics={}
    try:
        digest=digest_batch(folder)
        with c:
            prior=c.execute('SELECT digest FROM batch_manifest WHERE batch=?',(batch,)).fetchone()
            if prior and prior[0]!=digest: raise ValueError('Batch ID reused with changed bytes')
            c.execute('INSERT OR IGNORE INTO batch_manifest VALUES (?,?)',(batch,digest))
            for t in STREAMS:
                events,latest,bad,m=prepare(folder/f'{t}.csv'); metrics[t]=m
                c.executemany('INSERT OR IGNORE INTO quarantine VALUES (?,?,?,?,?)',[(batch,t,*r) for r in bad])
                # Compare every valid event to the currently stored version, including superseded events.
                for k,v,op,a in events:
                    r=c.execute(f'SELECT version,operation,amount_cents FROM {t} WHERE payment_id=?',(k,)).fetchone()
                    if r and r[0]==v and tuple(r)!=(v,op,a): raise ValueError(f'Conflict with stored state: {t}/{k}/{v}')
                changed=0
                for event in latest:
                    cur=c.execute(f'''INSERT INTO {t} VALUES (?,?,?,?) ON CONFLICT(payment_id) DO UPDATE SET version=excluded.version,operation=excluded.operation,amount_cents=excluded.amount_cents WHERE excluded.version>{t}.version''',event)
                    changed+=cur.rowcount
                m['changed_keys']=changed
                if t=='payments' and fail_after_first: raise RuntimeError('Injected failure after first stream')
            c.execute('INSERT INTO audit VALUES (?,?,?,?,?,?)',(run,batch,'SUCCEEDED',json.dumps(metrics),None,int((time.monotonic()-start)*1000)))
    except Exception as e:
        with c: c.execute('INSERT INTO audit VALUES (?,?,?,?,?,?)',(run,batch,'FAILED',json.dumps(metrics),str(e),int((time.monotonic()-start)*1000)))
        raise
    return metrics

def summary(c): return [dict(r) for r in c.execute(SUMMARY)]

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--data',type=Path,default=Path('data')); ap.add_argument('--db',type=Path,default=Path('output/demo.db')); a=ap.parse_args(); generate(a.data); a.db.parent.mkdir(parents=True,exist_ok=True)
    c=connect(a.db)
    for batch in ['batch1','batch2','batch2']:
        print(batch,json.dumps(apply(c,a.data/batch),indent=2)); print(json.dumps(summary(c),indent=2))
    (a.db.parent/'reconciliation_summary.json').write_text(json.dumps(summary(c),indent=2)); c.close()
if __name__=='__main__': main()
