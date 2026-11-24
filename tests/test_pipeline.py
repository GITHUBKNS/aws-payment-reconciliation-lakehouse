import sys, tempfile, unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from pipeline import *

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.root=generate(Path(self.tmp.name)); self.c=connect(':memory:')
    def tearDown(self): self.c.close(); self.tmp.cleanup()
    def counts(self): return {r['status']:r['records'] for r in summary(self.c)}
    def baseline(self): apply(self.c,self.root/'batch1')
    def test_baseline(self):
        self.baseline(); self.assertEqual(self.counts(),{'MATCHED':9980,'MISSING_SETTLEMENT':10,'AMOUNT_MISMATCH':10,'ORPHAN_SETTLEMENT':10})
    def test_incremental_and_replay(self):
        self.baseline(); m=apply(self.c,self.root/'batch2'); self.assertEqual(self.counts(),{'MATCHED':9970,'AMOUNT_MISMATCH':10,'ORPHAN_SETTLEMENT':20})
        self.assertEqual(m['payments']['changed_keys'],120); self.assertEqual(m['settlements']['changed_keys'],120)
        before=summary(self.c); replay=apply(self.c,self.root/'batch2'); self.assertEqual(before,summary(self.c)); self.assertEqual(sum(x['changed_keys'] for x in replay.values()),0)
        self.assertEqual(self.c.execute('SELECT COUNT(*) FROM quarantine').fetchone()[0],2)
        self.assertEqual(tuple(self.c.execute("SELECT version,operation FROM payments WHERE payment_id='P000201'").fetchone()),(2,'D'))
    def test_late_batch_does_not_resurrect(self):
        self.baseline(); apply(self.c,self.root/'batch2'); before=summary(self.c); apply(self.c,self.root/'batch1'); self.assertEqual(before,summary(self.c))
    def test_rollback_and_recovery(self):
        self.baseline(); before=summary(self.c)
        with self.assertRaises(RuntimeError): apply(self.c,self.root/'batch2',True)
        self.assertEqual(before,summary(self.c)); self.assertEqual(self.c.execute("SELECT COUNT(*) FROM audit WHERE status='FAILED'").fetchone()[0],1)
        apply(self.c,self.root/'batch2'); self.assertEqual(self.counts()['MATCHED'],9970)
    def test_conflicting_same_version(self):
        self.baseline(); p=self.root/'conflict'; write_csv(p/'payments.csv',[('P000001',1,'U',0)]); write_csv(p/'settlements.csv',[])
        with self.assertRaises(ValueError): apply(self.c,p)
    def test_conflict_inside_batch(self):
        p=self.root/'conflict'; write_csv(p/'payments.csv',[('P000001',1,'U',0),('P000001',1,'U',1)]); write_csv(p/'settlements.csv',[])
        with self.assertRaises(ValueError): apply(self.c,p)
    def test_batch_immutability(self):
        self.baseline(); p=self.root/'batch1'/'payments.csv'; p.write_text(p.read_text()+'P000001,1,U,0\n')
        with self.assertRaises(ValueError): apply(self.c,self.root/'batch1')
if __name__=='__main__': unittest.main()
