"""Deploy a small Athena/Iceberg demo using an existing private S3 bucket.
Not tested in a live AWS account. Use a dedicated database/prefix, one writer only.
"""
import argparse, json, re, time, uuid
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from pipeline import STREAMS,FIELDS,generate,prepare,write_csv,digest_batch,RECON,SUMMARY,connect,apply,summary

class Cloud:
    def __init__(self,a):
        self.a=a; self.ath=boto3.client('athena',region_name=a.region); self.s3=boto3.client('s3',region_name=a.region)
        self.uri=f's3://{a.bucket}/{a.prefix}/'; self.log=[]; self.run=uuid.uuid4().hex; self.ready=False
    def put(self,key,body):
        self.s3.put_object(Bucket=self.a.bucket,Key=f'{self.a.prefix}/{key}',Body=body,ServerSideEncryption='AES256')
    def query(self,sql,fetch=False,context=True):
        kw=dict(QueryString=sql,WorkGroup=self.a.workgroup,ResultConfiguration={'OutputLocation':self.uri+'query-results/','EncryptionConfiguration':{'EncryptionOption':'SSE_S3'}})
        if context: kw['QueryExecutionContext']={'Catalog':'AwsDataCatalog','Database':self.a.database}
        q=self.ath.start_query_execution(**kw)['QueryExecutionId']; start=time.monotonic()
        while True:
            info=self.ath.get_query_execution(QueryExecutionId=q)['QueryExecution']; state=info['Status']['State']
            if state in ('SUCCEEDED','FAILED','CANCELLED'): break
            if time.monotonic()-start>600:
                self.ath.stop_query_execution(QueryExecutionId=q); raise TimeoutError(f'Query timeout: {q}')
            time.sleep(2)
        stats=info.get('Statistics',{}); self.log.append({'query_id':q,'status':state,'sql':sql,'bytes_scanned':stats.get('DataScannedInBytes'),'engine_ms':stats.get('EngineExecutionTimeInMillis')})
        if state!='SUCCEEDED': raise RuntimeError(f'{q}: {info["Status"].get("StateChangeReason",state)}')
        if not fetch: return
        rows=[]; first=True
        for page in self.ath.get_paginator('get_query_results').paginate(QueryExecutionId=q):
            for row in page['ResultSet']['Rows']:
                if first: first=False; continue
                rows.append([x.get('VarCharValue') for x in row['Data']])
        return rows
    def setup(self):
        self.query(f'CREATE DATABASE IF NOT EXISTS {self.a.database}',context=False)
        for t in STREAMS:
            self.query(f"CREATE TABLE IF NOT EXISTS {t} (payment_id string, version bigint, operation string, amount_cents bigint) LOCATION '{self.uri}iceberg/{t}/' TBLPROPERTIES ('table_type'='ICEBERG','format'='parquet')")
        self.query('CREATE OR REPLACE VIEW reconciliation AS '+RECON)
    def stage(self,folder):
        batch=folder.name; digest=digest_batch(folder); key=f'{self.a.prefix}/manifests/{batch}.json'; body=json.dumps({'sha256':digest}).encode()
        # Immutable batch identifier; reject altered replay. Single writer assumed.
        try:
            self.s3.put_object(Bucket=self.a.bucket,Key=key,Body=body,IfNoneMatch='*',ServerSideEncryption='AES256')
        except ClientError as e:
            if e.response['Error']['Code'] not in ('PreconditionFailed','412'): raise
            old=json.load(self.s3.get_object(Bucket=self.a.bucket,Key=key)['Body'])
            if old['sha256']!=digest: raise ValueError('Batch ID reused with changed bytes')
        metrics={}
        for t in STREAMS:
            path=folder/f'{t}.csv'; events,latest,bad,m=prepare(path); metrics[t]=m
            self.put(f'raw/{batch}/{t}.csv',path.read_bytes())
            self.put(f'quarantine/{batch}/{t}.json',json.dumps(bad).encode())
            clean=Path('output/stage')/batch/f'{t}.csv'; write_csv(clean,events)
            self.put(f'staged/{batch}/{t}/events.csv',clean.read_bytes())
            self.query(f"CREATE EXTERNAL TABLE IF NOT EXISTS stage_{batch}_{t} (payment_id string, version bigint, operation string, amount_cents bigint) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '{self.uri}staged/{batch}/{t}/' TBLPROPERTIES ('skip.header.line.count'='1')")
        # Check both streams before any state mutation.
        for t in STREAMS:
            conflicts=self.query(f'SELECT count(*) FROM stage_{batch}_{t} s JOIN {t} t ON s.payment_id=t.payment_id AND s.version=t.version WHERE s.operation<>t.operation OR s.amount_cents<>t.amount_cents',True)
            if int(conflicts[0][0]): raise ValueError(f'Conflicting stored version in {t}')
        for t in STREAMS:
            source=f'(SELECT payment_id,version,operation,amount_cents FROM (SELECT *, row_number() OVER (PARTITION BY payment_id ORDER BY version DESC) AS rn FROM stage_{batch}_{t}) WHERE rn=1)'
            metrics[t]['changed_keys']=int(self.query(f'SELECT count(*) FROM {source} s LEFT JOIN {t} t ON s.payment_id=t.payment_id WHERE t.payment_id IS NULL OR s.version>t.version',True)[0][0])
            self.query(f'''MERGE INTO {t} t USING {source} s ON t.payment_id=s.payment_id
            WHEN MATCHED AND s.version>t.version THEN UPDATE SET version=s.version, operation=s.operation, amount_cents=s.amount_cents
            WHEN NOT MATCHED THEN INSERT (payment_id,version,operation,amount_cents) VALUES (s.payment_id,s.version,s.operation,s.amount_cents)''')
        return metrics
    def results(self):
        fields=['status','records','expected_cents','settled_cents','variance_cents']
        return [dict(zip(fields,[row[0],*map(int,row[1:])])) for row in self.query(SUMMARY,True)]
    def execute(self):
        root=generate(Path('data')); c=connect(':memory:'); status='FAILED'; reports=[]
        try:
            self.setup()
            for batch in ('batch1','batch2','batch2'):
                expected=apply(c,root/batch); observed=self.stage(root/batch); results=self.results()
                if not (self.a.resume and batch=='batch1') and results!=summary(c): raise AssertionError(f'{batch}: Cloud reconciliation differs from local reference. Use a fresh database and prefix for a full demo.')
                if not self.a.resume and observed!=expected: raise AssertionError(f'{batch}: Cloud and local event counts differ')
                if len(reports)==2 and any(m['changed_keys'] for m in observed.values()): raise AssertionError('Replay changed keys')
                reports.append({'batch':batch,'metrics':observed,'summary':results}); print(json.dumps(reports[-1],indent=2),flush=True)
            status='SUCCEEDED'; print('PASS: AWS results match local reference; replay changes zero keys.')
        finally:
            c.close(); evidence={'status':status,'runs':reports,'queries':self.log}; dest=Path('output')/f'aws_run_{self.run}.json'; dest.parent.mkdir(exist_ok=True); dest.write_text(json.dumps(evidence,indent=2))
            try: self.put(f'audit/{self.run}.json',dest.read_bytes())
            except Exception as e: print(f'Could not upload audit log: {e}')

def main():
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('--bucket',required=True); p.add_argument('--region',required=True); p.add_argument('--database',default='payment_demo'); p.add_argument('--prefix',default='payment-demo'); p.add_argument('--workgroup',default='primary'); p.add_argument('--resume',action='store_true',help='Replay both immutable batches after a partial failure; verify final state'); a=p.parse_args()
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,40}',a.database) or not re.fullmatch(r'[a-z0-9][a-z0-9/_-]{0,80}',a.prefix) or not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]',a.bucket): p.error('Use simple database, S3 prefix and bucket names')
    Cloud(a).execute()
if __name__=='__main__': main()
