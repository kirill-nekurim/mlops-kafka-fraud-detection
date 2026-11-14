"""Пять повторов уже записанного сообщения: счётчики INSERT и histogram не растут."""

import json,re,subprocess,time,urllib.request
from pathlib import Path
root=Path(__file__).resolve().parents[1]
def docker(*args):
 return subprocess.check_output(['docker','compose',*args],cwd=root,text=True,timeout=90)
def metrics():
 with urllib.request.urlopen('http://localhost:18001/metrics',timeout=10) as r:s=r.read().decode()
 return {k:float(re.search(r'^'+k+r' (.+)$',s,re.M).group(1)) for k in ['writer_scores_total','writer_duplicates_total','writer_score_count']}
docker('pause','simulator')
try:
 # Сначала даём writer дописать остаток очереди, иначе он смешается с проверкой.
 before=metrics()
 stable_since=time.time()
 deadline=time.time()+45
 while time.time()<deadline:
  time.sleep(.5)
  current=metrics()
  if current!=before:
   before=current
   stable_since=time.time()
  if time.time()-stable_since>=3:break
 else:raise RuntimeError('Очередь не успела опустеть за 45 секунд')
 code='''import os,json,psycopg2,psycopg2.extras
from confluent_kafka import Producer
c=psycopg2.connect(os.environ['DATABASE_URL'])
with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as q:
 q.execute('SELECT transaction_id,score,fraud_flag,model_version,us_state,merch,cat_id,amount,scored_at,produced_at FROM scores ORDER BY id DESC LIMIT 1')
 row=dict(q.fetchone())
p=Producer({'bootstrap.servers':'kafka:9092'})
for _ in range(5):p.produce('scores',key=row['transaction_id'],value=json.dumps(row,default=str))
p.flush(10)
print(row['transaction_id'])'''
 transaction=docker('exec','-T','db_writer','python','-c',code).strip()
 deadline=time.time()+12
 while time.time()<deadline:
  after=metrics()
  if after['writer_duplicates_total']>=before['writer_duplicates_total']+5:break
  time.sleep(.5)
 assert after['writer_duplicates_total']==before['writer_duplicates_total']+5,(before,after)
 assert before['writer_scores_total']==after['writer_scores_total'],(before,after)
 assert before['writer_score_count']==after['writer_score_count'],(before,after)
 report={'transaction_id':transaction,'copies_replayed':5,'before':before,'after':after,'result':'PASS'}
 (root/'docs/duplicate-delivery-check.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
finally:
 docker('unpause','simulator')
