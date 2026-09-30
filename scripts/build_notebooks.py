"""주차별 노트북과 Marp 원고를 재생성하는 유지보수용 소스."""
from pathlib import Path
import json
import textwrap
ROOT = Path(__file__).resolve().parents[1]

BOOT = '''from pathlib import Path
import sys
here = Path.cwd().resolve()
root = next((p for p in [here, *here.parents] if (p / 'labkit').is_dir()), None)
if root is None:
    candidate = here / '강의자료' / '실습코드'
    if candidate.is_dir(): root = candidate
if root is None:
    raise RuntimeError('실습코드 폴더에서 Jupyter를 시작하세요. README.md를 확인하세요.')
sys.path.insert(0, str(root))
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from labkit.common import ensure_data, DATA, output_dir, save_json
ensure_data()
'''
LABS = []
def add(week, slug, title, pages, goal, steps, assignment, legacy=''):
    LABS.append(dict(week=week, slug=slug, title=title, pages=pages, goal=goal, steps=steps, assignment=assignment, legacy=legacy))

add(1,'bigdata_overview','빅데이터 문제 정의와 데이터 프로파일','4–19, 27–34',
    '광고 이벤트의 품질과 규모를 측정하고 분석 문제 및 레이크하우스 구조를 정의한다.',[
('데이터 규모와 품질','''df = pd.read_json(DATA / 'events.jsonl', lines=True)
profile = pd.DataFrame({'dtype': df.dtypes.astype(str), 'missing': df.isna().sum(), 'distinct': df.nunique()})
display(profile)
print('행:', len(df), '메모리 bytes:', df.memory_usage(deep=True).sum())
print('중복 event_id:', df.event_id.duplicated().sum())
assert df.event_id.is_unique
out = output_dir('week01')
profile.to_csv(out / 'profile.csv')'''),
('분포와 다중 검정','''rng = np.random.default_rng(42)
tail = rng.pareto(2, 10000) + 1
fig, ax = plt.subplots(1, 2, figsize=(10, 3))
ax[0].hist(tail, bins=80); ax[0].set(xlim=(1, 15), title='Synthetic long tail')
pvalues = rng.uniform(size=(2000, 100))  # 귀무가설이 모두 참인 모의실험
rates = [(pvalues < .05).any(axis=1).mean(), (pvalues < .05 / 100).any(axis=1).mean()]
ax[1].bar(['Unadjusted', 'Bonferroni'], rates); ax[1].set_ylabel('Family-wise error rate')
plt.tight_layout(); plt.show()
save_json(out / 'problem_definition.json', {'question': '상품별 이벤트량과 매출성 value를 어떻게 집계할 것인가?',
 'unit': 'event', 'key': 'event_id', 'time': 'event_time UTC', 'layers': ['Landing', 'Bronze', 'Silver', 'Gold'],
 'note': '합성 데이터이며 value는 실제 매출을 뜻하지 않음'})''')],
    '5V 각각에 측정 지표를 하나씩 제안하세요. Landing→Bronze→Silver→Gold의 입력·출력을 그려 제출하고, 실제 매출 KPI에 필요한 추가 조건을 설명하세요.')

add(2,'distributed_architecture','파티셔닝과 저장 포맷','5–18',
    '키 편향과 파일 포맷을 측정하여 레이크하우스 설계 근거를 만든다.',[
('Range와 Hash 분할','''from labkit.common import stable_hash
df = pd.read_json(DATA / 'events.jsonl', lines=True)
df['range_partition'] = df.id // 600
df['product_partition'] = df.product_id.map(lambda x: stable_hash(x) % 4)
df['event_partition'] = df.event_id.map(lambda x: stable_hash(x) % 4)
counts = pd.DataFrame({c: df[c].value_counts().reindex(range(4), fill_value=0) for c in ['range_partition', 'product_partition', 'event_partition']})
display(counts)
counts.plot.bar(title='Rows per partition'); plt.tight_layout(); plt.show()
assert counts.sum().eq(len(df)).all()'''),
('CSV·JSON·Parquet 비교','''import time
out = output_dir('week02')
base = df.drop(columns=['range_partition', 'product_partition', 'event_partition'])
base.to_csv(out / 'events.csv', index=False)
base.to_json(out / 'events.jsonl', orient='records', lines=True)
base.to_parquet(out / 'events.parquet', index=False, compression='snappy')
report = []
for name, read in [('events.csv', pd.read_csv), ('events.jsonl', lambda p: pd.read_json(p, lines=True)), ('events.parquet', pd.read_parquet)]:
    t = time.perf_counter(); loaded = read(out / name)
    report.append({'format': name, 'bytes': (out / name).stat().st_size, 'read_seconds': time.perf_counter() - t, 'rows': len(loaded)})
    assert len(loaded) == len(base)
display(pd.DataFrame(report))
pd.DataFrame(report).to_csv(out / 'formats.csv', index=False)''')],
    '동일 키의 Hash 분할로 편향이 해결되지 않는 이유를 설명하세요. 네트워크 분할 상황의 CP/AP 선택과 HDFS 복제 수, 날짜 파티셔닝 전략을 포함한 설계서를 제출하세요. 로컬 Parquet는 ACID 레이크하우스 구현이 아닙니다.')

add(3,'apache_spark_basics','Spark RDD·DataFrame·I/O','6–15',
    '지연 실행과 DAG를 확인하고 명시적 스키마로 원천 데이터를 구조화한다.',[
('Spark 시작과 RDD','''from labkit.common import spark_session, event_schema
from pyspark.sql import functions as F
spark = spark_session('week03')
rdd = spark.sparkContext.parallelize(['spark data', 'spark sql', 'data lake'], 2)
counts = rdd.flatMap(lambda s: s.split()).map(lambda s: (s, 1)).reduceByKey(lambda a, b: a + b)
print(counts.toDebugString().decode())  # action 이전에는 lineage만 구성
assert dict(counts.collect()) == {'spark': 2, 'data': 2, 'sql': 1, 'lake': 1}'''),
('스키마와 집계 계획','''df = spark.read.schema(event_schema()).json(str(DATA / 'events.jsonl'))
df.printSchema()
agg = df.groupBy('product_id').agg(F.count('*').alias('events'), F.sum('value').alias('value'))
agg.explain(mode='formatted')
agg.show()
assert agg.agg(F.sum('events')).first()[0] == df.count()
out = output_dir('week03')
df.write.mode('overwrite').parquet(str(out / 'bronze'))
assert spark.read.parquet(str(out / 'bronze')).count() == 2400
spark.stop()''')],
    'select/filter/join/groupBy를 각각 추가하고 narrow/wide 변환을 구분하세요. scripts/00_fetch_to_landing.py와 10_bronze_batch.py를 실행해 원본·스키마·저장 포맷의 차이를 설명하세요.')

add(4,'statistics_fundamentals','분포·가설검정·신뢰구간','4–13',
    'PDF/CDF를 구분하고 효과 크기·불확실성·상관관계를 함께 해석한다.',[
('기술 통계와 확률 분포','''from scipy import stats
rng = np.random.default_rng(42)
samples = {'Normal': rng.normal(0, 1, 2400), 'Exponential': rng.exponential(1, 2400),
           'Pareto': rng.pareto(2, 2400) + 1, 'Poisson': rng.poisson(3, 2400)}
display(pd.DataFrame(samples).describe())
fig, axes = plt.subplots(2, 2, figsize=(10, 6))
for (name, values), ax in zip(samples.items(), axes.flat):
    x = np.sort(values)
    ax.step(x, np.arange(1, len(x)+1)/len(x), where='post'); ax.set(title=name, ylabel='ECDF')
plt.tight_layout(); plt.show()
print('Normal P(-1 <= X <= 1):', stats.norm.cdf(1)-stats.norm.cdf(-1))'''),
('Welch 검정과 평균 차이 구간','''a, b = rng.normal(10, 2, 1000), rng.normal(10.3, 3, 1000)
test = stats.ttest_ind(b, a, equal_var=False)
v1, v2 = b.var(ddof=1)/len(b), a.var(ddof=1)/len(a)
se = np.sqrt(v1+v2)
dof = (v1+v2)**2 / (v1**2/(len(b)-1) + v2**2/(len(a)-1))
delta = b.mean()-a.mean()
ci = delta + np.array([-1,1])*stats.t.ppf(.975,dof)*se
out = output_dir('week04')
save_json(out/'inference.json', {'difference': delta, 'pvalue': test.pvalue, 'ci95': ci.tolist()})
print('차이, p, 95% CI:', delta, test.pvalue, ci)
z = rng.normal(size=2400); x = z+rng.normal(size=2400); y=z+rng.normal(size=2400)
print('공통 원인 z로 만든 x/y 상관:', stats.pearsonr(x,y).statistic)
assert ci[0] < ci[1]''')],
    '기본 실행은 합성 데이터입니다. PDF 과제 제출 시 출처가 있는 실제 관측치 1,000개 이상으로 교체하여 histogram/PDF·ECDF·QQ plot과 통계 리포트를 작성하세요. p-value를 귀무가설이 참일 확률로 해석하지 마세요.')

add(5,'feature_engineering','누수 없는 특성 공학','5–18',
    '학습 세트 안에서 전처리·변수 선택을 학습하고 검증 성능을 비교한다.',[
('기준 모델과 특성 선택','''from labkit.common import ctr_pipeline, split_ctr
from sklearn.metrics import roc_auc_score
from sklearn.feature_selection import SelectFromModel
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
df = pd.read_csv(DATA/'ctr.csv'); train, valid, test = split_ctr(df)
baseline = ctr_pipeline().fit(train, train.clicked)
selected = ctr_pipeline()
selected.steps.insert(1, ('select', SelectFromModel(LogisticRegression(penalty='l1', solver='liblinear', C=.1, random_state=42))))
selected.fit(train, train.clicked)
results = pd.DataFrame([{'model': name, 'validation_auc': roc_auc_score(valid.clicked, m.predict_proba(valid)[:,1])}
                        for name,m in [('baseline',baseline),('L1 selection',selected)]])
display(results)
out = output_dir('week05'); results.to_csv(out/'feature_comparison.csv', index=False)'''),
('Permutation importance와 PCA','''from sklearn.inspection import permutation_importance
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
importance = permutation_importance(baseline, valid, valid.clicked, scoring='roc_auc', n_repeats=3, random_state=42)
display(pd.Series(importance.importances_mean, index=valid.columns).sort_values(ascending=False))
numeric = ['age','hour','bid_price','ad_quality','popularity']
scaler = StandardScaler().fit(train[numeric])
pca = PCA(n_components=2).fit(scaler.transform(train[numeric]))
print('PCA explained variance:', pca.explained_variance_ratio_)
assert pca.transform(scaler.transform(valid[numeric])).shape == (len(valid),2)
assert 'clicked' not in baseline['pre'].get_feature_names_out().tolist()
print('test는 아직 모델 선택에 사용하지 않았습니다.')''')],
    '결측값을 주입하고 imputer 전략 및 L1 강도를 바꿔 검증 AUC·선택 변수 수를 비교하세요. 기존 회귀 실습과 분류 실습의 중요도·평가 지표를 구분하세요.',
    'Feature_Engineering_(AI).ipynb')

add(6,'streaming_algorithms','Reservoir·Bloom·CMS·HLL','8–40',
    '정확 계산을 기준으로 확률적 자료구조의 오차와 저장 공간을 비교한다.',[
('샘플링과 Bloom false positive','''from labkit.sketches import reservoir, BloomFilter, CountMinSketch, HyperLogLog
from collections import Counter
sample = reservoir(iter(range(10000)), 200)
assert len(sample) == len(set(sample)) == 200
seen = list(range(1000)); absent = list(range(1000,11000))
records = []
for m in [4096,8192,16384]:
    bloom = BloomFilter(m=m, k=5)
    for x in seen: bloom.add(x)
    assert all(x in bloom for x in seen)  # false negative 없음
    records.append({'bits': m, 'payload_bytes': len(bloom.bits), 'false_positive_rate': np.mean([x in bloom for x in absent])})
display(pd.DataFrame(records))'''),
('빈도와 고유값 근사','''stream = pd.read_json(DATA/'events.jsonl', lines=True).user_id.tolist()
exact = Counter(stream)
rows = []
for width in [32,128,512]:
    cms = CountMinSketch(width=width)
    for x in stream: cms.add(x)
    errors = [cms.count(x)-v for x,v in exact.items()]
    assert min(errors) >= 0
    rows.append({'width': width, 'mean_overcount': np.mean(errors), 'payload_bytes': width*5*8})
display(pd.DataFrame(rows))
hll = HyperLogLog(p=10)
for x in stream: hll.add(x)
estimate = hll.count(); truth=len(exact)
print('HLL estimate / exact / relative error:', estimate, truth, abs(estimate-truth)/truth)
out=output_dir('week06'); pd.DataFrame(rows).to_csv(out/'cms_errors.csv',index=False)
save_json(out/'hll.json', {'estimate':estimate, 'exact':truth, 'register_bytes':len(hll.registers)})''')],
    '두 알고리즘 이상에서 파라미터·seed·입력 크기를 바꾸어 오차와 처리 시간을 비교하세요. payload bytes와 Python 객체 전체 메모리는 다릅니다. 파티션별 동일 크기 reservoir를 합치면 전체 균등 표본이 되는지 반례를 만드세요.')

add(7,'structured_streaming','파일 스트림·워터마크·복구','7–30',
    '기존 Kafka 메시지 스키마로 유한한 파일 스트림을 실행하고 체크포인트 재시작을 검증한다.',[
('입력과 워터마크 집계','''import uuid
from labkit.common import spark_session, event_schema, make_events
from pyspark.sql import functions as F
spark=spark_session('week07')
out=output_dir('week07')/uuid.uuid4().hex; incoming=out/'incoming'; incoming.mkdir(parents=True)
events=make_events(600)
events.iloc[:300].to_json(incoming/'batch01.json',orient='records',lines=True)
stream=spark.readStream.schema(event_schema()).json(str(incoming))
timed=stream.withColumn('event_time_ts',F.to_timestamp('event_time'))
windowed=(timed.withWatermark('event_time_ts','2 minutes')
 .groupBy(F.window('event_time_ts','5 minutes'), 'product_id').count())
def start_query():
    return (windowed.writeStream.format('parquet').outputMode('append')
     .option('path',str(out/'result')).option('checkpointLocation',str(out/'checkpoint'))
     .trigger(availableNow=True).start())
q=start_query()
try:
    if not q.awaitTermination(120): raise TimeoutError('stream timeout')
    print(q.lastProgress)
finally:
    q.stop()'''),
('워터마크 전진과 동일 체크포인트 재시작','''# 미래 이벤트를 추가해야 이전 window가 append 출력으로 확정된다.
future=make_events(1); future['event_id']='future'; future['event_time']='2026-01-01T00:20:00Z'
future.to_json(incoming/'batch02.json',orient='records',lines=True)
q=start_query()
try:
    if not q.awaitTermination(120): raise TimeoutError('stream timeout')
    save_json(out/'progress.json',q.recentProgress)
finally: q.stop()
result=spark.read.parquet(str(out/'result')); result.show(truncate=False)
before=result.agg(F.sum('count')).first()[0]
assert before == 300
q=start_query()  # 새 파일 없음: 같은 offset을 다시 출력하지 않는다.
try:
    if not q.awaitTermination(120): raise TimeoutError('stream timeout')
finally: q.stop()
assert spark.read.parquet(str(out/'result')).agg(F.sum('count')).first()[0] == before
spark.stop()''')],
    '5분 tumbling을 10분/5분 sliding으로 바꾸고 별도 체크포인트에서 실행하세요. 지연 이벤트를 후속 배치로 넣어 recentProgress의 numRowsDroppedByWatermark를 관찰하세요. 워터마크 밖 데이터는 처리될 수도 있으며 완전성 보장을 벗어납니다.',
    'W07_Spark_Structured_Streaming.ipynb')

add(8,'spark_ml_pipeline','Spark ML 학습·튜닝·저장','6–25',
    '전처리를 포함한 Pipeline을 검증 세트에서 선택하고 저장·재로딩한다.',[
('데이터 분할과 Pipeline','''from labkit.common import spark_session
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
spark=spark_session('week08')
df=spark.read.csv(str(DATA/'ctr.csv'),header=True,inferSchema=True).withColumnRenamed('clicked','label')
train,valid,test=df.randomSplit([.6,.2,.2],seed=42)
indexer=StringIndexer(inputCol='device',outputCol='device_idx',handleInvalid='keep')
encoder=OneHotEncoder(inputCol='device_idx',outputCol='device_vec',handleInvalid='keep')
assembler=VectorAssembler(inputCols=['age','hour','bid_price','ad_quality','popularity','device_vec'],outputCol='raw_features')
scaler=StandardScaler(inputCol='raw_features',outputCol='features')
evaluator=BinaryClassificationEvaluator(metricName='areaUnderROC')
trials=[]; models=[]
for reg in [.01,.1]:
    pipeline=Pipeline(stages=[indexer,encoder,assembler,scaler,LogisticRegression(regParam=reg,maxIter=30)])
    model=pipeline.fit(train); auc=evaluator.evaluate(model.transform(valid))
    trials.append({'regParam':reg,'validation_auc':auc}); models.append(model)
display(pd.DataFrame(trials))'''),
('홀드아웃 평가와 재로딩','''import uuid
best=models[int(np.argmax([r['validation_auc'] for r in trials]))]
out=output_dir('week08')/uuid.uuid4().hex
best.write().save(str(out/'model'))
loaded=PipelineModel.load(str(out/'model'))
auc=evaluator.evaluate(loaded.transform(test))
assert abs(auc-evaluator.evaluate(best.transform(test))) < 1e-10
save_json(out/'metrics.json',{'trials':trials,'test_auc':auc,'spark_version':spark.version})
print('test AUC:',auc)
spark.stop()''')],
    'ParamGridBuilder와 CrossValidator로 학습 세트 내부 CV를 추가하세요. 저장된 PipelineModel.transform을 7주차 readStream의 광고 스키마에 적용하는 흐름을 설계하세요. 스트림 예측과 온라인 학습을 구분하세요.')

add(9,'multi_objective_optimization','CTR 모델과 Pareto 의사결정','6–23',
    '성능·처리 시간·정책 비용의 목적 방향을 통일하고 Pareto 후보를 찾는다.',[
('모델 후보 비교','''import time
from sklearn.metrics import roc_auc_score, precision_score, recall_score
from labkit.common import ctr_pipeline, split_ctr, pareto_mask
df=pd.read_csv(DATA/'ctr.csv'); train,valid,test=split_ctr(df)
rows=[]; models=[]
for c in [.01,.1,1,10]:
    model=ctr_pipeline().set_params(model__C=c).fit(train,train.clicked)
    model.predict_proba(valid)  # warm-up
    times=[]
    for _ in range(5):
        t=time.perf_counter(); pred=model.predict_proba(valid)[:,1]; times.append(time.perf_counter()-t)
    rows.append({'C':c,'auc':roc_auc_score(valid.clicked,pred),'inference_ms':np.median(times)*1000})
    models.append(model)
results=pd.DataFrame(rows)
results['pareto']=pareto_mask(results[['auc','inference_ms']].to_numpy()*[-1,1])
display(results)
assert pareto_mask([[1,2],[1,2],[2,3]]).tolist()==[True,True,False]'''),
('임계값과 정책 비용','''best=models[int(results.auc.argmax())]; probabilities=best.predict_proba(valid)[:,1]
policy=[]
for threshold in np.linspace(.1,.9,9):
    chosen=probabilities>=threshold
    policy.append({'threshold':threshold,'precision':precision_score(valid.clicked,chosen,zero_division=0),
      'recall':recall_score(valid.clicked,chosen), 'total_bid':valid.loc[chosen,'bid_price'].sum(), 'selected':int(chosen.sum())})
policies=pd.DataFrame(policy)
policies['pareto']=pareto_mask(policies[['precision','recall','total_bid']].to_numpy()*[-1,-1,1])
display(policies)
ax=results.plot.scatter(x='inference_ms',y='auc',c=results.pareto.astype(int),colormap='viridis'); plt.show()
out=output_dir('week09'); results.to_csv(out/'model_pareto.csv',index=False); policies.to_csv(out/'threshold_pareto.csv',index=False)
print('test는 최종 정책 확정 후 한 번만 사용합니다.')''')],
    '기존 노트북의 NSGA-II 단계로 이어가세요. 비지배 정렬·crowding·부모/자식 통합 선택을 설명하고 세 seed로 비교하세요. total_bid는 단순 정책 비용 proxy이며 실제 집행 비용이 아닙니다. popularity 표준편차를 공정성 전체로 해석하지 마세요.',
    'week09_ctr_multi_objective_optimization_notebook.ipynb')

add(11,'data_pipeline_practice','레이크하우스 통합과 품질 검증','4–14, 18–30',
    'Landing→Gold 실행, 품질 격리, 재실행 멱등성과 기록을 검증한다.',[
('단계 실행과 멱등성','''from labkit.lakehouse import run_pipeline, clean_events
out=output_dir('week11')
display(pd.DataFrame(run_pipeline(out)))
gold=pd.read_parquet(out/'gold.parquet')
run_pipeline(out)
pd.testing.assert_frame_equal(gold,pd.read_parquet(out/'gold.parquet'))
display(gold)
assert int(gold.events.sum())==2400'''),
('오류 주입과 격리','''raw=pd.read_json(DATA/'events.jsonl',lines=True)
dirty=pd.concat([raw,raw.iloc[:5]],ignore_index=True)
dirty.loc[0,'price']=-1
dirty['event_time']=dirty.event_time.astype(str)
dirty.loc[1,'event_time']='invalid'
clean,rejected=clean_events(dirty)
assert len(rejected)==2
assert clean.event_id.is_unique
assert len(clean)+len(rejected)+dirty.loc[~dirty.index.isin(rejected.index)].event_id.duplicated().sum()==len(dirty)
save_json(out/'injected_quality.json',{'input':len(dirty),'clean':len(clean),'rejected':len(rejected)})
print('격리:',len(rejected),'정제:',len(clean))''')],
    'scripts의 네 단계 파일을 순서대로 실행하고 실행 로그·품질 리포트·계층별 입출력을 제출하세요. 7주차 스트리밍과 batch 정제 조건을 동일하게 맞추고 같은 입력의 결과를 비교하세요. CDC/Delta MERGE는 이 로컬 스냅샷 구현의 확장 과제입니다.')

add(12,'spark_optimization','Spark 파티션·캐시·조인 실험','4–34',
    '동일한 결과를 유지하며 실행 계획과 반복 측정으로 튜닝 효과를 비교한다.',[
('파티션 수와 반복 측정','''import time
from labkit.common import spark_session
from pyspark.sql import functions as F
spark=spark_session('week12')
base=spark.range(100000).withColumn('key',F.when(F.col('id')%10<8,0).otherwise(F.col('id')%100))
spark.conf.set('spark.sql.adaptive.enabled','false')
rows=[]
expected=None
for partitions in [2,4,8]:
    spark.conf.set('spark.sql.shuffle.partitions',str(partitions))
    query=base.groupBy('key').count()
    result=sorted((r.key,r['count']) for r in query.collect())
    if expected is None: expected=result
    assert result==expected
    for repeat in range(3):
        start=time.perf_counter(); query.collect()
        rows.append({'partitions':partitions,'repeat':repeat,'seconds':time.perf_counter()-start})
display(pd.DataFrame(rows).groupby('partitions').seconds.agg(['median','min','max']))'''),
('캐시와 broadcast join 계획','''cached=base.cache(); cached.count()  # 캐시 채움은 warm-up으로 분리
try:
    start=time.perf_counter(); cached.groupBy('key').count().collect()
    print('cached query seconds:',time.perf_counter()-start)
    dimension=spark.range(100).withColumnRenamed('id','key').withColumn('segment',F.col('key')%3)
    joined=cached.join(F.broadcast(dimension),'key')
    joined.explain(mode='formatted')
    assert joined.count()==100000
finally:
    cached.unpersist()
out=output_dir('week12'); pd.DataFrame(rows).to_csv(out/'timings.csv',index=False)
print('Spark UI:',spark.sparkContext.uiWebUrl)
# UI 캡처가 필요하면 다음 stop을 실행하기 전에 확인하세요.
spark.stop()''')],
    'Spark UI에서 Shuffle Read/Write·spill·task 편향을 기록하세요. AQE on/off, 일반 join/broadcast를 동일 입력으로 비교하고 warm-up을 제외한 중앙값을 보고하세요. 작은 로컬 데이터의 속도 향상을 보장하지 않습니다.')

add(13,'mlops_pipeline','실험 기록·모델 버전·배포 판단','4–16, 19–27',
    '모델과 데이터 버전을 기록하고 모의 배포 게이트·드리프트 경보를 설계한다.',[
('실험 기록과 모델 산출물','''import hashlib, pickle, uuid, platform
import sklearn
from labkit.common import ctr_pipeline, split_ctr
from sklearn.metrics import roc_auc_score
df=pd.read_csv(DATA/'ctr.csv'); train,valid,test=split_ctr(df)
model=ctr_pipeline().fit(train,train.clicked)
out=output_dir('week13')/uuid.uuid4().hex; out.mkdir()
auc=roc_auc_score(valid.clicked,model.predict_proba(valid)[:,1])
model_path=out/'model.pkl'
model_path.write_bytes(pickle.dumps(model))
record={'validation_auc':auc,'seed':42,'python':platform.python_version(),'sklearn':sklearn.__version__,
 'data_sha256':hashlib.sha256((DATA/'ctr.csv').read_bytes()).hexdigest(),
 'model_sha256':hashlib.sha256(model_path.read_bytes()).hexdigest(),
 'state':'candidate','model_file':str(model_path)}
save_json(out/'run.json',record)
restored=pickle.loads(model_path.read_bytes())  # 이 실습에서 직접 만든 파일만 로드
assert np.allclose(model.predict_proba(valid),restored.predict_proba(valid))
display(record)'''),
('드리프트·승인·롤백 모의실험','''from scipy.stats import ks_2samp
shifted=valid.copy(); shifted['ad_quality']=(shifted.ad_quality+.3).clip(0,1)
ks=ks_2samp(train.ad_quality,shifted.ad_quality)
quality_pass=auc>=.65  # 수업용 예시 기준, 서비스별 기준을 별도 정의
drift_alert=ks.statistic>.2
human_approved=False
decision='promote' if quality_pass and not drift_alert and human_approved else 'hold'
save_json(out/'deployment_decision.json',{'quality_pass':quality_pass,'ks_statistic':ks.statistic,
 'drift_alert':bool(drift_alert),'human_approved':human_approved,'decision':decision,'rollback_to':'previous_champion'})
assert decision=='hold'
print('모의 배포 결정:',decision,'/ 입력 드리프트는 성능 저하 자체의 증거는 아닙니다.')''')],
    '수집→검증→학습→등록→승인→배포→모니터링 DAG를 작성하세요. MLflow로 run.json의 파라미터·지표·모델을 옮기고 Canary 비율, p95 지연·오류율·성능 경보 및 롤백 기준을 설계하세요. 기본 실습은 로컬 모의 실행이며 실제 서비스 배포가 아닙니다.')

def main():
    mapping=[]
    for lab in LABS:
        week=lab['week']; name=f"week{week:02}_{lab['slug']}_lab.ipynb"
        cells=[]
        def md(text): cells.append({'cell_type':'markdown','metadata':{},'source':text.splitlines(True)})
        def code(text): cells.append({'cell_type':'code','metadata':{},'source':textwrap.dedent(text).strip().splitlines(True),'execution_count':None,'outputs':[]})
        source=f"week{week:02}_{lab['slug']}.pdf"
        md(f"# {week:02}주차 실습: {lab['title']}\n\n강의: [PDF](../pdf/{source}) · **PDF 실제 페이지 {lab['pages']}** (인쇄 번호와 다를 수 있음)\n\n## 학습 목표\n{lab['goal']}\n\n권장 구성: 개념 확인 10분 → 코드 실행 30분 → 변경 실험 30분 → 결과 토의 20분.\n설치는 README.md를 따릅니다. 새 커널에서 위에서 아래로 실행하세요.")
        if lab['legacy']: md(f"## 기존 실습 연결\n기본 실습 후 [{lab['legacy']}]({lab['legacy']})로 이어갑니다. 호환 방법과 수정 사항은 COMPATIBILITY.md를 확인하세요.")
        code(BOOT)
        for title,body in lab['steps']:
            md('## '+title); code(body)
        md('## 확장 과제 및 제출\n'+lab['assignment']+'\n\n제출: 실행한 노트북, 결과 표/그림, 변경한 조건과 해석. outputs/weekXX에 저장된 자료를 첨부하세요.\n\n평가 기준: 개념 연결 25%, 실행·재현성 30%, 비교 실험 25%, 해석·한계 20%.')
        for i,c in enumerate(cells): c['id']=f'w{week:02}-cell-{i:02}'
        nb={'cells':cells,'metadata':{'kernelspec':{'display_name':'Python 3','language':'python','name':'python3'},'language_info':{'name':'python'},'bigdata_lab':{'week':week,'source_pdf':source,'source_pages':lab['pages'],'requires_spark':week in [3,7,8,12]}},'nbformat':4,'nbformat_minor':5}
        (ROOT/name).write_text(json.dumps(nb,ensure_ascii=False,indent=1),encoding='utf-8')
        mapping.append({k:v for k,v in lab.items() if k!='steps'}|{'notebook':name,'source_pdf':source})
    (ROOT/'curriculum.json').write_text(json.dumps(mapping,ensure_ascii=False,indent=2),encoding='utf-8')
    print('Generated',len(LABS),'notebooks')
if __name__=='__main__': main()
