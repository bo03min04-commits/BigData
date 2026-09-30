from pathlib import Path
import hashlib
import json
import os
import sys
import tempfile
import uuid
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'

def output_dir(week):
    """공유 드라이브의 Spark 파일 잠금 문제를 피하도록 출력 위치 변경 가능."""
    base = Path(os.environ.get('BIGDATA_OUTPUT_DIR', str(ROOT / 'outputs')))
    path = base / week
    path.mkdir(parents=True, exist_ok=True)
    return path

def save_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding='utf-8')

def make_ctr(n=2400, seed=42):
    """기존 Week09와 같은 열 이름을 갖는 교육용 합성 광고 데이터."""
    rng = np.random.default_rng(seed)
    campaign = rng.integers(0, 30, n)
    quality = rng.uniform(0, 1, n)
    device = rng.choice(['mobile', 'desktop', 'tablet'], n)
    age = rng.integers(18, 70, n)
    hour = rng.integers(0, 24, n)
    logit = -2.2 + 2.6 * quality + .4 * (device == 'mobile') - .012 * (age - 35)
    return pd.DataFrame({
        'age': age, 'device': device, 'hour': hour,
        'day_of_week': rng.integers(0, 7, n),
        'ad_category': np.array(['tech', 'fashion', 'food'])[campaign % 3],
        'campaign_id': [f'campaign_{i:02}' for i in campaign],
        'bid_price': rng.uniform(.1, 3, n).round(4),
        'ad_quality': quality, 'popularity': rng.uniform(0, 1, n),
        'clicked': rng.binomial(1, 1 / (1 + np.exp(-logit)))})

def make_events(n=2400, seed=42):
    """기존 W07 events 토픽의 12개 필드 및 타입을 유지."""
    rng = np.random.default_rng(seed)
    times = pd.date_range('2026-01-01', periods=n, freq='s', tz='UTC')
    price = rng.uniform(5, 100, n).round(2)
    qty = rng.integers(1, 5, n)
    return pd.DataFrame({
        'event_id': [f'e{i:06}' for i in range(n)], 'id': np.arange(n),
        'user_id': [f'u{i}' for i in rng.integers(0, 300, n)],
        'product_id': rng.choice(['p1', 'p2', 'p3'], n, p=[.75, .15, .1]),
        'event_type': rng.choice(['view', 'click', 'purchase'], n),
        'message': ['synthetic teaching event'] * n,
        'event_time': times.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'processing_hint_time': (times + pd.to_timedelta(rng.integers(0, 120, n), unit='s')).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'timestamp': times.astype('int64') / 1e9,
        'value': (price * qty).round(2), 'quantity': qty, 'price': price})

def ensure_data():
    DATA.mkdir(exist_ok=True)
    if not (DATA / 'ctr.csv').exists():
        make_ctr().to_csv(DATA / 'ctr.csv', index=False)
    if not (DATA / 'events.jsonl').exists():
        make_events().to_json(DATA / 'events.jsonl', orient='records', lines=True)
    return DATA

def spark_session(name):
    # 과거 설치의 환경변수가 남아 있으면 pip로 설치한 Spark 경로를 사용한다.
    import pyspark
    configured = os.environ.get('SPARK_HOME')
    launcher = 'spark-submit.cmd' if os.name == 'nt' else 'spark-submit'
    if configured and not (Path(configured) / 'bin' / launcher).exists():
        os.environ['SPARK_HOME'] = str(Path(pyspark.__file__).resolve().parent)
    java_home = os.environ.get('JAVA_HOME')
    if java_home and not (Path(java_home) / 'bin' / ('java.exe' if os.name == 'nt' else 'java')).exists():
        import shutil
        if shutil.which('java'):
            del os.environ['JAVA_HOME']  # Spark launcher가 PATH의 Java를 사용
    os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')
    os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master('local[2]').appName(name)
        .config('spark.sql.shuffle.partitions', '4')
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.warehouse.dir', Path(tempfile.gettempdir(), 'bigdata_warehouse').as_uri())
        .getOrCreate())
    session.sparkContext.setLogLevel('ERROR')
    return session

def event_schema():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    ints = {'id', 'quantity'}
    doubles = {'timestamp', 'value', 'price'}
    return StructType([StructField(c, IntegerType() if c in ints else DoubleType() if c in doubles else StringType(), True)
                       for c in make_events(1).columns])

def ctr_pipeline():
    from sklearn.compose import ColumnTransformer
    from sklearn.preprocessing import OneHotEncoder, StandardScaler
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.linear_model import LogisticRegression
    numeric = ['age', 'hour', 'day_of_week', 'bid_price', 'ad_quality', 'popularity']
    categorical = ['device', 'ad_category', 'campaign_id']
    pre = ColumnTransformer([
        ('num', Pipeline([('impute', SimpleImputer()), ('scale', StandardScaler())]), numeric),
        ('cat', OneHotEncoder(handle_unknown='ignore'), categorical)])
    return Pipeline([('pre', pre), ('model', LogisticRegression(max_iter=1000, random_state=42))])

def split_ctr(df):
    from sklearn.model_selection import train_test_split
    train, rest = train_test_split(df, test_size=.4, stratify=df.clicked, random_state=42)
    valid, test = train_test_split(rest, test_size=.5, stratify=rest.clicked, random_state=42)
    return train, valid, test

def pareto_mask(costs):
    """모든 열 최소화. 동률 후보는 서로 지배하지 않는다."""
    costs = np.asarray(costs, dtype=float)
    if costs.ndim != 2 or not np.isfinite(costs).all():
        raise ValueError('costs must be a finite 2D array')
    return np.array([not np.any(np.all(costs <= row, axis=1) & np.any(costs < row, axis=1)) for row in costs])

def stable_hash(value, seed=0):
    return int.from_bytes(hashlib.blake2b(f'{seed}:{value}'.encode(), digest_size=8).digest(), 'big')
