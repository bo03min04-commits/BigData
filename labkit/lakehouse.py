"""로컬 Parquet 레이크하우스: 교육용 배치 스냅샷, ACID 테이블은 아님."""
from pathlib import Path
import hashlib
import shutil
import time
import pandas as pd
from .common import DATA, ensure_data, save_json

def clean_events(frame):
    data = frame.copy()
    data['event_time'] = pd.to_datetime(data.event_time, utc=True, errors='coerce')
    valid = (data.event_id.notna() & data.event_time.notna()
             & data.price.gt(0) & data.quantity.gt(0))
    rejected = data.loc[~valid].copy()
    clean = data.loc[valid].drop_duplicates('event_id').copy()
    clean['event_date'] = clean.event_time.dt.strftime('%Y-%m-%d')
    return clean, rejected

def run_stage(stage, base):
    base = Path(base)
    base.mkdir(parents=True, exist_ok=True)
    start = time.perf_counter()
    rows = 0
    if stage == 'landing':
        ensure_data()
        target = base / 'landing.jsonl'
        if target.exists() and target.read_bytes() != (DATA / 'events.jsonl').read_bytes():
            raise ValueError('Landing 원본이 다릅니다. 새 실행 디렉터리를 선택하세요.')
        if not target.exists():
            shutil.copyfile(DATA / 'events.jsonl', target)
        rows = len(pd.read_json(target, lines=True))
    elif stage == 'bronze':
        data = pd.read_json(base / 'landing.jsonl', lines=True)
        target = base / 'bronze.parquet'
        if target.exists():
            pd.testing.assert_frame_equal(pd.read_parquet(target), data)
        else:
            data.to_parquet(target, index=False)
        rows = len(data)
    elif stage == 'silver':
        raw = pd.read_parquet(base / 'bronze.parquet')
        clean, rejected = clean_events(raw)
        clean.to_parquet(base / 'silver.parquet', index=False)
        rejected.to_parquet(base / 'rejected.parquet', index=False)
        rows = len(clean)
        save_json(base / 'quality.json', {'input': len(raw), 'output': rows,
            'invalid': len(rejected), 'duplicates': len(raw) - len(rejected) - rows})
    elif stage == 'gold':
        clean = pd.read_parquet(base / 'silver.parquet')
        gold = clean.groupby(['event_date', 'product_id'], as_index=False).agg(
            events=('event_id', 'size'), total_value=('value', 'sum'))
        gold.to_parquet(base / 'gold.parquet', index=False)
        rows = len(gold)
    else:
        raise ValueError(stage)
    record = {'stage': stage, 'rows': rows, 'seconds': time.perf_counter() - start}
    with (base / 'run_log.jsonl').open('a', encoding='utf-8') as f:
        import json
        f.write(json.dumps(record) + '\n')
    return record

def run_pipeline(base):
    return [run_stage(s, base) for s in ['landing', 'bronze', 'silver', 'gold']]
