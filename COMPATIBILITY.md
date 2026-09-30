# 기존 실습과의 호환

원본 3개는 내용과 파일명을 보존했습니다. `compatible/`에는 필요한 실행 수정을 반영한 사본을 제공합니다. 원본 해시는 `compatible/original_sha256.json`에 기록했습니다. 사본 생성기는 `scripts/prepare_legacy.py`입니다. 사본은 기존 수업 내용을 유지하므로 전체 실행 가능 여부는 아래 환경·데이터 조건을 따릅니다.

| 원본 | 새 진입점 | 사본의 변경 | 남은 실행 조건 |
|---|---|---|---|
| Feature_Engineering_(AI).ipynb | week05_feature_engineering_lab.ipynb | HOUSING_CSV 경로 지원, TSNE max_iter | 실제 California Housing CSV 제공 |
| W07_Spark_Structured_Streaming.ipynb | week07_structured_streaming_lab.ipynb | PySpark 3.5.3 고정, Kafka connector 3.5.3/Scala 2.12 일치 | Linux/Colab, Java, Kafka broker 및 포트 |
| week09_ctr_multi_objective_optimization_notebook.ipynb | week09_multi_objective_optimization_lab.ipynb | 코드 보존, 출력 초기화, 안내 추가 | requirements.txt 환경 |

## Feature Engineering

Colab에 `/content/sample_data/california_housing_train.csv`가 있으면 그대로 사용합니다. 로컬에서는 사본 첫 코드 셀 전에 다음을 실행합니다.

```python
import os
os.environ['HOUSING_CSV'] = r'C:\data\california_housing_train.csv'
```

필요 열: longitude, latitude, housing_median_age, total_rooms, total_bedrooms, population, households, median_income, median_house_value. 이 데이터와 새 CTR 분류 데이터는 다른 데이터입니다. 새 노트북은 외부 주택 데이터가 없어도 진행할 수 있습니다.

주의할 해석: 원본 회귀 트리의 기본 `feature_importances_`는 분산/MSE 감소량 기반으로, Gini 또는 entropy 기반 분류 중요도와 다릅니다. 원본은 탐색 예제이며 전체 자료로 학습한 중요도를 홀드아웃 성능으로 해석하면 안 됩니다. 새 5주차에서 학습/검증/테스트 분리와 전처리 누수 방지를 실습합니다.

## Structured Streaming

기존 사본의 Kafka 설치 셀은 셸 명령을 포함한 Linux/Colab 전용입니다. Windows에서 그대로 실행하지 말고 새 파일 소스 실습을 사용합니다. 사본을 실행할 때는 새 커널에서 PySpark 3.5.3 설치 → 커널 재시작 → Kafka/Zookeeper 시작 → connector 포함 SparkSession 생성 → Producer → Consumer 순서입니다.

기존 메시지 스키마와 새 `data/events.jsonl`의 공통 필드:

| 타입 | 필드 |
|---|---|
| string | event_id, user_id, product_id, event_type, message, event_time, processing_hint_time |
| integer | id, quantity |
| double | timestamp, value, price |

`event_time`은 ISO 8601 UTC 문자열이고 Spark에서 `to_timestamp`로 변환합니다. `timestamp`는 epoch seconds입니다. `processing_hint_time`은 producer가 기록한 시간이며 Spark 처리 시각 자체가 아닙니다. `impressions`·`clicks` 토픽은 별도 스키마이고 events 파일로 대체하지 않습니다.

새 실습은 파일 소스+Parquet sink+체크포인트 복구를 사용합니다. 모든 쿼리는 시간 제한과 `finally: q.stop()`을 갖습니다. 워터마크는 관측된 이벤트 시간이 전진해야 갱신되며, 시간만 기다린다고 이전 window가 확정되지 않습니다. 워터마크 밖 이벤트는 항상 즉시 버려지는 것이 아니라 보장 범위 밖입니다. Exactly-once는 source/sink/체크포인트 조건을 함께 봐야 하며 임의의 foreachBatch 외부 쓰기에 자동 보장되지 않습니다.

Kafka 확장에서는 기존 사본의 `reset_path`가 실습 체크포인트를 삭제하므로 복구 실험 중에는 호출하지 마세요. `awaitTermination(120)`은 시간 초과 시 쿼리를 자동 중지하지 않으므로 실험 종료 셀에서 자신이 시작한 `query.stop()`을 호출하세요.

## CTR 다목적 최적화

새 `ctr.csv`는 기존 노트북과 동일한 열 이름을 제공합니다. 기존 노트북의 자체 데이터 생성 셀 대신 다음을 넣으면 새 기본 실습과 같은 데이터를 사용할 수 있습니다.

```python
from pathlib import Path
import pandas as pd
# compatible 폴더에서 Jupyter를 시작한 경우
df = pd.read_csv(Path('..') / 'data' / 'ctr.csv')
```

기존 생성 셀에서 만드는 추가 변수에 의존하는 코드는 자체 생성 경로를 유지하세요. 기본 권장은 새 진입점에서 Pareto 개념을 확인한 다음 원본의 NSGA-II 실습을 자체 데이터로 실행하는 것입니다. 무작위 시드와 데이터 분포가 다르면 결과 수치도 달라집니다.

새 기본 실습은 검증 세트로 모델·임계값을 비교하고 test를 최종 선택 이후에 사용하도록 분리했습니다. 기존 노트북을 확장할 때도 같은 규칙을 적용하세요. `popularity` 표준편차는 정책 분산의 대리 지표이며 보호 집단에 대한 공정성 지표가 아닙니다.
