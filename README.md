# 빅데이터분석 실습

현재 `강의자료/pdf`의 12개 강의에 대응하는 실습입니다. **1–9, 11–13주차**를 다루며, 10주차는 PDF가 없어 만들지 않았습니다. PDF 실제 페이지를 기준으로 연결했습니다. 1주차 후반과 2주차의 아키텍처 내용은 일부 겹칩니다.

## 시작 방법

1. `실습코드` 폴더 전체를 로컬 작업 폴더로 복사합니다. 노트북만 복사하면 `labkit`과 데이터 경로가 누락됩니다.
2. Python 3.10–3.12 가상환경을 만들고 아래 명령을 실행합니다. 기본 실습에는 외부 데이터 다운로드나 API 키가 필요하지 않습니다.
3. Jupyter에서 해당 `weekXX_..._lab.ipynb`를 열고 **커널 재시작 → 모두 실행**합니다.

```sh
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
# Linux/macOS: source .venv/bin/activate
python -m pip install -r requirements.txt
python -m jupyterlab
```

**Spark 주차(03·07·08·12)**는 Java 11 또는 17과 아래 추가 설치가 필요합니다. Windows의 Hadoop 네이티브 파일 처리 문제를 피하려면 WSL2/Linux 환경에서 폴더 전체를 열어 실행하세요. Java의 실제 설치 디렉터리를 `JAVA_HOME`으로 지정합니다. 오래된 `SPARK_HOME`이 남아 있으면 제거하거나 설치 버전에 맞게 수정하세요.

```sh
python -m pip install -r requirements-spark.txt
java -version
python -c "import pyspark; print(pyspark.__version__)"
```

기준 환경은 PySpark **3.5.3 / Scala 2.12**입니다. 기존 W07의 Kafka 확장은 Linux/Colab에서 별도 진행합니다. Spark 4.x가 이미 실행 중인 커널에서 패키지만 바꾸지 말고 새 환경과 커널을 사용하세요.

Colab에서는 `실습코드` 전체 폴더를 업로드하거나 Drive에 둔 뒤 `%cd`로 그 폴더에 이동하고 `%pip install -r requirements-spark.txt`를 실행합니다. 환경 설치 후 런타임을 재시작합니다. 한 노트북만 업로드하는 방식은 지원하지 않습니다.

## 주차별 구성

| 주차 | 실행 노트북 | 핵심 실험 | 결과 |
|---|---|---|---|
| 01 | [개요](week01_bigdata_overview_lab.ipynb) | 데이터 프로파일·다중 검정 | profile.csv, 문제 정의 |
| 02 | [분산 아키텍처](week02_distributed_architecture_lab.ipynb) | 키 편향·CSV/JSON/Parquet | formats.csv |
| 03 | [Spark 기초](week03_apache_spark_basics_lab.ipynb) | RDD·실행 계획·Parquet | bronze 디렉터리 |
| 04 | [통계](week04_statistics_fundamentals_lab.ipynb) | ECDF·Welch 검정·95% CI | inference.json |
| 05 | [특성 공학](week05_feature_engineering_lab.ipynb) | L1 선택·Permutation·PCA | feature_comparison.csv |
| 06 | [스트리밍 알고리즘](week06_streaming_algorithms_lab.ipynb) | Reservoir·Bloom·CMS·HLL | 오차 비교표 |
| 07 | [Structured Streaming](week07_structured_streaming_lab.ipynb) | 워터마크·append·체크포인트 복구 | result, progress.json |
| 08 | [Spark ML](week08_spark_ml_pipeline_lab.ipynb) | 학습/검증/테스트·튜닝·저장 | PipelineModel, metrics.json |
| 09 | [다목적 최적화](week09_multi_objective_optimization_lab.ipynb) | CTR·Pareto·임계값 | 모델/정책 Pareto 표 |
| 11 | [통합 파이프라인](week11_data_pipeline_practice_lab.ipynb) | 계층별 적재·오류 격리·멱등성 | Parquet, 품질·실행 로그 |
| 12 | [성능 튜닝](week12_spark_optimization_lab.ipynb) | 파티션·캐시·broadcast | timings.csv |
| 13 | [MLOps](week13_mlops_pipeline_lab.ipynb) | 실험/모델 해시·드리프트·배포 게이트 | 모델, run/decision JSON |

각 노트북은 학습 목표 → 실행 예제 → assert 확인 → 변경 실험 → 제출 과제 순서입니다. 기본 실습을 먼저 실행하고 기존 노트북을 심화 자료로 사용합니다. 강의의 모든 도구를 설치하는 구성은 아니며, **Kafka·NSGA-II는 기존 실습과 연결하고, CDC/Delta·CV·MLflow·오케스트레이션은 명시된 확장 과제로 구분**합니다.

## 파일과 데이터 규약

- 기존 3개 노트북은 보존했습니다. [호환 안내](COMPATIBILITY.md)와 `compatible/` 사본을 확인하세요.
- `data/events.jsonl`: 2,400개 합성 이벤트. W07 `events` 토픽 필드 이름과 타입에 맞춥니다.
- `data/ctr.csv`: 2,400개 합성 광고 관측치. 기존 Week09 열 이름에 맞추며 생성 분포까지 동일하지는 않습니다.
- `labkit/`: 공통 데이터, Spark 세션, 확률적 자료구조, 레이크하우스 코드.
- `scripts/`: PDF의 단계별 스크립트 이름에 맞춘 실행 파일과 유지보수 도구.
- `outputs/weekXX/`: 결과 저장. 재실행하면 해당 결과 파일이 갱신됩니다. 스트리밍과 모델 저장은 실행별 하위 폴더를 만듭니다.
- `curriculum.json`: PDF·페이지·목표·과제·기존 실습 연결을 정리한 기계 판독용 목차.

공유 드라이브 대신 로컬 디스크에 결과를 쓰려면 시작 전에 `BIGDATA_OUTPUT_DIR`를 설정하세요.

```powershell
$env:BIGDATA_OUTPUT_DIR = "$env:LOCALAPPDATA/bigdata-lab/outputs"
```

합성 데이터는 교육용이며 실제 고객·광고 성능이나 일반화 성능을 나타내지 않습니다. **4주차 PDF의 실제 데이터 1,000개 이상 과제**는 출처가 있는 데이터로 교체해야 완료됩니다. 생성 seed는 42입니다. 데이터 재생성은 `labkit.common.make_events`, `make_ctr`를 호출해 새 경로에 저장하세요.

## 단계별 파이프라인 실행

`실습코드` 폴더에서 실행합니다. 이 기본 구현은 pandas/Parquet 기반의 로컬 배치 스냅샷이며 HDFS·Delta ACID를 대신 구현하지 않습니다. Spark I/O는 3주차, 스트림은 7주차에서 별도로 실습합니다.

```sh
python scripts/00_fetch_to_landing.py
python scripts/10_bronze_batch.py
python scripts/20_silver_batch.py
python scripts/30_gold_batch.py
# 또는 전체 단계
python scripts/run_stage.py --stage all
```

Landing/Bronze가 이미 있으면 동일 입력인지 검사하고 재사용합니다. 원본이 바뀌면 `--output`으로 새 경로를 사용하세요. Silver/Gold는 재생성합니다. 실행 로그는 누적되지만 데이터는 중복 적재되지 않습니다.

## 개요 슬라이드

[실습 개요 Marp](../실습개요.marp.md)를 Marp for VS Code에서 열어 미리보기합니다. HTML도 함께 제공됩니다. 수정 후 Marp CLI로 다시 변환할 수 있습니다.

```sh
npx @marp-team/marp-cli ../실습개요.marp.md -o ../실습개요.html
```

## 검증과 수업 운영

```sh
python scripts/verify_labs.py
python scripts/verify_labs.py --spark --week 7
```

검증기는 노트북 구조를 검사하고 새 Python 네임스페이스에서 모든 코드 셀을 순서대로 실행합니다. Jupyter 화면 자체를 시험하는 도구는 아닙니다. 결과는 `outputs/verification.json` 및 주차별 로그에 남습니다. 환경별 실제 확인 범위는 [검증 기록](VALIDATION.md)을 참조하세요.

수업 권장 배분은 90분: 개념 10분 / 기본 실행 30분 / 조건 변경 30분 / 토의 20분입니다. 기본 실습의 assert가 통과한 뒤 확장 과제를 수행합니다. 제출 기준은 개념 연결 25%, 재현성 30%, 비교 실험 25%, 해석과 한계 20%입니다.

버전/동작 참고: [Spark 3.5 설치 안내](https://spark.apache.org/docs/3.5.6/api/python/getting_started/install.html), [Structured Streaming](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html), [Marp](https://marp.app/). 참고 문서의 패치 버전은 3.5.6이며 실습 의존성 고정은 3.5.3입니다.
