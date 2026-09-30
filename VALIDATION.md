# 실행 검증 기록

검증일: 2026-09-14

## 새 주차별 실습

**12개 모두 통과**: 01, 02, 03, 04, 05, 06, 07, 08, 09, 11, 12, 13.

검증 환경: 설치된 Ubuntu(WSL), Python 3.10.6, OpenJDK 11.0.17, PySpark 3.5.3, NumPy 1.26.4, pandas 2.2.3, scikit-learn 1.5.2, scipy 1.14.1, matplotlib 3.9.2, pyarrow 18.1.0.

`scripts/verify_labs.py --spark`로 각 노트북을 독립 네임스페이스에서 실행했습니다. 코드 셀을 수정하거나 생략하지 않았습니다. 노트북 구조 검증과 모든 assert를 포함합니다. [실행 결과 JSON](verification/last_run.json)에 주차별 결과와 소요 시간을 기록했습니다.

확인한 주요 동작:

- Spark RDD word count와 DataFrame 집계 결과 일치, Parquet 읽기/쓰기.
- 스트리밍 window 확정 출력 300건과 같은 체크포인트 재시작 후 중복 출력 없음.
- Spark ML 저장·재로딩 전후 AUC 일치.
- Bloom false negative 없음, CMS 과소 추정 없음, Pareto 동률/지배 관계 처리.
- Landing→Gold 재실행 결과 일치 및 잘못된 입력 2건 격리.
- 파티션별 집계 결과 일치, broadcast join의 행 수 보존.
- 로컬 모델 직렬화 전후 예측 일치와 모의 배포 hold 조건.

Windows Python 3.12에서도 일반 Python 실습 8개가 통과했습니다. 이 컴퓨터의 Windows Spark 환경에서는 과거 Spark·Java 경로와 Python worker 종료 문제가 확인되었습니다. 없는 경로에 대한 방어 처리를 공통 세션에 추가했지만 **Windows 네이티브 Spark 전체 실행을 통과했다고 간주하지 않습니다.** 수업용 Spark 실행 경로는 검증된 WSL2/Linux를 따릅니다.

이 검증은 Jupyter 화면의 UI 자동화, 실제 Kafka broker 운영, 실제 주택 데이터 실습, MLflow 서버 또는 서비스 배포를 포함하지 않습니다. 그림 셀은 비대화형 backend에서 실행했으며 경고가 발생할 수 있으나 계산 실패는 아닙니다.

## 기존 자료와 문서

- 기존 3개 노트북과 저장한 SHA-256 해시 일치 확인.
- 새 노트북 12개 및 호환 사본 3개의 nbformat 구조 확인.
- 새 노트북의 PDF 링크 대상 12개 존재 확인.
- 원본 수정 없이 환경/경로 호환 사본 생성.
- 기존 Week09 호환 사본의 모든 코드 셀 실행 통과(기존 12,000행 데이터 및 NSGA-II 25세대 설정 유지, Windows Python 3.12).
- README의 `scripts/run_stage.py --stage all` 실행 통과: Landing/Bronze/Silver 각 2,400행, Gold 3행.

## Marp

20장 전체를 Marp CLI로 HTML과 PNG로 변환하고 배치를 확인했습니다. 확인 중 발견한 강조 표시와 어두운 표지의 코드 색상을 수정한 뒤 다시 렌더링했습니다. 최종 제공 파일은 수정 가능한 `실습개요.marp.md`와 발표용 `실습개요.html`입니다.
