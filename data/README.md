# 공통 교육 데이터

두 파일은 `labkit/common.py`에서 NumPy seed 42로 생성한 합성 데이터입니다. 관측치 수는 각 2,400개입니다. 외부 고객·주택 데이터가 포함되지 않습니다.

## events.jsonl

각 행은 이벤트 1개입니다. event_id는 고유 키, id는 정수 일련번호, user_id와 product_id는 합성 식별자입니다. event_type은 view/click/purchase, message는 설명 문자열입니다. event_time과 processing_hint_time은 UTC ISO 8601 문자열이고 timestamp는 epoch seconds입니다. quantity는 정수 수량, price는 합성 단가, value는 price×quantity입니다. 실제 매출 지표로 해석하지 않습니다.

W07의 events JSON 필드와 이름·타입을 맞췄습니다. clicks와 impressions 토픽은 별도 데이터 구조입니다. 오류 데이터는 기본 파일에 저장하지 않고 11주차 실행 중 주입합니다.

## ctr.csv

age, device, hour, day_of_week, ad_category, campaign_id, bid_price, ad_quality, popularity, clicked 열을 가집니다. clicked는 0/1 레이블입니다. quality·device·age에 의존하는 확률로 생성하며 나머지 변수는 혼합된 맥락/잡음입니다. campaign_id는 30개 캠페인 중 하나입니다. 기존 Week09 노트북의 컬럼명과 호환되지만 생성 분포는 다릅니다.

훈련·검증·테스트 분할은 고정 seed 42입니다. 모델 비교를 위해 test를 반복 조회하지 않습니다.
