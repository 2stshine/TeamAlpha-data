"""silver 계층 — 로컬 bronze 를 읽어 정규화해 RDS 에 적재.

구현:
  - asset/assets_identifier: KRX 티커 + bronze corpCode.xml 기반 DART corp_code 매핑
  - price_daily: 주식/지수 일봉, 가격수정 adj_close 계산
  - fundamental: DART 주요계정 long 정규화 + PIT available_date 계산
  - backfill: quality_stage 파티션 검사 후 전체 Silver 원자적 승격
  - incremental(day): 후보 품질 검사 후 해당 날짜 가격·변경 재무를 원자적 반영
"""
