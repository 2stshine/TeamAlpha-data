# Silver quality gate

Silver는 이 폴더의 규칙을 통과한 데이터만 담는다. 모든 적재는 규칙 실행을 거치며
결과는 `dq_run`·`dq_result`에 기록된다. 원천(KRX·DART) 오류는 임의로 고치지 않고
보존·플래그하며, `0 → NULL` 정규화나 명시적 제외처럼 결정적인 변환은
`MODIFIED`로 기록한다. FMP는 별도의 source-aware gate를 거치며 ETF/fund·비주식
상품 제외 건수도 `MODIFIED`에 남긴다. 제외된 원문 행은 Bronze에서 삭제하지 않는다.

인증된 일별 증분의 warning은 `dq_warning_state`에 누적된다. 동일한 실행 모드·
대상일(또는 명시적 파티션)·데이터셋·규칙을 다시 검사해 PASS가 나올 때만
`RESOLVED`가 되며, 현재 미해결 항목은 `dq_open_warning`에서 조회한다. 실패하여
Silver에 publish되지 않은 실행과 전체 감사는 이 상태를 변경하지 않는다.
최초 마이그레이션은 기존 인증된 증분 이력도 같은 기준으로 초기화한다.

FMP 편입 범위는 NASDAQ·NYSE·AMEX의 common stock, preferred stock, ADR,
REIT이다. ETF, fund, ETN, warrant, unit, listed note, 분류가 모호한 행은 제외하고
사유별 건수와 표본을 DQ 결과에 남긴다. USD 가격·재무와 `USDKRW` FX도 동일한
source-aware 검사를 거친다.

FMP commodities는 금융선물과 micro 중복을 제외한 28개 물리 원자재 연속선물만
허용한다. `USX`는 USD로 단위 변환하며, 선물 가격은 음수가 가능하므로 양수 대신
유한값과 OHLC 순서를 검사한다. 20% 초과 일간 변동은 원천을 보존한 채 잠재적
롤오버 warning으로 누적한다. 일요일 날짜의 야간 선물 세션은 유지하고, 토요일
원천행은 Bronze에만 남긴 뒤 Silver 제외 사실을 `MODIFIED`로 기록한다.

FMP 전용 핵심 규칙은 다음과 같다.

| 규칙 | 등급 | 의미 |
|---|---|---|
| `FMP_REQUIRED_UNIVERSE` | Critical | 편입 가능한 미국 주식 유니버스가 비어 있지 않음 |
| `FMP_SILVER_UNIVERSE` | Critical | NASDAQ·NYSE·AMEX의 비 ETF/fund 주식성 자산만 편입 |
| `FMP_PRICE_OHLC` | Error | 복원 OHLC 범위와 양수 조건 |
| `FMP_EXPECTED_DAILY_PRICE` | Critical | 개장일에 편입 미국 주식 가격이 최소 1행 존재 |
| `FMP_DAILY_PRICE_COVERAGE_FLOOR` | Error | 일별 편입 주식 행 수 ≥ 최근 baseline median의 50% (부분 eod-bulk 세션 차단, 개장일 한정) |
| `FMP_*_IDENTIFIER_MAPPING` | Critical | 가격·재무·기업행사가 편입 자산에 매핑됨 |
| `FMP_SILVER_UNIVERSE_EXCLUDED` | Modified | Bronze에 보존된 제외 행의 사유·건수 |
| `FMP_COMMODITY_UNIVERSE_COMPLETE` | Critical | 물리·비 micro 원자재 28개가 정확히 존재 |
| `FMP_COMMODITY_PROVIDER_LIST` | Critical | FMP 목록의 심볼·원천 통화가 allowlist와 일치 |
| `FMP_COMMODITY_PRICE_SEMANTICS` | Error | USD 단위, `adj_close=close`, 주식 전용 필드 NULL |
| `FMP_COMMODITY_WEEKDAY` | Error | 일~금 세션 날짜만 허용하고 토요일 행 차단 |
| `FMP_COMMODITY_NON_SESSION_EXCLUDED` | Modified | Bronze의 토요일 원천행을 Silver에서 제외한 건수 |
| `FMP_COMMODITY_POSSIBLE_ROLL` | Warning | 절대 일간 변동 20% 초과, 원천값 보존 |

**등급**

- **Critical / Error** — publish 차단.
- **Warning** — 비차단. 사람이 검토해야 할 미해결 이상.
- **Modified** — 비차단. 파이프라인이 값을 변경한 곳 기록(덮어쓰기·행 추가/제거·NULL화·제외). 실제 변경이 있을 때만 남는다.

규칙은 daily 적재(`python -m pipeline.daily_full`)와 전체 감사
(`python -m pipeline.silver_quality.s3_domain_audit`)에서 자동 실행되며 우회 옵션은 없다.

## RDS 방어 제약

Python gate의 Critical/Error 중 한 행만으로 결정 가능한 불변조건은 RDS CHECK·
PK·UNIQUE·FK로도 강제한다. 현재 DB guard는 인증 실행 연결, 필수 문자열, 자산
유니버스 형태, 자산별 가격 부호 정책·비음수/유한값/OHLC/시가총액 대사, 재무 PIT·통화·
배당 metric-unit, FMP 기업행사 효력일을 검사한다. DB guard 위반은 publish
transaction을 즉시 rollback한다.

시장 완전성, 거래일 캘린더, 시계열 수정종가, 소스 간 대사와 Warning은 여러 행이나
외부 문맥이 필요하므로 Python gate에 유지한다. Warning 원천값을 DB CHECK로 차단하지
않는다.

## 규칙 목록

`dq_result`에서 본 `rule_code`를 여기서 찾는다.

### 공통·구조

| 규칙 | 등급 | 검사 |
|---|---|---|
| `COMMON_DUPLICATE_KEY` | Critical | business key 중복 |
| `COMMON_NULL_KEY` | Critical | 필수 키 NULL·빈 문자열 |
| `ASSET_IDENTIFIER_ORPHAN` | Error | 외부 식별자의 asset 매핑 누락 |
| `RECONCILIATION_ROW_BALANCE` | Error | 원본·변환·제외·실패 행 수 대사 |

이 외에 필수 컬럼·타입·enum·양수·식별자 매핑 등 기본 스키마 검사가 있으며 실패 시 publish를 차단한다.

### 가격

| 규칙 | 등급 | 검사 |
|---|---|---|
| `PRICE_REQUIRED_POSITIVE` | Error | 필수 가격 필드 양수 (주식 close·adj_close·market_cap, 지수 close·adj_close) |
| `PRICE_OHLC_LOGIC` | Error | OHLC 대소관계·부분 누락 |
| `PRICE_SOURCE_PARTITION_DATE` | Error | S3 날짜 파티션 = 내부 거래일 |
| `PRICE_MARKET_COMPLETENESS` | Critical | 거래일마다 시장별 개시일 이후 종목 존재 (KOSPI 상시, KOSDAQ 1996-07-01~) |
| `PRICE_MARKET_COVERAGE_FLOOR` | Error | 일별 시장별 종목 수 ≥ 최근 baseline median의 50% (부분·절단 시장데이터 차단, 개장일 한정) |
| `PRICE_MARKET_CAP_RECONCILIATION` | Error | `market_cap ≈ close × shares` (1%) |
| `PRICE_BENCHMARK_COMPLETENESS` | Critical | 거래일마다 벤치마크 각 1행 (KOSPI200/1028·KOSDAQ150/2203 모두 2010-01-04~; KRX가 KOSDAQ150을 기준일 2010-01-04까지 소급 제공. 지수 개시 전 거래일은 벤치마크 불요) |
| `SOURCE_INCOMPLETE_OHLC` | Modified | 거래 있으나 O/H/L=0 → NULL 유지, close·거래량·시총은 보존 |
| `SOURCE_NO_TRADE_OHLC` | Modified | 무거래 행의 O/H/L 누락 기록 |
| `UNSUPPORTED_MARKET_EXCLUDED` | Modified | KONEX 가격을 유니버스에서 제외 |
| `NONPOSITIVE_PRICE_EXCLUDED` | Modified | close/상장주식수/시가총액 비양수 주식 행을 Bronze 보존·Silver 제외 (정지·상폐 과정 과거행) |
| `UNSUPPORTED_MARKET_ASSET_EXCLUDED` | Modified | KONEX 이력만 있는 자산의 재무 제외 |

### 수정종가 (adj_close)

| 규칙 | 등급 | 검사 |
|---|---|---|
| `ADJ_CLOSE_SOURCE_FIELDS` | Error | 직전 종가가 있는 행은 등락률 존재 (직전 종가 없는 행은 아래 no-baseline로 처리) |
| `PRICE_NO_ADJUSTMENT_BASELINE` | Modified | 직전 유효 종가가 없는 행(신규상장 첫날·거래재개, 전일대비 결측/함의 전일종가≤0) — adj_close=close(계수1)로 두고 등락률·산술 검사 제외 |
| `ADJ_CLOSE_RECONCILIATION` | Error | 전체 시계열 수정종가 독립 재계산 대사 |
| `ADJ_CLOSE_RETURN_CONTINUITY` | Error | 기업행사 전후 수정주가 수익률 = KRX 기준가 수익률 |
| `ADJ_CLOSE_FULL_SERIES_STREAMING_RECONCILIATION` | Error | 전 기간 누적 KRX 계수 독립 재계산·대사 |
| `ADJ_CLOSE_POST_PUBLISH` | Critical | 일별 소급조정 후 commit 전 재검증 |

### 기업행사·기준가 리셋

| 규칙 | 등급 | 검사 |
|---|---|---|
| `PRICE_ADJUSTMENT_WITHOUT_DART_EVENT` | Warning | 0.5% 초과 조정계수에 DART 기업행사·거래재개 근거 없음 |
| `PRICE_RETURN_SPIKE` | Warning | 조정 후 절대수익률 30.5% 초과, DART 특별거래로 미설명 |
| `PRICE_ROUND_TRIP_SPIKE` | Warning | 급등락 후 3일 내 원래 가격 복귀 |
| `PRICE_SCALE_JUMP` | Warning | 미설명 10배·100배 단위 변화 |
| `CORPORATE_ACTION_FACTOR_MISMATCH` | Warning | DART 계수 vs KRX 계수 2% 초과 불일치 |
| `DART_SHARE_COUNT_FACTOR_MISMATCH` | Warning | 균등감자 DART 주식수 vs KRX 상장주식수 2% 초과 불일치 |
| `DART_ACTION_WITHOUT_KRX_ADJUSTMENT` | Warning | 가격조정형 DART 효력일 ±15일 창에 KRX 리셋 없음 |

### 커버리지·분포

| 규칙 | 등급 | 검사 |
|---|---|---|
| `PRICE_COVERAGE_DRIFT` | Warning | 시장별 종목 수 20일 median 대비 10% 초과 감소 |
| `PRICE_DISTRIBUTION_DRIFT` | Warning | 횡단면 수익률 median의 MAD 이상치 |
| `PRICE_DISTRIBUTION_DRIFT_BENCHMARK_CONSISTENCY` | Error | drift일 벤치마크·종목폭 방향 일치 검사 |

### 재무 (fundamental)

| 규칙 | 등급 | 검사 |
|---|---|---|
| `FUNDAMENTAL_PIT_ORDER` | Critical | 공시·사용가능일 순서 |
| `FUNDAMENTAL_CURRENCY_CONSISTENCY` | Error | filing 내 통화 일관성 |
| `FUNDAMENTAL_ACCOUNTING_EQUATION` | Warning | 회계식 상대오차 1% |
| `FUNDAMENTAL_MAJOR_METRIC_COVERAGE` | Warning | revision별 자산·매출·순이익 중 1개 이상 존재 |
| `NO_TRADABLE_PRICE_ASSET` | Modified | 가격 이력 없는 DART-only 기업 제외 |
| `DART_FULL_STATEMENT_SUPPLEMENT` | Modified | 주요계정에 없는 키만 전체재무제표로 보강 |
| `DART_ACCOUNTING_EQUATION_SUPPLEMENT_REPLACEMENT` | Modified | 전체재무제표가 회계식 만족 시 자산·부채·자본 원자 교체(출처 기록) |
| `DART_SOURCE_ACCOUNTING_INCONSISTENCY` | Warning | 두 DART API가 동일한 비대사값 반환 → 원천 오류로 보존·플래그 |
| `DART_NET_INCOME_ORD_DUPLICATE` | Modified | 순이익 `ord` 중복 시 작은 `ord` 선택 |
| `DART_FULL_STATEMENT_PRESENTATION_DUPLICATE` | Modified | 순이익이 IS·CIS 중복 표시 시 IS 선택 |
| `DART_UNEXPECTED_EXACT_DUPLICATE` | Error | 위 패턴 외 정확 중복 → publish 차단 |

## 설계 원칙

- 수정종가는 크기와 무관하게 **KRX 기준가 조정계수만** 사용한다. DART 감자 전후
  주식수 비율은 가격계수가 아니라 메타데이터로 보존하고 KRX 상장주식수 변화와 대사한다.
- 원천(KRX·DART) 오류는 Silver에서 고치지 않고 **보존 + 플래그**한다. 독립 신뢰
  출처가 회계식을 대사할 때만 교체한다(`DART_ACCOUNTING_EQUATION_SUPPLEMENT_REPLACEMENT`).
- `당기순이익`/`당기순이익(손실)`은 부호 있는 동일 `net_income`으로 통합하고,
  연결(`CFS`)·별도(`OFS`)는 키로 구분한다.

## 규칙 추가

`rules/`에 구현하고 `registry.py`에 등록하며 정상·실패·예외 fixture 테스트를 함께
추가한다. 의미나 임계치가 바뀌면 `QUALITY_RULESET_VERSION`을 올린다.
