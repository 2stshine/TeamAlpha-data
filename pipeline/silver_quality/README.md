# Silver quality gate

Silver는 정규화된 데이터가 아니라 이 폴더의 규칙을 통과한 데이터만 의미한다.
`CRITICAL`과 `ERROR` 실패는 publish를 차단하고, `WARNING`은 적재하되 `dq_result`에 남긴다.

최초 backfill은 RDS 영구 staging 대신 S3 candidate Parquet을 사용한다. 연도별
후보 객체는 Bronze fingerprint와 ruleset version, 행 수, SHA-256을 함께 기록한다.
전체기간 검사 후 최종 Silver만 RDS에 bounded COPY하며, 중단 시 같은 run ID로
검증된 S3 candidate와 이미 적재된 연도를 재사용한다. 일별 적재는 기존 PostgreSQL
temporary staging과 단일 publish transaction을 유지한다.

| 규칙 | 등급 | 검사 |
|---|---|---|
| `COMMON_DUPLICATE_KEY` | Critical | publish 전 business key 중복 |
| `COMMON_NULL_KEY` | Critical | 필수 키 NULL·빈 문자열 |
| `ASSET_IDENTIFIER_ORPHAN` | Error | 외부 식별자의 asset 매핑 |
| `PRICE_REQUIRED_POSITIVE` | Error | 주식은 close·adj_close·market_cap, 지수는 close·adj_close 필수 |
| `PRICE_OHLC_LOGIC` | Error | 일부 O/H/L 누락과 OHLC 대소관계 오류 |
| `UNSUPPORTED_MARKET_EXCLUDED` | Info/Pass | KONEX 가격을 Silver 유니버스에서 명시적으로 제외하고 건수·표본 기록 |
| `UNSUPPORTED_MARKET_ASSET_EXCLUDED` | Info/Pass | KONEX 가격 이력만 있는 자산의 재무를 명시적으로 제외 |
| `SOURCE_INCOMPLETE_OHLC` | Warning | 거래가 발생했지만 O/H/L 전체 누락; NULL을 유지하고 임의 보정하지 않음 |
| `SOURCE_NO_TRADE_OHLC` | Info/Pass | 거래량·거래대금 0인 무거래 행의 O/H/L 누락 기록 |
| `PRICE_SOURCE_PARTITION_DATE` | Error | S3 날짜 파티션과 내부 거래일 일치 |
| `PRICE_MARKET_COMPLETENESS` | Critical | 거래일마다 KOSPI·KOSDAQ 종목 존재 |
| `PRICE_MARKET_CAP_RECONCILIATION` | Error | `market_cap ≈ close × shares` 1% |
| `PRICE_BENCHMARK_COMPLETENESS` | Critical | 코스피200·코스닥150 각 1행 |
| `ADJ_CLOSE_SOURCE_FIELDS` | Error | 수정종가 검증에 필요한 전일대비·등락률 존재 |
| `ADJ_CLOSE_RECONCILIATION` | Error | 전체 시계열 수정종가를 독립 재계산해 소수 4자리 값 대사 |
| `ADJ_CLOSE_RETURN_CONTINUITY` | Error | 기업행사 전후 수정주가 수익률과 KRX 기준가 수익률 일치 |
| `ADJ_CLOSE_POST_PUBLISH` | Critical | 일별 소급조정 후 RDS 직전·당일 행을 commit 전에 재검증 |
| `PRICE_RETURN_SPIKE` | Warning | KRX 기준가 조정 후 일간 절대수익률 30.5% 초과 |
| `PRICE_ROUND_TRIP_SPIKE` | Warning | 급등락 후 3일 내 원래 가격 복귀 |
| `PRICE_SCALE_JUMP` | Warning | 기업행사로 설명되지 않는 10배·100배 단위 변화 |
| `PRICE_ADJUSTMENT_FACTOR_CHANGE` | Info/Pass | KRX 기준가 수정계수 0.5% 초과 기업행사 기록 |
| `PRICE_ADJUSTMENT_WITHOUT_DART_EVENT` | Warning | 0.5% 초과 KRX 조정계수에 인접한 DART 기업행사 근거가 없음 |
| `CORPORATE_ACTION_FACTOR_MISMATCH` | Warning | 계산 가능한 DART 주식수 조정계수와 KRX 계수가 2% 초과 불일치 |
| `DART_ACTION_WITHOUT_KRX_ADJUSTMENT` | Warning | 가격조정형 DART 효력일 근처에 KRX 조정계수가 없음 |
| `PRICE_COVERAGE_DRIFT` | Warning | 시장별 종목 수가 20일 median 대비 10% 초과 감소 |
| `PRICE_COVERAGE_GROWTH` | Info/Pass | 시장별 종목 수가 20일 median 대비 10% 초과 증가 |
| `PRICE_DISTRIBUTION_DRIFT` | Warning | 횡단면 수익률 median의 MAD 이상 |
| `FUNDAMENTAL_PIT_ORDER` | Critical | 공시 및 사용가능일 순서 |
| `NO_TRADABLE_PRICE_ASSET` | Warning | 전체 가격 기간에 없는 DART-only 기업을 명시적으로 제외하고 건수·표본 기록 |
| `FUNDAMENTAL_CURRENCY_CONSISTENCY` | Error | filing 내 통화 일관성 |
| `FUNDAMENTAL_ACCOUNTING_EQUATION` | Warning | 회계식 상대오차 1% |
| `FUNDAMENTAL_MAJOR_METRIC_COVERAGE` | Warning | 공시 revision별 자산·매출·순이익 중 하나 이상 존재 |
| `DART_NET_INCOME_ORD_DUPLICATE` | Warning/Pass | 원본의 동일 순이익 행이 `ord`만 달리 두 번 온 경우 작은 `ord`를 결정적으로 선택하고 건수·표본 기록 |
| `DART_UNEXPECTED_EXACT_DUPLICATE` | Error | 위 순이익 `ord` 패턴 이외의 정확 중복은 제거하지 않고 publish 차단 |
| `RECONCILIATION_ROW_BALANCE` | Error | 원본·변환·제외·실패 행 수 대사 |

수정종가 계산과 소급조정에는 유효한 KRX 기준가 조정계수를 크기와
관계없이 적용한다. `PRICE_ADJUSTMENT_FACTOR_CHANGE`의 0.5% 임계치는
검토할 기업행사를 요약하는 기준이며, 계산에서 작은 계수를 버리는
허용오차가 아니다.

`당기순이익`과 `당기순이익(손실)`은 별도 지표가 아니라 부호가 있는 동일
`net_income` 지표로 정규화한다. 연결(`CFS`)과 별도(`OFS`) 재무제표는 기존 키로
구분한다. 알려진 DART 중복은 계정명·금액·통화·공시 revision을 포함한 모든 원본
필드가 같고 `ord`만 다른 정확히 두 행일 때만 허용한다.

규칙을 추가할 때는 `rules/`에 구현하고 `registry.py`에 등록하며 정상·실패·예외 fixture
테스트를 함께 추가한다. 의미나 임계치가 바뀌면 `QUALITY_RULESET_VERSION`을 올린다.
