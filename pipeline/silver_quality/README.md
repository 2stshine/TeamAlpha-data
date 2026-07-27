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
| `SOURCE_INCOMPLETE_OHLC` | Explained(Info/Pass) | 거래가 발생했지만 원천 O/H/L이 모두 0인 행. Silver에서는 NULL을 유지하고 close·거래량·거래대금·시총은 원본 그대로 보존 |
| `SOURCE_NO_TRADE_OHLC` | Info/Pass | 거래량·거래대금 0인 무거래 행의 O/H/L 누락 기록 |
| `PRICE_SOURCE_PARTITION_DATE` | Error | S3 날짜 파티션과 내부 거래일 일치 |
| `PRICE_MARKET_COMPLETENESS` | Critical | 거래일마다 KOSPI·KOSDAQ 종목 존재 |
| `PRICE_MARKET_CAP_RECONCILIATION` | Error | `market_cap ≈ close × shares` 1% |
| `PRICE_BENCHMARK_COMPLETENESS` | Critical | 코스피200·코스닥150 각 1행 |
| `ADJ_CLOSE_SOURCE_FIELDS` | Error | 수정종가 검증에 필요한 전일대비·등락률 존재 |
| `ADJ_CLOSE_RECONCILIATION` | Error | 전체 시계열 수정종가를 독립 재계산해 소수 4자리 값 대사 |
| `ADJ_CLOSE_RETURN_CONTINUITY` | Error | 기업행사 전후 수정주가 수익률과 KRX 기준가 수익률 일치 |
| `ADJ_CLOSE_POST_PUBLISH` | Critical | 일별 소급조정 후 RDS 직전·당일 행을 commit 전에 재검증 |
| `LISTING_EPISODE_BOUNDARY` | Info/Pass | 동일 ticker가 365일 초과 사라졌다 재등장하면 과거 발행회사와 수정주가·수익률 사슬 분리 |
| `PRICE_RETURN_SPIKE` | Warning | KRX 기준가 조정 후 일간 절대수익률 30.5% 초과이며 DART 특별거래 공시로 설명되지 않음 |
| `PRICE_ROUND_TRIP_SPIKE` | Warning | 급등락 후 3일 내 원래 가격 복귀 |
| `SETTLEMENT_TRADING_PRICE_SPIKE` | Explained(Info/Pass) | 전수 검토된 상장폐지 종목의 종료된 시계열 마지막 7거래일 급변을 가격제한폭 없는 정리매매로 설명 |
| `PRICE_SCALE_JUMP` | Warning | DART·KRX 주식수/시총·특별거래로 설명되지 않는 10배·100배 단위 변화 |
| `CORPORATE_ACTION_INFERRED_FROM_KRX_STRUCTURE` | Info/Pass | 가격과 주식수가 반대로 10배·100배 변하고 시가총액이 유지된 구조변경 |
| `SPECIAL_TRADING_EVENT` | Info/Pass | 최근 120일 내 정리매매·상장폐지·거래재개·재상장·변경상장 DART 공시로 설명되는 실제 30.5% 초과 가격 변화. 보통주 공시는 이름이 유일하게 일치하는 우선주에도 발행회사 근거로 상속하며 가격값은 수정하지 않음 |
| `PRICE_ADJUSTMENT_FACTOR_CHANGE` | Info/Pass | KRX 기준가 수정계수 0.5% 초과 기업행사 기록 |
| `PRICE_ADJUSTMENT_WITHOUT_DART_EVENT` | Warning | 0.5% 초과 KRX 조정계수에 인접한 DART 기업행사 근거가 없음 |
| `CORPORATE_ACTION_FACTOR_MISMATCH` | Warning | DART가 실제 가격계수를 제공하는 행사와 KRX 가격계수가 2% 초과 불일치. 감자 전후 주식 수는 비교하지 않음 |
| `DART_SHARE_COUNT_FACTOR_MISMATCH` | Warning | DART 감자 전후 보통주 수 비율과 KRX 실제 상장주식 수 변화가 2% 초과 불일치 |
| `DART_SHARE_COUNT_FACTOR_NOT_COMPARABLE` | Explained(Info/Pass) | 특정주주 소각·유상/액면감자·동시 주식분할 등 DART 감자비율과 전체 KRX 상장주식 수를 직접 비교할 수 없는 행사 |
| `DART_ACTION_WITHOUT_KRX_ADJUSTMENT` | Warning | 가격조정형 DART 효력일 근처에 KRX 조정계수가 없음 |
| `PRICE_COVERAGE_DRIFT` | Warning | 시장별 종목 수가 20일 median 대비 10% 초과 감소 |
| `PRICE_COVERAGE_GROWTH` | Info/Pass | 시장별 종목 수가 20일 median 대비 10% 초과 증가 |
| `PRICE_DISTRIBUTION_DRIFT` | Warning | 횡단면 수익률 median의 MAD 이상 |
| `FUNDAMENTAL_PIT_ORDER` | Critical | 공시 및 사용가능일 순서 |
| `NO_TRADABLE_PRICE_ASSET` | Info/Pass | 전체 가격 기간에 없는 DART-only 기업을 명시적으로 제외하고 건수·표본 기록 |
| `DART_FULL_STATEMENT_SUPPLEMENT` | Info/Pass | DART 주요계정에 없는 business key만 전체 재무제표의 BS·IS/CIS 계정으로 보강한 행·파일 수 기록 |
| `FUNDAMENTAL_CURRENCY_CONSISTENCY` | Error | filing 내 통화 일관성 |
| `FUNDAMENTAL_ACCOUNTING_EQUATION` | Warning | 회계식 상대오차 1% |
| `DART_ACCOUNTING_EQUATION_SUPPLEMENT_REPLACEMENT` | Info | 같은 공시 revision·기간·CFS/OFS의 DART 전체재무제표가 1% 이내로 회계식을 만족할 때만 자산·부채·자본 3개를 원자적으로 교체하고 원값·교체값·출처·전후 오차를 기록 |
| `DART_SOURCE_ACCOUNTING_INCONSISTENCY` | Warning | 주요계정과 같은 revision의 전체재무제표 API가 동일한 비대사 값을 반환하면 원천 오류로 분리하고 Silver 값은 수정하지 않음 |
| `FUNDAMENTAL_MAJOR_METRIC_COVERAGE` | Warning | 공시 revision별 자산·매출·순이익 중 하나 이상 존재 |
| `DART_NET_INCOME_ORD_DUPLICATE` | Warning/Pass | 원본의 동일 순이익 행이 `ord`만 달리 두 번 온 경우 작은 `ord`를 결정적으로 선택하고 건수·표본 기록 |
| `DART_UNEXPECTED_EXACT_DUPLICATE` | Error | 위 순이익 `ord` 패턴 이외의 정확 중복은 제거하지 않고 publish 차단 |
| `RECONCILIATION_ROW_BALANCE` | Error | 원본·변환·제외·실패 행 수 대사 |

수정종가 계산과 소급조정에는 유효한 KRX 기준가 조정계수를 크기와
관계없이 적용한다. `PRICE_ADJUSTMENT_FACTOR_CHANGE`의 0.5% 임계치는
검토할 기업행사를 요약하는 기준이며, 계산에서 작은 계수를 버리는
허용오차가 아니다.

DART 감자 전후 발행주식 수는 가격 조정계수로 해석하지 않는다. 해당 비율은
감자 방법과 함께 기업행사 메타데이터로 보존하고 KRX의 실제 상장주식 수 변화와
대사한다. 수정종가에는 계속 KRX 비교기준가 계수만 사용한다.

`당기순이익`과 `당기순이익(손실)`은 별도 지표가 아니라 부호가 있는 동일
`net_income` 지표로 정규화한다. 연결(`CFS`)과 별도(`OFS`) 재무제표는 기존 키로
구분한다. 알려진 DART 중복은 계정명·금액·통화·공시 revision을 포함한 모든 원본
필드가 같고 `ord`만 다른 정확히 두 행일 때만 허용한다.

규칙을 추가할 때는 `rules/`에 구현하고 `registry.py`에 등록하며 정상·실패·예외 fixture
테스트를 함께 추가한다. 의미나 임계치가 바뀌면 `QUALITY_RULESET_VERSION`을 올린다.
