# Silver 데이터 계약 (Data Contract)

> 버전 2026-08-11 · ruleset 1.27.0 · 대상: 팩터를 생성하는 research agent 및 그 가드레일
>
> 이 문서는 **각 필드가 무엇이고, 어떤 팩터에 써도 되는지/쓰면 안 되는지**를 규정한다.
> 목적은 "오류 없음"을 넘어 **조용한 편향(silent bias)** 을 막는 것이다 — 겉보기엔
> 멀쩡하나 숨은 함정이 있는 필드를 agent가 모르고 써서 그럴듯한 가짜 팩터를 만드는 일.
> 기계 판독본은 [`field_reliability.yaml`](field_reliability.yaml).

---

## 0. 한눈 요약 — 절대 혼동 금지

| 필드 | 정체 | ✅ 안전 | ❌ 금기 |
|---|---|---|---|
| `close` | 원시 종가(무조정, 명목) | 당일 값 표시 | **시계열 수익률**(분할·배당 미조정) |
| `adj_close` | **가격수정** 종가(분할·증자만, **배당 X**) | 가격 모멘텀·리버설·변동성 | **총수익·배당 팩터** |
| `total_return_close` | **총수익**(배당 재투자 O) | 총수익·배당·carry 팩터 | (없음) |
| `shares` | **총상장주식수** | 시총·발행주식 | **유동주식(free float) 아님** |

**adj_close ≠ 총수익.** adj_close로 총수익/배당 팩터를 만들면 배당수익률(연 1~5%)이 통째로 빠진다.

---

## 1. 커버리지 (있는 것)

| 소스 | 자산 | 기간 | 비고 |
|---|---|---|---|
| **KRX 주식** | ~6,677 | **1995-05-02 ~** | marcap(과거) + KRX OpenAPI(일별). **상장폐지 포함**(생존편향 없음) |
| KRX 지수 | 1028 KOSPI200·2203 KOSDAQ150 | 2010-01-04 ~ | 벤더 최소제공일 |
| FMP 미국주식 | ~8,720 (NYSE·NASDAQ·AMEX) | 2015-01-02 ~ | 상장폐지 포함 |
| FMP 원자재 | 28 | 2015 ~ | 연속선물(롤 주의) |
| FMP FX | USDKRW | 2015 ~ | |
| DART 재무 | BS·IS ~3,071 | 2015-03-31 ~ | 핵심계정만 |
| DART 배당 | ~1,903 | 2013-09-30 ~ | 정기보고서 |
| FMP 재무 | BS·IS·CF ~8,490 | 2015 ~ | **벤더 신뢰(미검증)** |

## 1b. 제공하지 않음 (명시적 울타리 — 억지로 대체 금지)

- **유동주식수(free float)** — `shares`는 총상장주식뿐
- **호가(order book / bid-ask)**
- **공매도 / 대차(short / securities lending)**
- **PIT 과거 업종분류(sector history)** — 현재 시점 업종만 가능(미적재)
- **상세 현금흐름·마진 계정** — CFO·CAPEX·감가상각·COGS·SG&A·현금 (KRX/DART 미적재; 미국은 FMP CF 있음)

> 위 항목이 필요한 팩터는 **만들 수 없다**(대체 필드로 우회 금지). 필요 시 벤더 조달 후 추가.

---

## 2. 필드 사전

### price_daily
| 필드 | 의미 | 단위 | 신뢰 | 금기/주의 |
|---|---|---|---|---|
| `close` | 원시 종가 | 통화 | 검증 | 시계열 수익률 금지 |
| `open/high/low` | 원시 시/고/저 | 통화 | 검증 | **거래량 0일은 NULL**(비거래) |
| `adj_close` | 가격수정 종가(분할·증자). back-adjusted(최신=원시종가) | 통화 | 검증 | **배당 미반영** → 총수익 금지 |
| `total_return_close` | 배당 재투자 총수익. 최신일==adj_close | 통화 | 검증(KRX 근사) | KRX 배당락일은 record_date 유도(±1~2거래일). **누적 정확, 일별 초단기만 미세오차** |
| `volume` | 거래량 | 주 | 검증 | 0=비거래(OHLC NULL) |
| `trading_value` | 거래대금 | 통화 | 검증 | |
| `shares` | **총상장주식수** | 주 | 검증 | **유동주식 아님** |
| `market_cap` | 시가총액(=close×shares) | 통화 | 검증(대사 게이트) | |
| `market` | KOSPI/KOSDAQ/KONEX | | 검증 | |
| `prev_diff` | KRX 전일대비(adj_close 산출 근거) | 통화 | 검증 | 내부용 |

### fundamental (long: metric/value)
- **DART(한국) — 검증됨**(ERROR 게이트: 값타당성 total_assets>0·revenue≥0, 회계항등식 gross>10% 제외, 음수배당 제외).
  - BS: `total_assets, current_assets, noncurrent_assets, total_liabilities, current_liabilities, noncurrent_liabilities, total_equity, capital_stock, retained_earnings`
  - IS: `revenue, operating_income, pretax_income, net_income, comprehensive_income`
  - DIVIDEND(정기보고서): `cash_dividend_per_share, dividend_yield, stock_dividend_per_share, payout_ratio`
  - **없음: COGS, SG&A, cash, CFO, CAPEX, 감가상각** (상세계정 미적재)
  - `available_date` = 공시 다음날(PIT 준수, look-ahead 없음 — FUNDAMENTAL_PIT_ORDER CRITICAL)
  - `fs_type` CFS(연결)/OFS(별도) 구분 — 혼용 금지
- **FMP(미국) — 벤더 신뢰(내부검증 안 함).** BS/IS/CF 제공. total_assets=0·음수 revenue·회계 불일치가 관행상 존재하나 **그대로 둠**(정의 차이). US 팩터는 이 전제 하에 사용.

### corporate_action / dividend_history
| 필드 | 의미 | 주의 |
|---|---|---|
| `action_type` | cash_dividend, stock_split, reverse_split, capital_reduction, bonus_issue, merger, … | |
| `ex_date` | 배당락/권리락일 | **KRX 현금배당은 NULL**(record_date에서 유도). FMP는 있음 |
| `record_date` | 배당기준일 | KRX 배당의 기준 날짜 |
| `cash_amount` | 주당 현금배당 | **KRX 커버리지 부분적**(~17,761/19,880) |
| `adjusted_cash_amount` | 분할조정 주당배당 | |

### asset / asset_identifier
- `asset` = asset_id, name, asset_type(stock/index/commodity/fx), exchange. **sector/업종 없음.**
- `asset_identifier` = 티커·ISIN·cik 매핑(valid_from/to). **재사용 티커는 다른 asset_id로 분리**(정체성 안전). cik는 1기업-N증권이라 중복 정상.

---

## 3. 조용한-편향 함정 (반드시 인지)

1. **총수익 ≠ 가격수익:** 총수익은 `total_return_close`, 가격수익은 `adj_close`. 섞지 말 것.
2. **FMP 재무는 미검증:** total_assets=0/음수 revenue가 있을 수 있음(관행). US 재무 팩터는 자체 정제 권장.
3. **DART 상세계정 없음:** CFO·CAPEX·COGS·SG&A 요청받으면 없는 것 — 대체 필드로 근사 금지.
4. **유동주식 없음:** float-adjusted 시총/가중은 불가(총주식뿐).
5. **극단 수익률은 대부분 진짜:** 동전주 ±수백% 등은 실제 이동 → **winsorize·유동성 필터 필수**(데이터 오류 아님).
6. **배당락일 근사:** KRX 총수익의 일별 배당락 타이밍은 ±1~2거래일. 초단기(1~2일) 배당락 민감 전략만 주의.

---

## 4. 품질 보증 (floor)

DQ 게이트가 보장하는 불변식(차단=CRITICAL/ERROR):
- null close 0, 비양수 가격 제외, `market_cap ≈ close×shares`(1%)
- 재무 PIT 순서(look-ahead 없음), 통화 일관성
- **재무 값타당성**(total_assets>0, revenue≥0, 음수배당 제외), **회계항등식 gross(>10%) 제외**
- adj_close 재구성 일관성, 시장/벤치마크 완전성
- 중복 price 행 0, 증권 식별자(ticker/ISIN) 활성중복 0

**보증 안 함:** 인코딩 안 된 오류 유형, WARNING(원천보존·검토대상), FMP 재무 정확성, 위 "제공 안 함" 항목.

## 5. 아웃라이어·검토 플래그
- 극단값·구조적 warning은 `dq_warning_state` 워크리스트에 누적, `pipeline.silver_quality.review`로 조회/ack.
- 팩터 구축단에서 **스파이크 winsorize + 저유동 필터** 적용 권장.

## 6. 팩터유형별 안전표
| 팩터 유형 | 쓸 필드 | 피할 것 / 주의 |
|---|---|---|
| 가격 모멘텀·리버설·변동성 | `adj_close` | `close`(무조정) |
| 총수익·배당수익률·carry | `total_return_close`, `dividend_history` | `adj_close`(배당 X) |
| 가치·퀄리티(KR) | DART BS/IS 핵심 + `market_cap` | 상세현금흐름(없음), FMP 혼용 |
| 유동성·규모 | `trading_value`, `market_cap`, `shares` | `shares`≠유동주식 |
| US 팩터 | FMP price/fundamental | 벤더 신뢰 전제 |
| float 가중·공매도·호가 기반 | — | **불가(미제공)** |
