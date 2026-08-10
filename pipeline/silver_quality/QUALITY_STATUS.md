# Silver 데이터 품질 현황

> 스냅샷: **2026-08-10 KST**, 운영 RDS 조회 및 ECS/CloudWatch
> 운영 로그 기준. (KRX 1995~ 히스토리 확장 재구축 및 FMP 재적재 직후 갱신;
> 일부 warning/DQ-run 세부 수치는 다음 `pipeline.silver.status` 전수 집계 때 갱신 예정)

이 문서는 현재 Silver 품질 상태를 운영 데이터와 `dq_run`·`dq_result`·
`dq_warning_state`에서 다시 집계한 결과다. 최신 증분 검사와 전체 역사 감사는 검사
범위가 다르므로 아래에서 분리해 기록한다.

## 결론

- 현재 배포 ruleset: **1.25.0**, ECS daily task definition **122** (image `f1602f5`)
- **warning 워크리스트 확장**: 이제 daily뿐 아니라 모든 backfill·재구축 모드의
  warning을 `dq_warning_state`로 추적하고, 검토 후 `ACKNOWLEDGED`로 내릴 수 있다
  (migration **008**, `pipeline.silver_quality.review`). 적재 전 구간 이력을 소급
  시딩: **OPEN 2,267 scope / 252,602행** (최다 `SOURCE_INCOMPLETE_OHLC` 168,972행
  = pre-2015 marcap 불완전 OHLC, 구조적)
- **KRX 히스토리 1995-05-02~2026-08-06 확장 완료** (15,149,757행 / 6,677자산, 전체 재구축 `fc97e8e9`, 무결성 0)
- FMP 재적재 완료 (14,530,164행 / 8,720자산, `e44c65d3`)
- FMP 원자재 28종 2015~2026 백필: **CERTIFIED**, one-off task definition **95**
- 최신 KRX/DART 증분: **CERTIFIED**, 대상일 `2026-08-05`
- 최신 FMP 증분: **CERTIFIED**, 대상일 `2026-08-04`
- 마지막 전체 역사 감사: **CERTIFIED**, 차단 Critical/Error 실패 **0**
- 미인증 Silver 행: 모든 핵심 테이블 **0**
- 핵심 NULL·비양수 가격·현재 식별자 중복: **0**
- RDS Critical/Error guard 5개: 모두 **validated=true**
- 현재 OPEN warning: **2,267개 검사 범위** (backfill 소급 포함; 이전 daily-only
  집계는 11범위·42건이었음 — 측정 대상 확장)

현재 데이터는 전략 개발에 사용할 수 있는 인증 상태다. 다만 Warning은 원천값을
보존한 검토 대상이며, 아래 항목을 해소된 데이터로 간주하면 안 된다.

## FMP 원자재 백필

| 항목 | 결과 |
|---|---|
| 대상 | 물리 원자재 연속선물 28종 |
| 기간 | `2015-01-01`~`2026-08-05` |
| Bronze | 28/28, 객체 58개, rate limit 0 |
| Silver 가격 | 82,960행, 자산 28개 |
| parent run ID | `60f63dc8-5d39-4fc6-911c-e4c40ca3fe74` |
| ruleset | `1.22.1` |
| 상태 | `CERTIFIED` |
| ECS | task definition `95`, exit code `0` |

2015~2026의 12개 연도 가격 파티션과 `asset:commodity` 파티션이 모두
인증됐다. FMP가 제공하는 일요일 저녁 선물 세션 657건은 정상 세션으로 유지했다.
거래 세션이 아닌 Saturday 원천행 30건과 OHLC 불일치 원천행 140건은 Bronze에
보존하고 Silver에서는 제외했으며 `MODIFIED` 결과로 기록한다.

`FMP_COMMODITY_POSSIBLE_ROLL`은 만기 롤오버로 추정되는 일간 20% 초과 변동을
보존·추적하는 비차단 Warning이다. Critical/Error 실패는 없으며 모든 연도
파티션이 publish됐다.

## 최신 일별 증분 검사

### KRX/DART

| 항목 | 결과 |
|---|---|
| 대상일 | `2026-08-05` |
| run ID | `f6368f67-2d97-4708-98ff-cb318536c8d4` |
| 실행 ruleset | `1.22.1` |
| 상태 | `CERTIFIED` |
| Critical | PASS 16, 실패 0 |
| Error | PASS 22, 실패 0 |
| Warning | FAIL 규칙 2, 실패 건수 16 |
| Modified | PASS 3 |

Warning 16건은 `DART_ACTION_WITHOUT_KRX_ADJUSTMENT` 1건과
`CASH_DIVIDEND_AMOUNT_COVERAGE` 15건이다. 차단 조건이 아니므로 원천값을
보존하고 warning 상태로 추적한다.

### FMP

| 항목 | 결과 |
|---|---|
| 대상일 | `2026-08-04` |
| run ID | `61e85c2a-50b9-474a-af6c-5c8c2400b297` |
| 실행 ruleset | `1.22.1` |
| 상태 | `CERTIFIED` |
| Critical | PASS 21, 실패 0 |
| Error | PASS 7, 실패 0 |
| Warning | 없음 |
| Modified | PASS 6 |

2026-08-06 08:30 KST 실행의 첫 FMP publish는 이전 원자재 종가 조회가 바깥
transaction을 남긴 상태에서 publish를 savepoint로 실행해 연결 종료 시 rollback됐다.
코드를 수정해 task definition 97로 배포한 뒤 오전 Bronze 1,764개 객체를 재사용했다.
12:31 KST 재실행에서 FMP 주식 6,020행, USDKRW 1행, 원자재 28행이 같은 인증
run으로 publish됐다. 잘못 `RUNNING`으로 남았던 run
`af1f6d36-38a4-47e3-903e-1e41a22044be`는 원인을 기록하고 `FAILED`로 마감했다.

## 현재 OPEN warning

`dq_open_warning` 기준이다. 같은 실행 모드·대상일/파티션·데이터셋·규칙을 다시
검사해 PASS가 나올 때만 RESOLVED가 된다.

| 규칙 | OPEN 범위 | 실패 건수 | 대상일 범위 | 의미 |
|---|---:|---:|---|---|
| `CASH_DIVIDEND_AMOUNT_COVERAGE` | 2 | 30 | 2026-08-04~2026-08-05 | DART 의사결정 문서에서 보통주 주당 현금액 미노출 |
| `DART_ACTION_WITHOUT_KRX_ADJUSTMENT` | 6 | 7 | 2026-07-27~2026-08-05 | 가격조정형 DART 행사 주변에 KRX 기준가 리셋 없음 |
| `PRICE_ADJUSTMENT_WITHOUT_DART_EVENT` | 3 | 5 | 2026-07-27~2026-08-03 | KRX 기준가 조정에 대응하는 DART/거래재개 근거 없음 |
| **합계** | **11** | **42** |  |  |

FMP daily의 OPEN warning은 0건이다.

## 마지막 전체 역사 감사

| 항목 | 결과 |
|---|---|
| parent run ID | `bfd6d724-c405-422e-b1a5-8b29a3bad37b` |
| 실행 시각 | 2026-08-05 00:01~00:10 KST |
| ruleset | `1.19.1` |
| 상태 | `CERTIFIED` |
| Critical | PASS 143, 실패 0 |
| Error | PASS 211, 실패 0 |
| Modified | PASS 63 |
| Warning | FAIL 결과 38개 |

역사 감사에서 확인된 비차단 warning은 다음과 같다.

| 규칙 | 실패 건수 | 판정 |
|---|---:|---|
| `PRICE_ADJUSTMENT_WITHOUT_DART_EVENT` | 303 | DART 근거가 없는 KRX 조정 이벤트, 원값 보존 |
| `DART_SOURCE_ACCOUNTING_INCONSISTENCY` | 30 | DART 두 API에서 동일한 회계식 불일치 확인, 원천 오류로 플래그 |
| `DART_ACTION_WITHOUT_KRX_ADJUSTMENT` | 17 | 행사·가격조정 시점 불일치 또는 비가격조정 행사 |
| `PRICE_DISTRIBUTION_DRIFT` | 10 | 실제 시장 급변일, 벤치마크·종목폭 확인 |
| `CASH_DIVIDEND_AMOUNT_COVERAGE` | 현재 물리행 745 | DART 현금배당 18,476행 중 cash amount가 있는 행 17,731개 |

`CASH_DIVIDEND_AMOUNT_COVERAGE`의 감사 파티션 합계 243,804는 연도별 파티션이
누적 기업행사 후보를 반복 평가한 값이므로 고유 결측 행 수가 아니다. 현재 Silver의
고유 물리행 기준 결측은 745건을 사용한다.

전체 감사 이후 배당과 원자재 적재, ruleset `1.22.1` 배포가 있었으므로 이 감사는
최신 daily 상태와 동일한 cutoff의 재감사가 아니다. 현재 증분의 차단 여부는 위 최신
일별 검사, 원자재는 별도 인증 백필, 전체 역사 품질의 마지막 확정선은 이 섹션을
기준으로 한다.

## 현재 Silver 적재 범위

### 가격

| source | 행 수 | 자산 수 | 기간 |
|---|---:|---:|---|
| KRX | 15,149,757 | 6,677 | **1995-05-02**~2026-08-06 |
| FMP | 14,530,164 | 8,720 | 2015-01-02~2026-08-05 |
| FMP_FX | 3,094 | 1 | 2015-01-01~2026-08-03 |
| FMP_COMMODITY | 82,965 | 28 | 2015-01-01~2026-08-05 |

> KRX 히스토리를 **1995-05-02**까지 확장(marcap, 2026-08-10 전체 재구축, s3_backfill
> run `fc97e8e9`). 2010 이전은 지수·벤치마크가 없어 벤치마크 완전성은 개시일 기준
> 으로 검사한다. 전체 재구축이 Silver 5테이블을 truncate하므로 FMP는 같은 창에서
> `fmp_backfill_ecs` 로 재적재했다(run `e44c65d3`). 무결성 미인증/NULL 0.

### 재무·배당 지표 (2026-08-10 전수 집계)

| source | statement | basis | 행 수 | 자산 수 | 기간 |
|---|---|---|---:|---:|---|
| DART | BS | STANDARDIZED | 1,485,159 | 3,071 | 2015-03-31~2026-06-30 |
| DART | IS | STANDARDIZED | 718,543 | 3,071 | 2015-03-31~2026-06-30 |
| DART | DIVIDEND | REPORTED | 97,003 | 1,903 | 2013-09-30~2026-04-30 |
| FMP | BS | STANDARDIZED | 2,603,924 | 8,494 | 2015-01-02~2026-07-15 |
| FMP | IS | STANDARDIZED | 3,294,172 | 8,489 | 2015-01-02~2026-07-15 |
| FMP | CF | STANDARDIZED | 1,300,371 | 8,461 | 2015-01-02~2026-07-26 |

> DART DIVIDEND(정기보고서 배당, `run_dart_extras`)은 전체 재구축 truncate 로 함께
> 지워져 2026-08-10 `dart_dividend_action_backfill` 로 재적재·인증했다(dividends
> 97,003 / actions 41,295, run `8270d84f`). 재구축 진입점(`krx_history_backfill_ecs`)
> 도 이후 FMP 와 함께 dart-extras 를 자동 재적재하도록 수정해 재발을 막았다.
> FMP 재무는 재적재가 2015+ 창(`FMP_SILVER_FROM_YEAR=2015`)이라 최소 period_end
> 가 2015 로 정렬됐다(이전 희소 2001–2014 미국 재무 제외). FMP 주가가 2015+ 이므로
> 백테스트 커버리지 손실은 없다.

### 주요 기업행사 (2026-08-10 전수 집계, 상위 유형)

| source | action | 행 수 | 자산 수 |
|---|---|---:|---:|
| DART_DISCLOSURE | cash_dividend | 19,861 | 1,875 |
| DART_STRUCTURED | paid_increase | 5,679 | 1,380 |
| FMP_DIVIDEND | cash_dividend | 112,039 | 3,640 |
| FMP_SPLIT | stock_split | 1,091 | 847 |

## DB 무결성 방어

기존 PK·UNIQUE·FK와 함께 다음 CHECK가 운영 RDS에서 모두 검증됐다.

| 테이블 | 제약 | validated |
|---|---|---|
| `asset` | `asset_critical_error_guard` | true |
| `asset_identifier` | `asset_identifier_critical_error_guard` | true |
| `price_daily` | `price_daily_critical_error_guard` | true |
| `fundamental` | `fundamental_critical_error_guard` | true |
| `corporate_action` | `corporate_action_critical_error_guard` | true |

대표적인 잘못된 쓰기 다섯 종류(빈 자산명, 빈 식별자, `close=0`, DART PIT 날짜
역전, 빈 action type)를 rollback transaction으로 시험했고 모두 CHECK 위반으로
거부됐다.

현재 무결성 집계:

- `asset`, `asset_identifier`, `price_daily`, `fundamental`, `corporate_action`
  미인증 행: 각각 0
- NULL/비양수 `price_daily.close`: 0
- NULL value·available date/time 재무행: 0
- action type/key 결측: 0
- CIK를 제외한 현재 ticker/CUSIP/ISIN 등 중복 식별자 값: 0

DB guard는 한 행만으로 판정 가능한 Critical/Error를 방어한다. 시장 완전성, 거래일
캘린더, 수정종가 전체 시계열, 기업행사 대사처럼 여러 행이나 외부 문맥이 필요한
규칙은 계속 Python 품질 게이트에서 검사한다. Warning은 원천 보존 정책상 DB에서
차단하지 않는다.

## 재검증 방법

현재 운영 상태와 OPEN warning 조회:

```bash
uv run python -m pipeline.silver.status
```

전체 역사 감사를 새 cutoff와 ruleset으로 갱신:

```bash
uv run python -m pipeline.silver_quality.s3_domain_audit --action init
uv run python -m pipeline.silver_quality.s3_domain_audit \
  --action domain --domain prices --parent-run-id <run-id>
uv run python -m pipeline.silver_quality.s3_domain_audit \
  --action domain --domain fundamentals --parent-run-id <run-id>
uv run python -m pipeline.silver_quality.s3_domain_audit \
  --action finalize --parent-run-id <run-id>
```

규칙 정의와 severity 정책은 [`README.md`](README.md)를 참고한다.
