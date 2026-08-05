# Silver 데이터 품질 현황

> 스냅샷: **2026-08-05 18:58 KST**, 운영 RDS 조회 및 ECS/CloudWatch
> 운영 로그 기준

이 문서는 현재 Silver 품질 상태를 운영 데이터와 `dq_run`·`dq_result`·
`dq_warning_state`에서 다시 집계한 결과다. 최신 증분 검사와 전체 역사 감사는 검사
범위가 다르므로 아래에서 분리해 기록한다.

## 결론

- 현재 배포 ruleset: **1.22.1**, ECS daily task definition **94**
- FMP 원자재 28종 2015~2026 백필: **CERTIFIED**, one-off task definition **95**
- 최신 KRX/DART 증분: **CERTIFIED**, 대상일 `2026-08-04`
- 최신 FMP 증분: **CERTIFIED**, 대상일 `2026-08-03`
- 마지막 전체 역사 감사: **CERTIFIED**, 차단 Critical/Error 실패 **0**
- 미인증 Silver 행: 모든 핵심 테이블 **0**
- 핵심 NULL·비양수 가격·현재 식별자 중복: **0**
- RDS Critical/Error guard 5개: 모두 **validated=true**
- 현재 OPEN warning: **9개 검사 범위, 26건**

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
| 대상일 | `2026-08-04` |
| run ID | `cf4ee001-2193-434e-abf0-e4c2b387d110` |
| 실행 ruleset | `1.19.3` |
| 상태 | `CERTIFIED` |
| Critical | PASS 13, 실패 0 |
| Error | PASS 19, 실패 0 |
| Warning | FAIL 규칙 2, 실패 건수 16 |
| Modified | PASS 2 |

Warning 16건은 `DART_ACTION_WITHOUT_KRX_ADJUSTMENT` 1건과
`CASH_DIVIDEND_AMOUNT_COVERAGE` 15건이다. 차단 조건이 아니므로 원천값을
보존하고 warning 상태로 추적한다.

### FMP

| 항목 | 결과 |
|---|---|
| 대상일 | `2026-08-03` |
| run ID | `27558df9-1d3c-418d-b7ea-f5ba5845ed8a` |
| 실행 ruleset | `1.19.3` |
| 상태 | `CERTIFIED` |
| Critical | PASS 18, 실패 0 |
| Error | PASS 5, 실패 0 |
| Warning | 없음 |
| Modified | PASS 5 |

2026-08-05 08:30 KST 최초 실행은 `ACTION_IDENTIFIER_MAPPING` 오류로 Silver
publish 전에 중단됐다. 수정 후 같은 대상일을 재실행해 KRX/DART는 15:12 KST,
FMP는 15:14 KST에 인증 완료했다. 오전 수집 Bronze는 재실행에서 그대로 사용됐다.

현재 ruleset `1.22.1`은 이 일별 인증 실행 이후 배포됐다. `1.21.0`에서 RDS의
단일행 Critical/Error 방어 제약을 추가했고, `1.22.1`에서 원자재 세션·단위·
연속선물 품질 규칙을 추가했다. 따라서 다음 daily가 첫 `1.22.1` 증분 실행이다.

## 현재 OPEN warning

`dq_open_warning` 기준이다. 같은 실행 모드·대상일/파티션·데이터셋·규칙을 다시
검사해 PASS가 나올 때만 RESOLVED가 된다.

| 규칙 | OPEN 범위 | 실패 건수 | 대상일 범위 | 의미 |
|---|---:|---:|---|---|
| `CASH_DIVIDEND_AMOUNT_COVERAGE` | 1 | 15 | 2026-08-04 | DART 의사결정 문서에서 보통주 주당 현금액 미노출 |
| `DART_ACTION_WITHOUT_KRX_ADJUSTMENT` | 5 | 6 | 2026-07-27~2026-08-04 | 가격조정형 DART 행사 주변에 KRX 기준가 리셋 없음 |
| `PRICE_ADJUSTMENT_WITHOUT_DART_EVENT` | 3 | 5 | 2026-07-27~2026-08-03 | KRX 기준가 조정에 대응하는 DART/거래재개 근거 없음 |
| **합계** | **9** | **26** |  |  |

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
| KRX | 6,769,289 | 3,303 | 2015-01-02~2026-08-04 |
| FMP | 14,505,614 | 8,718 | 2015-01-02~2026-08-03 |
| FMP_FX | 3,094 | 1 | 2015-01-01~2026-08-03 |
| FMP_COMMODITY | 82,960 | 28 | 2015-01-01~2026-08-05 |

### 재무·배당 지표

| source | statement | basis | 행 수 | 자산 수 | 기간 |
|---|---|---|---:|---:|---|
| DART | BS | STANDARDIZED | 1,479,913 | 3,038 | 2015-03-31~2026-06-30 |
| DART | IS | STANDARDIZED | 715,559 | 3,038 | 2015-03-31~2026-06-30 |
| DART | DIVIDEND | REPORTED | 96,763 | 1,888 | 2013-09-30~2026-04-30 |
| FMP | BS | STANDARDIZED | 2,602,497 | 8,462 | 2003-09-30~2026-07-15 |
| FMP | IS | STANDARDIZED | 3,291,802 | 8,457 | 2001-12-31~2026-07-15 |
| FMP | CF | STANDARDIZED | 1,299,567 | 8,430 | 2001-12-31~2026-07-26 |

### 주요 기업행사

| source | action | 행 수 | 자산 수 |
|---|---|---:|---:|
| DART_DISCLOSURE | cash_dividend | 18,476 | 1,813 |
| FMP_DIVIDEND | cash_dividend | 113,172 | 3,633 |
| FMP_SPLIT | stock_split | 1,104 | 854 |

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
