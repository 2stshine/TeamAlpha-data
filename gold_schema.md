# TeamAlpha Gold v1 — 확정안

Gold는 별도 RDS를 만들지 않고 기존 Silver RDS의 같은 PostgreSQL database 안에
`gold` schema로 둔다. 총 **3개 테이블, 20개 컬럼**이다.

## 1. `gold.factor` — 9개

| 컬럼 | 설명 |
|---|---|
| `factor_id` | 팩터 버전 내부 ID |
| `factor_key` | 팩터 계열 키 |
| `version` | 계산식·설정 버전 |
| `description` | 팩터 의미 |
| `implementation_uri` | 실제 SQL/Python 구현 위치 |
| `implementation_hash` | 실행 코드 식별 hash |
| `config` | 입력, 파라미터, universe, PIT 조건 JSONB |
| `evaluation` | IC, 평가 기간, 통과 기준과 결과 JSONB |
| `status` | CANDIDATE / APPROVED / REJECTED / RETIRED |

`(factor_key, version)`은 유일하며 같은 `factor_key`에서 APPROVED 버전은 하나만
허용한다. APPROVED는 `evaluation.passed=true`, REJECTED는 `false`여야 한다.

## 2. `gold.factor_value` — 5개

| 컬럼 | 설명 |
|---|---|
| `factor_id` | 승인된 팩터 버전 |
| `asset_id` | Silver `public.asset` 종목 ID |
| `as_of_date` | 값이 투자 판단에 사용 가능한 PIT 날짜 |
| `value` | 팩터 원값 |
| `rank` | 그날 기본 universe 내 순위 |

기본키는 `(factor_id, asset_id, as_of_date)`다. APPROVED 상태인 팩터만 값을
적재할 수 있다.

## 3. `gold.factor_correlation` — 6개

| 컬럼 | 설명 |
|---|---|
| `left_factor_id` | 첫 번째 승인 팩터 |
| `right_factor_id` | 두 번째 승인 팩터 |
| `period_start` | 계산 시작일 |
| `period_end` | 계산 종료일 |
| `correlation` | 일별 rank Spearman 상관계수 |
| `observation_count` | 계산 관측치 수 |

기본키는 `(left_factor_id, right_factor_id, period_start, period_end)`다.
`left_factor_id < right_factor_id`를 강제해 역순 중복을 막고 두 팩터가 모두
APPROVED일 때만 적재한다.

## RDS 배치

```text
기존 PostgreSQL RDS
├── public
│   ├── asset
│   ├── asset_identifier
│   ├── price_daily
│   └── fundamental
└── gold
    ├── factor
    ├── factor_value
    └── factor_correlation
```

같은 database에 두는 이유:

- `factor_value.asset_id`를 `public.asset`에 FK로 연결
- 별도 RDS 비용·Secret·네트워크·백업 불필요
- Silver를 읽어 Gold를 만드는 작업이 단순함

Gold 계산이 Silver 운영을 방해할 정도로 커질 때만 별도 RDS 또는 read replica를
검토한다. 물리적으로 분리하면 PostgreSQL FK를 사용할 수 없어 `asset` dimension
복제와 별도 정합성 검사가 필요하다.
