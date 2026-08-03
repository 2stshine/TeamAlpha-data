# TeamAlpha Data Pipeline

KRX·DART·FMP 데이터를 수집해 원천 데이터는 **S3 Bronze**에 보존하고,
분석 가능한 데이터는 **RDS PostgreSQL Silver**, 검증된 팩터 산출물은 같은
database의 **`gold` schema**에 저장하는 배치 파이프라인입니다.

## 프로젝트 개요

```text
KRX OpenAPI / OpenDART / marcap / FMP stable API
                    │
                    ▼
        bronze 수집기 -> S3 bronze
                    │
                    ▼
       ECS daily/backfill -> RDS public silver
                                      │
                                      ▼
                 research/manual -> RDS gold schema
```

- **bronze**: 소스의 원천 단위와 값을 S3에 보존합니다. FMP 응답은 byte-for-byte로
  저장합니다. ETF·펀드가 섞여 있어도 삭제하지 않으며 편입 필터와 타입 변환은
  Silver에서 수행합니다.
- **silver**: `asset` 중심으로 가격·재무·기업행사를 정규화하고, point-in-time 및
  source-aware 품질 게이트를 통과한 데이터만 publish합니다.
- **gold**: 팩터 정의·버전·평가와 종목별 값·순위, 팩터 간 상관관계를 저장합니다.
  현재 12-1 모멘텀 계산 SQL이 구현되어 있으며 평가 기준은 아직 확정 전입니다.
- **운영 스케줄**: 화~토 오전 08:30 KST에 실행해 전날 KRX 데이터를 적재합니다.
- **자동 배포**: `main` 브랜치에 push하면 GitHub Actions가 ECR/ECS/Scheduler를 갱신합니다.
- **결과 알림**: daily ECS task가 종료되면 SNS 이메일로 성공/실패 결과를 받습니다.

## 폴더 구조

```text
.
├── .github/workflows/deploy.yml  # GitHub Actions 자동 배포
├── deploy/Dockerfile             # ECS/Fargate 실행 이미지
├── pipeline/                     # Bronze 수집, Silver 적재, Gold 계산 코드
│   ├── bronze/                   # S3 bronze 원천 데이터 수집기
│   ├── common/                   # 경로, 저장, DB 공통 유틸
│   ├── gold/                     # 팩터 계산 구현
│   ├── silver/                   # RDS silver 후보 생성/적재
│   └── silver_quality/           # 품질 규칙, DQ 이력, staging/backfill
├── sql/schema.sql                # RDS silver schema
├── sql/gold_schema.sql           # 같은 RDS의 gold schema
├── schema_tables.md              # silver 테이블 설계 상세 문서
├── gold_schema.md                # gold 3테이블 설계 상세 문서
├── pyproject.toml                # Python 프로젝트/의존성 설정
└── uv.lock                       # 의존성 lock 파일
```

## AWS 구조

운영에 필요한 핵심 흐름만 요약하면 다음과 같습니다.

```text
EventBridge Scheduler
  -> ECS Fargate task
  -> S3 bronze 저장
  -> RDS public silver 적재
  -> SNS 이메일 알림

Research/manual factor job
  -> RDS public silver 조회
  -> RDS gold 적재
```

### 배포 흐름

코드가 `main` 브랜치에 push되면 GitHub Actions가 새 실행 이미지를 만들고 운영 ECS 설정을 갱신합니다.

```text
GitHub main push
  -> GitHub Actions 실행
  -> deploy/Dockerfile로 Docker 이미지 빌드
  -> ECR repository에 이미지 push
     - 태그 1: Git commit SHA
     - 태그 2: latest
  -> ECS task definition 새 revision 등록
     - 새 revision이 방금 push한 ECR 이미지를 바라봄
  -> EventBridge Scheduler target 갱신
     - 다음 스케줄 실행부터 새 task definition 사용
```

즉, GitHub에 코드를 push하면 새 Docker 이미지가 ECR에 올라가고, ECS는 다음 실행부터 그 이미지를 받아 실행합니다.

### 스케줄 실행 흐름

매일 실행은 EventBridge Scheduler가 시작합니다.

```text
EventBridge Scheduler
  -> ECS Fargate task 실행
  -> ECS가 ECR에서 Docker 이미지 pull
  -> 컨테이너에서 python -m pipeline.daily_full 실행
  -> KRX/DART/FMP API 호출
  -> S3 bronze 저장
  -> 필요한 S3 객체를 /app/data로 다운로드
  -> RDS silver 적재
  -> ECS task 종료
  -> EventBridge rule이 STOPPED 이벤트 감지
  -> SNS 이메일 알림
```

핵심 리소스 종류:

| 구분 | 설명 |
|---|---|
| 리전 | `ap-northeast-2` |
| S3 bronze bucket | 원천 데이터를 저장하는 S3 bucket |
| ECR repository | ECS에서 실행할 Docker 이미지를 저장하는 repository |
| ECS cluster | daily batch task를 실행하는 Fargate cluster |
| ECS task definition | 파이프라인 컨테이너, role, secret 주입 설정 |
| Scheduler | daily ECS task를 시작하는 EventBridge Scheduler |
| Scheduler 시간 | `cron(30 8 ? * TUE-SAT *)`, `Asia/Seoul` |
| RDS PostgreSQL | `public` Silver와 `gold` 팩터 테이블을 함께 저장하는 private database |
| SNS topic | daily task 결과 이메일 알림 |

운영 task에는 AWS Secrets Manager 값이 환경변수로 주입됩니다.

```text
KRX_API_KEY
DART_API_KEY
FMP_API_KEY
S3_BRONZE_BUCKET
SILVER_DB_URL
```

FMP key는 GitHub Actions repository variable `FMP_API_SECRET_ARN`이 가리키는
AWS Secrets Manager 값으로 ECS의 `FMP_API_KEY`에 주입합니다. 새 task definition에
이 secret이 없으면 배포 workflow가 중단됩니다.

`.env`, API key, DB 비밀번호, 로컬 `data/`는 커밋하면 안 됩니다.

## S3 Bronze 구조

버킷:

```text
s3://<bronze-bucket>/
```

경로 구조:

```text
stock/
  marcap/
    date=YYYY-MM-DD/
      all.parquet

  krxapi/
    date=YYYY-MM-DD/
      kospi.parquet
      kosdaq.parquet

index/
  krxapi/
    date=YYYY-MM-DD/
      kospi.parquet
      kosdaq.parquet
      krx.parquet

financials/
  dart/
    corpCode.xml
    year=YYYY/
      corp=<ticker>/
        11011.json   # FY
        11013.json   # Q1
        11012.json   # Q2
        11014.json   # Q3

corporate_actions/
  dart/
    disclosures/
      year=YYYY/date=YYYY-MM-DD/corp=<ticker>/
        rcept=<접수번호>.json
    structured/
      event=<행사종류>/year=YYYY/corp=<ticker>/
        rcept=<접수번호>.json
    documents/
      year=YYYY/corp=<ticker>/
        rcept=<접수번호>.zip
    documents_unavailable/
      year=YYYY/corp=<ticker>/
        rcept=<접수번호>.xml  # DART status=014 원문
    manifests/
      from=YYYYMMDD/to=YYYYMMDD/
        disclosures.json
        structured_complete.json
        documents_complete.json

stock/fmp/
  universe/                         # stock-list, screener, profile bulk 원문
  eod-bulk/date=YYYY-MM-DD/         # 글로벌 CSV 응답 전체
financials/fmp/                     # 글로벌 bulk + 변경 종목별 JSON 원문
corporate_actions/fmp/              # 배당·분할 calendar 전체 응답
fx/fmp/pair=USDKRW/                 # USD/KRW 원문
market/fmp/                         # 미국 거래소 시간·휴일 원문
```

bronze 원칙:

- 가능한 한 원천 응답 단위에 맞춰 파티션을 나눕니다.
- 값은 원천 응답 그대로 저장합니다.
- FMP 글로벌/broad 응답을 미국 주식만 골라 다시 쓰지 않습니다. `response.*`와
  SHA-256·요청정보만 담은 별도 `manifest.json`을 저장하며 API key는 기록하지 않습니다.
- FMP 과거 가격은 XNYS의 실제 완료 거래일만 대상으로 `eod-bulk`를 날짜당 한 번
  호출합니다. 현재 미국 세션과 미래 날짜를 빈 immutable 파티션으로 확정하지 않습니다.
- 재개 시 S3 payload 전체를 다시 내려받지 않고 완료 manifest와 객체 크기로 빠르게
  판정합니다. 전체 SHA-256 재검증 함수는 별도 품질 감사에 사용할 수 있습니다.
- EOD Bulk가 `429`를 반환하면 엔드포인트별 실질 호출 간격을 학습하며, 재무·유니버스
  등 작은 endpoint의 속도는 별도로 유지합니다.
- Bronze에서는 `isEtf`·`isFund` 조건으로 행을 제거하지 않습니다. ETF/fund 및
  비주식 상품 제외는 Silver 후보 생성과 DQ에서만 수행합니다.
- `stock/marcap`은 과거 주식 가격 백필에 사용합니다.
- `stock/krxapi`, `index/krxapi`는 daily 증분 적재에 사용합니다.
- `financials/dart/corpCode.xml`은 bronze에 저장하고 silver에서 재사용합니다.
- DART 공시 목록과 유상·무상증자, 감자, 합병·분할, 주식교환의
  구조화 API 응답은 JSON 원문으로 저장합니다.
- 액면분할·병합, 권리락·배당락처럼 가격조정 효력일·비율 확인에 원문이
  필요한 공시는 `document.xml` 응답인 ZIP도 함께 저장합니다. 배당결정,
  변경상장, 거래정지·상장폐지는 목록 JSON을 보존하되 ZIP은 받지 않습니다.
- 목록에는 있지만 DART 원문 파일이 없는 `status=014` 응답은
  `documents_unavailable`에 원문 XML로 기록하고 재요청하지 않습니다.
- 기간별 manifest와 단계 완료 marker를 저장해 중단 후 완료된 API 단계를
  반복 호출하지 않고 재개합니다.

## RDS Silver 구조

핵심 silver는 PostgreSQL 테이블 5개로 구성됩니다.

```text
asset
asset_identifier
price_daily
fundamental
corporate_action
```

관계:

```text
asset
  -> asset_identifier  # KRX/DART/FMP ticker·corp_code·CIK·CUSIP·ISIN
  -> price_daily       # 주식/지수 일봉, 수정종가, 거래량, 시가총액
  -> fundamental       # DART 재무 지표 long format
  -> corporate_action  # DART/FMP 배당·분할·자본변동
```

| 테이블 | 역할 | 주요 키 |
|---|---|---|
| `asset` | 종목/지수 마스터 | `asset_id` |
| `asset_identifier` | KRX/DART/FMP 식별자 매핑과 유효기간 | `(asset_id, source, identifier_type, identifier, valid_from)` |
| `price_daily` | 주식/벤치마크 지수 일봉 | `(asset_id, source, trade_date)` |
| `fundamental` | DART/FMP 재무계정 long format | `(asset_id, source, statement_type, data_basis, period_end, fiscal_period, fs_type, revision_key, metric)` |
| `corporate_action` | 배당·분할·증자·감자 등 기업행사 | `(asset_id, source, action_key)` |

FMP Silver 편입 대상은 NASDAQ·NYSE·AMEX에서 거래되는 common stock,
preferred stock, ADR, REIT입니다. ETF, fund, ETN, warrant, unit, listed note는
Silver에서 제외하고 제외 건수·사유를 `dq_result`/`dq_metric`에 남깁니다.
FMP `close`는 `adj_close`(분할조정), `adjClose`는 `total_return_close`(배당조정)로
보존하고, 원 OHLC는 수집한 split ratio로 복원합니다. USD 가격·보고통화는 각
행의 `currency`에 기록하며 `USDKRW`도 FX asset의 `price_daily`로 적재합니다.

컬럼별 상세 설계는 [schema_tables.md](schema_tables.md)와 [sql/schema.sql](sql/schema.sql)를 참고합니다.

## RDS Gold 구조

Gold는 별도 인스턴스를 만들지 않고 Silver와 같은 PostgreSQL database의 `gold`
schema에 둡니다. Silver의 `public.asset`을 FK로 직접 참조하므로 별도 종목 마스터
복제나 동기화가 필요 없습니다.

```text
gold.factor
  -> gold.factor_value
  -> gold.factor_correlation
```

| 테이블 | 역할 | 주요 키 |
|---|---|---|
| `gold.factor` | 팩터 정의, 구현 버전, 설정, 최신 평가와 상태 | `(factor_key, version)` |
| `gold.factor_value` | 승인 팩터의 종목×PIT 날짜별 원값과 순위 | `(factor_id, asset_id, as_of_date)` |
| `gold.factor_correlation` | 두 승인 팩터의 기간별 rank Spearman 상관 | `(left_factor_id, right_factor_id, period_start, period_end)` |

상태는 `CANDIDATE`, `APPROVED`, `REJECTED`, `RETIRED`이며, 값과 상관관계는
승인된 팩터만 적재할 수 있도록 DB trigger가 강제합니다. 동일한 `factor_key`에서
`APPROVED` 버전은 하나만 허용합니다.

현재 첫 구현은 **12-1 모멘텀**입니다.

```text
value = adj_close[t-21 거래일] / adj_close[t-252 거래일] - 1
rank  = 같은 as_of_date KOSPI·KOSDAQ 유니버스 내 내림차순 순위
```

구현은 [`pipeline/gold/factors/momentum_12_1.sql`](pipeline/gold/factors/momentum_12_1.sql)에
있습니다. 현재 테스트 스냅샷 적재까지 완료했으며 기준 IC·평가 구간·통과 임계값과
자동 갱신 주기는 아직 확정하지 않았습니다. 따라서 Gold는 현재 daily task에 자동으로
연결하지 않고 연구/평가 단계에서 명시적으로 실행합니다.

상세 설계와 DDL은 [gold_schema.md](gold_schema.md),
[sql/gold_schema.sql](sql/gold_schema.sql)을 참고합니다.

## Daily 실행 흐름

운영 진입점:

```bash
python -m pipeline.daily_full
```

대상 날짜:

- `PIPELINE_DATE`가 있으면 해당 날짜를 사용합니다.
- 없으면 `Asia/Seoul` 기준 어제 날짜를 사용합니다.

실행 순서:

1. 대상 날짜의 KRX 주식/지수 Bronze를 S3에 저장합니다.
2. 당해 연도 DART 재무와 기업행사를 확인하고 변경 원문만 저장합니다.
3. 필요한 S3 객체만 ECS 컨테이너의 `/app/data`로 다운로드합니다.
4. KRX/DART Silver 후보를 생성하고 자동 품질 검사를 수행합니다.
5. Critical/Error가 없을 때만 대상 날짜 교체와 upsert를 하나의 transaction으로 반영합니다.
6. 완료된 직전 미국 세션의 FMP Bronze와 Silver를 별도 transaction으로 처리합니다.
7. 실패하면 이미 인증된 Silver는 유지하고 `dq_run`·`dq_result`에 원인을 기록합니다.

Gold 팩터는 평가 정책과 갱신 주기가 확정되기 전까지 daily 흐름에 포함하지 않습니다.

KRX OpenAPI는 당일 데이터를 안정적으로 제공하지 않기 때문에 다음날 오전에 전날 데이터를 가져옵니다.

```text
화요일 08:30 KST -> 월요일 데이터
수요일 08:30 KST -> 화요일 데이터
...
토요일 08:30 KST -> 금요일 데이터
```

## 로컬 설정

```bash
uv sync
cp .env.example .env
```

`.env` 예시:

```text
KRX_API_KEY=...
DART_API_KEY=...
FMP_API_KEY=...
AWS_PROFILE=<aws-profile>
S3_BRONZE_BUCKET=<bronze-bucket>
SILVER_DB_URL=postgresql://<user>:<password>@<rds-endpoint>:5432/<database>
```

AWS CLI 로그인:

```bash
aws sso login --profile <aws-profile>
```

## 자주 쓰는 명령

문법 확인:

```bash
uv run python -m compileall -q pipeline
```

Silver quality DB migration:

```bash
uv run python -m pipeline.silver_quality.migrate
```

`pipeline.daily_full`은 수집 전에 migration checksum을 읽기 전용으로 확인하며
미적용 DDL을 자동 실행하지 않습니다. 최초 v2 전환은 대형 `price_daily`·
`fundamental`의 타입/PK 변경을 포함하므로 스케줄을 중지하고 RDS snapshot을 만든
maintenance window에서 위 명령을 one-off로 먼저 실행해야 합니다.

최초 Silver backfill:

```bash
uv run python -m pipeline.silver_quality.s3_backfill
# 실패 원인을 수정한 뒤 같은 S3 candidate에서 재개
uv run python -m pipeline.silver_quality.s3_backfill --resume <dq-run-uuid>
```

기존 Silver 품질 감사:

```bash
uv run python -m pipeline.silver_quality.audit --scope all
```

최초 backfill은 연도별 Silver 후보를 S3
`quality/candidates/silver-backfill/run=<run-id>/`에 Parquet으로 고정합니다.
연도 내부와 전체 기간 검사를 모두 통과한 뒤에만 RDS Silver를 월·연도 단위의
제한된 트랜잭션으로 적재하고 `CERTIFIED`로 변경합니다. RDS `quality_stage`에는
전체 후보를 누적하지 않습니다. 일별 `pipeline.daily_full`과 수동 incremental도
동일한 품질 게이트를 자동 실행하며 우회 옵션은 없습니다. 규칙과 severity는
[`pipeline/silver_quality/README.md`](pipeline/silver_quality/README.md)에 정리되어 있습니다.

특정 날짜를 production daily 방식으로 실행:

```bash
PIPELINE_DATE=20260713 uv run python -m pipeline.daily_full
```

bronze 수집기 수동 실행:

```bash
uv run python -m pipeline.bronze.stock_marcap --from 2015 --to 2026 --dest s3
uv run python -m pipeline.bronze.stock_krxapi --from 20260713 --to 20260713 --dest s3
uv run python -m pipeline.bronze.index --from 20260713 --to 20260713 --dest s3
uv run python -m pipeline.bronze.financials --from 2026 --to 2026 --dest s3
uv run python -m pipeline.bronze.financials_full \
  --scope 004990:2015:11011:CFS --dest s3
uv run python -m pipeline.bronze.corporate_actions --from 20150101 --to 20260713 --dest s3
uv run python -m pipeline.bronze.fmp --mode backfill --from 2015 --to 2026 --dest s3
uv run python -m pipeline.bronze.fmp --mode daily --date 20260713 --dest s3
```

FMP 전체 Bronze 이후 Silver를 연도별로 이어서 실행하는 ECS용 진입점:

```bash
uv run python -m pipeline.fmp_backfill_ecs --phase full --from 2015 --to 2026
```

완료된 `response.*`/`manifest.json` 파티션은 재호출하지 않으므로 같은 범위로
재실행해도 이어서 진행합니다. 운영에서는 daily task와 겹치지 않도록 Scheduler를
중지한 maintenance window에서 one-off ECS task로 실행합니다.

로컬 `./data`에서 silver 적재:

```bash
uv run python -m pipeline.silver.load --mode backfill
uv run python -m pipeline.silver.load --mode incremental --date 20260713
uv run python -m pipeline.silver.fmp_load --mode backfill --from 2015 --to 2026
uv run python -m pipeline.silver.fmp_load --mode backfill --resume <dq-run-uuid>
uv run python -m pipeline.silver.fmp_load --mode daily --date 20260713
```

Gold schema 생성:

```bash
psql "$SILVER_DB_URL" -v ON_ERROR_STOP=1 -f sql/gold_schema.sql
```

12-1 모멘텀 SQL은 호출자가 승인된 `factor_id`를 전달하고 transaction을 소유하는
형태입니다. 평가 기준이 확정되기 전에는 자동 배치에 연결하지 않습니다.

GitHub Actions를 쓰지 못할 때 수동 이미지 배포:

```bash
AWS_ACCOUNT_ID=<aws-account-id>
AWS_REGION=ap-northeast-2
ECR_REPOSITORY=<ecr-repository>

AWS_PROFILE=<aws-profile> aws ecr get-login-password --region ap-northeast-2 \
  | docker login --username AWS --password-stdin \
      "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

docker buildx build \
  --platform linux/amd64 \
  -f deploy/Dockerfile \
  -t "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:latest" \
  --push \
  .
```

## 자동 배포

자동 배포는 [.github/workflows/deploy.yml](.github/workflows/deploy.yml)에서 관리합니다.

`main` 브랜치에 push하면 다음 작업이 실행됩니다.

1. PostgreSQL integration test를 포함한 전체 pytest를 실행합니다.
2. GitHub OIDC로 AWS deploy role을 assume합니다.
3. `linux/amd64` Docker 이미지를 빌드합니다.
4. ECR에 commit SHA 태그와 `latest` 태그를 push합니다.
5. ECS task definition 새 revision을 등록합니다.
6. EventBridge Scheduler target을 새 task definition으로 갱신합니다.

필요한 GitHub secret:

```text
AWS_DEPLOY_ROLE_ARN=arn:aws:iam::<aws-account-id>:role/<deploy-role-name>
```

GitHub Actions는 repo 이름을 기준으로 ECR/ECS/Scheduler 이름을 추론합니다. 실제 리소스 이름이 기본 naming convention과 다르면 아래 variables로 override합니다.

```text
ECR_REPOSITORY=<ecr-repository>
ECS_TASK_FAMILY=<ecs-task-definition-family>
CONTAINER_NAME=<ecs-container-name>
SCHEDULE_NAME=<eventbridge-scheduler-name>
FMP_API_SECRET_ARN=<fmp-api-key-secret-arn>
```

## 알림

daily task 결과 알림은 다음 흐름으로 동작합니다.

```text
ECS task STOPPED 이벤트
  -> EventBridge rule
  -> SNS topic
  -> 이메일 구독
```

메일에는 task 상태, exit code, 종료 이유, 시작/종료 시각, task ARN, task definition ARN이 포함됩니다.

```text
Exit code 0 -> 정상 종료
그 외 값 또는 exit code 없음 -> CloudWatch 로그 확인 필요
```

## Git 관리

커밋하지 않는 로컬/생성 파일:

```text
.env
data/
.venv/
__pycache__/
.DS_Store
docs_cache/
```

push 전 확인:

```bash
git status --short
uv run python -m compileall -q pipeline
```
