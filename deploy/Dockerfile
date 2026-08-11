# TeamAlpha-data 파이프라인 이미지 (ECS/Fargate 배치)
# EventBridge 스케줄 → ECS Task 로 전일 bronze 수집 + silver 증분 적재를 실행한다.
FROM python:3.12-slim

ARG SOURCE_COMMIT
ARG SOURCE_REPOSITORY="https://github.com/danielhO9/TeamAlpha-data"

# Every production image must identify the exact clean Git commit used by the
# builder.  The ECR digest fixes the deployed bytes; these OCI labels bind those
# bytes back to auditable source.
RUN printf '%s' "${SOURCE_COMMIT}" \
    | grep -Eq '^[0-9a-f]{40}$'
LABEL org.opencontainers.image.revision="${SOURCE_COMMIT}" \
      org.opencontainers.image.source="${SOURCE_REPOSITORY}"

# uv 설치
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY pipeline/ ./pipeline/

# 자격증명: 로컬 .env 아님. ECS Task Role(S3/RDS) + Secrets(KRX/DART 키) 로 주입.
ENTRYPOINT ["uv", "run", "python", "-m", "pipeline.daily_full"]
