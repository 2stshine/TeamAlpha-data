# Silver 데이터 품질 현황

Silver 품질 게이트의 현재 상태 기록. 아래 **전체 silver 수치**는 최근 전체 감사
(`s3_domain_audit`) 기준이고, 일별 증분은 그날 델타만 평가하므로 전체 수치를
바꾸지 않는다. 전체 수치는 새 전체 감사를 돌릴 때 갱신한다.

- 룰셋: **1.17.0** — 2026-07-28 운영 배포 완료
- 전체 수치 기준: Bronze cutoff `f6d0af48…` (2026-07-24), 가격 6,749,929행 · 재무 2,195,327행
- 운영 검증: 2026-07-27 daily 증분 **CERTIFIED**(2,767행 publish, 차단 실패 0,
  Modified 정상 기록) — 새 룰셋이 운영에서 정상 동작 확인
- 종합: **차단 실패 0, 파이프라인發 데이터 오류 0.** Warning 376건은 전부 원인별로
  분류·설명했고 의심되는 극단치는 개별 확인함(실제 이벤트 또는 DART 원천 오류).
  Modified는 결정적 규칙 변환.

---

## 차단 (Critical / Error) — **없음**

전체 감사에서 CRITICAL·ERROR 실패 0건. publish 게이트 통과 상태.
중복키·필수키·OHLC 논리·시장 완전성·벤치마크·수정종가 대사(전체 시계열/스트리밍)·
회계식·통화·행수 대사 등 모든 차단 검사 통과.

## Warning (비차단, 검토 대상) — 총 **376건**

376건 전부 원인별로 분류·설명했고 의심되는 극단치는 개별 확인했다.
**모두 정당한 값이며 파이프라인이 만든 데이터 오류가 아니다.**

| 규칙 | 건수 | 성격 | 판정 |
|---|---:|---|---|
| `PRICE_ADJUSTMENT_WITHOUT_DART_EVENT` | 316 | DART·거래재개 근거 없는 KRX 기준가 리셋 | 권리락·배당락·펀드분배 등 실제 이벤트. 값 정상 |
| `DART_ACTION_WITHOUT_KRX_ADJUSTMENT` | 20 | DART 효력일에 KRX 조정계수 없음 | 효력일 오정렬/거래정지/무효력. adj_close 무결 |
| `PRICE_DISTRIBUTION_DRIFT` | 10 | 횡단면 수익률 이상치 | 전부 실제 시장 급변일(벤치마크·종목폭 확인) |
| `DART_SOURCE_ACCOUNTING_INCONSISTENCY` | 30 | 재무 회계식 불일치 | DART 원천 오류(우리 매핑 정확). 원값 보존+플래그 |

### `PRICE_ADJUSTMENT_WITHOUT_DART_EVENT` 316 세부

| 구성 | 대략 | 비고 |
|---|---:|---|
| 유상증자 권리락 | ~203 | KOSPI. DART에 권리락일이 없어 자동 확인 불가 |
| 연말 배당락류 | ~63 | 연말 기준가 조정 |
| 무상증자 권리락 | ~18 | |
| 미분류 | ~32 | 자원·개발펀드 원금상환/분배 등. 개별 확인 완료, 데이터 오류 0 (극단치 예: 152550 한국ANKOR유전) |

## Modified (실제 값 변경) — 수정 행 수 포함

Silver 생성 시 **행 안의 값을 실제로 바꾼** 것만 기록한다. 모두 규칙 기반 결정적 변환.

| 규칙 | 값 변경 | 수정 행 수 |
|---|---|---:|
| `SOURCE_NO_TRADE_OHLC` | 무거래 행의 O/H/L `0 → NULL` (실거래 없는 sentinel 정규화) | 206,005 |
| `SOURCE_INCOMPLETE_OHLC` | 거래 있으나 O/H/L=0인 행의 O/H/L `0 → NULL` (close·거래량·시총은 원본 보존) | 10 |
| `DART_ACCOUNTING_EQUATION_SUPPLEMENT_REPLACEMENT` | 회계식 불일치 시 자산·부채·자본을 전체재무제표 값으로 교체 | 0 |

- 대부분(206,005행)은 무거래일 O/H/L 결측 정규화이고, 실거래 값을 덮어쓴 건 아니다.
- 값 교체(REPLACEMENT)는 독립 신뢰 출처(같은 revision 전체재무제표)가 회계식을
  대사할 때만 하며, 발동 시 원값·교체값·출처를 기록한다. 이 스냅샷에선 0건.
- 원천(KRX·DART) 오류는 값을 고치지 않고 보존 + 플래그가 원칙이다.

---

## 갱신 방법

이 문서는 스냅샷 기준 시점 기록이다. 운영 Silver는 매일 증분이 누적되므로,
현재 전체 상태를 다시 확정하려면 전체 감사를 재실행하고 수치를 갱신한다.

```bash
uv run python -m pipeline.silver_quality.s3_domain_audit --action init
uv run python -m pipeline.silver_quality.s3_domain_audit --action domain --domain prices --parent-run-id <run-id>
uv run python -m pipeline.silver_quality.s3_domain_audit --action domain --domain fundamentals --parent-run-id <run-id>
uv run python -m pipeline.silver_quality.s3_domain_audit --action finalize --parent-run-id <run-id>
```

일별 증분은 그날 델타만 평가하므로 `dq_run`·`dq_result`의 daily 기록은 "그날
무엇이 flag됐나"의 로그다. 전체 Silver 진단은 위 전체 감사 결과를 기준으로 한다.
규칙 정의는 [`README.md`](README.md) 참고.
