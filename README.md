# 동일한 혼합 자료형 데이터에 대한 5개 데이터베이스 성능 비교

**실험일** 2026년 11월 11일 · **실험 범위** 단일 클라이언트, 합성 데이터 50,000행

[검증된 실행 기록](https://github.com/CheonYohan/db-type-benchmark/actions/runs/42950079690) · [측정값 JSON](results/github-actions-2026-11-11.json) · [전체 데이터 JSONL 아티팩트](https://github.com/CheonYohan/db-type-benchmark/actions/runs/42950079690#artifacts)

## 초록

동일한 합성 레코드 50,000행을 SQLite, DuckDB, PostgreSQL, MySQL, MongoDB에 적재하고, 1,000행 단위 배치 적재의 처리량과 12개 읽기 작업의 지연을 측정했다. 5개 엔진에서 읽어 온 데이터를 동일한 형식으로 변환한 뒤 계산한 SHA-256이 원본과 일치했으며, 필터 건수와 날짜 범위의 DECIMAL 합계도 일치했다. SQLite는 배치 적재와 기본키 조회에서 가장 빠른 관측값을 보였고, DuckDB는 날짜 범위 집계 및 인덱스 없는 TEXT·JSON 조회에서 가장 낮은 p50을 기록했다. 서버형 엔진에서는 MongoDB가 적재 처리량에서, PostgreSQL이 읽기 작업 12개 중 11개의 p50에서 앞섰다.

**주제어** 데이터베이스 벤치마크, 자료형, 배치 적재, 읽기 지연, 재현성

## 1. 서론

혼합 자료형을 사용하는 애플리케이션은 같은 논리 값을 엔진마다 다른 저장 형식으로 표현한다. 배치 적재와 기본키 조회, 범위 집계의 비용도 각각 다르므로, 동일한 입력과 조회 조건을 사용하고 결과의 일치 여부를 확인한 뒤 작업별 성능을 비교했다.

1. 엔진마다 저장 형식이 달라도 동일한 논리 값과 조회 결과를 보존하는가?
2. 동일한 데이터와 배치 크기에서 적재 처리량과 인덱스 준비 시간은 어떻게 다른가?
3. 기본키 조회와 자료형별 필터에서 p50과 p95는 어떻게 달라지는가?

## 2. 자료 및 방법

### 2.1 데이터셋

시드 `20261111`로 13개 필드를 가진 레코드 50,000행을 생성했다. ID는 1부터 50,000까지 순차 부여했다. 레코드에는 한글과 이모지가 포함된 TEXT, 양수와 음수의 고정소수점 값, 중첩 객체·배열·null이 포함된 JSON, 날짜와 밀리초 단위 시간, UUID, 24바이트 이진값 및 NULL을 포함했다. 원본 타임스탬프의 시간 기준은 UTC다. [데이터 생성기](dataset.py) · [샘플 5행](data/sample.jsonl)

**표 1. 논리 자료형과 엔진별 저장 표현**

| 필드 및 논리 자료형 | SQLite | DuckDB | PostgreSQL | MySQL | MongoDB |
|---|---|---|---|---|---|
| `id`, `units` · BIGINT / INTEGER | INTEGER | BIGINT / INTEGER | BIGINT / INTEGER | BIGINT / INTEGER | BSON int32 |
| `amount` · DECIMAL(18,4) | 10,000배 INTEGER | DECIMAL(18,4) | DECIMAL(18,4) | DECIMAL(18,4) | BSON Decimal128 |
| `ratio` · DOUBLE | REAL | DOUBLE | DOUBLE PRECISION | DOUBLE | BSON double |
| `label`, `note` · TEXT / NULL | TEXT / NULL | TEXT / NULL | TEXT / NULL | TEXT / NULL | BSON string / null |
| `payload` · JSON | 유효성 검사를 적용한 TEXT | JSON | JSONB | JSON | BSON document / array |
| `day` · DATE | ISO TEXT | DATE | DATE | DATE | UTC 자정의 BSON datetime |
| `event_ts`, `clock` · TIMESTAMP / TIME | ISO TEXT | TIMESTAMP / TIME | TIMESTAMP(3) / TIME(3) | DATETIME(3) / TIME(3) | BSON datetime / ISO 문자열 |
| `enabled` · BOOLEAN | INTEGER | BOOLEAN | BOOLEAN | BOOLEAN(TINYINT) | BSON bool |
| `uid` · UUID | TEXT | UUID | UUID | CHAR(36) | BSON Binary UUID |
| `raw` · BLOB | BLOB | BLOB | BYTEA | BLOB | BSON Binary |

### 2.2 실행 환경 및 측정 절차

- **환경** GitHub Actions 호스팅 러너, Ubuntu 22.04 x86-64, Intel Xeon Platinum 8573C, 노출된 논리 CPU 4개, Python 3.12.16. SQLite와 DuckDB는 같은 프로세스의 파일 기반 DB로 실행했다. PostgreSQL, MySQL, MongoDB는 같은 러너의 Docker 컨테이너에서 실행하고 루프백으로 연결했다. DuckDB에는 스레드 1개를 설정했다.
- **적재** 엔진마다 새 테이블 또는 컬렉션을 만들었다. 기본키가 있는 상태에서 1,000행씩 순차 적재했다. SQL 엔진은 드라이버의 `executemany`로 배치당 한 트랜잭션을 실행했다. SQLite는 WAL 및 `synchronous=FULL`, MongoDB는 순서 보장 배치 삽입 및 저널 쓰기 승인(`w=1, j=true`)을 사용했다. 적재 시간에는 변환, 드라이버 호출, 배치 커밋 또는 쓰기 승인 대기가 포함된다. 처리량은 전체 행 수를 적재 시간으로 나눴다.
- **인덱스와 검증** 적재 후 날짜, 금액, UUID에 보조 인덱스를 생성했다. 표 3의 준비 시간에는 SQL 엔진의 통계 수집도 포함된다. 전체 행을 ID 순서로 읽어 표준 JSONL 표현으로 변환한 뒤 SHA-256을 비교했다. 필터 건수와 DECIMAL 합계는 Python에서 계산한 기준값과 비교했다.
- **조회** 검증 후 필터별 추가 워밍업 3회를 거쳐 25회 측정했다. 기본키 조회는 동일하게 뽑은 ID 60개를 매 회차 실행해 총 1,500건을 측정했다. 타이머에는 쿼리 실행과 클라이언트가 결과를 읽는 시간이 포함된다. p50은 중앙값, p95는 오름차순으로 정렬한 표본에서 `ceil(0.95 × n)`번째 관측값이다.

**표 2. 읽기 작업의 조건과 검증 기준값**

| 작업 | 조건 | 기대 결과 |
|---|---|---:|
| 기본키 전체 행 조회 | 시드로 뽑은 ID 60개를 각 회차에 개별 조회 | 조회당 1행 |
| DATE 범위 건수 및 DECIMAL 합계 | `2023-06-30 ≤ day < 2023-07-30`에서 `COUNT` 및 `SUM(amount)` | 2,060행, 합계 `-64263.5210` |
| DECIMAL 범위 | `−125.4321 ≤ amount < 125.4321` | 1,238행 |
| TEXT 접두사 | `label`이 `sku-03`으로 시작 | 1,550행 |
| JSON 중첩 속성 | `payload.attributes.segment = "s03"` | 3,163행 |
| TIMESTAMP 범위 | `2023-06-30 12:00 ≤ event_ts < 2023-07-03 12:00` (UTC 기준) | 195행 |
| TIME 범위 | `08:30 ≤ clock < 09:00` | 1,067행 |
| BOOLEAN 및 INTEGER 조건 | `enabled = true` 및 `units ≥ 8000` | 2,566행 |
| DOUBLE 범위 | `−1.25 ≤ ratio < 1.25` | 104행 |
| UUID 일치 | `uid`가 표본 행의 UUID와 일치 | 1행 |
| BLOB 일치 | `raw`가 표본 행의 24바이트 값과 일치 | 1행 |
| NULL 필터 | `note`의 값이 `NULL` | 7,142행 |

범위 조건은 시작값을 포함하고 종료값은 제외한다. 날짜 범위는 건수와 합계를, 기본키 조회는 전체 레코드를 반환한다. 나머지 필터는 일치하는 행 수를 반환한다. TEXT는 SQL의 접두사 `LIKE`와 MongoDB의 접두사 정규식으로 구현했다. TEXT와 JSON 필터에는 보조 인덱스를 만들지 않았다.

## 3. 결과

### 3.1 데이터 무결성

모든 엔진이 50,000행을 반환했고, 표 2의 결과가 기준값과 일치했다. 원본과 5개 엔진의 검증 SHA-256은 다음과 같다.

```text
7848c807db9fc685185810641f83ddbcfe13c531a79452a36e60b624ff0a489c
```

### 3.2 배치 적재 및 인덱스 준비

**표 3. 엔진별 적재 및 인덱스 준비 시간**

| 엔진 | 버전 | 적재 시간(초) | 처리량(행/초) | 인덱스·통계 준비(초) |
|---|---|---:|---:|---:|
| SQLite | 3.37.2 | 0.911 | 54,884 | 0.110 |
| DuckDB | 1.4.4 | 69.117 | 723 | 0.575 |
| PostgreSQL | 17.11 | 2.527 | 19,786 | 0.810 |
| MySQL | 8.4.13 | 3.086 | 16,202 | 0.356 |
| MongoDB | 8.0.34 | 1.290 | 38,773 | 0.256 |

### 3.3 읽기 지연

**표 4. 작업별 p50 / p95 (ms)**

굵게 표시한 값은 각 작업의 최저 p50이다. 수치는 소수점 넷째 자리까지 표시했다.

| 작업 | SQLite | DuckDB | PostgreSQL | MySQL | MongoDB |
|---|---:|---:|---:|---:|---:|
| 기본키 전체 행 조회 | **0.0089** / 0.0130 | 0.5548 / 0.6812 | 0.2112 / 0.3325 | 0.2815 / 0.4813 | 0.3157 / 0.5594 |
| DATE 범위 건수 및 DECIMAL 합계 | 2.4586 / 2.7868 | **0.9179** / 1.0800 | 1.6404 / 1.7505 | 4.4969 / 5.7068 | 4.1075 / 6.1872 |
| DECIMAL 범위 | **0.0665** / 0.0820 | 0.8044 / 1.0480 | 1.1799 / 1.2843 | 0.5851 / 0.8998 | 1.2133 / 1.3806 |
| TEXT 접두사 | 6.7946 / 7.1220 | **2.1122** / 2.2156 | 6.0975 / 7.0079 | 16.1042 / 17.8080 | 31.6900 / 42.3897 |
| JSON 중첩 속성 | 31.5169 / 34.0745 | **9.2648** / 10.3094 | 11.9558 / 12.3902 | 23.7007 / 28.4391 | 13.5235 / 15.6899 |
| TIMESTAMP 범위 | 6.8465 / 7.0926 | **0.7570** / 0.9632 | 7.0666 / 7.3699 | 7.8445 / 8.6607 | 12.4627 / 14.4575 |
| TIME 범위 | 7.0016 / 7.1899 | **0.7309** / 0.8478 | 7.2696 / 7.8592 | 8.0514 / 9.1895 | 13.6526 / 16.1332 |
| BOOLEAN 및 INTEGER 조건 | 6.3147 / 6.4756 | **0.7585** / 0.9345 | 7.0224 / 7.6044 | 8.1014 / 8.4906 | 15.2720 / 16.5715 |
| DOUBLE 범위 | 6.0314 / 6.2444 | **1.3557** / 1.5537 | 5.2948 / 5.5460 | 7.2468 / 7.4691 | 11.9166 / 16.3189 |
| UUID 일치 | **0.0354** / 0.0482 | 0.4905 / 0.6448 | 0.2870 / 0.3662 | 0.3229 / 0.4845 | 0.9547 / 1.1286 |
| BLOB 일치 | 6.1058 / 6.2855 | **0.7633** / 1.1138 | 7.1832 / 7.5309 | 14.9697 / 15.7347 | 13.2321 / 14.5450 |
| NULL 필터 | 5.9805 / 6.1864 | **0.7133** / 0.9146 | 6.7337 / 7.0121 | 14.1561 / 17.5615 | 13.5800 / 14.8122 |

## 4. 논의

**기본키 조회와 필터는 다른 경로를 측정한다.** 기본키 조회는 한 행 전체를 읽으며, TEXT와 JSON 필터는 보조 인덱스 없이 조건에 맞는 행 수를 계산한다. SQLite의 기본키 지연에는 프로세스 내부 연결의 이점이 포함되고, DuckDB의 결과는 여러 행을 읽고 집계하는 작업에서의 성능을 보여준다.

**서버형 엔진의 순위도 작업에 따라 달랐다.** PostgreSQL은 서버형 3개 중 기본키 조회 p50이 0.2112 ms였고, 날짜 범위 집계에서는 1.6404 ms를 기록했다. DECIMAL 범위 조회의 최저 p50은 MySQL의 0.5851 ms였다. MongoDB의 배치 적재는 초당 38,773행이었으며, 이 수치에는 단일 서버의 저널 쓰기 승인 대기가 포함된다.

**적재 결과는 선택한 인터페이스의 영향을 받는다.** DuckDB의 초당 723행은 Python `executemany` 경로의 관측값이다. 파일이나 Arrow를 통한 대량 적재는 별도의 실험 조건이다. SQLite의 DECIMAL 범위 조회는 10,000배 정수 표현을 사용했으므로 다른 엔진의 네이티브 DECIMAL 연산과 저장 비용을 직접 비교하기 어렵다.

## 5. 한계

- 데이터 규모는 50,000행 한 가지이며, 조회는 검증과 워밍업 이후 단일 클라이언트로 실행했다. 동시 쓰기, 캐시가 비어 있는 상태, 대용량 데이터 및 분산 환경은 각각 별도 측정 조건이다.
- 엔진을 같은 러너에서 고정 순서로 한 번씩 실행했다. 적재는 엔진별 1회였으며, p95는 한 실행 내 관측값이다. 필터는 각각 25회이므로 p95는 정렬한 표본의 24번째 값이다.
- 임베디드 호출과 루프백 통신, 드라이버 구현, JSON·DECIMAL 저장 방식, 쓰기 승인 방식의 차이가 측정값에 포함된다. 배치 적재, 전체 행 단건 조회, 필터 및 집계의 결과를 각 작업별로 해석해야 한다.

## 6. 결론

비교 단위는 명시한 실행 조건의 엔진과 드라이버 조합이다. 이 조건에서 데이터의 논리 값과 조회 결과를 동일하게 유지하면서도, 적재와 읽기 작업에 따라 성능 순위가 달라졌다. 실제 도입 판단에는 대상 서비스의 조회 유형, 쓰기 패턴, 데이터 규모 및 동시성을 반영한 측정이 필요하다.

## 7. 재현 방법 및 실험 자료

Python 3.12와 Docker Compose를 사용한다. 서버 세 개는 로컬 루프백 포트에만 연결된다. 실행 코드는 고유한 벤치마크 테이블 또는 컬렉션을 만든 뒤 작업 종료 시 정리한다.

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
docker compose up -d --wait
python bench.py --rows 50000 --seed 20261111 --batch 1000 \
  --repeats 25 --points 60 --warmups 3 \
  --output results/latest.json --export-data results/dataset.jsonl
docker compose down --volumes
```

`--engines`에 엔진 이름을 공백으로 나열하면 지정한 순서로 실행한다. SQLite 단독 실행은 Python 표준 라이브러리로 가능하다.

```sh
python3 bench.py --engines sqlite --output results/latest.json
```

- [실험 실행에 사용한 코드](https://github.com/CheonYohan/db-type-benchmark/tree/54ccf1f39e305e50e54d5a615fb13616cfadc340)
- [측정값 JSON](results/github-actions-2026-11-11.json) 및 [실행 보고서](results/github-actions-2026-11-11.md)
- [GitHub Actions 워크플로](https://github.com/CheonYohan/db-type-benchmark/actions/workflows/benchmark.yml) 및 [성공한 실행](https://github.com/CheonYohan/db-type-benchmark/actions/runs/42950079690)
- [전체 합성 데이터 JSONL](https://github.com/CheonYohan/db-type-benchmark/actions/runs/42950079690#artifacts). 아티팩트 보관 기간 이후에는 위 명령과 동일한 시드로 재생성할 수 있다.
