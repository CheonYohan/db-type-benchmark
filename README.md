# Database type benchmark

PostgreSQL, MySQL, MongoDB, SQLite, DuckDB에 동일한 50,000개 합성 레코드를 적재하고 저장 정확도, 쓰기 처리량, 읽기 지연을 비교합니다.

[GitHub Actions 실행 기록](../../actions/workflows/benchmark.yml)에서 동일 러너에서 실행한 엔진별 요약과 원본 JSON·전체 JSONL을 내려받을 수 있습니다. [결과 디렉터리](results/)에는 검증한 실행 기록을 보관합니다.

## 재현

Python 3.12와 Docker Compose가 필요합니다. 서버 3개는 루프백에만 연결됩니다. Compose의 계정은 이 벤치마크에서만 사용하는 예제 계정입니다.

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

SQLite 단독 실행은 Python 표준 라이브러리로 가능합니다.

```sh
python3 bench.py --engines sqlite --output results/latest.json
```

`--engines`에 이름을 공백으로 나열하면 그 순서로 실행합니다. 각 실행은 고유한 벤치마크 테이블 또는 컬렉션을 만들고 작업 종료 시 해당 객체를 정리합니다.

## 데이터와 자료형

원본은 [결정론적 생성기](dataset.py)에서 생성합니다. [샘플 5행](data/sample.jsonl)은 기본 시드로 생성한 첫 5개입니다. JSONL에는 DECIMAL을 고정 소수점 문자열, UUID를 표준 문자열, BLOB을 16진 문자열로 기록합니다. 시간값은 밀리초 해상도이며 타임스탬프의 기준 시간대는 UTC입니다.

| 논리 형식 | PostgreSQL | MySQL | DuckDB | SQLite | MongoDB |
|---|---|---|---|---|---|
| BIGINT / INTEGER | BIGINT / INT | BIGINT / INT | BIGINT / INT | INTEGER | BSON int32 (이 데이터의 ID 범위) |
| DECIMAL(18,4) | DECIMAL(18,4) | DECIMAL(18,4) | DECIMAL(18,4) | 10,000배 INTEGER | Decimal128 |
| DOUBLE | DOUBLE PRECISION | DOUBLE | DOUBLE | REAL | BSON double |
| TEXT / NULL | TEXT / NULL | TEXT / NULL | TEXT / NULL | TEXT / NULL | BSON string / null |
| JSON 객체·배열 | JSONB | JSON | JSON | JSON 유효성 검사를 적용한 TEXT | BSON document / array |
| DATE | DATE | DATE | DATE | ISO TEXT | UTC 자정 BSON datetime |
| TIMESTAMP / TIME | TIMESTAMP(3) / TIME(3) | DATETIME(3) / TIME(3) | TIMESTAMP / TIME | ISO TEXT | BSON datetime / ISO string |
| BOOLEAN | BOOLEAN | BOOLEAN (TINYINT) | BOOLEAN | INTEGER | BSON bool |
| UUID | UUID | CHAR(36) | UUID | TEXT | BSON Binary UUID |
| BLOB | BYTEA | BLOB | BLOB | BLOB | BSON Binary |

적재 후 모든 행을 ID 순서로 읽어 표준 형식으로 복원하고 SHA-256을 비교합니다. 각 필터의 결과 건수와 날짜 구간의 정확한 소수 합계도 Python 기준값과 비교합니다.

## 측정 범위

- **적재** — 기본키가 있는 새 테이블에서 1,000행 배치 쓰기. 데이터 생성과 인덱스 생성은 제외하고, 드라이버의 인코딩·전송 및 배치 커밋/응답은 포함합니다. SQL 엔진은 배치별 트랜잭션을 쓰고 MongoDB는 저널 쓰기 승인을 사용하는 순서 보장 배치 삽입을 씁니다.
- **인덱스** — 날짜, 금액, UUID에 보조 인덱스를 생성합니다. SQL 엔진의 통계 수집도 이 시간에 포함합니다. DuckDB의 인덱스 사용 여부는 옵티마이저가 결정합니다.
- **읽기** — 기본키로 모든 필드를 반환하는 단건 조회와 자료형별 필터 11개를 측정합니다. 날짜 필터는 건수와 DECIMAL 합계를 반환합니다. 텍스트 접두사와 중첩 JSON 조건은 별도 인덱스 없이 실행합니다.
- **반복** — 필터별 워밍업 3회, 측정 25회, 단건 조회 1,500회. p50은 중앙값이며 p95는 관측값의 nearest-rank 백분위수입니다.
- **환경** — 단일 클라이언트, 검증 후 워밍업 상태, DuckDB 1스레드. 임베디드 엔진은 프로세스 내부에서, 서버 엔진은 루프백 네트워크를 통해 실행합니다. 수치는 이 설정의 드라이버·엔진 조합에 해당합니다. 부하가 큰 동시 요청, 장애 복구, 분산 구성, 전문·벡터 검색은 이번 실행 범위 밖입니다.
