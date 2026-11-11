# Benchmark results

Rows **50,000** · Seed **20261111** · Batch **1,000**

Verified dataset SHA-256: `7848c807db9fc685185810641f83ddbcfe13c531a79452a36e60b624ff0a489c`

| Engine | Version | Load (s) | Rows/s | Index + statistics (s) |
|---|---|---:|---:|---:|
| sqlite | 3.53.1 | 0.656 | 76,234 | 0.109 |

Query latency in milliseconds. Each cell shows **p50 / p95**.

| Operation | sqlite |
|---|---:|
| point_full_row | 0.0061 / 0.0111 |
| date_range_sum | 2.9562 / 4.6006 |
| decimal_range | 0.0569 / 0.0919 |
| text_prefix | 6.6306 / 10.8700 |
| json_nested_filter | 33.7640 / 42.4390 |
| timestamp_range | 7.1965 / 10.8498 |
| time_range | 7.0561 / 11.5605 |
| boolean_integer_filter | 6.1391 / 8.7546 |
| float_range | 6.5224 / 9.3504 |
| uuid_equality | 0.0289 / 0.2342 |
| binary_equality | 6.4096 / 9.9140 |
| null_filter | 6.1637 / 8.6128 |

Queries used 25 measured rounds and 3 filter warm-ups.
Point lookups used 60 seeded IDs per measured round.

Single client with warm caches. Times include the client driver and result transfer. SQLite and DuckDB are embedded. Server engines run over loopback.
