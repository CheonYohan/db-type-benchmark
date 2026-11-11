"""Load, verify and measure the same synthetic dataset in multiple databases."""

import argparse
import json
import math
import operator
import os
import platform
import random
import sys
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from statistics import median
from tempfile import TemporaryDirectory
from time import perf_counter_ns
from uuid import uuid4

from dataset import START, digest, generate
from engines import ENGINE_NAMES, open_engine

PREDICATES = {
    "=": operator.eq,
    ">=": operator.ge,
    "<": operator.lt,
    "prefix": str.startswith,
    "null": operator.is_,
}


@dataclass(frozen=True)
class Case:
    name: str
    conditions: tuple
    sum_amount: bool = False

    def expected(self, rows):
        def matches(row):
            for field, op, value in self.conditions:
                item = row.payload["attributes"]["segment"] if field == "segment" else getattr(row, field)
                if not PREDICATES[op](item, value):
                    return False
            return True

        selected = [row for row in rows if matches(row)]
        if self.sum_amount:
            return len(selected), sum((row.amount for row in selected), Decimal(0))
        return len(selected)


def cases(rows):
    sample = rows[len(rows) // 2]
    start = START + timedelta(days=180)
    end = start + timedelta(days=30)
    return (
        Case("date_range_sum", (("day", ">=", start), ("day", "<", end)), sum_amount=True),
        Case("decimal_range", (("amount", ">=", Decimal("-125.4321")), ("amount", "<", Decimal("125.4321")))),
        Case("text_prefix", (("label", "prefix", "sku-03"),)),
        Case("json_nested_filter", (("segment", "=", "s03"),)),
        Case("timestamp_range", (("event_ts", ">=", datetime.combine(start, time(12))),
                                 ("event_ts", "<", datetime.combine(start + timedelta(days=3), time(12))))),
        Case("time_range", (("clock", ">=", time(8, 30)), ("clock", "<", time(9)))),
        Case("boolean_integer_filter", (("enabled", "=", True), ("units", ">=", 8_000))),
        Case("float_range", (("ratio", ">=", -1.25), ("ratio", "<", 1.25))),
        Case("uuid_equality", (("uid", "=", sample.uid),)),
        Case("binary_equality", (("raw", "=", sample.raw),)),
        Case("null_filter", (("note", "null", None),)),
    )


def latency(samples):
    values = sorted(samples)
    return {
        "samples": len(values),
        "p50_ms": round(median(values) / 1_000_000, 6),
        "p95_ms": round(values[math.ceil(0.95 * len(values)) - 1] / 1_000_000, 6),
    }


def measure(adapter, rows, checksum, workload, expected, point_ids, args):
    started = perf_counter_ns()
    for offset in range(0, len(rows), args.batch):
        adapter.batch(rows[offset:offset + args.batch])
    load_seconds = (perf_counter_ns() - started) / 1_000_000_000
    started = perf_counter_ns()
    adapter.indexes()
    index_seconds = (perf_counter_ns() - started) / 1_000_000_000

    actual_hash = digest(adapter.rows())
    if actual_hash != checksum:
        raise ValueError(f"Round-trip SHA-256 differs: {actual_hash} != {checksum}")
    for case in workload:
        actual = adapter.filter(case.conditions, case.sum_amount)
        if actual != expected[case.name]:
            raise ValueError(f"{case.name}: {actual!r} != {expected[case.name]!r}")

    for identifier in point_ids[:min(100, len(point_ids))]:
        if adapter.point(identifier) is None:
            raise ValueError(f"Missing point lookup for id={identifier}")
    for _ in range(args.warmups):
        for case in workload:
            adapter.filter(case.conditions, case.sum_amount)

    samples = {"point_full_row": []} | {case.name: [] for case in workload}
    for repeat in range(args.repeats):
        for identifier in point_ids:
            started = perf_counter_ns()
            row = adapter.point(identifier)
            samples["point_full_row"].append(perf_counter_ns() - started)
            if row is None:
                raise ValueError(f"Missing point lookup for id={identifier}")
        shuffled = list(workload)
        random.Random(args.seed + repeat).shuffle(shuffled)
        for case in shuffled:
            started = perf_counter_ns()
            actual = adapter.filter(case.conditions, case.sum_amount)
            samples[case.name].append(perf_counter_ns() - started)
            if actual != expected[case.name]:
                raise ValueError(f"{case.name}: {actual!r} != {expected[case.name]!r}")

    return {
        "version": adapter.version(),
        "verified_rows": len(rows),
        "verified_sha256": actual_hash,
        "load_seconds": round(load_seconds, 6),
        "rows_per_second": round(len(rows) / load_seconds, 2),
        "index_and_statistics_seconds": round(index_seconds, 6),
        "operations": {name: latency(values) for name, values in samples.items()},
    }


def machine():
    metadata = {
        "os": platform.system(),
        "architecture": platform.machine(),
        "python": platform.python_version(),
        "visible_logical_cpus": os.cpu_count(),
    }
    for name, path in (
        ("cgroup_cpu_max", "/sys/fs/cgroup/cpu.max"),
        ("cgroup_memory_max", "/sys/fs/cgroup/memory.max"),
    ):
        try:
            metadata[name] = Path(path).read_text().strip()
        except OSError:
            pass
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                metadata["cpu_model"] = line.partition(":")[2].strip()
                break
    except OSError:
        pass
    return metadata


def markdown(report):
    lines = [
        "# Benchmark results",
        "",
        f"Rows **{report['rows']:,}** · Seed **{report['seed']}** · Batch **{report['batch']:,}**",
        "",
        f"Verified dataset SHA-256: `{report['dataset_sha256']}`",
        "",
        "| Engine | Version | Load (s) | Rows/s | Index + statistics (s) |",
        "|---|---|---:|---:|---:|",
    ]
    for name, result in report["engines"].items():
        version = result["version"].replace("|", "/")
        lines.append(
            f"| {name} | {version} | {result['load_seconds']:.3f} | "
            f"{result['rows_per_second']:,.0f} | {result['index_and_statistics_seconds']:.3f} |"
        )
    if report["engines"]:
        lines += ["", "Query latency in milliseconds. Each cell shows **p50 / p95**.", ""]
        lines.append("| Operation | " + " | ".join(report["engines"]) + " |")
        lines.append("|---|" + "---:|" * len(report["engines"]))
        for operation in next(iter(report["engines"].values()))["operations"]:
            cells = []
            for result in report["engines"].values():
                stats = result["operations"][operation]
                cells.append(f"{stats['p50_ms']:.4f} / {stats['p95_ms']:.4f}")
            lines.append(f"| {operation} | " + " | ".join(cells) + " |")
    if report["errors"]:
        lines += ["", "Engines with no result: " + ", ".join(report["errors"])]
    lines += [
        "",
        f"Queries used {report['repeats']} measured rounds and {report['warmups']} filter warm-ups.",
        f"Point lookups used {report['points_per_round']} seeded IDs per measured round.",
        "",
        "Single client with warm caches. Times include the client driver and result transfer. "
        "SQLite and DuckDB are embedded. Server engines run over loopback.",
        "",
    ]
    return "\n".join(lines)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engines", nargs="+", choices=ENGINE_NAMES, default=list(ENGINE_NAMES))
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--seed", type=int, default=20261111)
    parser.add_argument("--batch", type=int, default=1_000)
    parser.add_argument("--repeats", type=int, default=25)
    parser.add_argument("--points", type=int, default=60)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--output", type=Path, default=Path("results/latest.json"))
    parser.add_argument("--export-data", type=Path, help="Write canonical JSONL used for the dataset SHA-256")
    args = parser.parse_args()
    if min(args.rows, args.batch, args.repeats, args.points) < 1 or args.warmups < 0:
        parser.error("rows, batch, repeats and points must be positive and warmups nonnegative")
    if len(set(args.engines)) != len(args.engines):
        parser.error("each engine can appear only once")
    return args


def main():
    args = parse_args()
    rows = list(generate(args.rows, args.seed))
    if args.export_data:
        args.export_data.parent.mkdir(parents=True, exist_ok=True)
        with args.export_data.open("wb") as output:
            checksum = digest(rows, output)
    else:
        checksum = digest(rows)
    workload = cases(rows)
    expected = {case.name: case.expected(rows) for case in workload}
    rng = random.Random(args.seed)
    point_ids = [rng.randrange(1, len(rows) + 1) for _ in range(args.points)]
    report = {
        "rows": len(rows), "seed": args.seed, "batch": args.batch, "repeats": args.repeats,
        "points_per_round": args.points, "warmups": args.warmups, "dataset_sha256": checksum,
        "environment": machine(), "query_expected": expected, "engine_order": args.engines,
        "engines": {}, "errors": {},
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    for engine in args.engines:
        print(f"Measuring {engine} with {len(rows):,} records", flush=True)
        table = f"bench_{uuid4().hex}"
        try:
            with TemporaryDirectory(prefix=f"{engine}-", dir=args.output.parent) as directory:
                adapter = open_engine(engine, table, Path(directory))
                try:
                    result = measure(adapter, rows, checksum, workload, expected, point_ids, args)
                finally:
                    adapter.close()
            report["engines"][engine] = result
            print(f"Verified {result['verified_rows']:,} rows. Load: {result['rows_per_second']:,.0f} rows/s", flush=True)
        except Exception as error:
            report["errors"][engine] = type(error).__name__
            print(f"{engine}: {type(error).__name__}: {error}", file=sys.stderr, flush=True)

    args.output.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    args.output.with_suffix(".md").write_text(markdown(report), encoding="utf-8")
    return int(bool(report["errors"]))


if __name__ == "__main__":
    sys.exit(main())
