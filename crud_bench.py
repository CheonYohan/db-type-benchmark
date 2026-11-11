"""Compare all-row CRUD and query latency with and without secondary indexes."""

import argparse
import json
import random
import sys
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from statistics import median
from tempfile import TemporaryDirectory
from time import perf_counter_ns
from uuid import uuid4

from bench import cases, latency, machine
from crud_engines import chunks, open_crud_engine
from dataset import digest, generate
from engines import ENGINE_NAMES, INDEX_FIELDS

PROFILES = ("pk_only", "secondary")
WRITES = ("insert", "update_note", "update_amount", "delete")
PLAN_CASES = ("date_range_sum", "decimal_range", "uuid_equality")
NOTE = "수정 완료"
AMOUNT_DELTA = Decimal("0.2500")


@contextmanager
def fixture(engine, directory):
    with TemporaryDirectory(prefix=f"{engine}-", dir=directory) as path:
        adapter = open_crud_engine(engine, f"bench_{uuid4().hex}", Path(path))
        try:
            yield adapter
        finally:
            adapter.close()


def time_batches(action, values, size):
    samples = []
    started = perf_counter_ns()
    for group in chunks(values, size):
        batch_started = perf_counter_ns()
        action(group)
        samples.append(perf_counter_ns() - batch_started)
    return perf_counter_ns() - started, samples


def read_queries(adapter, workload, expected, point_ids, args, trial):
    samples = {"point_full_row": []} | {case.name: [] for case in workload}
    for _ in range(args.warmups):
        for case in workload:
            adapter.filter(case.conditions, case.sum_amount)
        for identifier in point_ids:
            adapter.point(identifier)
    for repeat in range(args.query_rounds):
        for identifier in point_ids:
            started = perf_counter_ns()
            row = adapter.point(identifier)
            samples["point_full_row"].append(perf_counter_ns() - started)
            if row is None:
                raise ValueError(f"Missing primary key {identifier}")
        ordered = list(workload)
        random.Random(args.seed + trial * args.query_rounds + repeat).shuffle(ordered)
        for case in ordered:
            started = perf_counter_ns()
            actual = adapter.filter(case.conditions, case.sum_amount)
            samples[case.name].append(perf_counter_ns() - started)
            if actual != expected[case.name]:
                raise ValueError(f"{case.name}: expected {expected[case.name]!r}, got {actual!r}")
    return samples


def run_trial(adapter, profile, rows, checksum, modified_checksum, workload, expected, args, trial):
    if profile == "secondary":
        adapter.indexes()

    insert = time_batches(adapter.insert_batch, rows, args.batch)
    adapter.analyze()

    started = perf_counter_ns()
    retrieved = list(adapter.rows())
    scan = perf_counter_ns() - started
    if len(retrieved) != len(rows) or digest(retrieved) != checksum:
        raise ValueError("Read-back does not match the original dataset")
    del retrieved

    point_rng = random.Random(args.seed + trial)
    point_ids = point_rng.sample(range(1, len(rows) + 1), min(args.points, len(rows)))
    queries = read_queries(adapter, workload, expected, point_ids, args, trial)
    plans = {case.name: adapter.plan(case) for case in workload if case.name in PLAN_CASES} if trial == 0 else None

    ids = list(range(1, len(rows) + 1))
    random.Random(args.seed + trial).shuffle(ids)
    update_note = time_batches(lambda group: adapter.update_note(group, NOTE), ids, args.batch)
    update_amount = time_batches(lambda group: adapter.update_amount(group, AMOUNT_DELTA), ids, args.batch)
    if digest(adapter.rows()) != modified_checksum:
        raise ValueError("Full read-back does not match the expected updated dataset")

    delete = time_batches(adapter.delete_batch, ids, args.batch)
    if adapter.count() != 0:
        raise ValueError("Rows remain after deleting all primary keys")
    return {
        "writes": dict(zip(WRITES, (insert, update_note, update_amount, delete), strict=True)),
        "scan": scan,
        "queries": queries,
        "plans": plans,
    }


def seconds(nanoseconds):
    return round(nanoseconds / 1_000_000_000, 6)


def summarize_operation(totals, row_count):
    middle = median(totals)
    return {
        "trial_seconds": [seconds(nanoseconds) for nanoseconds in totals],
        "median_seconds": seconds(middle),
        "min_seconds": seconds(min(totals)),
        "max_seconds": seconds(max(totals)),
        "rows_per_second": round(row_count * 1_000_000_000 / middle, 2),
    }


def summarize_profile(trials, row_count):
    operations = {"read_all": summarize_operation([trial["scan"] for trial in trials], row_count)}
    for name in WRITES:
        runs = [trial["writes"][name] for trial in trials]
        operations[name] = summarize_operation([total for total, _ in runs], row_count)
        operations[name]["batch_latency"] = latency([value for _, values in runs for value in values])
    query_latency = {
        name: latency([value for trial in trials for value in trial["queries"][name]])
        for name in trials[0]["queries"]
    }
    return {
        "operations": operations,
        "query_latency": query_latency,
        "query_plans": trials[0]["plans"],
        "verified_trials": len(trials),
    }


def run_engine(engine, rows, checksum, modified_checksum, workload, expected, args):
    samples = {profile: [] for profile in PROFILES}
    for trial in range(args.trials):
        profiles = PROFILES if trial % 2 == 0 else PROFILES[::-1]
        for profile in profiles:
            print(f"{engine} · {profile} · trial {trial + 1}/{args.trials}", flush=True)
            with fixture(engine, args.output.parent) as adapter:
                version = adapter.version()
                run = run_trial(adapter, profile, rows, checksum, modified_checksum, workload, expected, args, trial)
                samples[profile].append(run)

    print(f"{engine} · building indexes on populated data", flush=True)
    with fixture(engine, args.output.parent) as adapter:
        for batch in chunks(rows, args.batch):
            adapter.insert_batch(batch)
        if adapter.count() != len(rows):
            raise ValueError("Index-build fixture has the wrong row count")
        started = perf_counter_ns()
        adapter.indexes()
        index_build_seconds = seconds(perf_counter_ns() - started)

    return {
        "version": version,
        "index_build_seconds": index_build_seconds,
        "profiles": {name: summarize_profile(trials, len(rows)) for name, trials in samples.items()},
    }


def markdown(report):
    lines = [
        "# CRUD and secondary-index benchmark",
        "",
        f"Rows **{report['rows']:,}** · Batch **{report['batch']:,}** · Trials per profile **{report['trials']}**",
        "",
        f"Verified original SHA-256: `{report['dataset_sha256']}`",
        "",
        "Each operation covers every row. Values are median total seconds per trial.",
        "",
        "| Engine | Profile | Insert | Ordered read | Update note | Update amount | Delete |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for engine, result in report["engines"].items():
        for profile, data in result["profiles"].items():
            values = [data["operations"][name]["median_seconds"] for name in ("insert", "read_all", *WRITES[1:])]
            lines.append(f"| {engine} | {profile} | " + " | ".join(f"{value:.4f}" for value in values) + " |")
    if report["errors"]:
        lines += ["", "Engines with errors: " + ", ".join(report["errors"])]
    return "\n".join(lines) + "\n"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engines", nargs="+", choices=ENGINE_NAMES, default=list(ENGINE_NAMES))
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--batch", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=20261111)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--query-rounds", type=int, default=15)
    parser.add_argument("--points", type=int, default=40)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--output", type=Path, default=Path("results/latest-crud.json"))
    args = parser.parse_args()
    if min(args.rows, args.batch, args.trials, args.query_rounds, args.points) < 1 or args.warmups < 0:
        parser.error("rows, batch, trials, query-rounds and points must be positive and warmups nonnegative")
    if len(set(args.engines)) != len(args.engines):
        parser.error("each engine can appear only once")
    return args


def main():
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    rows = list(generate(args.rows, args.seed))
    checksum = digest(rows)
    modified_checksum = digest(row._replace(note=NOTE, amount=row.amount + AMOUNT_DELTA) for row in rows)
    workload = cases(rows)
    expected = {case.name: case.expected(rows) for case in workload}
    report = {
        "rows": len(rows), "batch": args.batch, "seed": args.seed, "trials": args.trials,
        "query_rounds": args.query_rounds, "points_per_round": min(args.points, args.rows), "warmups": args.warmups,
        "dataset_sha256": checksum, "updated_dataset_sha256": modified_checksum, "deleted_remaining_rows": 0,
        "environment": machine(), "query_expected": expected, "secondary_indexes": INDEX_FIELDS,
        "engine_order": args.engines, "engines": {}, "errors": {},
    }
    for engine in args.engines:
        try:
            report["engines"][engine] = run_engine(engine, rows, checksum, modified_checksum, workload, expected, args)
        except Exception as error:
            report["errors"][engine] = type(error).__name__
            print(f"{engine}: {type(error).__name__}: {error}", file=sys.stderr, flush=True)

    args.output.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    args.output.with_suffix(".md").write_text(markdown(report), encoding="utf-8")
    return int(bool(report["errors"]))


if __name__ == "__main__":
    sys.exit(main())
