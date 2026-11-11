"""Measure concurrent joins and atomic order checkout on server databases."""

import argparse
import hashlib
import json
import random
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from threading import Barrier, BrokenBarrierError
from time import perf_counter_ns, sleep
from typing import NamedTuple

from bench import latency, machine
from oltp_engines import ForcedRollback, MongoStore, SqlStore

ENGINES = ("postgresql", "mysql", "mongodb")
MODES = ("join", "checkout", "mixed")
WRITES = {"commit", "rollback"}
INITIAL_STOCK = 1_000_000


class Request(NamedTuple):
    operation: str
    key: int


@dataclass
class Dataset:
    tables: dict
    point: dict
    history: dict
    purchases: dict

    def digest(self):
        encoded = json.dumps(self.tables, ensure_ascii=False, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


def generate_dataset(order_count, seed):
    rng = random.Random(seed)
    customer_count = max(1, order_count // 10)
    customers = [(i, f"고객 {i:05}") for i in range(1, customer_count + 1)]
    products = [(i, INITIAL_STOCK, rng.randrange(99, 50_000)) for i in range(1, 257)]
    prices = {sku: price for sku, _, price in products}
    orders, items, points, purchases = [], [], {}, {}
    histories = {i: (0, 0) for i, _ in customers}
    for order_id in range(1, order_count + 1):
        customer_id, customer_name = customers[(order_id - 1) % customer_count]
        skus = rng.sample(range(1, 9) if order_id % 5 else range(1, 257), 2)
        lines = [(seq, sku, rng.randrange(1, 6), prices[sku]) for seq, sku in enumerate(skus, 1)]
        total = sum(qty * price for _, _, qty, price in lines)
        orders.append((order_id, customer_id, "pending", total))
        items.extend((order_id, *line) for line in lines)
        points[order_id] = tuple((order_id, customer_name, *line, total) for line in lines)
        purchases[order_id] = (total, [(sku, qty) for _, sku, qty, _ in lines])
        count, cents = histories[customer_id]
        histories[customer_id] = (count + len(lines), cents + total)
    return Dataset({"customers": customers, "products": products, "orders": orders, "items": items},
                   points, histories, purchases)


def make_trace(mode, trial, dataset, requests, seed):
    rng = random.Random(seed + trial * len(MODES) + MODES.index(mode))
    read_count = requests if mode == "join" else requests * 7 // 10 if mode == "mixed" else 0
    write_count = requests - read_count
    trace = [
        Request("join_history", rng.randrange(1, len(dataset.history) + 1))
        if index % 4 == 3 else Request("join_point", rng.randrange(1, len(dataset.point) + 1))
        for index in range(read_count)
    ]
    order_ids = rng.sample(range(1, len(dataset.point) + 1), write_count)
    trace.extend(Request("rollback" if index % 20 == 19 else "commit", order_id)
                 for index, order_id in enumerate(order_ids))
    rng.shuffle(trace)
    return trace


def checkout(worker, request):
    expect_rollback = request.operation == "rollback"
    jitter = random.Random(request.key)
    for retries in range(49):
        try:
            worker.checkout(request.key, expect_rollback)
        except ForcedRollback:
            if not expect_rollback:
                raise
            return retries
        except Exception as error:
            if retries == 48 or not worker.retryable(error):
                raise
            sleep(min(0.001 * 2**retries, 0.032) * (0.5 + jitter.random()))
            continue
        if expect_rollback:
            raise ValueError(f"Order {request.key} should have rolled back")
        return retries
    raise AssertionError("Retry loop exhausted")


def execute_worker(worker, trace, dataset, barrier):
    try:
        worker.point(1)
        worker.history(1)
        barrier.wait()
        samples = []
        for request in trace:
            started = perf_counter_ns()
            retries = 0
            if request.operation in WRITES:
                retries = checkout(worker, request)
                duration = perf_counter_ns() - started
            else:
                if request.operation == "join_point":
                    actual = worker.point(request.key)
                    expected = dataset.point[request.key]
                else:
                    actual = worker.history(request.key)
                    expected = dataset.history[request.key]
                duration = perf_counter_ns() - started
                if actual != expected:
                    raise ValueError(f"{request.operation} returned different data for key {request.key}")
            samples.append((request.operation, duration, retries))
        return perf_counter_ns(), samples
    except BaseException:
        barrier.abort()
        raise


def verify_state(store, dataset, trace):
    expected_stock = {sku: stock for sku, stock, _ in dataset.tables["products"]}
    expected_receipts = {}
    for request in trace:
        if request.operation != "commit":
            continue
        total, lines = dataset.purchases[request.key]
        expected_receipts[request.key] = total
        for sku, qty in lines:
            expected_stock[sku] -= qty
    actual_stock, paid, receipts = store.snapshot()
    if actual_stock != expected_stock:
        raise ValueError("Product stock does not match committed transactions")
    if paid != expected_receipts.keys() or receipts != expected_receipts:
        raise ValueError("Paid orders or receipts do not match committed transactions")


def run_trial(store, dataset, trace, clients):
    store.reset(INITIAL_STOCK)
    start = []
    barrier = Barrier(clients + 1, action=lambda: start.append(perf_counter_ns()), timeout=60)
    with ExitStack() as stack:
        workers = [stack.enter_context(store.worker()) for _ in range(clients)]
        with ThreadPoolExecutor(max_workers=clients) as pool:
            futures = [
                pool.submit(execute_worker, worker,
                            trace[i * len(trace) // clients:(i + 1) * len(trace) // clients], dataset, barrier)
                for i, worker in enumerate(workers)
            ]
            try:
                barrier.wait()
            except BrokenBarrierError:
                for future in futures:
                    error = future.exception()
                    if error is not None and not isinstance(error, BrokenBarrierError):
                        raise error
                raise
            completed = [future.result() for future in futures]
    elapsed = (max(ended for ended, _ in completed) - start[0]) / 1_000_000_000
    samples = [sample for _, batch in completed for sample in batch]
    verify_state(store, dataset, trace)
    return elapsed, samples


def summarize(trials):
    request_rates, commit_rates, rollback_rates = [], [], []
    groups, retry_counts = {}, {}
    total_samples = []
    for elapsed, samples in trials:
        request_rates.append(len(samples) / elapsed)
        commit_rates.append(sum(op == "commit" for op, _, _ in samples) / elapsed)
        rollback_rates.append(sum(op == "rollback" for op, _, _ in samples) / elapsed)
        for op, duration, retries in samples:
            groups.setdefault(op, []).append(duration)
            retry_counts[op] = retry_counts.get(op, 0) + retries
            total_samples.append(duration)
    return {
        "requests_per_second": round(median(request_rates), 2),
        "commits_per_second": round(median(commit_rates), 2),
        "rollbacks_per_second": round(median(rollback_rates), 2),
        "trial_requests_per_second": [round(rate, 2) for rate in request_rates],
        "trial_elapsed_seconds": [round(elapsed, 6) for elapsed, _ in trials],
        "latency": latency(total_samples),
        "operations": {op: latency(values) | {"retries": retry_counts[op]} for op, values in groups.items()},
        "retry_attempts": sum(retry_counts.values()),
        "verified_trials": len(trials),
    }


def run_engine(engine, dataset, args):
    store = MongoStore() if engine == "mongodb" else SqlStore(engine)
    expected_counts = {table: len(rows) for table, rows in dataset.tables.items()} | {"receipts": 0}
    try:
        print(f"{engine} · creating fixture", flush=True)
        store.setup(dataset.tables)
        if store.counts() != expected_counts:
            raise ValueError("Loaded table or collection counts differ from the fixture")
        scenarios = {}
        for mode in MODES:
            runs = {clients: [] for clients in args.clients}
            for trial in range(args.trials):
                trace = make_trace(mode, trial, dataset, args.requests, args.seed)
                order = args.clients[trial % len(args.clients):] + args.clients[:trial % len(args.clients)]
                for clients in order:
                    print(f"{engine} · {mode} · {clients} clients · trial {trial + 1}/{args.trials}", flush=True)
                    runs[clients].append(run_trial(store, dataset, trace, clients))
            scenarios[mode] = {str(clients): summarize(trials) for clients, trials in runs.items()}
        return {"version": store.version, "fixture_counts": expected_counts, "scenarios": scenarios}
    finally:
        store.close()


def markdown(report):
    lines = [
        "# Concurrent order benchmark", "",
        f"Orders **{report['orders']:,}** · Requests per trial **{report['requests_per_trial']:,}** · "
        f"Trials per scenario **{report['trials']}**", "",
        "| Engine | Workload | Clients | Requests/s | Commits/s | p95 (ms) | Transaction retries |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for engine, result in report["engines"].items():
        for mode, clients in result["scenarios"].items():
            for count, data in clients.items():
                lines.append(
                    f"| {engine} | {mode} | {count} | {data['requests_per_second']:,.2f} | "
                    f"{data['commits_per_second']:,.2f} | {data['latency']['p95_ms']:.3f} | {data['retry_attempts']} |"
                )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engines", nargs="+", choices=ENGINES, default=list(ENGINES))
    parser.add_argument("--orders", type=int, default=50_000)
    parser.add_argument("--requests", type=int, default=600)
    parser.add_argument("--clients", nargs="+", type=int, default=[1, 4, 16])
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20261111)
    parser.add_argument("--output", type=Path, default=Path("results/latest-oltp.json"))
    args = parser.parse_args()
    if not 70 <= args.requests <= args.orders or args.trials < 1 or not 1 <= min(args.clients) <= max(args.clients) <= 32:
        parser.error("use orders >= requests >= 70, trials >= 1 and 1–32 clients")
    if len(set(args.engines)) != len(args.engines) or len(set(args.clients)) != len(args.clients):
        parser.error("each engine and client count must appear once")

    dataset = generate_dataset(args.orders, args.seed)
    report = {
        "orders": args.orders, "requests_per_trial": args.requests, "clients": args.clients,
        "trials": args.trials, "seed": args.seed, "dataset_sha256": dataset.digest(), "environment": machine(),
        "engines": {}, "errors": {},
    }
    for engine in args.engines:
        try:
            report["engines"][engine] = run_engine(engine, dataset, args)
        except Exception as error:
            report["errors"][engine] = type(error).__name__
            print(f"{engine}: {type(error).__name__}: {error}", file=sys.stderr, flush=True)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    args.output.with_suffix(".md").write_text(markdown(report), encoding="utf-8")
    return int(bool(report["errors"]))


if __name__ == "__main__":
    sys.exit(main())
