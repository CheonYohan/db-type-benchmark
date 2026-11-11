"""Batched CRUD operations and query-plan inspection for the comparison."""

import json
import re
import sqlite3
from contextlib import contextmanager
from functools import lru_cache

from dataset import FIELDS
from engines import Mongo, Sql


def chunks(sequence, size):
    for offset in range(0, len(sequence), size):
        yield sequence[offset:offset + size]


def objects(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from objects(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            yield from objects(child)


def observed_plan(operators, index_names=(), *, secondary_used=None):
    operators = list(dict.fromkeys(operators))
    index_names = list(dict.fromkeys(index_names))
    if secondary_used is None:
        secondary_used = any(name not in ("PRIMARY", "_id_") for name in index_names)
    return {"operators": operators, "indexes": index_names, "secondary_index_used": secondary_used}


@lru_cache(maxsize=32)
def placeholders(marker, count):
    return ", ".join([marker] * count)


class CrudSql(Sql):
    def __init__(self, name, table, directory):
        super().__init__(name, table, directory)
        self.parameter_limit = (
            self.connection.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER)
            if name == "sqlite" else 65_535
        )

    @contextmanager
    def write_transaction(self):
        if self.name == "postgresql":
            with self.connection.transaction():
                yield
            return
        if self.name == "mysql":
            self.connection.begin()
        else:
            self.cursor.execute("BEGIN TRANSACTION")
        try:
            yield
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def insert_batch(self, rows):
        row_marks = f"({placeholders(self.placeholder, len(FIELDS))})"
        max_rows = self.parameter_limit // len(FIELDS)
        with self.write_transaction():
            for group in chunks(rows, max_rows):
                marks = ", ".join([row_marks] * len(group))
                values = tuple(
                    self.value(field, value)
                    for row in group
                    for field, value in zip(FIELDS, row, strict=True)
                )
                self.cursor.execute(f"INSERT INTO {self.table} VALUES {marks}", values)

    def update_note(self, ids, value):
        self.write_by_id(f"UPDATE {self.table} SET note = {self.placeholder}", ids, (value,))

    def update_amount(self, ids, delta):
        self.write_by_id(
            f"UPDATE {self.table} SET amount = amount + {self.placeholder}",
            ids,
            (self.value("amount", delta),),
        )

    def delete_batch(self, ids):
        self.write_by_id(f"DELETE FROM {self.table}", ids)

    def write_by_id(self, prefix, ids, values=()):
        with self.write_transaction():
            for group in chunks(ids, self.parameter_limit - len(values)):
                markers = placeholders(self.placeholder, len(group))
                self.cursor.execute(f"{prefix} WHERE id IN ({markers})", (*values, *group))

    def count(self):
        self.cursor.execute(f"SELECT COUNT(*) FROM {self.table}")
        return int(self.cursor.fetchone()[0])

    def plan(self, case):
        statement, values = self.filter_statement(case.conditions, case.sum_amount)
        if self.name == "sqlite":
            self.cursor.execute(f"EXPLAIN QUERY PLAN {statement}", values)
            details = [row[3] for row in self.cursor.fetchall()]
            indexes = [
                match.group(1) for detail in details
                if (match := re.search(r"USING (?:COVERING )?INDEX (\w+)", detail))
            ]
            return observed_plan(details, indexes)
        if self.name == "duckdb":
            self.cursor.execute(f"EXPLAIN ANALYZE {statement}", values)
            details = " ".join(str(row[1]) for row in self.cursor.fetchall())
            scans = re.findall(r"\b(?:Sequential Scan|Index Scan|TABLE_SCAN)\b", details)
            return observed_plan(scans, secondary_used="Index Scan" in scans)
        if self.name == "postgresql":
            self.cursor.execute(f"EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF) {statement}", values)
            nodes = list(objects(self.cursor.fetchone()[0]))
            return observed_plan(
                (node["Node Type"] for node in nodes if "Node Type" in node),
                (node["Index Name"] for node in nodes if "Index Name" in node),
            )
        self.cursor.execute(f"EXPLAIN FORMAT=JSON {statement}", values)
        nodes = list(objects(json.loads(self.cursor.fetchone()[0])))
        return observed_plan(
            (node["access_type"] for node in nodes if "access_type" in node),
            (node["key"] for node in nodes if "access_type" in node and isinstance(node.get("key"), str)),
        )


class CrudMongo(Mongo):
    def __init__(self, table):
        super().__init__(table)
        self.collection.database.create_collection(table)

    insert_batch = Mongo.batch

    def analyze(self):
        # MongoDB maintains planner statistics without an ANALYZE statement.
        pass

    def update_note(self, ids, value):
        result = self.collection.update_many({"_id": {"$in": ids}}, {"$set": {"note": value}})
        if result.modified_count != len(ids):
            raise ValueError("MongoDB note update did not modify the entire batch")

    def update_amount(self, ids, delta):
        result = self.collection.update_many(
            {"_id": {"$in": ids}}, {"$inc": {"amount": self.value("amount", delta)}}
        )
        if result.modified_count != len(ids):
            raise ValueError("MongoDB amount update did not modify the entire batch")

    def delete_batch(self, ids):
        result = self.collection.delete_many({"_id": {"$in": ids}})
        if result.deleted_count != len(ids):
            raise ValueError("MongoDB delete did not remove the entire batch")

    def count(self):
        return self.collection.count_documents({})

    def plan(self, case):
        match = self.filter_query(case.conditions)
        group = {"_id": None, "count": {"$sum": 1}}
        if case.sum_amount:
            group["amount"] = {"$sum": "$amount"}
        command = {
            "aggregate": self.collection.name,
            "pipeline": [{"$match": match}, {"$group": group}],
            "cursor": {},
        }
        explain = self.collection.database.command("explain", command, verbosity="executionStats")
        plans = [node["winningPlan"] for node in objects(explain) if "winningPlan" in node]
        nodes = list(objects(plans))
        return observed_plan(
            (node["stage"] for node in nodes if "stage" in node),
            (node["indexName"] for node in nodes if "indexName" in node),
        )


def open_crud_engine(name, table, directory):
    return CrudMongo(table) if name == "mongodb" else CrudSql(name, table, directory)
