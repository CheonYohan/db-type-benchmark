"""Database adapters. Each adapter writes only to its own disposable table."""

import json
import os
import sqlite3
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

from dataset import FIELDS, SCALE, Row

ENGINE_NAMES = ("sqlite", "duckdb", "postgresql", "mysql", "mongodb")

COMMON = (
    "id BIGINT PRIMARY KEY, units INTEGER NOT NULL, amount DECIMAL(18,4) NOT NULL, "
    "ratio {ratio} NOT NULL, label TEXT NOT NULL, payload {json} NOT NULL, "
    "day DATE NOT NULL, event_ts {stamp} NOT NULL, clock {clock} NOT NULL, "
    "enabled BOOLEAN NOT NULL, uid {uid} NOT NULL, raw {raw} NOT NULL, note TEXT"
)
SCHEMAS = {
    "sqlite": (
        "id INTEGER PRIMARY KEY, units INTEGER NOT NULL, amount INTEGER NOT NULL, "
        "ratio REAL NOT NULL, label TEXT NOT NULL, payload TEXT NOT NULL CHECK(json_valid(payload)), "
        "day TEXT NOT NULL, event_ts TEXT NOT NULL, clock TEXT NOT NULL, "
        "enabled INTEGER NOT NULL, uid TEXT NOT NULL, raw BLOB NOT NULL, note TEXT"
    ),
    "duckdb": COMMON.format(ratio="DOUBLE", json="JSON", stamp="TIMESTAMP", clock="TIME", uid="UUID", raw="BLOB"),
    "postgresql": COMMON.format(
        ratio="DOUBLE PRECISION", json="JSONB", stamp="TIMESTAMP(3)", clock="TIME(3)", uid="UUID", raw="BYTEA"
    ),
    "mysql": COMMON.format(ratio="DOUBLE", json="JSON", stamp="DATETIME(3)", clock="TIME(3)", uid="CHAR(36)", raw="BLOB"),
}
INDEX_FIELDS = ("day", "amount", "uid")
JSON_SEGMENT = {
    "sqlite": "json_extract(payload, '$.attributes.segment')",
    "duckdb": "json_extract_string(payload, '$.attributes.segment')",
    "postgresql": "payload #>> '{attributes,segment}'",
    "mysql": "JSON_UNQUOTE(JSON_EXTRACT(payload, '$.attributes.segment'))",
}


def normalize(values, scaled=False):
    row = dict(zip(FIELDS, values, strict=True))
    if scaled:
        row["amount"] = Decimal(row["amount"]).scaleb(-4)
    if isinstance(row["payload"], str):
        row["payload"] = json.loads(row["payload"])
    if isinstance(row["day"], str):
        row["day"] = date.fromisoformat(row["day"])
    if isinstance(row["event_ts"], str):
        row["event_ts"] = datetime.fromisoformat(row["event_ts"])
    if isinstance(row["clock"], str):
        row["clock"] = time.fromisoformat(row["clock"])
    elif isinstance(row["clock"], timedelta):
        row["clock"] = (datetime.min + row["clock"]).time()
    row["enabled"] = bool(row["enabled"])
    if not isinstance(row["uid"], UUID):
        row["uid"] = UUID(row["uid"])
    row["raw"] = bytes(row["raw"])
    return Row(**row)


class Sql:
    def __init__(self, name, table, directory):
        self.name, self.table = name, table
        self.placeholder = "?" if name in ("sqlite", "duckdb") else "%s"
        self.connection = self.connect(directory)
        self.cursor = self.connection if name == "duckdb" else self.connection.cursor()
        self.cursor.execute(f"CREATE TABLE {table} ({SCHEMAS[name]})")
        self.connection.commit()
        marks = ", ".join([self.placeholder] * len(FIELDS))
        self.insert = f"INSERT INTO {table} VALUES ({marks})"

    def connect(self, directory):
        if self.name == "sqlite":
            connection = sqlite3.connect(directory / "sqlite.db")
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            return connection
        if self.name == "duckdb":
            import duckdb

            return duckdb.connect(str(directory / "duckdb.db"), config={"threads": 1})
        if self.name == "postgresql":
            import psycopg

            return psycopg.connect(
                os.getenv("BENCH_POSTGRES_DSN", "host=127.0.0.1 port=55432 dbname=bench user=bench password=bench"),
                autocommit=True,
                prepare_threshold=None,
            )
        import pymysql

        return pymysql.connect(
            host=os.getenv("BENCH_MYSQL_HOST", "127.0.0.1"),
            port=int(os.getenv("BENCH_MYSQL_PORT", "53306")),
            user=os.getenv("BENCH_MYSQL_USER", "bench"),
            password=os.getenv("BENCH_MYSQL_PASSWORD", "bench"),
            database=os.getenv("BENCH_MYSQL_DATABASE", "bench"),
            charset="utf8mb4",
            autocommit=True,
        )

    def value(self, field, value):
        if field == "payload":
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        if self.name == "sqlite":
            if field == "amount":
                return int(value * SCALE)
            if field in ("day", "uid"):
                return str(value)
            if field in ("clock", "event_ts"):
                return value.isoformat(timespec="milliseconds")
        if self.name == "mysql" and field == "uid":
            return str(value)
        return value

    def batch(self, rows):
        encoded = [tuple(self.value(field, value) for field, value in zip(FIELDS, row)) for row in rows]
        if self.name == "postgresql":
            with self.connection.transaction():
                self.cursor.executemany(self.insert, encoded)
            return
        if self.name == "duckdb":
            self.cursor.execute("BEGIN TRANSACTION")
            try:
                self.cursor.executemany(self.insert, encoded)
            except Exception:
                self.cursor.rollback()
                raise
            self.cursor.commit()
            return
        if self.name == "mysql":
            self.connection.begin()
        try:
            self.cursor.executemany(self.insert, encoded)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def indexes(self):
        for field in INDEX_FIELDS:
            self.cursor.execute(f"CREATE INDEX {self.table}_{field} ON {self.table} ({field})")
        self.connection.commit()
        if self.name in ("postgresql", "sqlite", "duckdb"):
            self.cursor.execute(f"ANALYZE {self.table}")
        if self.name == "mysql":
            self.cursor.execute(f"ANALYZE TABLE {self.table}")
            self.cursor.fetchall()
        self.connection.commit()

    def rows(self):
        self.cursor.execute(f"SELECT * FROM {self.table} ORDER BY id")
        while batch := self.cursor.fetchmany(2_000):
            for row in batch:
                yield normalize(row, scaled=self.name == "sqlite")

    def point(self, identifier):
        self.cursor.execute(f"SELECT * FROM {self.table} WHERE id = {self.placeholder}", (identifier,))
        return self.cursor.fetchone()

    def filter(self, conditions, sum_amount=False):
        clauses, values = [], []
        for field, operator, value in conditions:
            column = JSON_SEGMENT[self.name] if field == "segment" else field
            if operator == "null":
                clauses.append(f"{column} IS NULL")
                continue
            if operator == "prefix":
                clauses.append(f"{column} LIKE {self.placeholder}")
                values.append(value + "%")
                continue
            clauses.append(f"{column} {operator} {self.placeholder}")
            values.append(self.value(field, value))
        select = "COUNT(*), COALESCE(SUM(amount), 0)" if sum_amount else "COUNT(*)"
        self.cursor.execute(f"SELECT {select} FROM {self.table} WHERE {' AND '.join(clauses)}", tuple(values))
        result = self.cursor.fetchone()
        if not sum_amount:
            return int(result[0])
        amount = Decimal(result[1]).scaleb(-4) if self.name == "sqlite" else Decimal(result[1])
        return int(result[0]), amount

    def version(self):
        expression = "sqlite_version()" if self.name == "sqlite" else "version()"
        self.cursor.execute(f"SELECT {expression}")
        return str(self.cursor.fetchone()[0])

    def close(self):
        try:
            self.cursor.execute(f"DROP TABLE {self.table}")
            self.connection.commit()
        finally:
            if self.cursor is not self.connection:
                self.cursor.close()
            self.connection.close()


class Mongo:
    def __init__(self, table):
        from bson import Binary, Decimal128
        from pymongo import MongoClient
        from pymongo.write_concern import WriteConcern

        self.binary_type, self.decimal_type = Binary, Decimal128
        self.client = MongoClient(
            os.getenv("BENCH_MONGO_URI", "mongodb://127.0.0.1:57017"),
            serverSelectionTimeoutMS=10_000,
        )
        self.client.admin.command("ping")
        self.collection = self.client["db_type_benchmark"].get_collection(
            table, write_concern=WriteConcern(w=1, j=True)
        )

    def value(self, field, value):
        if field == "amount":
            return self.decimal_type(value)
        if field == "uid":
            return self.binary_type.from_uuid(value)
        if field == "day":
            return datetime.combine(value, time())
        if field == "clock":
            return value.isoformat(timespec="milliseconds")
        return value

    def batch(self, rows):
        documents = [
            {"_id" if field == "id" else field: self.value(field, value) for field, value in zip(FIELDS, row)}
            for row in rows
        ]
        self.collection.insert_many(documents, ordered=True)

    def indexes(self):
        for field in INDEX_FIELDS:
            self.collection.create_index(field)

    def rows(self):
        with self.collection.find().sort("_id", 1).batch_size(2_000) as cursor:
            for document in cursor:
                document["id"] = document.pop("_id")
                document["amount"] = document["amount"].to_decimal()
                document["day"] = document["day"].date()
                document["uid"] = document["uid"].as_uuid()
                yield normalize([document[field] for field in FIELDS])

    def point(self, identifier):
        return self.collection.find_one({"_id": identifier})

    def filter(self, conditions, sum_amount=False):
        operators = {"=": "$eq", ">=": "$gte", "<": "$lt", "prefix": "$regex", "null": "$type"}
        query = {}
        for field, operator, value in conditions:
            key = "payload.attributes.segment" if field == "segment" else field
            if operator == "prefix":
                value = "^" + value  # Workload prefixes consist of ASCII letters, digits and a hyphen.
            elif operator == "null":
                value = 10  # BSON null type, distinct from a missing key.
            else:
                value = self.value(field, value)
            query.setdefault(key, {})[operators[operator]] = value
        if not sum_amount:
            return self.collection.count_documents(query)
        pipeline = [{"$match": query}, {"$group": {"_id": None, "count": {"$sum": 1}, "amount": {"$sum": "$amount"}}}]
        with self.collection.aggregate(pipeline) as cursor:
            result = next(cursor, None)
        return (0, Decimal(0)) if result is None else (result["count"], result["amount"].to_decimal())

    def version(self):
        return self.client.server_info()["version"]

    def close(self):
        try:
            self.collection.drop()
        finally:
            self.client.close()


def open_engine(name, table, directory):
    return Mongo(table) if name == "mongodb" else Sql(name, table, directory)
