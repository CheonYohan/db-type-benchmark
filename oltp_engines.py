"""Storage adapters for the concurrent order checkout benchmark."""

import os
from contextlib import contextmanager
from uuid import uuid4

TABLES = ("customers", "products", "orders", "items", "receipts")


class ForcedRollback(Exception):
    """The benchmark requests a rollback after all transaction writes."""


class SqlStore:
    def __init__(self, engine):
        self.engine = engine
        prefix = f"oltp_{uuid4().hex[:12]}"
        self.names = {table: f"{prefix}_{table}" for table in TABLES}

    def connect(self):
        if self.engine == "postgresql":
            import psycopg

            return psycopg.connect(
                os.getenv("BENCH_POSTGRES_DSN", "host=127.0.0.1 port=55432 dbname=bench user=bench password=bench"),
                autocommit=True,
                prepare_threshold=None,
                connect_timeout=10,
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
            connect_timeout=10,
        )

    @contextmanager
    def worker(self):
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                if self.engine == "postgresql":
                    cursor.execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED")
                else:
                    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                yield SqlWorker(self.engine, self.names, connection, cursor)
        finally:
            connection.close()

    def setup(self, dataset):
        t = self.names
        schema = {
            "customers": "id INTEGER PRIMARY KEY, name VARCHAR(64) NOT NULL",
            "products": "id INTEGER PRIMARY KEY, stock INTEGER NOT NULL CHECK (stock >= 0), price_cents BIGINT NOT NULL",
            "orders": (
                "id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, "
                "status VARCHAR(10) NOT NULL CHECK (status IN ('pending', 'paid')), total_cents BIGINT NOT NULL, "
                f"FOREIGN KEY (customer_id) REFERENCES {t['customers']}(id)"
            ),
            "items": (
                "order_id INTEGER NOT NULL, line_no INTEGER NOT NULL, product_id INTEGER NOT NULL, "
                "qty INTEGER NOT NULL CHECK (qty > 0), unit_price_cents BIGINT NOT NULL, "
                f"PRIMARY KEY (order_id, line_no), FOREIGN KEY (order_id) REFERENCES {t['orders']}(id), "
                f"FOREIGN KEY (product_id) REFERENCES {t['products']}(id)"
            ),
            "receipts": (
                f"order_id INTEGER PRIMARY KEY, amount_cents BIGINT NOT NULL, "
                f"FOREIGN KEY (order_id) REFERENCES {t['orders']}(id)"
            ),
        }
        with self.worker() as worker:
            for table in TABLES:
                worker.cursor.execute(f"CREATE TABLE {t[table]} ({schema[table]})")
            worker.cursor.execute(f"CREATE INDEX {t['orders']}_customer ON {t['orders']}(customer_id)")
            worker.cursor.execute(f"CREATE INDEX {t['orders']}_status ON {t['orders']}(status)")
            for table, rows in dataset.items():
                marks = ", ".join(["%s"] * len(rows[0]))
                for offset in range(0, len(rows), 1000):
                    with worker.transaction():
                        worker.cursor.executemany(
                            f"INSERT INTO {t[table]} VALUES ({marks})", rows[offset:offset + 1000]
                        )
            if self.engine == "postgresql":
                for table in TABLES:
                    worker.cursor.execute(f"ANALYZE {t[table]}")
            else:
                for table in TABLES:
                    worker.cursor.execute(f"ANALYZE TABLE {t[table]}")
                    worker.cursor.fetchall()
            worker.cursor.execute("SELECT version()")
            self.version = str(worker.cursor.fetchone()[0])

    def counts(self):
        with self.worker() as worker:
            result = {}
            for table in TABLES:
                worker.cursor.execute(f"SELECT COUNT(*) FROM {self.names[table]}")
                result[table] = worker.cursor.fetchone()[0]
            return result

    def reset(self, initial_stock):
        t = self.names
        with self.worker() as worker, worker.transaction():
            worker.cursor.execute(f"DELETE FROM {t['receipts']}")
            worker.cursor.execute(f"UPDATE {t['orders']} SET status = 'pending' WHERE status = 'paid'")
            worker.cursor.execute(f"UPDATE {t['products']} SET stock = %s", (initial_stock,))

    def snapshot(self):
        t = self.names
        with self.worker() as w:
            w.cursor.execute(f"SELECT id, stock FROM {t['products']}")
            stock = dict(w.cursor.fetchall())
            w.cursor.execute(f"SELECT id FROM {t['orders']} WHERE status = 'paid'")
            paid = {row[0] for row in w.cursor.fetchall()}
            w.cursor.execute(f"SELECT order_id, amount_cents FROM {t['receipts']}")
            return stock, paid, dict(w.cursor.fetchall())

    def close(self):
        with self.worker() as w:
            for table in reversed(TABLES):
                w.cursor.execute(f"DROP TABLE IF EXISTS {self.names[table]}")


class SqlWorker:
    def __init__(self, engine, names, connection, cursor):
        self.engine, self.tables, self.connection, self.cursor = engine, names, connection, cursor

    @contextmanager
    def transaction(self):
        if self.engine == "postgresql":
            with self.connection.transaction():
                yield
            return
        self.connection.begin()
        try:
            yield
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def retryable(self, error):
        if self.engine == "postgresql":
            return getattr(error, "sqlstate", None) in {"40001", "40P01"}
        return bool(error.args) and error.args[0] in (1205, 1213)

    def point(self, order_id):
        t = self.tables
        self.cursor.execute(
            f"SELECT o.id,c.name,i.line_no,p.id,i.qty,i.unit_price_cents,o.total_cents "
            f"FROM {t['orders']} o JOIN {t['customers']} c ON c.id=o.customer_id "
            f"JOIN {t['items']} i ON i.order_id=o.id JOIN {t['products']} p ON p.id=i.product_id "
            "WHERE o.id=%s ORDER BY i.line_no",
            (order_id,),
        )
        return tuple(self.cursor.fetchall())

    def history(self, customer_id):
        t = self.tables
        self.cursor.execute(
            f"SELECT COUNT(*),COALESCE(SUM(i.qty*i.unit_price_cents),0) "
            f"FROM {t['orders']} o JOIN {t['items']} i ON i.order_id=o.id WHERE o.customer_id=%s",
            (customer_id,),
        )
        return tuple(map(int, self.cursor.fetchone()))

    def checkout(self, order_id, force_rollback):
        t = self.tables
        with self.transaction():
            self.cursor.execute(
                f"SELECT status,total_cents FROM {t['orders']} WHERE id=%s FOR UPDATE", (order_id,)
            )
            order = self.cursor.fetchone()
            if order is None or order[0] != "pending":
                raise ValueError(f"Order {order_id} is not pending")
            self.cursor.execute(
                f"SELECT product_id,qty FROM {t['items']} WHERE order_id=%s ORDER BY product_id", (order_id,)
            )
            lines = self.cursor.fetchall()
            if len(lines) != 2:
                raise ValueError(f"Order {order_id} does not have two lines")
            for sku, qty in lines:
                self.cursor.execute(
                    f"UPDATE {t['products']} SET stock=stock-%s WHERE id=%s AND stock >= %s", (qty, sku, qty)
                )
                if self.cursor.rowcount != 1:
                    raise ValueError(f"Insufficient stock for product {sku}")
            self.cursor.execute(f"UPDATE {t['orders']} SET status='paid' WHERE id=%s", (order_id,))
            self.cursor.execute(f"INSERT INTO {t['receipts']} VALUES (%s,%s)", (order_id, order[1]))
            if force_rollback:
                raise ForcedRollback()


class MongoStore:
    def __init__(self):
        from pymongo import MongoClient

        uri = os.getenv("BENCH_MONGO_OLTP_URI", "mongodb://127.0.0.1:57017/?replicaSet=rs0&directConnection=true")
        self.client = MongoClient(uri, serverSelectionTimeoutMS=10_000, maxPoolSize=64)
        self.database = self.client[f"db_type_oltp_{uuid4().hex}"]

    @contextmanager
    def worker(self):
        with self.client.start_session() as session:
            yield MongoWorker(self.database, session)

    def setup(self, dataset):
        fields = {
            "customers": ("_id", "name"),
            "products": ("_id", "stock", "price_cents"),
            "orders": ("_id", "customer_id", "status", "total_cents"),
            "items": ("order_id", "line_no", "product_id", "qty", "unit_price_cents"),
        }
        for table in TABLES:
            self.database.create_collection(table)
        self.database.orders.create_index("customer_id")
        self.database.orders.create_index("status")
        self.database.items.create_index([("order_id", 1), ("line_no", 1)], unique=True)
        for table, rows in dataset.items():
            for offset in range(0, len(rows), 1000):
                docs = [dict(zip(fields[table], row, strict=True)) for row in rows[offset:offset + 1000]]
                self.database[table].insert_many(docs)
        self.version = self.client.server_info()["version"]

    def counts(self):
        return {table: self.database[table].count_documents({}) for table in TABLES}

    def reset(self, initial_stock):
        self.database.receipts.delete_many({})
        self.database.orders.update_many({"status": "paid"}, {"$set": {"status": "pending"}})
        self.database.products.update_many({}, {"$set": {"stock": initial_stock}})

    def snapshot(self):
        stock = {p["_id"]: p["stock"] for p in self.database.products.find({}, {"stock": 1})}
        paid = {o["_id"] for o in self.database.orders.find({"status": "paid"}, {"_id": 1})}
        receipts = {r["_id"]: r["amount_cents"] for r in self.database.receipts.find({})}
        return stock, paid, receipts

    def close(self):
        try:
            self.client.drop_database(self.database)
        finally:
            self.client.close()


class MongoWorker:
    def __init__(self, database, session):
        self.db, self.session = database, session

    def retryable(self, error):
        return getattr(error, "has_error_label", lambda _: False)("TransientTransactionError")

    def point(self, order_id):
        pipeline = [
            {"$match": {"_id": order_id}},
            {"$lookup": {"from": "customers", "localField": "customer_id", "foreignField": "_id", "as": "customer"}},
            {"$unwind": "$customer"},
            {"$lookup": {"from": "items", "localField": "_id", "foreignField": "order_id", "as": "line"}},
            {"$unwind": "$line"},
            {"$lookup": {"from": "products", "localField": "line.product_id", "foreignField": "_id", "as": "product"}},
            {"$unwind": "$product"},
            {"$sort": {"line.line_no": 1}},
        ]
        with self.db.orders.aggregate(pipeline, session=self.session) as cursor:
            return tuple(
                (o["_id"], o["customer"]["name"], o["line"]["line_no"], o["product"]["_id"],
                 o["line"]["qty"], o["line"]["unit_price_cents"], o["total_cents"])
                for o in cursor
            )

    def history(self, customer_id):
        pipeline = [
            {"$match": {"customer_id": customer_id}},
            {"$lookup": {"from": "items", "localField": "_id", "foreignField": "order_id", "as": "line"}},
            {"$unwind": "$line"},
            {"$group": {"_id": None, "count": {"$sum": 1}, "total": {
                "$sum": {"$multiply": ["$line.qty", "$line.unit_price_cents"]}
            }}},
        ]
        with self.db.orders.aggregate(pipeline, session=self.session) as cursor:
            row = next(cursor, None)
            return (0, 0) if row is None else (row["count"], row["total"])

    def checkout(self, order_id, force_rollback):
        from pymongo.read_concern import ReadConcern
        from pymongo.write_concern import WriteConcern

        s = self.session
        with s.start_transaction(read_concern=ReadConcern("snapshot"), write_concern=WriteConcern(w="majority", j=True)):
            order = self.db.orders.find_one({"_id": order_id}, session=s)
            if order is None or order["status"] != "pending":
                raise ValueError(f"Order {order_id} is not pending")
            lines = list(self.db.items.find({"order_id": order_id}, session=s).sort("product_id", 1))
            if len(lines) != 2:
                raise ValueError(f"Order {order_id} does not have two lines")
            for line in lines:
                sku, qty = line["product_id"], line["qty"]
                result = self.db.products.update_one(
                    {"_id": sku, "stock": {"$gte": qty}}, {"$inc": {"stock": -qty}}, session=s
                )
                if result.modified_count != 1:
                    raise ValueError(f"Insufficient stock for product {sku}")
            updated = self.db.orders.update_one(
                {"_id": order_id, "status": "pending"}, {"$set": {"status": "paid"}}, session=s
            )
            if updated.modified_count != 1:
                raise ValueError(f"Order {order_id} was not updated")
            self.db.receipts.insert_one({"_id": order_id, "amount_cents": order["total_cents"]}, session=s)
            if force_rollback:
                raise ForcedRollback()
