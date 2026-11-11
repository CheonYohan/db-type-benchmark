"""Deterministic mixed-type records and their canonical JSONL representation."""

import hashlib
import json
import random
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import NamedTuple
from uuid import UUID

SCALE = 10_000
START = date(2023, 1, 1)


class Row(NamedTuple):
    id: int
    units: int
    amount: Decimal
    ratio: float
    label: str
    payload: dict
    day: date
    event_ts: datetime
    clock: time
    enabled: bool
    uid: UUID
    raw: bytes
    note: str | None


FIELDS = Row._fields


def generate(count, seed):
    rng = random.Random(seed)
    for number in range(1, count + 1):
        day = START + timedelta(days=rng.randrange(730))
        stamp = datetime.combine(day, time()) + timedelta(
            seconds=rng.randrange(86_400), milliseconds=rng.randrange(1_000)
        )
        group = rng.randrange(32)
        yield Row(
            number,
            rng.randrange(-10_000, 10_001),
            Decimal(rng.randrange(-50_000_000, 50_000_001)).scaleb(-4),
            rng.randrange(-65_536, 65_537) / 128,
            f'sku-{group:02} 상품, "café" 📦 — {number:09}',
            {
                "attributes": {"segment": f"s{rng.randrange(16):02}", "active": number % 2 == 0},
                "tags": ["benchmark", "한글", f"g{group:02}"],
                "optional": None,
                "rank": number % 127,
            },
            day,
            stamp,
            stamp.time(),
            bool(rng.getrandbits(1)),
            UUID(int=rng.getrandbits(128), version=4),
            rng.randbytes(24),
            None if number % 7 == 0 else ("", "note", "메모")[number % 3],
        )


def canonical(row):
    record = row._asdict()
    record["amount"] = format(row.amount, ".4f")
    record["day"] = row.day.isoformat()
    record["event_ts"] = row.event_ts.isoformat(timespec="milliseconds") + "Z"
    record["clock"] = row.clock.isoformat(timespec="milliseconds")
    record["uid"] = str(row.uid)
    record["raw"] = row.raw.hex()
    return json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def digest(rows, output=None):
    checksum = hashlib.sha256()
    for row in rows:
        line = canonical(row).encode("utf-8")
        checksum.update(line)
        if output is not None:
            output.write(line)
    return checksum.hexdigest()
