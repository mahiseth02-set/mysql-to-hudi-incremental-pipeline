import os
import sys
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.extract import _where  # noqa: E402
from pipeline.transform import latest_per_key, prepare  # noqa: E402
from pipeline.watermark import WatermarkStore  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s
    s.stop()


def _row(order_id, amount, is_deleted, updated):
    return (order_id, 1, "Pune", "Books", Decimal(amount), "PLACED", is_deleted,
            datetime(2026, 7, 1, 10, 0), datetime(2026, 9, 1, 10, updated))


COLS = ["order_id", "customer_id", "city", "category", "amount", "status", "is_deleted", "created_at", "updated_at"]


def test_latest_per_key_keeps_newest_version(spark):
    df = spark.createDataFrame([_row(1, "100", 0, 1), _row(1, "150", 0, 5), _row(2, "10", 0, 1)], COLS)
    out = {r.order_id: r.amount for r in latest_per_key(df, "order_id", "updated_at").collect()}
    assert out == {1: Decimal("150.00"), 2: Decimal("10.00")}


def test_prepare_marks_soft_deletes_and_partition(spark):
    df = spark.createDataFrame([_row(1, "100", 1, 1), _row(2, "10", 0, 1)], COLS)
    rows = {r.order_id: r for r in prepare(df, "order_id", "updated_at").collect()}
    assert rows[1]["_hoodie_is_deleted"] is True
    assert rows[2]["_hoodie_is_deleted"] is False
    assert rows[2]["order_date"] == "2026-07-01"


def test_where_clause_bounds():
    upper = datetime(2026, 9, 1, 12, 0, 0)
    assert _where({"watermark_col": "updated_at"}, None, upper) == "updated_at <= '2026-09-01 12:00:00.000000'"
    lower = datetime(2026, 9, 1, 11, 50, 0)
    assert _where({"watermark_col": "updated_at"}, lower, upper).startswith("updated_at > '2026-09-01 11:50:00.000000' AND")


def test_watermark_roundtrip(tmp_path):
    store = WatermarkStore(str(tmp_path / "state" / "wm.json"))
    assert store.read() is None
    ts = datetime(2026, 9, 26, 13, 23, 48, 960000)
    store.write(ts, 42)
    assert store.read() == ts
