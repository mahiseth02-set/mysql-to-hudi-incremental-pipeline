"""Parallel JDBC extraction from MySQL."""
import logging
from datetime import datetime
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)


def _jdbc(spark: SparkSession, src: dict, query: str, **extra) -> DataFrame:
    reader = (spark.read.format("jdbc")
              .option("url", src["url"])
              .option("driver", src["driver"])
              .option("user", src["user"])
              .option("password", src["password"])
              .option("dbtable", f"({query}) q")
              .option("fetchsize", src.get("fetch_size", 10000)))
    for k, v in extra.items():
        reader = reader.option(k, v)
    return reader.load()


def snapshot_upper_bound(spark: SparkSession, src: dict) -> Optional[datetime]:
    """Fix the upper edge of this run up-front, so rows changing *during* the run go to the next run."""
    # Read as a string in the source's own time zone: avoids silent JVM / driver time-zone shifts
    wm = src["watermark_col"]
    row = _jdbc(spark, src, f"SELECT DATE_FORMAT(MAX({wm}), '%Y-%m-%d %H:%i:%s.%f') AS max_ts "
                            f"FROM {src['table']}").first()
    return datetime.strptime(row["max_ts"], "%Y-%m-%d %H:%M:%S.%f") if row["max_ts"] else None


def _where(src: dict, lower: Optional[datetime], upper: datetime) -> str:
    wm = src["watermark_col"]
    cond = f"{wm} <= '{upper:%Y-%m-%d %H:%M:%S.%f}'"
    if lower is not None:
        cond = f"{wm} > '{lower:%Y-%m-%d %H:%M:%S.%f}' AND " + cond
    return cond


def extract(spark: SparkSession, src: dict, lower: Optional[datetime], upper: datetime) -> Tuple[DataFrame, int]:
    """
    Reads rows with lower < watermark_col <= upper.
    Splits the read into `num_partitions` parallel JDBC queries on the primary key range.
    """
    where = _where(src, lower, upper)
    key = src["key"]
    b = _jdbc(spark, src,
              f"SELECT MIN({key}) AS lo, MAX({key}) AS hi, COUNT(*) AS cnt FROM {src['table']} WHERE {where}").first()
    count = int(b["cnt"] or 0)
    log.info("Source window %s -> %s rows", where, count)
    if count == 0:
        return None, 0

    parts = max(1, min(int(src.get("num_partitions", 8)), count // 1000 + 1))
    df = _jdbc(spark, src, f"SELECT * FROM {src['table']} WHERE {where}",
               partitionColumn=key, lowerBound=int(b["lo"]), upperBound=int(b["hi"]) + 1,
               numPartitions=parts)
    return df, count
