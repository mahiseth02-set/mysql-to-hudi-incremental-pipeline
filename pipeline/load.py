"""Writers: Apache Hudi (production) and plain Parquet (local smoke test without Hudi jars)."""
import os
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .transform import latest_per_key


def hudi_options(tgt: dict, operation: str) -> dict:
    return {
        "hoodie.table.name": tgt["table_name"],
        "hoodie.datasource.write.table.type": tgt.get("table_type", "MERGE_ON_READ"),
        "hoodie.datasource.write.operation": operation,
        "hoodie.datasource.write.recordkey.field": tgt["key"],
        "hoodie.datasource.write.precombine.field": tgt["precombine"],
        "hoodie.datasource.write.partitionpath.field": tgt["partition"],
        "hoodie.datasource.write.hive_style_partitioning": "true",
        # Partition value is derived from created_at and never changes -> cheap SIMPLE index is enough
        "hoodie.index.type": "SIMPLE",
        # MOR: upserts land in small log files; compaction merges them into parquet every N commits
        "hoodie.compact.inline": "true",
        "hoodie.compact.inline.max.delta.commits": str(tgt.get("compact_every_commits", 3)),
        "hoodie.cleaner.commits.retained": "10",
        "hoodie.parquet.compression.codec": "zstd",
        "hoodie.parquet.max.file.size": str(128 * 1024 * 1024),
        "hoodie.parquet.small.file.limit": str(100 * 1024 * 1024),
        "hoodie.upsert.shuffle.parallelism": str(tgt.get("parallelism", 8)),
        "hoodie.bulkinsert.shuffle.parallelism": str(tgt.get("parallelism", 8)),
    }


def write_hudi(df: DataFrame, tgt: dict, full_load: bool) -> None:
    operation = "bulk_insert" if full_load else "upsert"
    (df.write.format("hudi")
       .options(**hudi_options(tgt, operation))
       .mode("overwrite" if full_load else "append")
       .save(tgt["path"]))


def write_parquet_upsert(spark: SparkSession, df: DataFrame, tgt: dict, full_load: bool) -> None:
    """
    Emulates Hudi upsert semantics with plain Parquet (read existing, union, keep latest, drop deletes).
    Only for running the pipeline locally without Hudi - it rewrites the whole table.
    """
    path, key, pre = tgt["path"], tgt["key"], tgt["precombine"]
    cols = [c for c in df.columns]
    merged = df
    if not full_load and os.path.exists(path):
        existing = spark.read.parquet(path).withColumn("_hoodie_is_deleted", F.lit(False)).select(*cols)
        merged = existing.unionByName(df)
    result = latest_per_key(merged, key, pre).filter(~F.col("_hoodie_is_deleted")).drop("_hoodie_is_deleted")

    tmp = path.rstrip("/") + "__tmp"
    result.write.mode("overwrite").partitionBy(tgt["partition"]).parquet(tmp)
    shutil.rmtree(path, ignore_errors=True)
    os.replace(tmp, path)


def read_target(spark: SparkSession, tgt: dict, sink: str) -> DataFrame:
    if sink == "hudi":
        return spark.read.format("hudi").load(tgt["path"])
    return spark.read.parquet(tgt["path"])
