"""Pure transformations - no I/O, so they are easy to unit test."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def latest_per_key(df: DataFrame, key: str, order_col: str) -> DataFrame:
    """Keep only the newest version of each key inside the batch (overlap windows can bring duplicates)."""
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def prepare(df: DataFrame, key: str, precombine: str) -> DataFrame:
    """
    Shape source rows for the lake:
      * order_date partition (yyyy-MM-dd from created_at - never changes for a key, so SIMPLE index is safe)
      * _hoodie_is_deleted from the soft-delete flag -> Hudi physically removes the record
      * ingest timestamp for lineage
    """
    return (latest_per_key(df, key, precombine)
            .withColumn("order_date", F.date_format("created_at", "yyyy-MM-dd"))
            .withColumn("_hoodie_is_deleted", F.col("is_deleted") == F.lit(1))
            .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
            .withColumn("ingested_at", F.current_timestamp()))
