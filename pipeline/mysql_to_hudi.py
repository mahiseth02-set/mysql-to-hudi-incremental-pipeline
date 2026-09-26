#!/usr/bin/env python3
"""
MySQL -> Apache Hudi incremental pipeline.

  full         : first load of the whole table (bulk_insert)
  incremental  : rows changed since the last watermark (upsert, soft deletes become Hudi deletes)

The watermark is committed only after the lake write succeeds, so a failed run is simply re-run.
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import timedelta

from pyspark.sql import SparkSession

# Make the repo root importable when launched via spark-submit (which only adds this file's folder)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.extract import extract, snapshot_upper_bound  # noqa: E402
from pipeline.load import read_target, write_hudi, write_parquet_upsert  # noqa: E402
from pipeline.transform import prepare  # noqa: E402
from pipeline.watermark import WatermarkStore  # noqa: E402

log = logging.getLogger("mysql_to_hudi")


def load_config(path: str) -> dict:
    with open(path) as f:
        cfg = json.load(f)
    src = cfg["source"]
    src["password"] = os.environ.get(src.get("password_env", "MYSQL_PASSWORD"), src.get("password", ""))
    cfg["target"].setdefault("key", src["key"])
    return cfg


def build_spark(sink: str) -> SparkSession:
    b = SparkSession.builder.appName("mysql_to_hudi")
    if sink == "hudi":
        b = (b.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
              .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
              .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog"))
    return b.getOrCreate()


def run(cfg: dict, mode: str, sink: str, validate: bool) -> int:
    src, tgt = cfg["source"], cfg["target"]
    store = WatermarkStore(cfg["state_path"])
    spark = build_spark(sink)
    t0 = time.time()

    upper = snapshot_upper_bound(spark, src)
    if upper is None:
        log.info("Source table is empty - nothing to do")
        return 0

    full_load = mode == "full"
    lower = None
    if not full_load:
        last = store.read()
        if last is None:
            log.error("No watermark found - run with --mode full first")
            return 2
        # Overlap window catches rows committed late by long transactions; duplicates are removed by precombine
        lower = last - timedelta(minutes=cfg.get("overlap_minutes", 10))

    df, count = extract(spark, src, lower, upper)
    if count == 0:
        log.info("No changes since %s", lower)
        store.write(upper, 0)
        return 0

    prepared = prepare(df, src["key"], tgt["precombine"]).cache()
    deletes = prepared.filter("_hoodie_is_deleted").count()
    log.info("Writing %s rows (%s deletes) to %s [%s]", count, deletes, tgt["path"], sink)

    if sink == "hudi":
        write_hudi(prepared, tgt, full_load)
    else:
        write_parquet_upsert(spark, prepared, tgt, full_load)

    store.write(upper, count)          # commit the watermark only after a successful write
    log.info("Done in %.1fs - watermark moved to %s", time.time() - t0, upper)

    if validate:
        target_rows = read_target(spark, tgt, sink).count()
        src_rows = extract_count(spark, src, upper)
        status = "OK" if target_rows == src_rows else "MISMATCH"
        log.info("Validation %s: source active rows=%s, lake rows=%s", status, src_rows, target_rows)
        if status != "OK":
            return 3
    return 0


def extract_count(spark: SparkSession, src: dict, upper) -> int:
    from pipeline.extract import _jdbc
    q = (f"SELECT COUNT(*) AS c FROM {src['table']} WHERE is_deleted = 0 "
         f"AND {src['watermark_col']} <= '{upper:%Y-%m-%d %H:%M:%S.%f}'")
    return int(_jdbc(spark, src, q).first()["c"])


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default="config/pipeline.json")
    p.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    p.add_argument("--sink", choices=["hudi", "parquet"], default="hudi",
                   help="parquet = local smoke test without Hudi jars")
    p.add_argument("--validate", action="store_true", help="compare source vs lake row counts after the load")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    sys.exit(run(load_config(args.config), args.mode, args.sink, args.validate))


if __name__ == "__main__":
    main()
