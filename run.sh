#!/usr/bin/env bash
# Submit the pipeline to Spark (YARN or local) with Apache Hudi.
# Usage: ./run.sh full|incremental [extra spark-submit args]
#   MYSQL_PASSWORD=... SPARK_MASTER=yarn ./run.sh incremental
set -euo pipefail
cd "$(dirname "$0")"

MODE="${1:-incremental}"; shift || true
SPARK_MASTER="${SPARK_MASTER:-local[4]}"
HUDI_BUNDLE="${HUDI_BUNDLE:-org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0}"
MYSQL_JDBC="${MYSQL_JDBC:-com.mysql:mysql-connector-j:8.4.0}"

# Simple lock so two cron runs never overlap
exec 9>/tmp/mysql_to_hudi.lock
flock -n 9 || { echo "Another run is in progress - exiting"; exit 0; }

# Package the modules so executors / cluster-mode driver can import them
rm -f pipeline.zip && zip -qr pipeline.zip pipeline -x "*/__pycache__/*"

spark-submit \
  --master "$SPARK_MASTER" \
  --packages "$HUDI_BUNDLE,$MYSQL_JDBC" \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --conf spark.sql.shuffle.partitions=16 \
  --py-files pipeline.zip \
  "$@" \
  pipeline/mysql_to_hudi.py --config config/pipeline.json --mode "$MODE" --sink hudi --validate
