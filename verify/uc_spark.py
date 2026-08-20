#!/usr/bin/env python3
"""Unity Catalog helper for the sql/03 UC demo, run via Spark + unitycatalog-spark.

The Flink UC catalog is sink-only (createTable/createDatabase are notSupported),
so the schema + managed Delta table must be created out-of-band. This script does
that and also reads the table back for verification. It is intended to run INSIDE
the neighbouring unitycatalog-playground's `marimo-spark` container, where UC and
RustFS are reachable in-network (see the just uc-setup / uc-read recipes):

    setup  -> CREATE SCHEMA unity.flink_playground + managed table clickstream
    read   -> SELECT COUNT(*) and a sample from unity.flink_playground.clickstream

Config comes from env (defaults match the in-container hostnames of that stack):
    UC_URI        default http://unitycatalog:8080
    UC_TOKEN      default "" (local OSS UC runs without auth)
    S3_ENDPOINT   default http://rustfs:9000
    DELTA_VERSION / UC_SPARK_VERSION / HADOOP_AWS_VERSION / AWS_SDK_BUNDLE
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

UC_URI = os.environ.get("UC_URI", "http://unitycatalog:8080")
UC_TOKEN = os.environ.get("UC_TOKEN", "")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://rustfs:9000")
CATALOG = os.environ.get("UC_CATALOG", "unity")
SCHEMA = os.environ.get("UC_SCHEMA", "flink_playground")
TABLE = os.environ.get("UC_TABLE", "clickstream")

SPARK_LINE = os.environ.get("SPARK_VERSION", "4.2")
DELTA_VERSION = os.environ.get("DELTA_VERSION", "4.4.0")
UC_SPARK_VERSION = os.environ.get("UNITY_CATALOG_VERSION", "0.6.0")
HADOOP_AWS = os.environ.get("HADOOP_VERSION", "3.4.2")
AWS_BUNDLE = os.environ.get("AWS_SDK_BUNDLE", "2.29.52")

FQN = f"{CATALOG}.{SCHEMA}.{TABLE}"


def spark_session() -> SparkSession:
    packages = ",".join([
        f"io.delta:delta-spark_{SPARK_LINE}_2.13:{DELTA_VERSION}",
        f"io.unitycatalog:unitycatalog-spark_{SPARK_LINE}_2.13:{UC_SPARK_VERSION}",
        f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS}",
        f"software.amazon.awssdk:bundle:{AWS_BUNDLE}",
    ])
    builder = (
        SparkSession.builder.appName("dfp-uc-helper")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(f"spark.sql.catalog.{CATALOG}", "io.unitycatalog.spark.UCSingleCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.uri", UC_URI)
        .config(f"spark.sql.catalog.{CATALOG}.token", UC_TOKEN)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )
    if os.environ.get("MAVEN_PROXY_URL"):
        builder = builder.config("spark.jars.repositories", os.environ["MAVEN_PROXY_URL"])
    return builder.getOrCreate()


def do_setup(spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {FQN} (
            id   BIGINT,
            name STRING
        ) USING delta
        TBLPROPERTIES (
            'delta.feature.catalogManaged' = 'supported',
            'delta.enableDeletionVectors'  = 'true'
        )
        """
    )
    print(f"[uc] ready: {FQN}")
    spark.sql(f"DESCRIBE EXTENDED {FQN}").show(truncate=False)


def do_read(spark: SparkSession) -> None:
    total = spark.sql(f"SELECT COUNT(*) AS n FROM {FQN}").collect()[0]["n"]
    print(f"[uc] {FQN} row count = {total}")
    spark.sql(f"SELECT * FROM {FQN} LIMIT 5").show(truncate=False)
    if total <= 0:
        raise SystemExit("[uc] FAIL: table is empty - did the Flink insert run?")
    print("[uc] PASS")


def main(argv: list[str]) -> int:
    cmd = argv[0] if argv else "read"
    spark = spark_session()
    try:
        if cmd == "setup":
            do_setup(spark)
        elif cmd == "read":
            do_read(spark)
        else:
            print(f"unknown command '{cmd}' (use: setup | read)", file=sys.stderr)
            return 2
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
