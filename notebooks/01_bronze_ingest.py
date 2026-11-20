# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze: raw CSV files to Delta tables
# MAGIC Reads every dated CSV the daily GitHub run (and the one-off backfill) put in the landing Volume.
# MAGIC Everything stays text. Typing and cleaning happen in Silver.
# MAGIC Each row keeps its source file name and load time.

# COMMAND ----------

from pyspark.sql import functions as F

BASE = "/Volumes/workspace/bronze/landing"
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")

for name in ["prices", "dividends", "reference", "face_value"]:
    df = (
        spark.read.option("header", True).option("inferSchema", False)
        .csv(f"{BASE}/{name}/*.csv")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_loaded_at", F.current_timestamp())
    )
    df.write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"workspace.bronze.{name}")
    print(name, df.count())
