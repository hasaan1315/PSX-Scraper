# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver: clean prices, clean dividends, tax logic
# MAGIC Reads `workspace.bronze.*`, writes `workspace.silver.*`.
# MAGIC Safe to re-run: every table is rebuilt from Bronze (tax rate history is kept).

# COMMAND ----------

from pyspark.sql import functions as F, Window

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

# Fallback face value, used only when a ticker has no scraped value.
# PSX shows dividends as % of face value. Most stocks are Rs.10, but not all (AHCL is Rs.1).
FACE_VALUE = 10.0

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Tax rates (SCD Type 2)
# MAGIC One row per rate per period. A rate change closes the old row and opens a new one,
# MAGIC so old dividends keep the tax rate that applied at that time.
# MAGIC **Verify the rates and dates with FBR before you trust the numbers.**

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.silver.dim_tax_rate (
  filer_status   STRING,
  tax_rate       DOUBLE,
  effective_from DATE,
  effective_to   DATE,
  is_current     BOOLEAN
)
""")

if spark.table("workspace.silver.dim_tax_rate").count() == 0:
    spark.sql("""
    INSERT INTO workspace.silver.dim_tax_rate VALUES
      ('filer',     0.15, DATE'2000-01-01', NULL, true),
      ('non_filer', 0.30, DATE'2000-01-01', NULL, true)
    """)


def change_tax_rate(filer_status, new_rate, effective_from):
    """Close the current row and open a new one. Example:
    change_tax_rate('non_filer', 0.35, '2027-07-01')"""
    spark.sql(f"""
        UPDATE workspace.silver.dim_tax_rate
        SET effective_to = date_sub(DATE'{effective_from}', 1), is_current = false
        WHERE filer_status = '{filer_status}' AND is_current = true
    """)
    spark.sql(f"""
        INSERT INTO workspace.silver.dim_tax_rate
        VALUES ('{filer_status}', {new_rate}, DATE'{effective_from}', NULL, true)
    """)


display(spark.table("workspace.silver.dim_tax_rate"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Prices: types, dedupe, quarantine bad rows

# COMMAND ----------

raw = spark.table("workspace.bronze.prices")

typed = raw.select(
    F.col("ticker"),
    F.to_date("date").alias("trade_date"),
    F.col("open").cast("double").alias("open"),
    F.col("high").cast("double").alias("high"),
    F.col("low").cast("double").alias("low"),
    F.col("close").cast("double").alias("close"),
    F.col("volume").cast("double").cast("long").alias("volume"),
    (F.lower(F.col("is_anomaly")) == "true").alias("is_anomaly"),
    F.to_date("ingestion_date").alias("ingestion_date"),
    F.col("_loaded_at"),
)

# Each daily run re-pulls 5 days, so the same ticker+date repeats. Keep the latest copy.
w = Window.partitionBy("ticker", "trade_date").orderBy(
    F.col("ingestion_date").desc(), F.col("_loaded_at").desc()
)
dedup = typed.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

is_good = (
    F.col("ticker").isNotNull()
    & F.col("trade_date").isNotNull()
    & (F.col("close") > 0)
    & (~F.col("is_anomaly"))
)

dedup.filter(is_good).write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.prices")
dedup.filter(~is_good).write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.prices_quarantine")

print("silver.prices            ", spark.table("workspace.silver.prices").count())
print("silver.prices_quarantine ", spark.table("workspace.silver.prices_quarantine").count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Dividends: types, dedupe, split into cash vs corporate actions

# COMMAND ----------

d = spark.table("workspace.bronze.dividends")

dt = d.select(
    F.col("ticker"),
    F.to_timestamp("announcement_ts").alias("announcement_ts"),
    # PSX sometimes doubles the period text ("30/06/2026(YR) - 30/06/2026(YR)")
    F.trim(F.split(F.col("period"), " - ").getItem(0)).alias("period"),
    F.col("payout_pct").cast("double").alias("payout_pct"),
    F.col("payout_tag"),
    F.col("payout_type"),
    F.col("right_price").cast("double").alias("right_price"),
    F.col("right_kind"),
    F.to_date("book_closure_start").alias("book_closure_start"),
    F.to_date("book_closure_end").alias("book_closure_end"),
    F.col("parse_status"),
    F.to_date("ingestion_date").alias("ingestion_date"),
    F.col("_loaded_at"),
)

key = ["ticker", "announcement_ts", "period", "payout_type", "payout_tag", "payout_pct"]
w = Window.partitionBy(*key).orderBy(F.col("ingestion_date").desc(), F.col("_loaded_at").desc())
dd = dt.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

# Face value per ticker, scraped from each PSX company page (scrape_face_value.py).
# Tickers without a scraped value fall back to FACE_VALUE and are marked "assumed".
try:
    fv_raw = spark.table("workspace.bronze.face_value")
except Exception:
    fv_raw = spark.createDataFrame([], "ticker string, face_value string, _source_file string")

wf = Window.partitionBy("ticker").orderBy(F.col("_source_file").desc())
fv = (
    fv_raw.withColumn("rn", F.row_number().over(wf)).filter("rn = 1")
    .select("ticker", F.col("face_value").cast("double").alias("fv_psx"))
)

# Cash dividends (type D) with a per-share amount in rupees
cash = (
    dd.filter((F.col("payout_type") == "D") & F.col("payout_pct").isNotNull())
    .join(fv, "ticker", "left")
    .withColumn("face_value", F.coalesce(F.col("fv_psx"), F.lit(FACE_VALUE)))
    .withColumn("face_value_source", F.when(F.col("fv_psx").isNotNull(), "psx").otherwise("assumed"))
    .drop("fv_psx")
    .withColumn("gross_dps", F.round(F.col("payout_pct") / 100 * F.col("face_value"), 4))
    .withColumn(
        "effective_date",
        F.coalesce(F.col("book_closure_start"), F.to_date("announcement_ts")),
    )
)
cash.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.dividends_cash")

# Bonus (B) and right (R) rows are NOT cash: they change share count, not income.
actions = dd.filter(F.col("payout_type").isin("B", "R"))
actions.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.corporate_actions")

print("silver.dividends_cash    ", spark.table("workspace.silver.dividends_cash").count())
print("silver.corporate_actions ", spark.table("workspace.silver.corporate_actions").count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Net dividend after withholding tax
# MAGIC Joins each cash dividend to the tax rate valid on its effective date.
# MAGIC Produces one row per dividend per filer status (filer and non_filer).

# COMMAND ----------

rates = spark.table("workspace.silver.dim_tax_rate")
c = spark.table("workspace.silver.dividends_cash")

net = (
    c.alias("c")
    .join(
        rates.alias("r"),
        (F.col("c.effective_date") >= F.col("r.effective_from"))
        & (F.col("r.effective_to").isNull() | (F.col("c.effective_date") <= F.col("r.effective_to"))),
        "inner",
    )
    .select("c.*", F.col("r.filer_status"), F.col("r.tax_rate"))
    .withColumn("tax_dps", F.round(F.col("gross_dps") * F.col("tax_rate"), 4))
    .withColumn("net_dps", F.round(F.col("gross_dps") - F.col("tax_dps"), 4))
)
net.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.dividends_net")

print("silver.dividends_net     ", spark.table("workspace.silver.dividends_net").count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Checks

# COMMAND ----------

display(spark.sql("""
SELECT ticker, period, payout_pct, gross_dps, filer_status, tax_rate, tax_dps, net_dps, effective_date
FROM workspace.silver.dividends_net
WHERE ticker = 'HBL'
ORDER BY effective_date DESC, filer_status
LIMIT 10
"""))

# Every cash dividend should appear exactly twice (filer + non_filer). Expect 0 rows below.
display(spark.sql("""
SELECT ticker, announcement_ts, period, payout_tag, COUNT(*) AS n
FROM workspace.silver.dividends_net
GROUP BY ticker, announcement_ts, period, payout_tag
HAVING COUNT(*) <> 2
"""))

# Duplicate check on prices. Expect 0 rows.
display(spark.sql("""
SELECT ticker, trade_date, COUNT(*) AS n
FROM workspace.silver.prices
GROUP BY ticker, trade_date
HAVING COUNT(*) > 1
"""))
