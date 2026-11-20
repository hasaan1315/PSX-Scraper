# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold: star schema for Power BI
# MAGIC Reads `workspace.silver.*` and `workspace.bronze.reference`, writes `workspace.gold.*`.
# MAGIC
# MAGIC | Table | Grain |
# MAGIC |---|---|
# MAGIC | `dim_date` | one row per calendar day |
# MAGIC | `dim_asset` | one row per ticker (equities and ETFs, no debt) |
# MAGIC | `fact_equities` | one row per ticker per trading day (prices, daily return) |
# MAGIC | `fact_dividends` | one row per cash dividend (gross and net for filer and non-filer) |
# MAGIC | `v_dividend_yield` | view: trailing 12 month yield per ticker |
# MAGIC
# MAGIC Dividends sit in their own fact table. Prices only go back as far as Bronze has data,
# MAGIC so joining dividends onto price rows would silently drop older dividends.

# COMMAND ----------

from pyspark.sql import functions as F, Window

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_date

# COMMAND ----------

dim_date = spark.sql("""
SELECT
  CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
  d                                       AS date,
  year(d)                                 AS year,
  quarter(d)                              AS quarter,
  month(d)                                AS month,
  date_format(d, 'MMMM')                  AS month_name,
  day(d)                                  AS day,
  date_format(d, 'EEEE')                  AS day_name,
  dayofweek(d)                            AS day_of_week,
  (dayofweek(d) IN (1, 7))                AS is_weekend
FROM (SELECT explode(sequence(DATE'2020-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) AS d)
""")
dim_date.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold.dim_date")
print("dim_date", dim_date.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_asset
# MAGIC Source: the symbols file saved by the scraper each day (latest copy per ticker).
# MAGIC `asset_key` is a hash of the ticker, so it stays the same on every rebuild.

# COMMAND ----------

ref = spark.table("workspace.bronze.reference")

w = Window.partitionBy("symbol").orderBy(F.col("_source_file").desc())
latest = ref.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

dim_asset = (
    latest
    .filter(F.lower(F.col("is_debt")) != "true")
    .select(
        F.xxhash64("symbol").alias("asset_key"),
        F.col("symbol").alias("ticker"),
        F.col("name").alias("company_name"),
        F.col("sector_name").alias("sector"),
        (F.lower(F.col("is_etf")) == "true").alias("is_etf"),
        (F.lower(F.col("is_gem")) == "true").alias("is_gem"),
    )
)
dim_asset.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold.dim_asset")
print("dim_asset", dim_asset.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## fact_equities (daily closing valuation)

# COMMAND ----------

p = spark.table("workspace.silver.prices")
w = Window.partitionBy("ticker").orderBy("trade_date")

fact_equities = (
    p.join(spark.table("workspace.gold.dim_asset").select("ticker"), "ticker", "inner")
    .withColumn("prev_close", F.lag("close").over(w))
    .withColumn(
        "daily_return",
        F.when(F.col("prev_close") > 0, F.round(F.col("close") / F.col("prev_close") - 1, 6)),
    )
    .select(
        F.date_format("trade_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.xxhash64("ticker").alias("asset_key"),
        "ticker", "trade_date",
        "open", "high", "low", "close", "volume",
        "prev_close", "daily_return",
    )
)
fact_equities.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold.fact_equities")
print("fact_equities", fact_equities.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## fact_dividends (gross, tax and net, for both filer statuses)

# COMMAND ----------

n = spark.table("workspace.silver.dividends_net")

fact_dividends = (
    n.join(spark.table("workspace.gold.dim_asset").select("ticker"), "ticker", "inner")
    .groupBy(
        "ticker", "announcement_ts", "period", "payout_tag", "payout_pct",
        "face_value", "face_value_source", "gross_dps", "effective_date", "book_closure_start", "book_closure_end",
    )
    .agg(
        F.max(F.when(F.col("filer_status") == "filer", F.col("tax_rate"))).alias("tax_rate_filer"),
        F.max(F.when(F.col("filer_status") == "filer", F.col("net_dps"))).alias("net_dps_filer"),
        F.max(F.when(F.col("filer_status") == "non_filer", F.col("tax_rate"))).alias("tax_rate_non_filer"),
        F.max(F.when(F.col("filer_status") == "non_filer", F.col("net_dps"))).alias("net_dps_non_filer"),
    )
    .withColumn("date_key", F.date_format("effective_date", "yyyyMMdd").cast("int"))
    .withColumn("asset_key", F.xxhash64("ticker"))
)
fact_dividends.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold.fact_dividends")
print("fact_dividends", fact_dividends.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## v_dividend_yield (trailing 12 months, latest close)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW workspace.gold.v_dividend_yield AS
WITH latest AS (
  SELECT ticker, trade_date, close,
         ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS rn
  FROM workspace.gold.fact_equities
),
ttm AS (
  SELECT ticker,
         ROUND(SUM(gross_dps), 4)         AS gross_dps_12m,
         ROUND(SUM(net_dps_filer), 4)     AS net_dps_filer_12m,
         ROUND(SUM(net_dps_non_filer), 4) AS net_dps_non_filer_12m,
         MIN(face_value_source)           AS face_value_source
  FROM workspace.gold.fact_dividends
  WHERE effective_date >  date_sub(current_date(), 365)
    AND effective_date <= current_date()
  GROUP BY ticker
)
SELECT l.ticker,
       a.company_name,
       a.sector,
       a.is_etf,
       l.trade_date AS price_date,
       l.close,
       t.gross_dps_12m,
       t.net_dps_filer_12m,
       t.net_dps_non_filer_12m,
       ROUND(t.gross_dps_12m         / l.close, 4) AS gross_yield,
       ROUND(t.net_dps_filer_12m     / l.close, 4) AS net_yield_filer,
       ROUND(t.net_dps_non_filer_12m / l.close, 4) AS net_yield_non_filer,
       t.face_value_source,
       (t.gross_dps_12m / l.close > 0.25 OR t.face_value_source = 'assumed') AS needs_review
FROM latest l
JOIN ttm t ON t.ticker = l.ticker
JOIN workspace.gold.dim_asset a ON a.ticker = l.ticker
WHERE l.rn = 1 AND l.close > 0
""")

display(spark.sql("""
SELECT * FROM workspace.gold.v_dividend_yield
ORDER BY net_yield_filer DESC
LIMIT 15
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checks (each should return 0 rows)

# COMMAND ----------

# Facts with no matching asset
display(spark.sql("""
SELECT 'fact_equities' AS tbl, COUNT(*) AS orphans
FROM workspace.gold.fact_equities f LEFT JOIN workspace.gold.dim_asset a USING (asset_key)
WHERE a.asset_key IS NULL
UNION ALL
SELECT 'fact_dividends', COUNT(*)
FROM workspace.gold.fact_dividends f LEFT JOIN workspace.gold.dim_asset a USING (asset_key)
WHERE a.asset_key IS NULL
"""))

# Two tickers hashing to the same key (extremely unlikely). Expect 0 rows.
display(spark.sql("""
SELECT asset_key, COUNT(*) AS n FROM workspace.gold.dim_asset GROUP BY asset_key HAVING COUNT(*) > 1
"""))

# Facts with a date outside dim_date. Expect 0.
display(spark.sql("""
SELECT COUNT(*) AS missing_dates FROM workspace.gold.fact_equities f
LEFT JOIN workspace.gold.dim_date d USING (date_key) WHERE d.date_key IS NULL
"""))
