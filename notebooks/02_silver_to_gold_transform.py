# Databricks notebook source
# Silver to Gold transformation for AI-Powered Retail Operations Command Center
# Reads cleaned Silver Delta tables from ADLS and writes business-ready Gold Delta tables.

# COMMAND ----------

# 1. Storage configuration
# Replace this placeholder only when running inside Databricks.
# Do NOT commit real storage keys to GitHub.

storage_account = "retailcmdlake26"
storage_key = "<PASTE_STORAGE_ACCOUNT_KEY_HERE>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# COMMAND ----------

# 2. Define Silver and Gold paths

silver_base_path = f"abfss://silver@{storage_account}.dfs.core.windows.net"
gold_base_path = f"abfss://gold@{storage_account}.dfs.core.windows.net"

silver_sales_path = f"{silver_base_path}/sales"
silver_inventory_path = f"{silver_base_path}/inventory"
silver_returns_path = f"{silver_base_path}/returns"

gold_sales_summary_path = f"{gold_base_path}/sales_summary"
gold_inventory_risk_path = f"{gold_base_path}/inventory_risk"
gold_return_analysis_path = f"{gold_base_path}/return_analysis"

# COMMAND ----------

# 3. Read Silver Delta tables

sales_silver_df = spark.read.format("delta").load(silver_sales_path)
inventory_silver_df = spark.read.format("delta").load(silver_inventory_path)
returns_silver_df = spark.read.format("delta").load(silver_returns_path)

print("Silver sales:", sales_silver_df.count())
print("Silver inventory:", inventory_silver_df.count())
print("Silver returns:", returns_silver_df.count())

# COMMAND ----------

# 4. Create Gold Sales Summary
# Business question:
# How much did we sell by date, store, product, and category?

from pyspark.sql.functions import (
    col,
    sum,
    count,
    countDistinct,
    round,
    current_timestamp,
    when
)

gold_sales_summary_df = (
    sales_silver_df
    .groupBy(
        "event_date",
        "store_id",
        "product_id",
        "product_name",
        "category"
    )
    .agg(
        count("*").alias("sales_event_count"),
        sum("quantity").alias("total_units_sold"),
        round(sum("total_amount"), 2).alias("total_sales_amount"),
        countDistinct("customer_id").alias("unique_customers")
    )
    .withColumn("_gold_processed_ts", current_timestamp())
)

display(gold_sales_summary_df)

# COMMAND ----------

# 5. Create Gold Inventory Risk
# Business question:
# Which products are healthy, low-stock, or out-of-stock?

gold_inventory_risk_df = (
    inventory_silver_df
    .withColumn(
        "inventory_status",
        when(col("stock_on_hand") <= 0, "OUT_OF_STOCK")
        .when(col("stock_on_hand") <= col("reorder_level"), "LOW_STOCK")
        .otherwise("HEALTHY")
    )
    .withColumn(
        "units_above_reorder_level",
        col("stock_on_hand") - col("reorder_level")
    )
    .withColumn("_gold_processed_ts", current_timestamp())
)

display(gold_inventory_risk_df)

# COMMAND ----------

# 6. Create Gold Return Analysis
# Business question:
# Which products/categories are being returned and why?

gold_return_analysis_df = (
    returns_silver_df
    .groupBy(
        "event_date",
        "store_id",
        "product_id",
        "product_name",
        "category",
        "return_reason"
    )
    .agg(
        count("*").alias("return_event_count"),
        sum("quantity").alias("total_units_returned"),
        round(sum("refund_amount"), 2).alias("total_refund_amount")
    )
    .withColumn("_gold_processed_ts", current_timestamp())
)

display(gold_return_analysis_df)

# COMMAND ----------

# 7. Write Gold Delta tables
# Current mode is overwrite because this is a development/full-refresh notebook.
# Production enhancement: replace with incremental MERGE/upsert logic.

gold_sales_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .save(gold_sales_summary_path)

gold_inventory_risk_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .save(gold_inventory_risk_path)

gold_return_analysis_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .save(gold_return_analysis_path)

# COMMAND ----------

# 8. Verify Gold Delta tables

gold_sales_check_df = spark.read.format("delta").load(gold_sales_summary_path)
gold_inventory_check_df = spark.read.format("delta").load(gold_inventory_risk_path)
gold_returns_check_df = spark.read.format("delta").load(gold_return_analysis_path)

print("Gold sales summary:", gold_sales_check_df.count())
print("Gold inventory risk:", gold_inventory_check_df.count())
print("Gold return analysis:", gold_returns_check_df.count())

display(gold_sales_check_df)
display(gold_inventory_check_df)
display(gold_returns_check_df)

# COMMAND ----------

# 9. Register Gold Delta tables for SQL querying

spark.sql("CREATE DATABASE IF NOT EXISTS retail_command_center_gold")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS retail_command_center_gold.sales_summary
USING DELTA
LOCATION '{gold_sales_summary_path}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS retail_command_center_gold.inventory_risk
USING DELTA
LOCATION '{gold_inventory_risk_path}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS retail_command_center_gold.return_analysis
USING DELTA
LOCATION '{gold_return_analysis_path}'
""")

# COMMAND ----------

# 10. Example SQL checks

display(spark.sql("""
SELECT *
FROM retail_command_center_gold.sales_summary
ORDER BY total_sales_amount DESC
LIMIT 10
"""))

display(spark.sql("""
SELECT *
FROM retail_command_center_gold.inventory_risk
WHERE inventory_status IN ('LOW_STOCK', 'OUT_OF_STOCK')
ORDER BY units_above_reorder_level ASC
"""))

display(spark.sql("""
SELECT *
FROM retail_command_center_gold.return_analysis
ORDER BY total_refund_amount DESC
LIMIT 10
"""))