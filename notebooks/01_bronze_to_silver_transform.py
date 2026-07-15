# Databricks notebook source
storage_account = "retailcmdlake26"
storage_key = "<PASTE_STORAGE_ACCOUNT_KEY_HERE>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)




# COMMAND ----------

storage_account = "retailcmdlake26"

bronze_base_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net"

bronze_sales_path = f"{bronze_base_path}/sales"
bronze_inventory_path = f"{bronze_base_path}/inventory"
bronze_returns_path = f"{bronze_base_path}/returns"

# COMMAND ----------

display(dbutils.fs.ls(bronze_sales_path))
display(dbutils.fs.ls(bronze_inventory_path))
display(dbutils.fs.ls(bronze_returns_path))

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@retailcmdlake26.dfs.core.windows.net/sales/year=2026/"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@retailcmdlake26.dfs.core.windows.net/sales/year=2026/month=06/"))

# COMMAND ----------

from pyspark.sql.functions import input_file_name, current_timestamp

sales_bronze_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(bronze_sales_path)
    .withColumn("_source_file", input_file_name())
    .withColumn("_bronze_read_ts", current_timestamp())
)

inventory_bronze_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(bronze_inventory_path)
    .withColumn("_source_file", input_file_name())
    .withColumn("_bronze_read_ts", current_timestamp())
)

returns_bronze_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(bronze_returns_path)
    .withColumn("_source_file", input_file_name())
    .withColumn("_bronze_read_ts", current_timestamp())
)

# COMMAND ----------

print("Sales records:", sales_bronze_df.count())
print("Inventory records:", inventory_bronze_df.count())
print("Returns records:", returns_bronze_df.count())

# COMMAND ----------

sales_bronze_df.printSchema()
inventory_bronze_df.printSchema()
returns_bronze_df.printSchema()

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@retailcmdlake26.dfs.core.windows.net/sales/year=2026/month=06/"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@retailcmdlake26.dfs.core.windows.net/sales/year=2026/month=06/day=30/"))

# COMMAND ----------

sample_sales_file = "abfss://bronze@retailcmdlake26.dfs.core.windows.net/sales/year=2026/month=06/day=30/0426eb63-fea4-4ec1-a53b-dab9240e7a9c.json"

print(dbutils.fs.head(sample_sales_file, 2000))

# COMMAND ----------

display(dbutils.fs.ls(sample_sales_file))

# COMMAND ----------

sales_bronze_df = (
    spark.read
    .option("multiLine", "true")
    .json(sample_sales_file)
)

sales_bronze_df.printSchema()
display(sales_bronze_df)

# COMMAND ----------

from pyspark.sql.functions import input_file_name, current_timestamp

sales_bronze_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("multiLine", "true")
    .json(bronze_sales_path)
    .withColumn("_source_file", input_file_name())
    .withColumn("_bronze_read_ts", current_timestamp())
)

inventory_bronze_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("multiLine", "true")
    .json(bronze_inventory_path)
    .withColumn("_source_file", input_file_name())
    .withColumn("_bronze_read_ts", current_timestamp())
)

returns_bronze_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("multiLine", "true")
    .json(bronze_returns_path)
    .withColumn("_source_file", input_file_name())
    .withColumn("_bronze_read_ts", current_timestamp())
)

# COMMAND ----------

print("Sales records:", sales_bronze_df.count())
print("Inventory records:", inventory_bronze_df.count())
print("Returns records:", returns_bronze_df.count())

# COMMAND ----------

print("SALES SCHEMA")
sales_bronze_df.printSchema()

print("INVENTORY SCHEMA")
inventory_bronze_df.printSchema()

print("RETURNS SCHEMA")
returns_bronze_df.printSchema()

# COMMAND ----------

display(sales_bronze_df.limit(5))
display(inventory_bronze_df.limit(5))
display(returns_bronze_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Silver

# COMMAND ----------

silver_base_path = f"abfss://silver@{storage_account}.dfs.core.windows.net"

silver_sales_path = f"{silver_base_path}/sales"
silver_inventory_path = f"{silver_base_path}/inventory"
silver_returns_path = f"{silver_base_path}/returns"

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, to_date, current_timestamp

sales_silver_df = (
    sales_bronze_df
    .withColumn("event_timestamp", to_timestamp(col("event_time")))
    .withColumn("event_date", to_date(col("event_timestamp")))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("total_amount", col("total_amount").cast("double"))
    .withColumn("_silver_processed_ts", current_timestamp())
    .dropDuplicates(["event_id"])
)

display(sales_silver_df.limit(5))
sales_silver_df.printSchema()

# COMMAND ----------

inventory_silver_df = (
    inventory_bronze_df
    .withColumn("event_timestamp", to_timestamp(col("event_time")))
    .withColumn("event_date", to_date(col("event_timestamp")))
    .withColumn("reorder_level", col("reorder_level").cast("int"))
    .withColumn("stock_on_hand", col("stock_on_hand").cast("int"))
    .withColumn("_silver_processed_ts", current_timestamp())
    .dropDuplicates(["event_id"])
)

display(inventory_silver_df.limit(5))
inventory_silver_df.printSchema()

# COMMAND ----------

print(inventory_bronze_df.columns)

# COMMAND ----------

print(returns_bronze_df.columns)

# COMMAND ----------

returns_silver_df = (
    returns_bronze_df
    .withColumn("event_timestamp", to_timestamp(col("event_time")))
    .withColumn("event_date", to_date(col("event_timestamp")))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("refund_amount", col("refund_amount").cast("double"))
    .withColumn("_silver_processed_ts", current_timestamp())
    .dropDuplicates(["event_id"])
)

display(returns_silver_df.limit(5))
returns_silver_df.printSchema()

# COMMAND ----------

silver_base_path = f"abfss://silver@{storage_account}.dfs.core.windows.net"

silver_sales_path = f"{silver_base_path}/sales"
silver_inventory_path = f"{silver_base_path}/inventory"
silver_returns_path = f"{silver_base_path}/returns"

# COMMAND ----------

sales_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .save(silver_sales_path)

inventory_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .save(silver_inventory_path)

returns_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .save(silver_returns_path)

# COMMAND ----------

sales_check_df = spark.read.format("delta").load(silver_sales_path)
inventory_check_df = spark.read.format("delta").load(silver_inventory_path)
returns_check_df = spark.read.format("delta").load(silver_returns_path)

print("Silver sales:", sales_check_df.count())
print("Silver inventory:", inventory_check_df.count())
print("Silver returns:", returns_check_df.count())