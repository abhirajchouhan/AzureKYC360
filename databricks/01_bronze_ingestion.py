# Databricks notebook source
# ── Cell 1: Configure ADLS Gen2 Access — Secure via Key Vault ───────
# Storage key is read from Azure Key Vault via Databricks Secret Scope.
# No credentials hardcoded in any notebook.
# Secret Scope: kyc360-scope
# Key Vault: kv-kyc360
# Secret: adls-storage-key

# storage_account_name = "adlskyc360"
# storage_account_key  = ""

storage_account_name = "adlskyc360"

# Read key securely from Key Vault
# storage_account_key = dbutils.secrets.get(
#     scope="kyc360-scope",
#     key="adls-storage-key"
# ) 

# spark.conf.set(
#     f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
#     storage_account_key
# )

# Define base paths — use these throughout all notebooks
landing = f"abfss://landing@{storage_account_name}.dfs.core.windows.net"
bronze  = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net"
silver  = f"abfss://silver@{storage_account_name}.dfs.core.windows.net"
gold    = f"abfss://gold@{storage_account_name}.dfs.core.windows.net"

print("✅ ADLS Gen2 access configured.")
print(f"   Landing : {landing}")
print(f"   Bronze  : {bronze}")
print(f"   Silver  : {silver}")
print(f"   Gold    : {gold}")

# COMMAND ----------

# ── Cell 2: Verify all 5 source files are visible ───────────────────

from datetime import datetime

print("=== LANDING ZONE CONTENTS ===\n")

folders = [
    "customer_onboarding",
    "transaction_feed",
    "kyc_documents",
    "watchlist_sanctions",
    "account_activity"
]

for folder in folders:
    try:
        files = dbutils.fs.ls(f"{landing}/{folder}/")
        print(f"✅ {folder}/")
        for f in files:
            print(f"   └── {f.name} ({round(f.size/1024, 1)} KB)")
    except Exception as e:
        print(f"❌ {folder}/ — NOT FOUND: {str(e)}")
    print()

# COMMAND ----------

# ── Cell 3: Bronze — Customer Onboarding CSV → Delta ────────────────
# Reads raw CSV from landing zone.
# Adds ingestion metadata columns.
# Partitions by ingest_year/ingest_month/ingest_day — dynamic.
# Writes as Delta format to bronze container.
# Delta gives ACID transactions, time travel, schema enforcement.

from pyspark.sql.functions import (
    current_timestamp, lit,
    year, month, dayofmonth
)

# Read CSV from landing
df_onboarding = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{landing}/customer_onboarding/customer_onboarding.csv")

# Add ingestion metadata + partition columns
df_onboarding = df_onboarding \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("customer_onboarding.csv")) \
    .withColumn("pipeline", lit("databricks_batch")) \
    .withColumn("ingest_year",  year(current_timestamp())) \
    .withColumn("ingest_month", month(current_timestamp())) \
    .withColumn("ingest_day",   dayofmonth(current_timestamp()))

# Write to bronze as Delta
df_onboarding.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("ingest_year", "ingest_month", "ingest_day") \
    .save(f"{bronze}/customer_onboarding/")

row_count = df_onboarding.count()

print(f"✅ Customer Onboarding → Bronze complete.")
print(f"   Rows written  : {row_count}")
print(f"   Format        : Delta")
print(f"   Partition     : ingest_year / ingest_month / ingest_day")
print(f"   Path          : bronze/customer_onboarding/")

# COMMAND ----------

# ── Cell 4: Bronze — Transaction Feed JSON → Delta ──────────────────
# Reads newline-delimited JSON from landing zone.
# 2000 transaction records across 200 customers.
# Adds ingestion metadata + partition columns.
# Writes as Delta to bronze/transaction_feed/

from pyspark.sql.functions import (
    current_timestamp, lit,
    year, month, dayofmonth
)

# Read JSON from landing
# multiLine=False because each line is one JSON record
df_transactions = spark.read.format("json") \
    .option("multiLine", "false") \
    .load(f"{landing}/transaction_feed/transaction_feed.json")

# Add ingestion metadata + partition columns
df_transactions = df_transactions \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("transaction_feed.json")) \
    .withColumn("pipeline", lit("databricks_streaming_sim")) \
    .withColumn("ingest_year",  year(current_timestamp())) \
    .withColumn("ingest_month", month(current_timestamp())) \
    .withColumn("ingest_day",   dayofmonth(current_timestamp()))

# Write to bronze as Delta
df_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("ingest_year", "ingest_month", "ingest_day") \
    .save(f"{bronze}/transaction_feed/")

row_count = df_transactions.count()

print(f"✅ Transaction Feed → Bronze complete.")
print(f"   Rows written  : {row_count}")
print(f"   Format        : Delta")
print(f"   Partition     : ingest_year / ingest_month / ingest_day")
print(f"   Path          : bronze/transaction_feed/")

# COMMAND ----------

# ── Cell 5: Bronze — KYC Documents JSON → Delta ─────────────────────
# Reads JSON array from landing zone.
# 200 KYC document records — one per customer.
# doc_status partition enables fast filtering in Gold layer.

from pyspark.sql.functions import (
    current_timestamp, lit,
    year, month, dayofmonth
)

# Read JSON — multiLine=True because entire file is one JSON array
df_kyc = spark.read.format("json") \
    .option("multiLine", "true") \
    .load(f"{landing}/kyc_documents/kyc_documents.json")

# Add ingestion metadata + partition columns
df_kyc = df_kyc \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("kyc_documents.json")) \
    .withColumn("pipeline", lit("databricks_timer_sim")) \
    .withColumn("ingest_year",  year(current_timestamp())) \
    .withColumn("ingest_month", month(current_timestamp())) \
    .withColumn("ingest_day",   dayofmonth(current_timestamp()))

# Write to bronze as Delta
df_kyc.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("ingest_year", "ingest_month", "ingest_day") \
    .save(f"{bronze}/kyc_documents/")

row_count = df_kyc.count()

print(f"✅ KYC Documents → Bronze complete.")
print(f"   Rows written  : {row_count}")
print(f"   Format        : Delta")
print(f"   Partition     : ingest_year / ingest_month / ingest_day")
print(f"   Path          : bronze/kyc_documents/")

# COMMAND ----------

# ── Cell 6: Bronze — Watchlist/Sanctions CSV → Delta ────────────────
# Reads sanctions list CSV from landing zone.
# 50 watchlist entities — used for AML name matching in Gold layer.

from pyspark.sql.functions import (
    current_timestamp, lit,
    year, month, dayofmonth
)

# Read CSV from landing
df_watchlist = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{landing}/watchlist_sanctions/watchlist_sanctions.csv")

# Add ingestion metadata + partition columns
df_watchlist = df_watchlist \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("watchlist_sanctions.csv")) \
    .withColumn("pipeline", lit("databricks_batch")) \
    .withColumn("ingest_year",  year(current_timestamp())) \
    .withColumn("ingest_month", month(current_timestamp())) \
    .withColumn("ingest_day",   dayofmonth(current_timestamp()))

# Write to bronze as Delta
df_watchlist.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("ingest_year", "ingest_month", "ingest_day") \
    .save(f"{bronze}/watchlist_sanctions/")

row_count = df_watchlist.count()

print(f"✅ Watchlist/Sanctions → Bronze complete.")
print(f"   Rows written  : {row_count}")
print(f"   Format        : Delta")
print(f"   Partition     : ingest_year / ingest_month / ingest_day")
print(f"   Path          : bronze/watchlist_sanctions/")

# COMMAND ----------

# ── Cell 7: Bronze — Account Activity Parquet → Delta (Autoloader) ──
# Autoloader monitors landing/account_activity/ continuously.
# Picks up new Parquet files as they arrive — hourly micro-batch.
# This is the correct tool for streaming file ingestion in Databricks.
# cloudFiles format = Databricks Autoloader.

from pyspark.sql.functions import (
    current_timestamp, lit,
    year, month, dayofmonth
)

# storage_account_key = dbutils.secrets.get(
#     scope="kyc360-scope",
#     key="adls-storage-key"
# )
# spark.conf.set(
#     "fs.azure.account.key.adlskyc360.dfs.core.windows.net",
#     storage_account_key
# )

checkpoint_path = f"{bronze}/_checkpoints/account_activity"

# Real Autoloader — cloudFiles format
# Monitors landing/account_activity/ continuously
# Picks up new Parquet files as they arrive
df_stream = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", checkpoint_path) \
    .load(f"{landing}/account_activity/")

from pyspark.sql.functions import current_timestamp, lit, year, month, dayofmonth

df_stream = df_stream \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("pipeline", lit("databricks_autoloader")) \
    .withColumn("ingest_year", year(current_timestamp())) \
    .withColumn("ingest_month", month(current_timestamp())) \
    .withColumn("ingest_day", dayofmonth(current_timestamp()))

query = df_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .partitionBy("ingest_year", "ingest_month", "ingest_day") \
    .trigger(availableNow=True) \
    .start(f"{bronze}/account_activity/")

query.awaitTermination()

print(f"✅ Account Activity → Bronze complete via Autoloader.")
print(f"   Format    : Delta")
print(f"   Partition : ingest_year / ingest_month / ingest_day")
print(f"   Path      : bronze/account_activity/")

# COMMAND ----------

# ── Cell 8: Verify all 5 Bronze Delta tables ────────────────────────
# Confirm all tables written correctly.
# Show row counts and schema for each.
# This is your screenshot for GitHub.

# storage_account_key = dbutils.secrets.get(
#     scope="kyc360-scope",
#     key="adls-storage-key"
# )
# spark.conf.set(
#     "fs.azure.account.key.adlskyc360.dfs.core.windows.net",
#     storage_account_key
# )

print("=== BRONZE LAYER SUMMARY ===\n")

bronze_tables = {
    "customer_onboarding" : f"{bronze}/customer_onboarding/",
    "transaction_feed"    : f"{bronze}/transaction_feed/",
    "kyc_documents"       : f"{bronze}/kyc_documents/",
    "watchlist_sanctions" : f"{bronze}/watchlist_sanctions/",
    "account_activity"    : f"{bronze}/account_activity/",
}

total_rows = 0

for table_name, path in bronze_tables.items():
    try:
        df = spark.read.format("delta").load(path)
        count = df.count()
        total_rows += count
        print(f"✅ {table_name}")
        print(f"   Rows    : {count}")
        print(f"   Columns : {len(df.columns)}")
        print(f"   Path    : {path}")
        print()
    except Exception as e:
        print(f"❌ {table_name} — ERROR: {str(e)}")
        print()

print(f"{'='*40}")
print(f"Total rows across all Bronze tables: {total_rows}")
print(f"All tables in Delta format. Ready for Silver layer.")

# COMMAND ----------

# ── Cell 9: Show sample rows from each Bronze table ─────────────────
# Shows data actually landed correctly with correct columns.

storage_account_key = dbutils.secrets.get(
    scope="kyc360-scope",
    key="adls-storage-key"
)
spark.conf.set(
    "fs.azure.account.key.adlskyc360.dfs.core.windows.net",
    storage_account_key
)

print("=== SAMPLE DATA PREVIEW ===\n")

bronze_tables = {
    "customer_onboarding" : f"{bronze}/customer_onboarding/",
    "transaction_feed"    : f"{bronze}/transaction_feed/",
    "kyc_documents"       : f"{bronze}/kyc_documents/",
    "watchlist_sanctions" : f"{bronze}/watchlist_sanctions/",
    "account_activity"    : f"{bronze}/account_activity/",
}

for table_name, path in bronze_tables.items():
    print(f"── {table_name} ──")
    df = spark.read.format("delta").load(path)
    df.show(2, truncate=True)
    print()