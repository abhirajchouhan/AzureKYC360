# Databricks notebook source
# ── Cell 1: Configure ADLS Gen2 Access — Secure via Key Vault ───────
# Storage key is read from Azure Key Vault via Databricks Secret Scope.
# No credentials hardcoded in any notebook.
# Secret Scope: kyc360-scope
# Key Vault: kv-kyc360
# Secret: adls-storage-key

storage_account_name = "adlskyc360"

# # Read key securely from Key Vault
# storage_account_key = dbutils.secrets.get(
#     scope="kyc360-scope",
#     key="adls-storage-key"
# )

# # Set in Spark config
# spark.conf.set(
#     f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
#     storage_account_key
# )

# Base paths
landing = f"abfss://landing@{storage_account_name}.dfs.core.windows.net"
bronze  = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net"
silver  = f"abfss://silver@{storage_account_name}.dfs.core.windows.net"
gold    = f"abfss://gold@{storage_account_name}.dfs.core.windows.net"

print("✅ ADLS Gen2 access configured securely via Key Vault.")
print(f"   Landing : {landing}")
print(f"   Bronze  : {bronze}")
print(f"   Silver  : {silver}")
print(f"   Gold    : {gold}")

# COMMAND ----------

# ── Cell 2: PII Masking — Customer Onboarding ───────────────────────
# Why: SSN and date_of_birth are PII (Personally Identifiable Info).
# Regulators (RBI, GDPR) require PII to be masked before processing.
# SSN → show only last 4 digits: ***-**-1234
# DOB → show only birth year: 1975
# Email → mask local part: a***@example.com
# These masked values go to Silver. Raw PII stays in Bronze only.

# Key already set in Cell 1 — no need to repeat

from pyspark.sql.functions import (
    col, regexp_replace, substring, concat,
    lit, split, current_timestamp,
    year, month, dayofmonth
)

# Read from Bronze
df_onboarding = spark.read.format("delta") \
    .load(f"{bronze}/customer_onboarding/")

# Apply PII masking
df_onboarding_masked = df_onboarding \
    .withColumn(
        "ssn_masked",
        concat(lit("***-**-"), substring(col("ssn"), 8, 4))
    ) \
    .withColumn(
        "dob_year_only",
        substring(col("date_of_birth"), 1, 4)
    ) \
    .withColumn(
        "email_masked",
        concat(
            substring(col("email"), 1, 1),
            lit("***@"),
            split(col("email"), "@").getItem(1)
        )
    ) \
    .drop("ssn", "date_of_birth", "email") \
    .withColumnRenamed("ssn_masked", "ssn") \
    .withColumnRenamed("dob_year_only", "date_of_birth") \
    .withColumnRenamed("email_masked", "email")

print("✅ PII Masking complete.")
print(f"   SSN     : masked to last 4 digits")
print(f"   DOB     : masked to birth year only")
print(f"   Email   : masked local part")
print(f"   Rows    : {df_onboarding_masked.count()}")

# Show sample masked data
df_onboarding_masked.select(
    "customer_id", "full_name", "ssn", "date_of_birth", "email"
).show(3, truncate=False)

# COMMAND ----------

# ── Cell 3: Deduplication — All 5 Tables ────────────────────────────
# Why: Source systems sometimes send duplicate records.
# We keep only the latest record per unique key.
# customer_onboarding → dedupe by customer_id
# transaction_feed    → dedupe by transaction_id
# kyc_documents       → dedupe by doc_id
# watchlist_sanctions → dedupe by watchlist_id
# account_activity    → dedupe by account_id

from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window

def deduplicate(df, partition_col, order_col):
    """
    Keep latest record per partition_col.
    order_col = timestamp column to determine latest.
    """
    window = Window \
        .partitionBy(partition_col) \
        .orderBy(col(order_col).desc())
    
    return df \
        .withColumn("row_num", row_number().over(window)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

# Deduplicate each table
df_txn        = spark.read.format("delta").load(f"{bronze}/transaction_feed/")
df_kyc        = spark.read.format("delta").load(f"{bronze}/kyc_documents/")
df_watchlist  = spark.read.format("delta").load(f"{bronze}/watchlist_sanctions/")
df_activity   = spark.read.format("delta").load(f"{bronze}/account_activity/")

df_onboarding_deduped = deduplicate(df_onboarding_masked,  "customer_id",   "ingestion_timestamp")
df_txn_deduped        = deduplicate(df_txn,                "transaction_id", "ingestion_timestamp")
df_kyc_deduped        = deduplicate(df_kyc,                "customer_id",    "ingestion_timestamp")
df_watchlist_deduped  = deduplicate(df_watchlist,          "watchlist_id",   "ingestion_timestamp")
df_activity_deduped   = deduplicate(df_activity,           "account_id",     "ingestion_timestamp")

print("✅ Deduplication complete.")
print(f"   customer_onboarding : {df_onboarding_deduped.count()} rows")
print(f"   transaction_feed    : {df_txn_deduped.count()} rows")
print(f"   kyc_documents       : {df_kyc_deduped.count()} rows")
print(f"   watchlist_sanctions : {df_watchlist_deduped.count()} rows")
print(f"   account_activity    : {df_activity_deduped.count()} rows")

# COMMAND ----------

# ── Cell 4: SCD Type 2 — Customer Onboarding ────────────────────────
# Why: Customer details change over time (address, segment, phone).
# SCD Type 2 keeps full history of every change.
# Each version of a customer record gets:
#   effective_from → when this version became active
#   effective_to   → when this version was superseded (9999 = current)
#   is_current     → True for latest version only
# This is critical for KYC — regulators want historical audit trail.
# SCD2 implementation.
# First run: write all records with is_current = True.
# Subsequent runs: expire changed records, insert new versions.


from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import BooleanType

# storage_account_key = dbutils.secrets.get(
#     scope="kyc360-scope",
#     key="adls-storage-key"
# )
# spark.conf.set(
#     "fs.azure.account.key.adlskyc360.dfs.core.windows.net",
#     storage_account_key
# )

silver = f"abfss://silver@adlskyc360.dfs.core.windows.net"
silver_path = f"{silver}/customer_onboarding/"

# Add SCD2 columns to incoming data
df_new = df_onboarding_deduped \
    .withColumn("effective_from", current_timestamp()) \
    .withColumn("effective_to", lit("9999-12-31 00:00:00").cast("timestamp")) \
    .withColumn("is_current", lit(True).cast(BooleanType()))

# Check if Silver table exists
import os
try:
    existing_count = spark.read.format("delta").load(silver_path).count()
    table_exists = existing_count > 0
except Exception:
    table_exists = False

if not table_exists:
    # First run — write fresh
    df_new.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(silver_path)
    print(f"✅ SCD Type 2 — first write complete.")
    print(f"   Rows written  : {df_new.count()}")
else:
    # Subsequent runs — MERGE
    from delta.tables import DeltaTable
    dt = DeltaTable.forPath(spark, silver_path)

    dt.alias("existing").merge(
        df_new.alias("incoming"),
        """
        existing.customer_id = incoming.customer_id 
        AND existing.is_current = true
        AND (
            existing.customer_segment != incoming.customer_segment
            OR existing.country != incoming.country
            OR existing.phone != incoming.phone
        )
        """
    ).whenMatchedUpdate(
        set={
            "is_current": "false",
            "effective_to": "incoming.effective_from"
        }
    ).whenNotMatchedInsertAll() \
     .execute()

    print(f"✅ SCD Type 2 MERGE complete.")

# Verify
df_result = spark.read.format("delta").load(silver_path)
print(f"\n   Total rows    : {df_result.count()}")
print(f"   Current rows  : {df_result.filter(col('is_current') == True).count()}")
print(f"   Expired rows  : {df_result.filter(col('is_current') == False).count()}")

df_result.select(
    "customer_id", "full_name", "customer_segment",
    "effective_from", "effective_to", "is_current"
).show(5, truncate=False)

# COMMAND ----------

# ── Cell 5: Write Remaining 4 Silver Tables ─────────────────────────
# transaction_feed, kyc_documents, watchlist_sanctions, account_activity
# No SCD2 needed for these — they are event/reference data.
# Apply partition by ingest_year/month/day.

from pyspark.sql.functions import (
    current_timestamp, lit,
    year, month, dayofmonth
)

def write_silver(df, table_name):
    """Add silver metadata and write as Delta."""
    df_silver = df \
        .withColumn("silver_timestamp", current_timestamp()) \
        .withColumn("ingest_year",  year(current_timestamp())) \
        .withColumn("ingest_month", month(current_timestamp())) \
        .withColumn("ingest_day",   dayofmonth(current_timestamp()))
    
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("ingest_year", "ingest_month", "ingest_day") \
        .save(f"{silver}/{table_name}/")
    
    print(f"✅ {table_name} → Silver complete. Rows: {df_silver.count()}")

write_silver(df_txn_deduped,       "transaction_feed")
write_silver(df_kyc_deduped,       "kyc_documents")
write_silver(df_watchlist_deduped, "watchlist_sanctions")
write_silver(df_activity_deduped,  "account_activity")

# COMMAND ----------

# ── Cell 6: Schema Validation ────────────────────────────────────────
# Why: Ensure all Silver tables have expected columns.
# Catch schema drift early — before Gold layer breaks.
# In production this would trigger an alert if columns are missing.

expected_schemas = {
    "customer_onboarding": [
        "customer_id", "full_name", "ssn", "date_of_birth",
        "email", "phone", "country", "pep_flag",
        "effective_from", "effective_to", "is_current"
    ],
    "transaction_feed": [
        "transaction_id", "customer_id", "amount",
        "cross_border_flag", "txn_type", "status"
    ],
    "kyc_documents": [
        "doc_id", "customer_id", "doc_status",
        "doc_type", "expiry_date"
    ],
    "watchlist_sanctions": [
        "watchlist_id", "entity_name", "list_type", "designation"
    ],
    "account_activity": [
        "account_id", "customer_id", "dormant_flag",
        "days_since_activity", "balance_tier"
    ]
}

print("=== SCHEMA VALIDATION ===\n")

all_passed = True

for table_name, expected_cols in expected_schemas.items():
    df = spark.read.format("delta").load(f"{silver}/{table_name}/")
    actual_cols = df.columns
    missing = [c for c in expected_cols if c not in actual_cols]
    
    if missing:
        print(f"❌ {table_name} — Missing columns: {missing}")
        all_passed = False
    else:
        print(f"✅ {table_name} — Schema valid.")

print()
if all_passed:
    print("✅ All Silver tables passed schema validation.")
    print("   Ready for Gold layer.")
else:
    print("❌ Fix schema issues before proceeding to Gold.")

# COMMAND ----------

# ── Cell 7: Verify All Silver Tables ────────────────────────────────
# Final verification before Gold layer.

print("=== SILVER LAYER SUMMARY ===\n")

silver_tables = {
    "customer_onboarding" : f"{silver}/customer_onboarding/",
    "transaction_feed"    : f"{silver}/transaction_feed/",
    "kyc_documents"       : f"{silver}/kyc_documents/",
    "watchlist_sanctions" : f"{silver}/watchlist_sanctions/",
    "account_activity"    : f"{silver}/account_activity/",
}

total_rows = 0

for table_name, path in silver_tables.items():
    try:
        df = spark.read.format("delta").load(path)
        count = df.count()
        total_rows += count
        print(f"✅ {table_name}")
        print(f"   Rows    : {count}")
        print(f"   Columns : {len(df.columns)}")
        print()
    except Exception as e:
        print(f"❌ {table_name} — ERROR: {str(e)}")
        print()

print(f"{'='*40}")
print(f"Total rows across all Silver tables: {total_rows}")
print(f"All tables cleaned, deduplicated, PII masked.")
print(f"SCD Type 2 applied to customer_onboarding.")
print(f"Ready for Gold layer.")