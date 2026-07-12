# Databricks notebook source
# ── Cell 1: Configure ADLS Gen2 Access ──────────────────────────────

# storage_account_name = "adlskyc360"
# storage_account_key  = ""

storage_account_name = "adlskyc360"

# Read key securely from Key Vault
storage_account_key = dbutils.secrets.get(
    scope="kyc360-scope",
    key="adls-storage-key"
)

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

silver  = f"abfss://silver@{storage_account_name}.dfs.core.windows.net"
gold    = f"abfss://gold@{storage_account_name}.dfs.core.windows.net"

print("✅ ADLS Gen2 access configured.")
print(f"   Silver : {silver}")
print(f"   Gold   : {gold}")

# COMMAND ----------

# ── Cell 2: Read All Silver Tables ──────────────────────────────────
# Read all 5 cleaned Silver tables.
# These are the inputs for all Gold layer calculations.

from pyspark.sql.functions import col

storage_account_name = "adlskyc360"

# Read key securely from Key Vault
storage_account_key = dbutils.secrets.get(
    scope="kyc360-scope",
    key="adls-storage-key"
)

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

df_customers  = spark.read.format("delta").load(f"{silver}/customer_onboarding/")
df_txns       = spark.read.format("delta").load(f"{silver}/transaction_feed/")
df_kyc        = spark.read.format("delta").load(f"{silver}/kyc_documents/")
df_watchlist  = spark.read.format("delta").load(f"{silver}/watchlist_sanctions/")
df_activity   = spark.read.format("delta").load(f"{silver}/account_activity/")

# Filter customers — current records only (SCD2)
# df_customers = df_customers.filter("is_current = true")
df_customers = df_customers.filter(col("is_current") == True)

print("✅ All Silver tables loaded.")
print(f"   Customers (current) : {df_customers.count()}")
print(f"   Transactions        : {df_txns.count()}")
print(f"   KYC Documents       : {df_kyc.count()}")
print(f"   Watchlist           : {df_watchlist.count()}")
print(f"   Account Activity    : {df_activity.count()}")

# COMMAND ----------

# ── Cell 3: AML Flag ─────────────────────────────────────────────────
# Why: Anti-Money Laundering screening.
# Match customer full_name against watchlist entity_name.
# If match found → aml_flag = True.
# This simulates OFAC/UN sanctions screening done by compliance teams.
# Name matching uses exact match here.
# In production → fuzzy matching (Levenshtein distance).

storage_account_name = "adlskyc360"

# Read key securely from Key Vault
storage_account_key = dbutils.secrets.get(
    scope="kyc360-scope",
    key="adls-storage-key"
)

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

from pyspark.sql.functions import (
    col, when, lit, countDistinct,
    sum as spark_sum, count, max as spark_max
)

# Get distinct watchlist names
watchlist_names = [
    row.entity_name 
    for row in df_watchlist.select("entity_name").distinct().collect()
]

# Flag customers whose name appears on watchlist
df_aml = df_customers.withColumn(
    "aml_flag",
    when(col("full_name").isin(watchlist_names), True).otherwise(False)
).select("customer_id", "full_name", "country", "pep_flag", "aml_flag")

aml_hits = df_aml.filter("aml_flag = true").count()

print(f"✅ AML Screening complete.")
print(f"   Total customers screened : {df_aml.count()}")
print(f"   AML flag hits            : {aml_hits}")
print(f"   Clean customers          : {df_aml.count() - aml_hits}")

df_aml.filter("aml_flag = true") \
    .select("customer_id", "full_name", "aml_flag") \
    .show(5, truncate=False)

# COMMAND ----------

# ── Cell 4: Transaction Velocity Score ──────────────────────────────
# Why: High transaction velocity = higher KYC risk.
# We calculate per customer in last 30 days:
#   - txn_count     : number of transactions
#   - txn_total_amt : total amount transacted
#   - cross_border_count : number of cross-border transactions
# Velocity thresholds:
#   HIGH   → txn_count > 20 OR txn_total_amt > 1,000,000
#   MEDIUM → txn_count > 10 OR txn_total_amt > 500,000
#   LOW    → everything else

storage_account_name = "adlskyc360"

# Read key securely from Key Vault
storage_account_key = dbutils.secrets.get(
    scope="kyc360-scope",
    key="adls-storage-key"
)

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

from pyspark.sql.functions import (
    col, when, count, sum as spark_sum,
    round as spark_round
)

# Aggregate transactions per customer
df_txn_velocity = df_txns \
    .filter(col("status") == "SUCCESS") \
    .groupBy("customer_id") \
    .agg(
        count("transaction_id").alias("txn_count"),
        spark_round(spark_sum("amount"), 2).alias("txn_total_amt"),
        spark_sum(
            when(col("cross_border_flag") == "Y", 1).otherwise(0)
        ).alias("cross_border_count")
    )

# Assign velocity risk level
df_txn_velocity = df_txn_velocity.withColumn(
    "velocity_risk",
    when(
        (col("txn_count") > 20) | (col("txn_total_amt") > 1000000),
        "HIGH"
    ).when(
        (col("txn_count") > 10) | (col("txn_total_amt") > 500000),
        "MEDIUM"
    ).otherwise("LOW")
)

print(f"✅ Transaction Velocity Score complete.")
print(f"   Customers scored : {df_txn_velocity.count()}")
df_txn_velocity.groupBy("velocity_risk").count().show()

# COMMAND ----------

# ── Cell 5: KYC Risk Score ───────────────────────────────────────────
# Why: Final KYC risk score per customer.
# Combines 4 signals:
#   1. Transaction velocity (Cell 4)
#   2. KYC document status (EXPIRED/PENDING = higher risk)
#   3. Account dormancy (dormant = higher risk)
#   4. PEP flag (Politically Exposed Person = higher risk)
#
# Scoring logic:
#   Start with score = 0
#   +3 → velocity_risk = HIGH
#   +2 → velocity_risk = MEDIUM
#   +3 → doc_status = EXPIRED
#   +2 → doc_status = PENDING
#   +2 → dormant_flag = Y
#   +3 → pep_flag = Y
#
# Final score:
#   >= 6 → HIGH
#   >= 3 → MEDIUM
#   < 3  → LOW

storage_account_name = "adlskyc360"

# Read key securely from Key Vault
storage_account_key = dbutils.secrets.get(
    scope="kyc360-scope",
    key="adls-storage-key"
)

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

from pyspark.sql.functions import (
    col, when, lit
)

# Join all signals
df_risk = df_customers \
    .join(df_txn_velocity, "customer_id", "left") \
    .join(
        df_kyc.select("customer_id", "doc_status", "expiry_date"),
        "customer_id", "left"
    ) \
    .join(
        df_activity.select("customer_id", "dormant_flag", "days_since_activity", "balance_tier"),
        "customer_id", "left"
    ) \
    .join(
        df_aml.select("customer_id", "aml_flag"),
        "customer_id", "left"
    )

# Calculate risk score
df_risk = df_risk.withColumn(
    "risk_score",
    (
        when(col("velocity_risk") == "HIGH",    lit(3)).otherwise(
        when(col("velocity_risk") == "MEDIUM",  lit(2)).otherwise(lit(0)))
        +
        when(col("doc_status") == "EXPIRED",    lit(3)).otherwise(
        when(col("doc_status") == "PENDING",    lit(2)).otherwise(lit(0)))
        +
        when(col("dormant_flag") == "Y",        lit(2)).otherwise(lit(0))
        +
        when(col("pep_flag") == "Y",            lit(3)).otherwise(lit(0))
    )
)

# Assign final KYC risk label
df_risk = df_risk.withColumn(
    "kyc_risk_level",
    when(col("risk_score") >= 6, "HIGH") \
    .when(col("risk_score") >= 3, "MEDIUM") \
    .otherwise("LOW")
)

print(f"✅ KYC Risk Score complete.")
df_risk.groupBy("kyc_risk_level").count().orderBy("kyc_risk_level").show()

# COMMAND ----------

# ── Cell 6: Dormancy Alert + High Risk Profile ───────────────────────
# Dormancy Alert:
#   Customer has no activity > 365 days AND KYC doc is EXPIRED.
#   Bank must contact customer or freeze account per RBI guidelines.
#
# High Risk Profile:
#   Customer has ALL THREE of:
#   - PEP flag = Y
#   - Cross-border transactions > 0
#   - AML flag = True (name on sanctions list)
#   These customers require enhanced due diligence (EDD).

storage_account_name = "adlskyc360"

# Read key securely from Key Vault
storage_account_key = dbutils.secrets.get(
    scope="kyc360-scope",
    key="adls-storage-key"
)

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

from pyspark.sql.functions import col, when, lit

# Dormancy Alert
df_risk = df_risk.withColumn(
    "dormancy_alert",
    when(
        (col("dormant_flag") == "Y") & (col("doc_status") == "EXPIRED"),
        True
    ).otherwise(False)
)

# High Risk Profile
df_risk = df_risk.withColumn(
    "high_risk_profile",
    when(
        (col("pep_flag") == "Y") &
        (col("cross_border_count") > 0) &
        (col("aml_flag") == True),
        True
    ).otherwise(False)
)

dormancy_count   = df_risk.filter("dormancy_alert = true").count()
high_risk_count  = df_risk.filter("high_risk_profile = true").count()

print(f"✅ Dormancy Alert + High Risk Profile complete.")
print(f"   Dormancy alerts    : {dormancy_count}")
print(f"   High risk profiles : {high_risk_count}")

df_risk.filter("dormancy_alert = true") \
    .select("customer_id", "dormant_flag", "doc_status", "dormancy_alert") \
    .show(5, truncate=False)

# COMMAND ----------

# ── Cell 7: Customer 360 Unified View ───────────────────────────────
# Why: Single unified view of each customer across all 5 sources.
# Joins all signals into one Gold table.
# This is what Synapse and Power BI will query.
# Contains: identity + risk score + AML + dormancy + high risk profile.

storage_account_key = dbutils.secrets.get(
    scope="kyc360-scope",
    key="adls-storage-key"
)
spark.conf.set(
    "fs.azure.account.key.adlskyc360.dfs.core.windows.net",
    storage_account_key
)

from pyspark.sql.functions import current_timestamp

# Build Customer 360
df_customer_360 = df_risk.select(
    # Identity
    "customer_id",
    "full_name",
    "country",
    "customer_segment",
    "pep_flag",
    # KYC
    "doc_status",
    "expiry_date",
    # Transactions
    "txn_count",
    "txn_total_amt",
    "cross_border_count",
    "velocity_risk",
    # Account
    "dormant_flag",
    "days_since_activity",
    "balance_tier",
    # Risk outputs
    "risk_score",
    "kyc_risk_level",
    "aml_flag",
    "dormancy_alert",
    "high_risk_profile",
    # SCD2
    "effective_from",
    "effective_to",
    "is_current"
).withColumn("gold_timestamp", current_timestamp())

# Write to Gold as Delta
df_customer_360.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{gold}/customer_360/")

print(f"✅ Customer 360 → Gold complete.")
print(f"   Rows    : {df_customer_360.count()}")
print(f"   Columns : {len(df_customer_360.columns)}")
print()
df_customer_360.select(
    "customer_id", "full_name", "kyc_risk_level",
    "aml_flag", "dormancy_alert", "high_risk_profile"
).show(10, truncate=False)

# COMMAND ----------

# ── Cell 8: Verify Gold Layer ────────────────────────────────────────
# Final verification of Gold layer.
# Take screenshot of this output for GitHub.

storage_account_key = dbutils.secrets.get(
    scope="kyc360-scope",
    key="adls-storage-key"
)
spark.conf.set(
    "fs.azure.account.key.adlskyc360.dfs.core.windows.net",
    storage_account_key
)
print("=== GOLD LAYER SUMMARY ===\n")

df_gold = spark.read.format("delta").load(f"{gold}/customer_360/")

total       = df_gold.count()
high_risk   = df_gold.filter("kyc_risk_level = 'HIGH'").count()
medium_risk = df_gold.filter("kyc_risk_level = 'MEDIUM'").count()
low_risk    = df_gold.filter("kyc_risk_level = 'LOW'").count()
aml_flagged = df_gold.filter("aml_flag = true").count()
dormant     = df_gold.filter("dormancy_alert = true").count()
high_risk_p = df_gold.filter("high_risk_profile = true").count()

print(f"✅ Customer 360 Gold Table")
print(f"   Total customers    : {total}")
print(f"")
print(f"   KYC Risk Levels:")
print(f"   HIGH               : {high_risk}")
print(f"   MEDIUM             : {medium_risk}")
print(f"   LOW                : {low_risk}")
print(f"")
print(f"   Alerts:")
print(f"   AML flagged        : {aml_flagged}")
print(f"   Dormancy alerts    : {dormant}")
print(f"   High risk profiles : {high_risk_p}")
print(f"")
print(f"{'='*40}")
print(f"Gold layer complete. Ready for Synapse.")