"""
AzureKYC360 — Synthetic Data Generator
Generates all 5 source datasets for the KYC360 portfolio project.
Run once. Outputs go to ./sample_data/ folder.
"""

import os
import json
import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)
Faker.seed(42)

os.makedirs("sample_data", exist_ok=True)

# ── Shared customer IDs ──────────────────────────────────────────────
# All 5 sources share the same 200 customer IDs.
# This is critical — the Gold layer joins across all 5 sources.
CUSTOMER_IDS = [f"CUST{str(i).zfill(5)}" for i in range(1, 201)]

# ── Watchlist names — used in Source 4 and matched in Source 1 ──────
# 10 of our 200 customers will have names that appear on the watchlist.
# This creates realistic AML flag hits in the Gold layer.
WATCHLIST_NAMES = [fake.name() for _ in range(50)]
FLAGGED_CUSTOMERS = random.sample(CUSTOMER_IDS, 10)

print("Generating all 5 data sources...\n")

# ════════════════════════════════════════════════════════════════════
# SOURCE 1 — Customer Onboarding
# Format : CSV
# Schedule: Daily batch via ADF
# Why     : Master record for each customer. Contains PII.
#           PII (SSN, DOB) will be masked in Silver layer.
#           Name is checked against watchlist in Gold (AML Flag).
# ════════════════════════════════════════════════════════════════════
print("1/5 — Customer Onboarding CSV...")

onboarding_rows = []
for cid in CUSTOMER_IDS:
    is_flagged = cid in FLAGGED_CUSTOMERS
    onboarding_rows.append({
        "customer_id":       cid,
        "full_name":         random.choice(WATCHLIST_NAMES) if is_flagged else fake.name(),
        "date_of_birth":     fake.date_of_birth(minimum_age=18, maximum_age=80).strftime("%Y-%m-%d"),
        "ssn":               fake.ssn(),                    # PII — will be masked in Silver
        "email":             fake.email(),
        "phone":             fake.phone_number(),
        "address":           fake.address().replace("\n", ", "),
        "country":           random.choice(["IN", "US", "GB", "AE", "SG", "CN", "NG", "RU"]),
        "onboarding_date":   fake.date_between(start_date="-5y", end_date="today").strftime("%Y-%m-%d"),
        "customer_segment":  random.choice(["Retail", "HNI", "Corporate", "SME"]),
        "pep_flag":          random.choice(["Y", "N", "N", "N", "N"]),  # ~20% PEP
        "source_system":     "ONBOARDING_PORTAL",
        "record_date":       datetime.today().strftime("%Y-%m-%d"),
    })

df1 = pd.DataFrame(onboarding_rows)
df1.to_csv("sample_data/customer_onboarding.csv", index=False)
print(f"   Saved: sample_data/customer_onboarding.csv ({len(df1)} rows)")


# ════════════════════════════════════════════════════════════════════
# SOURCE 2 — Transaction Feed
# Format : JSON (one record per line = newline-delimited JSON)
# Schedule: Real-time streaming via Azure Function + Event Grid
# Why     : Transaction velocity (count + amount in 30 days) drives
#           the KYC Risk Score in the Gold layer.
#           Cross-border transactions trigger High Risk Profile.
# ════════════════════════════════════════════════════════════════════
print("2/5 — Transaction Feed JSON...")

transactions = []
for _ in range(2000):   # 2000 transactions across 200 customers
    cid = random.choice(CUSTOMER_IDS)
    txn_country = random.choice(["IN", "US", "GB", "AE", "SG", "CN", "NG", "RU"])
    transactions.append({
        "transaction_id":   f"TXN{fake.unique.random_int(min=100000, max=999999)}",
        "customer_id":      cid,
        "txn_date":         fake.date_time_between(start_date="-30d", end_date="now").strftime("%Y-%m-%dT%H:%M:%S"),
        "amount":           round(random.uniform(100, 500000), 2),
        "currency":         random.choice(["INR", "USD", "GBP", "AED"]),
        "txn_type":         random.choice(["DEBIT", "CREDIT", "TRANSFER", "WITHDRAWAL"]),
        "channel":          random.choice(["NETBANKING", "MOBILE", "BRANCH", "ATM", "SWIFT"]),
        "beneficiary_country": txn_country,
        "cross_border_flag":   "Y" if txn_country != "IN" else "N",
        "status":           random.choice(["SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "PENDING"]),
        "source_system":    "TRANSACTION_ENGINE",
    })

with open("sample_data/transaction_feed.json", "w") as f:
    for t in transactions:
        f.write(json.dumps(t) + "\n")
print(f"   Saved: sample_data/transaction_feed.json ({len(transactions)} records)")


# ════════════════════════════════════════════════════════════════════
# SOURCE 3 — KYC Document Verification
# Format : JSON
# Schedule: Every 6 hours via Azure Function (timer trigger)
# Why     : Document status (VERIFIED / PENDING / EXPIRED) is a key
#           input to KYC Risk Score. Expired docs + no activity =
#           Dormancy Alert in the Gold layer.
# ════════════════════════════════════════════════════════════════════
print("3/5 — KYC Document Verification JSON...")

kyc_docs = []
for cid in CUSTOMER_IDS:
    verification_date = fake.date_between(start_date="-2y", end_date="today")
    expiry_date = verification_date + timedelta(days=random.choice([365, 730, 1095]))
    status = random.choice(["VERIFIED", "VERIFIED", "VERIFIED", "PENDING", "EXPIRED"])
    kyc_docs.append({
        "doc_id":             f"DOC{fake.unique.random_int(min=10000, max=99999)}",
        "customer_id":        cid,
        "doc_type":           random.choice(["PASSPORT", "AADHAAR", "PAN", "DRIVING_LICENSE"]),
        "doc_status":         status,
        "verification_date":  verification_date.strftime("%Y-%m-%d"),
        "expiry_date":        expiry_date.strftime("%Y-%m-%d"),
        "verified_by":        random.choice(["MANUAL_REVIEW", "AUTO_OCR", "THIRD_PARTY_API"]),
        "risk_notes":         "High value customer" if status == "EXPIRED" else "",
        "source_system":      "KYC_PORTAL",
        "batch_timestamp":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    })

with open("sample_data/kyc_documents.json", "w") as f:
    json.dump(kyc_docs, f, indent=2)
print(f"   Saved: sample_data/kyc_documents.json ({len(kyc_docs)} records)")


# ════════════════════════════════════════════════════════════════════
# SOURCE 4 — Watchlist / Sanctions Feed
# Format : CSV
# Schedule: Daily batch via ADF
# Why     : Customer names from Source 1 are matched against this
#           list in the Gold layer. A match sets AML Flag = TRUE.
#           This simulates OFAC / UN sanctions list screening.
# ════════════════════════════════════════════════════════════════════
print("4/5 — Watchlist / Sanctions Feed CSV...")

watchlist_rows = []
for i, name in enumerate(WATCHLIST_NAMES):
    watchlist_rows.append({
        "watchlist_id":     f"WL{str(i+1).zfill(4)}",
        "entity_name":      name,
        "alias":            fake.name() if random.random() > 0.7 else "",
        "list_type":        random.choice(["OFAC_SDN", "UN_SANCTIONS", "EU_SANCTIONS", "RBI_CAUTION"]),
        "country":          random.choice(["IN", "US", "RU", "CN", "NG", "IR", "KP"]),
        "designation":      random.choice(["TERRORIST", "ARMS_DEALER", "MONEY_LAUNDERER", "PEP"]),
        "listed_date":      fake.date_between(start_date="-10y", end_date="-1y").strftime("%Y-%m-%d"),
        "source_system":    "SANCTIONS_FEED",
        "record_date":      datetime.today().strftime("%Y-%m-%d"),
    })

df4 = pd.DataFrame(watchlist_rows)
df4.to_csv("sample_data/watchlist_sanctions.csv", index=False)
print(f"   Saved: sample_data/watchlist_sanctions.csv ({len(df4)} rows)")


# ════════════════════════════════════════════════════════════════════
# SOURCE 5 — Account Activity Summary
# Format : Parquet
# Schedule: Hourly micro-batch via Databricks Autoloader
# Why     : Last activity date drives Dormancy Alert.
#           Balance tier feeds into KYC Risk Score.
#           Parquet format demonstrates columnar storage knowledge.
# ════════════════════════════════════════════════════════════════════
print("5/5 — Account Activity Summary Parquet...")

activity_rows = []
for cid in CUSTOMER_IDS:
    last_activity = fake.date_between(start_date="-2y", end_date="today")
    days_since = (datetime.today().date() - last_activity).days
    activity_rows.append({
        "account_id":           f"ACC{cid[4:]}",   # ACC00001 matches CUST00001
        "customer_id":          cid,
        "account_type":         random.choice(["SAVINGS", "CURRENT", "NRE", "FD"]),
        "account_status":       random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "DORMANT", "CLOSED"]),
        "balance":              round(random.uniform(500, 10000000), 2),
        "balance_tier":         "HIGH" if random.random() > 0.8 else random.choice(["MID", "LOW"]),
        "last_activity_date":   last_activity.strftime("%Y-%m-%d"),
        "days_since_activity":  days_since,
        "dormant_flag":         "Y" if days_since > 365 else "N",
        "branch_code":          fake.numerify("BR####"),
        "source_system":        "CORE_BANKING",
        "batch_hour":           datetime.now().strftime("%Y-%m-%dT%H:00:00"),
    })

df5 = pd.DataFrame(activity_rows)
table = pa.Table.from_pandas(df5)
pq.write_table(table, "sample_data/account_activity.parquet")
print(f"   Saved: sample_data/account_activity.parquet ({len(df5)} rows)")

# ── Summary ──────────────────────────────────────────────────────────
print("\n✅ All 5 files generated in ./sample_data/")
print("\nFile summary:")
print(f"  customer_onboarding.csv     → {len(df1)} rows  | Source 1 | ADF batch")
print(f"  transaction_feed.json       → {len(transactions)} records | Source 2 | Azure Function + Event Grid")
print(f"  kyc_documents.json          → {len(kyc_docs)} records | Source 3 | Azure Function timer")
print(f"  watchlist_sanctions.csv     → {len(df4)} rows  | Source 4 | ADF batch")
print(f"  account_activity.parquet    → {len(df5)} rows  | Source 5 | Databricks Autoloader")
print("\nNext: Upload all 5 files to ADLS Gen2 Landing container.")