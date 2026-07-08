-- AzureKYC360 — Synapse Serverless External Tables
-- Creates external tables pointing to Gold Delta tables in ADLS Gen2.

CREATE DATABASE kyc360_serverless;

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourStrongPassword123!';

CREATE DATABASE SCOPED CREDENTIAL adls_credential
WITH IDENTITY = 'Managed Identity';

CREATE EXTERNAL DATA SOURCE gold_adls
WITH (
    LOCATION = 'abfss://gold@adlskyc360.dfs.core.windows.net',
    CREDENTIAL = adls_credential
);

CREATE EXTERNAL FILE FORMAT parquet_format
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

CREATE EXTERNAL TABLE ext_customer_360 (
    customer_id          VARCHAR(20),
    full_name            VARCHAR(200),
    country              VARCHAR(10),
    customer_segment     VARCHAR(50),
    pep_flag             VARCHAR(5),
    doc_status           VARCHAR(20),
    expiry_date          VARCHAR(20),
    txn_count            BIGINT,
    txn_total_amt        FLOAT,
    cross_border_count   BIGINT,
    velocity_risk        VARCHAR(10),
    dormant_flag         VARCHAR(5),
    days_since_activity  BIGINT,
    balance_tier         VARCHAR(10),
    risk_score           BIGINT,
    kyc_risk_level       VARCHAR(10),
    aml_flag             VARCHAR(10),
    dormancy_alert       VARCHAR(10),
    high_risk_profile    VARCHAR(10)
)
WITH (
    LOCATION = 'customer_360/',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = parquet_format
);