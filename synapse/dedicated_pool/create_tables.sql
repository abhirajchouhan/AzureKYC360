-- AzureKYC360 — Synapse Dedicated Pool Star Schema

CREATE TABLE dim_customer (
    customer_key        INT IDENTITY(1,1),
    customer_id         VARCHAR(20),
    full_name           VARCHAR(200),
    country             VARCHAR(10),
    customer_segment    VARCHAR(50),
    pep_flag            VARCHAR(5),
    effective_from      DATETIME2,
    effective_to        DATETIME2,
    is_current          BIT
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dim_kyc_status (
    kyc_key             INT IDENTITY(1,1),
    customer_id         VARCHAR(20),
    doc_status          VARCHAR(20),
    doc_type            VARCHAR(50),
    expiry_date         VARCHAR(20),
    verified_by         VARCHAR(50)
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dim_date (
    date_key            INT,
    full_date           DATE,
    year                INT,
    month               INT,
    day                 INT,
    quarter             INT,
    month_name          VARCHAR(20)
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE fact_customer_risk (
    risk_key             INT IDENTITY(1,1),
    customer_id          VARCHAR(20),
    date_key             INT,
    txn_count            BIGINT,
    txn_total_amt        FLOAT,
    cross_border_count   BIGINT,
    risk_score           INT,
    kyc_risk_level       VARCHAR(10),
    aml_flag             BIT,
    dormancy_alert       BIT,
    high_risk_profile    BIT,
    velocity_risk        VARCHAR(10),
    days_since_activity  INT,
    balance_tier         VARCHAR(10)
)
WITH (
    DISTRIBUTION = HASH(customer_id),
    CLUSTERED COLUMNSTORE INDEX
);