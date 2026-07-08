-- AzureKYC360 — Load Star Schema Tables

INSERT INTO dim_customer (
    customer_id, full_name, country,
    customer_segment, pep_flag
)
SELECT DISTINCT
    customer_id, full_name, country,
    customer_segment, pep_flag
FROM ext_customer_360_ded;

INSERT INTO fact_customer_risk (
    customer_id, txn_count, txn_total_amt,
    cross_border_count, risk_score, kyc_risk_level,
    aml_flag, dormancy_alert, high_risk_profile,
    velocity_risk, days_since_activity, balance_tier
)
SELECT
    customer_id, txn_count, txn_total_amt,
    cross_border_count, risk_score, kyc_risk_level,
    CASE WHEN aml_flag = '1' THEN 1 ELSE 0 END,
    CASE WHEN dormancy_alert = '1' THEN 1 ELSE 0 END,
    CASE WHEN high_risk_profile = '1' THEN 1 ELSE 0 END,
    velocity_risk, days_since_activity, balance_tier
FROM ext_customer_360_ded;