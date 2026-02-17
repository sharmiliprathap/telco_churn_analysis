CREATE SCHEMA IF NOT EXISTS churn_raw;
CREATE SCHEMA IF NOT EXISTS churn_analytics;

CREATE TABLE IF NOT EXISTS churn_raw.telco_customer_churn (
  customerid TEXT,
  gender TEXT,
  seniorcitizen INT,
  partner TEXT,
  dependents TEXT,
  tenure INT,
  phoneservice TEXT,
  multiplelines TEXT,
  internetservice TEXT,
  onlinesecurity TEXT,
  onlinebackup TEXT,
  deviceprotection TEXT,
  techsupport TEXT,
  streamingtv TEXT,
  streamingmovies TEXT,
  contract TEXT,
  paperlessbilling TEXT,
  paymentmethod TEXT,
  monthlycharges NUMERIC(10,2),
  totalcharges TEXT,
  churn TEXT
);

CREATE TABLE IF NOT EXISTS churn_analytics.customer_accounts (
  account_id TEXT PRIMARY KEY,
  gender TEXT,
  senior_citizen INT,
  partner TEXT,
  dependents TEXT,
  months_active INT,
  phone_service TEXT,
  multiple_lines TEXT,
  internet_service TEXT,
  online_security TEXT,
  online_backup TEXT,
  device_protection TEXT,
  tech_support TEXT,
  streaming_tv TEXT,
  streaming_movies TEXT,
  contract_type TEXT,
  paperless_billing TEXT,
  payment_method TEXT,
  mrr NUMERIC(10,2),
  lifetime_revenue NUMERIC(12,2),
  is_churned INT,
  churn_label TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

TRUNCATE TABLE churn_raw.telco_customer_churn;
COPY churn_raw.telco_customer_churn
FROM '/private/tmp/WA_Fn-UseC_-Telco-Customer-Churn.csv'
DELIMITER ',' CSV HEADER;

SELECT COUNT(*) AS raw_rows_loaded FROM churn_raw.telco_customer_churn;
SELECT COUNT(*) AS rows_missing_totalcharges
FROM churn_raw.telco_customer_churn
WHERE NULLIF(TRIM(totalcharges), '') IS NULL;

TRUNCATE TABLE churn_analytics.customer_accounts;

WITH cleaned AS (
  SELECT
    TRIM(customerid) account_id,
    gender,
    COALESCE(seniorcitizen, 0) senior_citizen,
    partner,
    dependents,
    GREATEST(COALESCE(tenure, 0), 0) months_active,
    phoneservice phone_service,
    multiplelines multiple_lines,
    internetservice internet_service,
    onlinesecurity online_security,
    onlinebackup online_backup,
    deviceprotection device_protection,
    techsupport tech_support,
    streamingtv streaming_tv,
    streamingmovies streaming_movies,
    contract contract_type,
    paperlessbilling paperless_billing,
    COALESCE(NULLIF(TRIM(paymentmethod), ''), 'Unknown') payment_method,
    GREATEST(COALESCE(monthlycharges, 0), 0)::NUMERIC(10,2) mrr,
    NULLIF(TRIM(totalcharges), '')::NUMERIC(12,2) lifetime_revenue_raw,
    (TRIM(churn) = 'Yes')::INT is_churned,
    CASE WHEN TRIM(churn) IN ('Yes', 'No') THEN TRIM(churn) ELSE 'No' END churn_label
  FROM churn_raw.telco_customer_churn
  WHERE NULLIF(TRIM(customerid), '') IS NOT NULL
),
dedupe AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY months_active DESC, mrr DESC) rn
  FROM cleaned
)
INSERT INTO churn_analytics.customer_accounts (
  account_id, gender, senior_citizen, partner, dependents, months_active, phone_service, multiple_lines,
  internet_service, online_security, online_backup, device_protection, tech_support, streaming_tv,
  streaming_movies, contract_type, paperless_billing, payment_method, mrr, lifetime_revenue, is_churned, churn_label
)
SELECT
  account_id, gender, senior_citizen, partner, dependents, months_active, phone_service, multiple_lines,
  internet_service, online_security, online_backup, device_protection, tech_support, streaming_tv,
  streaming_movies, contract_type, paperless_billing, payment_method, mrr,
  /* Source has blank TotalCharges on early-tenure rows; backfill with MRR * tenure. */
  COALESCE(lifetime_revenue_raw, ROUND(mrr * months_active, 2)) lifetime_revenue,
  is_churned, churn_label
FROM dedupe
WHERE rn = 1;

SELECT COUNT(*) AS duplicate_account_ids
FROM (
  SELECT account_id
  FROM churn_analytics.customer_accounts
  GROUP BY account_id
  HAVING COUNT(*) > 1
) d;

CREATE TABLE IF NOT EXISTS churn_analytics.customer_risk_profile (
  account_id TEXT PRIMARY KEY,
  tenure_band TEXT,
  mrr_band TEXT,
  service_count INT,
  security_dependency TEXT,
  high_value_account TEXT,
  risk_score INT,
  risk_tier TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

TRUNCATE TABLE churn_analytics.customer_risk_profile;

WITH scored AS (
  SELECT
    account_id,
    CASE WHEN months_active < 6 THEN '0-5 months'
         WHEN months_active <= 12 THEN '6-12 months'
         WHEN months_active <= 24 THEN '13-24 months'
         WHEN months_active <= 48 THEN '25-48 months'
         ELSE '49+ months' END tenure_band,
    CASE WHEN mrr < 35 THEN 'Low MRR'
         WHEN mrr <= 70 THEN 'Mid MRR'
         ELSE 'High MRR' END mrr_band,
    (phone_service = 'Yes')::INT
      + (multiple_lines = 'Yes')::INT
      + (internet_service IN ('DSL', 'Fiber optic'))::INT
      + (online_security = 'Yes')::INT
      + (online_backup = 'Yes')::INT
      + (device_protection = 'Yes')::INT
      + (tech_support = 'Yes')::INT
      + (streaming_tv = 'Yes')::INT
      + (streaming_movies = 'Yes')::INT AS service_count,
    CASE WHEN online_security = 'No' AND tech_support = 'No' THEN 'High dependency'
         WHEN online_security = 'No' OR tech_support = 'No' THEN 'Medium dependency'
         ELSE 'Low dependency' END security_dependency,
    CASE WHEN mrr >= 100 THEN 'Yes' ELSE 'No' END high_value_account,
    (CASE WHEN contract_type = 'Month-to-month' THEN 30 ELSE 0 END
      + CASE WHEN payment_method = 'Electronic check' THEN 20 ELSE 0 END
      + CASE WHEN internet_service = 'Fiber optic' THEN 10 ELSE 0 END
      + CASE WHEN tech_support = 'No' THEN 15 ELSE 0 END
      + CASE WHEN online_security = 'No' THEN 15 ELSE 0 END
      + CASE WHEN months_active < 12 THEN 10 ELSE 0 END) risk_score
  FROM churn_analytics.customer_accounts
)
INSERT INTO churn_analytics.customer_risk_profile (
  account_id, tenure_band, mrr_band, service_count, security_dependency, high_value_account, risk_score, risk_tier
)
SELECT
  account_id, tenure_band, mrr_band, service_count, security_dependency, high_value_account, risk_score,
  CASE WHEN risk_score >= 60 THEN 'High Risk'
       WHEN risk_score >= 35 THEN 'Medium Risk'
       ELSE 'Low Risk' END risk_tier
FROM scored;

SELECT risk_tier, COUNT(*) AS accounts
FROM churn_analytics.customer_risk_profile
GROUP BY risk_tier
ORDER BY accounts DESC;
