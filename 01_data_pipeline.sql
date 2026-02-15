-- Full data pipeline


CREATE SCHEMA IF NOT EXISTS churn_raw;
CREATE SCHEMA IF NOT EXISTS churn_analytics;

-- raw table 
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

-- clean table 
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

TRUNCATE TABLE churn_analytics.customer_accounts;

-- Handling missing values and duplicates
WITH cleaned AS (
  SELECT
    TRIM(customerid) AS account_id,
    CASE
      WHEN TRIM(gender) IN ('Male', 'Female') THEN TRIM(gender)
      ELSE 'Unknown'
    END AS gender,
    COALESCE(seniorcitizen, 0) AS senior_citizen,
    CASE
      WHEN TRIM(partner) IN ('Yes', 'No') THEN TRIM(partner)
      ELSE 'Unknown'
    END AS partner,
    CASE
      WHEN TRIM(dependents) IN ('Yes', 'No') THEN TRIM(dependents)
      ELSE 'Unknown'
    END AS dependents,
    GREATEST(COALESCE(tenure, 0), 0) AS months_active,
    CASE
      WHEN TRIM(phoneservice) IN ('Yes', 'No') THEN TRIM(phoneservice)
      ELSE 'Unknown'
    END AS phone_service,
    CASE
      WHEN TRIM(multiplelines) IN ('Yes', 'No', 'No phone service') THEN TRIM(multiplelines)
      ELSE 'Unknown'
    END AS multiple_lines,
    CASE
      WHEN TRIM(internetservice) IN ('DSL', 'Fiber optic', 'No') THEN TRIM(internetservice)
      ELSE 'Unknown'
    END AS internet_service,
    CASE
      WHEN TRIM(onlinesecurity) IN ('Yes', 'No', 'No internet service') THEN TRIM(onlinesecurity)
      ELSE 'Unknown'
    END AS online_security,
    CASE
      WHEN TRIM(onlinebackup) IN ('Yes', 'No', 'No internet service') THEN TRIM(onlinebackup)
      ELSE 'Unknown'
    END AS online_backup,
    CASE
      WHEN TRIM(deviceprotection) IN ('Yes', 'No', 'No internet service') THEN TRIM(deviceprotection)
      ELSE 'Unknown'
    END AS device_protection,
    CASE
      WHEN TRIM(techsupport) IN ('Yes', 'No', 'No internet service') THEN TRIM(techsupport)
      ELSE 'Unknown'
    END AS tech_support,
    CASE
      WHEN TRIM(streamingtv) IN ('Yes', 'No', 'No internet service') THEN TRIM(streamingtv)
      ELSE 'Unknown'
    END AS streaming_tv,
    CASE
      WHEN TRIM(streamingmovies) IN ('Yes', 'No', 'No internet service') THEN TRIM(streamingmovies)
      ELSE 'Unknown'
    END AS streaming_movies,
    CASE
      WHEN TRIM(contract) IN ('Month-to-month', 'One year', 'Two year') THEN TRIM(contract)
      ELSE 'Unknown'
    END AS contract_type,
    CASE
      WHEN TRIM(paperlessbilling) IN ('Yes', 'No') THEN TRIM(paperlessbilling)
      ELSE 'Unknown'
    END AS paperless_billing,
    COALESCE(NULLIF(TRIM(paymentmethod), ''), 'Unknown') AS payment_method,
    GREATEST(COALESCE(monthlycharges, 0), 0)::NUMERIC(10,2) AS mrr,
    NULLIF(TRIM(totalcharges), '')::NUMERIC(12,2) AS lifetime_revenue,
    CASE WHEN TRIM(churn) = 'Yes' THEN 1 ELSE 0 END AS is_churned,
    CASE WHEN TRIM(churn) IN ('Yes', 'No') THEN TRIM(churn) ELSE 'No' END AS churn_label
  FROM churn_raw.telco_customer_churn
  WHERE NULLIF(TRIM(customerid), '') IS NOT NULL
), 

deduplicate AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY account_id
      ORDER BY months_active DESC, mrr DESC
    ) AS rn
  FROM cleaned
)

INSERT INTO churn_analytics.customer_accounts (
  account_id,
  gender,
  senior_citizen,
  partner,
  dependents,
  months_active,
  phone_service,
  multiple_lines,
  internet_service,
  online_security,
  online_backup,
  device_protection,
  tech_support,
  streaming_tv,
  streaming_movies,
  contract_type,
  paperless_billing,
  payment_method,
  mrr,
  lifetime_revenue,
  is_churned,
  churn_label
)

SELECT
  account_id,
  gender,
  senior_citizen,
  partner,
  dependents,
  months_active,
  phone_service,
  multiple_lines,
  internet_service,
  online_security,
  online_backup,
  device_protection,
  tech_support,
  streaming_tv,
  streaming_movies,
  contract_type,
  paperless_billing,
  payment_method,
  mrr,
  COALESCE(lifetime_revenue, ROUND(mrr * months_active, 2)) AS lifetime_revenue,
  is_churned,
  churn_label
FROM deduplicate
WHERE rn = 1;



-- feature table 
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

INSERT INTO churn_analytics.customer_risk_profile (
  account_id,
  tenure_band,
  mrr_band,
  service_count,
  security_dependency,
  high_value_account,
  risk_score,
  risk_tier
)

SELECT
  account_id,
  CASE
    WHEN months_active < 6 THEN '0-5 months'
    WHEN months_active BETWEEN 6 AND 12 THEN '6-12 months'
    WHEN months_active BETWEEN 13 AND 24 THEN '13-24 months'
    WHEN months_active BETWEEN 25 AND 48 THEN '25-48 months'
    ELSE '49+ months'
  END AS tenure_band,
  CASE
    WHEN mrr < 35 THEN 'Low MRR'
    WHEN mrr BETWEEN 35 AND 70 THEN 'Mid MRR'
    ELSE 'High MRR'
  END AS mrr_band,
  (
    CASE WHEN phone_service = 'Yes' THEN 1 ELSE 0 END +
    CASE WHEN multiple_lines = 'Yes' THEN 1 ELSE 0 END +
    CASE WHEN internet_service IN ('DSL', 'Fiber optic') THEN 1 ELSE 0 END +
    CASE WHEN online_security = 'Yes' THEN 1 ELSE 0 END +
    CASE WHEN online_backup = 'Yes' THEN 1 ELSE 0 END +
    CASE WHEN device_protection = 'Yes' THEN 1 ELSE 0 END +
    CASE WHEN tech_support = 'Yes' THEN 1 ELSE 0 END +
    CASE WHEN streaming_tv = 'Yes' THEN 1 ELSE 0 END +
    CASE WHEN streaming_movies = 'Yes' THEN 1 ELSE 0 END
  ) AS service_count,
  CASE
    WHEN online_security = 'No' AND tech_support = 'No' THEN 'High dependency'
    WHEN online_security = 'No' OR tech_support = 'No' THEN 'Medium dependency'
    ELSE 'Low dependency'
  END AS security_dependency,
  CASE
    WHEN mrr >= (
      SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY mrr)
      FROM churn_analytics.customer_accounts
    ) THEN 'Yes'
    ELSE 'No'
  END AS high_value_account,
  (
    CASE WHEN contract_type = 'Month-to-month' THEN 30 ELSE 0 END +
    CASE WHEN payment_method = 'Electronic check' THEN 20 ELSE 0 END +
    CASE WHEN internet_service = 'Fiber optic' THEN 10 ELSE 0 END +
    CASE WHEN tech_support = 'No' THEN 15 ELSE 0 END +
    CASE WHEN online_security = 'No' THEN 15 ELSE 0 END +
    CASE WHEN months_active < 12 THEN 10 ELSE 0 END
  ) AS risk_score,
  CASE
    WHEN (
      CASE WHEN contract_type = 'Month-to-month' THEN 30 ELSE 0 END +
      CASE WHEN payment_method = 'Electronic check' THEN 20 ELSE 0 END +
      CASE WHEN internet_service = 'Fiber optic' THEN 10 ELSE 0 END +
      CASE WHEN tech_support = 'No' THEN 15 ELSE 0 END +
      CASE WHEN online_security = 'No' THEN 15 ELSE 0 END +
      CASE WHEN months_active < 12 THEN 10 ELSE 0 END
    ) >= 60 THEN 'High Risk'
    WHEN (
      CASE WHEN contract_type = 'Month-to-month' THEN 30 ELSE 0 END +
      CASE WHEN payment_method = 'Electronic check' THEN 20 ELSE 0 END +
      CASE WHEN internet_service = 'Fiber optic' THEN 10 ELSE 0 END +
      CASE WHEN tech_support = 'No' THEN 15 ELSE 0 END +
      CASE WHEN online_security = 'No' THEN 15 ELSE 0 END +
      CASE WHEN months_active < 12 THEN 10 ELSE 0 END
    ) BETWEEN 35 AND 59 THEN 'Medium Risk'
    ELSE 'Low Risk'
  END AS risk_tier
FROM churn_analytics.customer_accounts;

-- validation queries
SELECT COUNT(*) AS raw_rows_loaded FROM churn_raw.telco_customer_churn;
SELECT COUNT(*) AS clean_rows_final FROM churn_analytics.customer_accounts;
SELECT COUNT(*) AS feature_rows_final FROM churn_analytics.customer_risk_profile;

-- check for duplicates and nulls in critical fields
SELECT COUNT(*) AS duplicate_account_ids
FROM (
  SELECT account_id
  FROM churn_analytics.customer_accounts
  GROUP BY account_id
  HAVING COUNT(*) > 1
) d;

SELECT
  SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) AS null_account_id,
  SUM(CASE WHEN months_active IS NULL THEN 1 ELSE 0 END) AS null_months_active,
  SUM(CASE WHEN mrr IS NULL THEN 1 ELSE 0 END) AS null_mrr,
  SUM(CASE WHEN lifetime_revenue IS NULL THEN 1 ELSE 0 END) AS null_lifetime_revenue,
  SUM(CASE WHEN contract_type = 'Unknown' THEN 1 ELSE 0 END) AS unknown_contract_type
FROM churn_analytics.customer_accounts;
