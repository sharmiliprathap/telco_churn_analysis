CREATE OR REPLACE VIEW churn_analytics.vw_kpi_overview AS
SELECT
  COUNT(*) AS total_accounts,
  ROUND(AVG(mrr), 2) AS avg_mrr,
  ROUND(SUM(mrr), 2) AS total_mrr,
  SUM(is_churned) AS churned_accounts,
  ROUND(100.0 * AVG(is_churned), 2) AS customer_churn_rate_pct,
  ROUND(SUM(CASE WHEN is_churned = 1 THEN mrr ELSE 0 END), 2) AS churned_mrr,
  ROUND(100.0 * SUM(CASE WHEN is_churned = 1 THEN mrr ELSE 0 END)/ NULLIF(SUM(mrr), 0), 2) AS revenue_churn_rate_pct,
  ROUND(100.0 * SUM(CASE WHEN is_churned = 0 THEN mrr ELSE 0 END)/ NULLIF(SUM(mrr), 0), 2) AS retained_revenue_pct
FROM churn_analytics.customer_accounts;

CREATE OR REPLACE VIEW churn_analytics.vw_kpi_by_contract AS
SELECT
  contract_type,
  COUNT(*) AS total_accounts,
  ROUND(AVG(mrr), 2) AS avg_mrr,
  ROUND(100.0 * AVG(is_churned), 2) AS churn_rate_pct,
  ROUND(SUM(CASE WHEN is_churned = 1 THEN mrr ELSE 0 END), 2) AS churned_mrr
FROM churn_analytics.customer_accounts
GROUP BY contract_type
ORDER BY churn_rate_pct DESC;

CREATE OR REPLACE VIEW churn_analytics.vw_kpi_by_payment AS
SELECT
  payment_method,
  COUNT(*) AS total_accounts,
  ROUND(AVG(mrr), 2) AS avg_mrr,
  ROUND(100.0 * AVG(is_churned), 2) AS churn_rate_pct,
  ROUND(SUM(CASE WHEN is_churned = 1 THEN mrr ELSE 0 END), 2) AS churned_mrr
FROM churn_analytics.customer_accounts
GROUP BY payment_method
ORDER BY churn_rate_pct DESC;

CREATE OR REPLACE VIEW churn_analytics.vw_segment_matrix AS
SELECT
  contract_type,
  payment_method,
  COUNT(*) AS accounts,
  ROUND(AVG(mrr), 2) AS avg_mrr,
  ROUND(100.0 * AVG(is_churned), 2) AS churn_rate_pct,
  ROUND(SUM(CASE WHEN is_churned = 1 THEN mrr ELSE 0 END), 2) AS churned_mrr
FROM churn_analytics.customer_accounts 
GROUP BY contract_type, payment_method
ORDER BY churn_rate_pct DESC, churned_mrr DESC;

CREATE OR REPLACE VIEW churn_analytics.vw_customer_retention_tenure AS
SELECT
  cr.tenure_band,
  COUNT(*) AS accounts,
  SUM(ca.is_churned) AS churned_accounts,
  ROUND(100.0 * AVG(ca.is_churned), 2) AS churn_rate_pct,
  ROUND(100.0 * (1 - AVG(ca.is_churned)), 2) AS retention_rate_pct,
  ROUND(AVG(ca.mrr), 2) AS avg_mrr,
  ROUND(SUM(CASE WHEN ca.is_churned = 1 THEN ca.mrr ELSE 0 END), 2) AS churned_mrr
FROM churn_analytics.customer_accounts ca
JOIN churn_analytics.customer_risk_profile cr ON ca.account_id = cr.account_id
GROUP BY cr.tenure_band
ORDER BY CASE cr.tenure_band
  WHEN '0-5 months' THEN 1
  WHEN '6-12 months' THEN 2
  WHEN '13-24 months' THEN 3
  WHEN '25-48 months' THEN 4
  ELSE 5
END;

CREATE OR REPLACE VIEW churn_analytics.vw_at_risk_accounts AS
SELECT
  ca.account_id,
  ca.contract_type,
  ca.payment_method,
  ca.internet_service,
  ca.months_active,
  ca.mrr,
  cr.service_count,
  cr.security_dependency,
  cr.risk_score,
  cr.risk_tier,
  CASE
    WHEN cr.risk_tier = 'High Risk' AND ca.mrr >= 80 THEN 'Contact immediately'
    WHEN cr.risk_tier = 'High Risk' THEN 'Check in this week'
    WHEN cr.risk_tier = 'Medium Risk' AND ca.contract_type = 'Month-to-month' THEN 'Offer yearly plan discount'
    WHEN cr.risk_tier = 'Medium Risk' THEN 'Send help tips and follow up'
    ELSE 'Regular monthly check-in'
  END AS recommended_action
FROM churn_analytics.customer_accounts ca
JOIN churn_analytics.customer_risk_profile cr ON ca.account_id = cr.account_id
WHERE ca.is_churned = 0
ORDER BY cr.risk_score DESC, ca.mrr DESC;

CREATE OR REPLACE VIEW churn_analytics.vw_top_100_at_risk_customers AS
SELECT
  account_id,
  contract_type,
  payment_method,
  months_active,
  mrr,
  risk_score,
  risk_tier,
  recommended_action
FROM churn_analytics.vw_at_risk_accounts
WHERE risk_tier IN ('High Risk', 'Medium Risk')
ORDER BY risk_score DESC, mrr DESC
LIMIT 100;

SELECT COUNT(*) AS total_accounts FROM churn_analytics.customer_accounts;
SELECT COUNT(*) AS total_features FROM churn_analytics.customer_risk_profile;

SELECT SUM(mrr) AS total_mrr
FROM churn_analytics.vw_top_100_at_risk_customers;
