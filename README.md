# SaaS Churn & Revenue Retention Analysis (Telco Customer Churn)

A SQL-first churn analytics project that explains where customer loss is happening, how much revenue it is costing, and which active accounts should be prioritized for retention actions.

## Background

Subscription businesses grow when customers stay. This project uses the Telco Customer Churn dataset as a SaaS-style retention case to measure churn, revenue impact, segment-level risk, and action priority.

### Core questions:

- What is the overall customer churn rate and revenue churn rate?
- Which contract and payment segments are driving the highest churn?
- Are newer customers churning more than long-tenure customers?
- Which active customers are high risk and should be targeted first?
- Which low-risk customers are suitable for upsell opportunities?

## Data

This dataset contains 7,043 customer records. Each record includes customer profile fields, subscribed services, contract/payment details, monthly charges, total charges, tenure, and churn label (Yes/No).  
The dataset was obtained from Kaggle: Telco Customer Churn.

## Data Cleaning

Using SQL, the data pipeline standardizes text fields (TRIM + category validation), handles missing values (COALESCE/NULLIF with revenue fallback logic), and de-duplicates accounts using ROW_NUMBER() with a deterministic keep rule. It also validates numeric boundaries and produces a finalized clean fact table before feature engineering.

## Tools I Used

- **SQL (PostgreSQL):** Primary tool for data pipeline, cleaning, feature engineering, KPI analysis, and dashboard views.
- **Power BI:** For final business storytelling and operational dashboarding.
- **Visual Studio Code:** Development environment for organizing SQL and Python files.

## The Analysis

### 1. Overall Churn and Revenue Retention
To measure the current business health, I first calculated overall customer churn, revenue churn, and retained revenue levels.
```sql
SELECT
  COUNT(*) AS total_accounts,
  SUM(is_churned) AS churned_accounts,
  ROUND(100.0 * SUM(is_churned)::NUMERIC / NULLIF(COUNT(*), 0), 2) AS customer_churn_rate_pct,
  ROUND(AVG(mrr), 2) AS avg_nrr,
  ROUND(SUM(mrr), 2) AS total_mrr,
  ROUND(SUM(CASE WHEN is_churned = 1 THEN mrr ELSE 0 END), 2) AS churned_mrr,
  ROUND(100.0 * SUM(CASE WHEN is_churned = 1 THEN mrr ELSE 0 END)::NUMERIC / NULLIF(SUM(mrr), 0), 2) AS revenue_churn_rate_pct,
  ROUND(100.0 * (1 - SUM(CASE WHEN is_churned = 1 THEN mrr ELSE 0 END)::NUMERIC / NULLIF(SUM(mrr), 0)), 2) AS grr_pct
FROM churn_analytics.customer_accounts;
```
The dataset shows 26.54% customer churn (1,869 of 7,043 accounts). Revenue impact is stronger: 30.50% revenue churn, leaving 69.50% GRR (gross revenue retention). This indicates churn is concentrated in relatively higher-revenue accounts.

### 2. Churn by Contract Type
```sql
SELECT
  contract_type,
  COUNT(*) AS total_accounts,
  ROUND(100.0 * AVG(is_churned), 2) AS churn_rate_pct,
  ROUND(AVG(mrr), 2) AS avg_nrr,
  ROUND(SUM(CASE WHEN is_churned = 1 THEN mrr ELSE 0 END), 2) AS churned_mrr
FROM churn_analytics.customer_accounts
GROUP BY contract_type
ORDER BY churn_rate_pct DESC;
```
Month-to-month has the highest churn at 42.71%, far above One year (11.27%) and Two year (2.83%). This confirms contract commitment is the strongest retention lever.

### 3. Churn by Payment Method
```sql
SELECT
  payment_method,
  COUNT(*) AS total_accounts,
  ROUND(100.0 * AVG(is_churned), 2) AS churn_rate_pct,
  ROUND(AVG(mrr), 2) AS avg_nrr
FROM churn_analytics.customer_accounts
GROUP BY payment_method
ORDER BY churn_rate_pct DESC;
```
Electronic check is the highest-risk payment segment at 45.29% churn, much higher than automatic payment methods (about 15–17%). This points to payment behavior as a practical churn signal.

### 4. Contract + Payment Risk Matrix
```sql
SELECT
  contract_type,
  payment_method,
  COUNT(*) AS accounts,
  ROUND(100.0 * AVG(is_churned), 2) AS churn_rate_pct,
  ROUND(SUM(CASE WHEN is_churned = 1 THEN mrr ELSE 0 END), 2) AS churned_mrr
FROM churn_analytics.customer_accounts
GROUP BY contract_type, payment_method
ORDER BY churn_rate_pct DESC, churned_mrr DESC;
```
The highest-risk combination is Month-to-month + Electronic check:

1,850 accounts
53.73% churn rate
77,315.60 churned MRR
This is the primary retention intervention segment.

### 5. Customer Retention by Tenure
```sql
SELECT
  tenure_band,
  COUNT(*) AS accounts,
  ROUND(100.0 * AVG(is_churned), 2) AS churn_rate_pct,
  ROUND(100.0 * (1 - AVG(is_churned)), 2) AS retention_rate_pct
FROM churn_analytics.vw_customer_retention_tenure
GROUP BY tenure_band
ORDER BY CASE tenure_band
  WHEN '0-5 months' THEN 1
  WHEN '6-12 months' THEN 2
  WHEN '13-24 months' THEN 3
  WHEN '25-48 months' THEN 4
  ELSE 5
END;
```
Early-life churn is the biggest issue:

0–5 months: 54.27% churn
49+ months: 9.51% churn
Retention strategy should focus on first-year onboarding and support adoption.

### 6. Top At-Risk Active Accounts
```sql
SELECT
  account_id,
  contract_type,
  payment_method,
  months_active,
  mrr,
  risk_score,
  risk_tier,
  recommended_action
FROM churn_analytics.vw_top_100_at_risk_customers;
```
Top-risk active accounts are heavily concentrated in month-to-month customers with electronic check and high monthly charges. These accounts should be prioritized for immediate retention outreach.

## What I Learned
Churn is not evenly distributed. It is structurally concentrated in specific commercial behaviors: short tenure, month-to-month contracts, and electronic check payment users. A targeted retention program will outperform broad campaigns.

## Limitations
This is an observational dataset, not a controlled experiment.
Risk scoring in SQL is heuristic (rule-based), not statistically calibrated by default.

## Conclusions
### Insights
Overall churn is high, and revenue churn is even higher than customer churn.
Contract type is the strongest predictor of churn behavior.
The highest-loss segment is Month-to-month + Electronic check.
Early-tenure customers require stronger onboarding and support.

### Business Implications
Prioritize retention interventions for high-risk month-to-month accounts.
Push automatic payments and annual contracts for stability.
